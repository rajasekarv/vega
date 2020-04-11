use std::any::Any;
use std::clone::Clone;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::option::Option;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use crate::dag_scheduler::{CompletionEvent, TastEndReason};
use crate::dependency::ShuffleDependencyTrait;
use crate::env;
use crate::error::Result;
use crate::job::{Job, JobTracker};
use crate::map_output_tracker::MapOutputTracker;
use crate::rdd::{Rdd, RddBase};
use crate::result_task::ResultTask;
use crate::scheduler::{EventQueue, NativeScheduler};
use crate::serializable_traits::{Data, SerFunc};
use crate::shuffle::ShuffleMapTask;
use crate::stage::Stage;
use crate::task::{TaskBase, TaskContext, TaskOption, TaskResult};
use crate::utils;
use dashmap::DashMap;
use parking_lot::Mutex;

#[derive(Clone, Default)]
pub struct LocalScheduler {
    max_failures: usize,
    attempt_id: Arc<AtomicUsize>,
    resubmit_timeout: u128,
    poll_timeout: u64,
    event_queues: EventQueue,
    pub(crate) next_job_id: Arc<AtomicUsize>,
    next_run_id: Arc<AtomicUsize>,
    next_task_id: Arc<AtomicUsize>,
    next_stage_id: Arc<AtomicUsize>,
    stage_cache: Arc<DashMap<usize, Stage>>,
    shuffle_to_map_stage: Arc<DashMap<usize, Stage>>,
    cache_locs: Arc<DashMap<usize, Vec<Vec<Ipv4Addr>>>>,
    master: bool,
    framework_name: String,
    is_registered: bool, //TODO check if it is necessary
    active_jobs: HashMap<usize, Job>,
    active_job_queue: Vec<Job>,
    taskid_to_jobid: HashMap<String, usize>,
    taskid_to_slaveid: HashMap<String, String>,
    job_tasks: HashMap<usize, HashSet<String>>,
    slaves_with_executors: HashSet<String>,
    map_output_tracker: MapOutputTracker,
    // TODO fix proper locking mechanism
    scheduler_lock: Arc<Mutex<bool>>,
}

impl LocalScheduler {
    pub fn new(max_failures: usize, master: bool) -> Self {
        LocalScheduler {
            max_failures,
            attempt_id: Arc::new(AtomicUsize::new(0)),
            resubmit_timeout: 2000,
            poll_timeout: 50,
            event_queues: Arc::new(DashMap::new()),
            next_job_id: Arc::new(AtomicUsize::new(0)),
            next_run_id: Arc::new(AtomicUsize::new(0)),
            next_task_id: Arc::new(AtomicUsize::new(0)),
            next_stage_id: Arc::new(AtomicUsize::new(0)),
            stage_cache: Arc::new(DashMap::new()),
            shuffle_to_map_stage: Arc::new(DashMap::new()),
            cache_locs: Arc::new(DashMap::new()),
            master,
            framework_name: "spark".to_string(),
            is_registered: true, //TODO check if it is necessary
            active_jobs: HashMap::new(),
            active_job_queue: Vec::new(),
            taskid_to_jobid: HashMap::new(),
            taskid_to_slaveid: HashMap::new(),
            job_tasks: HashMap::new(),
            slaves_with_executors: HashSet::new(),
            map_output_tracker: env::Env::get().map_output_tracker.clone(),
            scheduler_lock: Arc::new(Mutex::new(true)),
        }
    }

    pub fn run_job<T: Data, U: Data, F>(
        self: Arc<Self>,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        partitions: Vec<usize>,
        allow_local: bool,
    ) -> Result<Vec<U>>
    where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        // acquiring lock so that only one job can run at same time this lock is just
        // a temporary patch for preventing multiple jobs to update cache locks which affects
        // construction of dag task graph. dag task graph construction needs to be altered
        let _lock = self.scheduler_lock.lock();
        let jt = JobTracker::from_scheduler(&*self, func, final_rdd.clone(), partitions);

        //TODO update cache

        if allow_local {
            if let Some(result) = LocalScheduler::local_execution(jt.clone())? {
                return Ok(result);
            }
        }

        self.event_queues.insert(jt.run_id, VecDeque::new());

        let self_clone = Arc::clone(&self);
        let jt_clone = jt.clone();
        // run in async executor
        let executor = env::Env::get_async_handle();
        let results = executor.enter(move || {
            let self_borrow = &*self_clone;
            let jt = jt_clone;
            let mut results: Vec<Option<U>> = (0..jt.num_output_parts).map(|_| None).collect();
            let mut fetch_failure_duration = Duration::new(0, 0);

            self_borrow.submit_stage(jt.final_stage.clone(), jt.clone());
            utils::yield_tokio_futures();
            log::debug!(
                "pending stages and tasks: {:?}",
                jt.pending_tasks
                    .lock()
                    .iter()
                    .map(|(k, v)| (k.id, v.iter().map(|x| x.get_task_id()).collect::<Vec<_>>()))
                    .collect::<Vec<_>>()
            );

            let mut num_finished = 0;
            while num_finished != jt.num_output_parts {
                let event_option = self_borrow.wait_for_event(jt.run_id, self_borrow.poll_timeout);
                let start = Instant::now();

                if let Some(evt) = event_option {
                    log::debug!("event starting");
                    let stage = self_borrow
                        .stage_cache
                        .get(&evt.task.get_stage_id())
                        .unwrap()
                        .clone();
                    log::debug!(
                        "removing stage #{} task from pending task #{}",
                        stage.id,
                        evt.task.get_task_id()
                    );
                    jt.pending_tasks
                        .lock()
                        .get_mut(&stage)
                        .unwrap()
                        .remove(&evt.task);
                    use super::dag_scheduler::TastEndReason::*;
                    match evt.reason {
                        Success => self_borrow.on_event_success(
                            evt,
                            &mut results,
                            &mut num_finished,
                            jt.clone(),
                        ),
                        FetchFailed(failed_vals) => {
                            self_borrow.on_event_failure(
                                jt.clone(),
                                failed_vals,
                                evt.task.get_stage_id(),
                            );
                            fetch_failure_duration = start.elapsed();
                        }
                        Error(error) => panic!("{}", error),
                        OtherFailure(msg) => panic!("{}", msg),
                    }
                }
            }

            if !jt.failed.lock().is_empty()
                && fetch_failure_duration.as_millis() > self_borrow.resubmit_timeout
            {
                self_borrow.update_cache_locs();
                for stage in jt.failed.lock().iter() {
                    self_borrow.submit_stage(stage.clone(), jt.clone());
                }
                utils::yield_tokio_futures();
                jt.failed.lock().clear();
            }
            results
        });

        self.event_queues.remove(&jt.run_id);
        Ok(results
            .into_iter()
            .map(|s| match s {
                Some(v) => v,
                None => panic!("some results still missing"),
            })
            .collect())
    }

    async fn run_task<T: Data, U: Data, F>(
        event_queues: Arc<DashMap<usize, VecDeque<CompletionEvent>>>,
        task: Vec<u8>,
        _id_in_job: usize,
        attempt_id: usize,
    ) where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        let des_task: TaskOption = bincode::deserialize(&task).unwrap();
        let result = des_task.run(attempt_id).await;
        match des_task {
            TaskOption::ResultTask(tsk) => {
                let result = match result {
                    TaskResult::ResultTask(r) => r,
                    _ => panic!("wrong result type"),
                };
                if let Ok(task_final) = tsk.downcast::<ResultTask<T, U, F>>() {
                    let task_final = task_final as Box<dyn TaskBase>;
                    LocalScheduler::task_ended(
                        event_queues,
                        task_final,
                        TastEndReason::Success,
                        result.into_any_send_sync(),
                    );
                }
            }
            TaskOption::ShuffleMapTask(tsk) => {
                let result = match result {
                    TaskResult::ShuffleTask(r) => r,
                    _ => panic!("wrong result type"),
                };
                if let Ok(task_final) = tsk.downcast::<ShuffleMapTask>() {
                    let task_final = task_final as Box<dyn TaskBase>;
                    LocalScheduler::task_ended(
                        event_queues,
                        task_final,
                        TastEndReason::Success,
                        result.into_any_send_sync(),
                    );
                }
            }
        };
    }

    fn task_ended(
        event_queues: Arc<DashMap<usize, VecDeque<CompletionEvent>>>,
        task: Box<dyn TaskBase>,
        reason: TastEndReason,
        result: Box<dyn Any + Send + Sync>,
        // TODO accumvalues needs to be done
    ) {
        let result = Some(result);
        if let Some(mut queue) = event_queues.get_mut(&(task.get_run_id())) {
            queue.push_back(CompletionEvent {
                task,
                reason,
                result,
                accum_updates: HashMap::new(),
            });
        } else {
            log::debug!("ignoring completion event for DAG Job");
        }
    }
}

impl NativeScheduler for LocalScheduler {
    /// Every single task is run in the local thread pool
    fn submit_task<T: Data, U: Data, F>(
        &self,
        task: TaskOption,
        id_in_job: usize,
        _server_address: SocketAddrV4,
    ) where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        log::debug!("inside submit task");
        let my_attempt_id = self.attempt_id.fetch_add(1, Ordering::SeqCst);
        let event_queues = self.event_queues.clone();
        let task = bincode::serialize(&task).unwrap();

        tokio::task::spawn_blocking(move || {
            LocalScheduler::run_task::<T, U, F>(event_queues, task, id_in_job, my_attempt_id)
        });
    }

    fn next_executor_server(&self, _rdd: &dyn TaskBase) -> SocketAddrV4 {
        // Just point to the localhost
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)
    }

    impl_common_scheduler_funcs!();
}
