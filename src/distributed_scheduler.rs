use std::any::Any;
use std::collections::{btree_set::BTreeSet, vec_deque::VecDeque, HashMap, HashSet};
use std::iter::FromIterator;
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use crate::dag_scheduler::{CompletionEvent, TastEndReason};
use crate::dependency::ShuffleDependencyTrait;
use crate::env;
use crate::error::Result;
use crate::job::{Job, JobTracker};
use crate::local_scheduler::LocalScheduler;
use crate::map_output_tracker::MapOutputTracker;
use crate::rdd::{Rdd, RddBase};
use crate::result_task::ResultTask;
use crate::scheduler::NativeScheduler;
use crate::serializable_traits::{Data, SerFunc};
use crate::serialized_data_capnp::serialized_data;
use crate::shuffle::ShuffleMapTask;
use crate::stage::Stage;
use crate::task::{TaskBase, TaskContext, TaskOption, TaskResult};
use crate::utils;
use capnp::serialize_packed;
use dashmap::DashMap;
use parking_lot::Mutex;

//just for now, creating an entire scheduler functions without dag scheduler trait. Later change it to extend from dag scheduler
#[derive(Clone, Default)]
pub struct DistributedScheduler {
    max_failures: usize,
    attempt_id: Arc<AtomicUsize>,
    resubmit_timeout: u128,
    poll_timeout: u64,
    event_queues: Arc<DashMap<usize, VecDeque<CompletionEvent>>>,
    next_job_id: Arc<AtomicUsize>,
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
    server_uris: Arc<Mutex<VecDeque<SocketAddrV4>>>,
    port: u16,
    map_output_tracker: MapOutputTracker,
    // TODO fix proper locking mechanism
    scheduler_lock: Arc<Mutex<bool>>,
}

impl DistributedScheduler {
    pub fn new(
        max_failures: usize,
        master: bool,
        servers: Option<Vec<SocketAddrV4>>,
        port: u16,
    ) -> Self {
        log::debug!(
            "starting distributed scheduler in client - {} {}",
            master,
            port
        );
        DistributedScheduler {
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
            server_uris: if let Some(servers) = servers {
                Arc::new(Mutex::new(VecDeque::from_iter(servers)))
            } else {
                Arc::new(Mutex::new(VecDeque::new()))
            },
            port,
            map_output_tracker: env::Env::get().map_output_tracker.clone(),
            scheduler_lock: Arc::new(Mutex::new(true)),
        }
    }

    fn task_ended(
        event_queues: Arc<DashMap<usize, VecDeque<CompletionEvent>>>,
        task: Box<dyn TaskBase>,
        reason: TastEndReason,
        result: Box<dyn Any + Send + Sync>,
        //TODO accumvalues needs to be done
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
        // acquiring lock so that only one job can run a same time this lock is just
        // a temporary patch for preventing multiple jobs to update cache locks which affects
        // construction of dag task graph. dag task graph construction need to be altered
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
                "pending stages and tasks {:?}",
                jt.pending_tasks
                    .lock()
                    .iter()
                    .map(|(k, v)| (k.id, v.iter().map(|x| x.get_task_id()).collect::<Vec<_>>()))
                    .collect::<Vec<_>>()
            );

            let mut num_finished = 0;
            while num_finished != jt.num_output_parts {
                let event_option = self_borrow.wait_for_event(jt.run_id, self_borrow.poll_timeout);
                let start_time = Instant::now();

                if let Some(evt) = event_option {
                    log::debug!("event starting");
                    let stage = self_borrow
                        .stage_cache
                        .get(&evt.task.get_stage_id())
                        .unwrap()
                        .clone();
                    log::debug!(
                        "removing stage task from pending tasks {} {}",
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
                            fetch_failure_duration = start_time.elapsed();
                        }
                        _ => {
                            //TODO error handling
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
}

impl NativeScheduler for DistributedScheduler {
    fn submit_task<T: Data, U: Data, F>(
        &self,
        task: TaskOption,
        _id_in_job: usize,
        target_executor: SocketAddrV4,
    ) where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        if !self.master {
            return;
        }
        log::debug!("inside submit task");
        let event_queues = self.event_queues.clone();
        let event_queues_clone = event_queues;
        // FIXME: probably does not need to be blocking in distributed mode; test it and change back to normal spawn
        tokio::task::spawn_blocking(move || {
            while let Err(_) = TcpStream::connect(&target_executor) {
                continue;
            }
            let ser_task = task;

            let task_bytes = bincode::serialize(&ser_task).unwrap();
            log::debug!(
                "task in executor {} {:?} master",
                target_executor.port(),
                ser_task.get_task_id()
            );
            let mut stream = TcpStream::connect(&target_executor).unwrap();
            log::debug!(
                "task in executor {} {} master task len",
                target_executor.port(),
                task_bytes.len()
            );
            let mut message = ::capnp::message::Builder::new_default();
            let mut task_data = message.init_root::<serialized_data::Builder>();
            log::debug!("sending data to server");
            task_data.set_msg(&task_bytes);
            serialize_packed::write_message(&mut stream, &message).unwrap();

            let r = ::capnp::message::ReaderOptions {
                traversal_limit_in_words: std::u64::MAX,
                nesting_limit: 64,
            };
            let mut stream_r = std::io::BufReader::new(&mut stream);
            let message_reader = serialize_packed::read_message(&mut stream_r, r).unwrap();
            let task_data = message_reader
                .get_root::<serialized_data::Reader>()
                .unwrap();
            log::debug!(
                "task in executor {} {} master task result len",
                target_executor.port(),
                task_data.get_msg().unwrap().len()
            );
            let result: TaskResult = bincode::deserialize(&task_data.get_msg().unwrap()).unwrap();
            match ser_task {
                TaskOption::ResultTask(tsk) => {
                    let result = match result {
                        TaskResult::ResultTask(r) => r,
                        _ => panic!("wrong result type"),
                    };
                    if let Ok(task_final) = tsk.downcast::<ResultTask<T, U, F>>() {
                        let task_final = task_final as Box<dyn TaskBase>;
                        DistributedScheduler::task_ended(
                            event_queues_clone,
                            task_final,
                            TastEndReason::Success,
                            // Can break in future. But actually not needed for distributed scheduler since task runs on different processes.
                            // Currently using this because local scheduler needs it. It can be solved by refactoring tasks differently for local and distributed scheduler
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
                        DistributedScheduler::task_ended(
                            event_queues_clone,
                            task_final,
                            TastEndReason::Success,
                            result.into_any_send_sync(),
                        );
                    }
                }
            };
        });
    }

    fn next_executor_server(&self, task: &dyn TaskBase) -> SocketAddrV4 {
        if !task.is_pinned() {
            // pick the first available server
            let socket_addrs = self.server_uris.lock().pop_back().unwrap();
            self.server_uris.lock().push_front(socket_addrs);
            socket_addrs
        } else {
            // seek and pick the selected host
            let servers = &mut *self.server_uris.lock();
            let location: Ipv4Addr = task.preferred_locations()[0];
            if let Some((pos, _)) = servers
                .iter()
                .enumerate()
                .find(|(_, e)| *e.ip() == location)
            {
                let target_host = servers.remove(pos).unwrap();
                servers.push_front(target_host.clone());
                target_host
            } else {
                unreachable!()
            }
        }
    }

    impl_common_scheduler_funcs!();
}
