use super::*;
use crate::job::JobTracker;
use crate::scheduler::SharedScheduler;

use std::any::Any;
use std::cell::RefCell;
use std::clone::Clone;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::option::Option;
use std::rc::Rc;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use threadpool::ThreadPool;

#[derive(Clone, Default)]
pub struct LocalScheduler {
    pub(crate) threads: usize,
    max_failures: usize,
    attempt_id: Arc<AtomicUsize>,
    resubmit_timeout: u128,
    poll_timeout: u64,
    event_queues: Arc<Mutex<HashMap<usize, VecDeque<CompletionEvent>>>>,
    pub(crate) next_job_id: Arc<AtomicUsize>,
    next_run_id: Arc<AtomicUsize>,
    next_task_id: Arc<AtomicUsize>,
    next_stage_id: Arc<AtomicUsize>,
    id_to_stage: Arc<Mutex<HashMap<usize, Stage>>>,
    shuffle_to_map_stage: Arc<Mutex<HashMap<usize, Stage>>>,
    cache_locs: Arc<Mutex<HashMap<usize, Vec<Vec<Ipv4Addr>>>>>,
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
}

impl LocalScheduler {
    pub fn new(threads: usize, max_failures: usize, master: bool) -> Self {
        LocalScheduler {
            threads,
            max_failures,
            attempt_id: Arc::new(AtomicUsize::new(0)),
            resubmit_timeout: 2000,
            poll_timeout: 500,
            event_queues: Arc::new(Mutex::new(HashMap::new())),
            next_job_id: Arc::new(AtomicUsize::new(0)),
            next_run_id: Arc::new(AtomicUsize::new(0)),
            next_task_id: Arc::new(AtomicUsize::new(0)),
            next_stage_id: Arc::new(AtomicUsize::new(0)),
            id_to_stage: Arc::new(Mutex::new(HashMap::new())),
            shuffle_to_map_stage: Arc::new(Mutex::new(HashMap::new())),
            cache_locs: Arc::new(Mutex::new(HashMap::new())),
            master,
            framework_name: "spark".to_string(),
            is_registered: true, //TODO check if it is necessary
            active_jobs: HashMap::new(),
            active_job_queue: Vec::new(),
            taskid_to_jobid: HashMap::new(),
            taskid_to_slaveid: HashMap::new(),
            job_tasks: HashMap::new(),
            slaves_with_executors: HashSet::new(),
            map_output_tracker: env::env.map_output_tracker.clone(),
        }
    }

    pub fn run_job<T: Data, U: Data, F, RT>(
        &self,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        partitions: Vec<usize>,
        allow_local: bool,
    ) -> Result<Vec<U>>
    where
        F: SerFunc((TasKContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        info!(
            "shuffle manager in final rdd of run job {:?}",
            env::env.shuffle_manager
        );

        let mut jt = JobTracker::from_scheduler(self, func.clone(), final_rdd.clone(), partitions);
        let mut results: Vec<Option<U>> = (0..jt.num_output_parts).map(|_| None).collect();
        let mut num_finished = 0;
        let mut fetch_failure_duration = Duration::new(0, 0);

        //TODO update cache
        //TODO logging

        if allow_local {
            if let Some(result) = LocalScheduler::local_execution(jt.clone())? {
                return Ok(result);
            }
        }

        self.event_queues.lock().insert(jt.run_id, VecDeque::new());

        self.submit_stage(jt.final_stage.clone(), jt.clone());
        info!(
            "pending stages and tasks {:?}",
            jt.pending_tasks
                .borrow()
                .iter()
                .map(|(k, v)| (k.id, v.iter().map(|x| x.get_task_id()).collect::<Vec<_>>()))
                .collect::<Vec<_>>()
        );

        while num_finished != jt.num_output_parts {
            let event_option = self.wait_for_event(jt.run_id, self.poll_timeout);
            let start = Instant::now();

            if let Some(mut evt) = event_option {
                info!("event starting");
                let stage = self.id_to_stage.lock()[&evt.task.get_stage_id()].clone();
                info!(
                    "removing stage task from pending tasks {} {}",
                    stage.id,
                    evt.task.get_task_id()
                );
                jt.pending_tasks
                    .borrow_mut()
                    .get_mut(&stage)
                    .unwrap()
                    .remove(&evt.task);
                use super::dag_scheduler::TastEndReason::*;
                match evt.reason {
                    Success => {
                        self.on_event_success(evt, &mut results, &mut num_finished, jt.clone())
                    }
                    FetchFailed(failed_vals) => {
                        self.on_event_failure(jt.clone(), failed_vals, evt.task.get_stage_id());
                        fetch_failure_duration = start.elapsed();
                    }
                    _ => {
                        //TODO error handling
                    }
                }
            }

            if !jt.failed.borrow().is_empty()
                && fetch_failure_duration.as_millis() > self.resubmit_timeout
            {
                self.update_cache_locs();
                for stage in jt.failed.borrow().iter() {
                    self.submit_stage(stage.clone(), jt.clone());
                }
                jt.failed.borrow_mut().clear();
            }
        }

        self.event_queues.lock().remove(&jt.run_id);

        Ok(results
            .into_iter()
            .map(|s| match s {
                Some(v) => v,
                None => panic!("some results still missing"),
            })
            .collect())
    }

    fn wait_for_event(&self, run_id: usize, timeout: u64) -> Option<CompletionEvent> {
        let end = Instant::now() + Duration::from_millis(timeout);
        while self.event_queues.lock().get(&run_id).unwrap().is_empty() {
            if Instant::now() > end {
                return None;
            } else {
                thread::sleep(end - Instant::now());
            }
        }
        self.event_queues
            .lock()
            .get_mut(&run_id)
            .unwrap()
            .pop_front()
    }

    fn run_task<T: Data, U: Data, RT, F>(
        event_queues: Arc<Mutex<HashMap<usize, VecDeque<CompletionEvent>>>>,
        task: Vec<u8>,
        id_in_job: usize,
        attempt_id: usize,
    ) where
        F: SerFunc((TasKContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        let des_task: TaskOption = bincode::deserialize(&task).unwrap();
        let result = des_task.run(attempt_id);
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
        event_queues: Arc<Mutex<HashMap<usize, VecDeque<CompletionEvent>>>>,
        task: Box<dyn TaskBase>,
        reason: TastEndReason,
        result: Box<dyn Any + Send + Sync>,
        //TODO accumvalues needs to be done
    ) {
        let result = Some(result);
        if let Some(queue) = event_queues.lock().get_mut(&(task.get_run_id())) {
            queue.push_back(CompletionEvent {
                task,
                reason,
                result,
                accum_updates: HashMap::new(),
            });
        } else {
            info!("ignoring completion event for DAG Job");
        }
    }
}

impl SharedScheduler for LocalScheduler {
    /// Every single task in run in the local thread pool
    fn submit_task<T: Data, U: Data, RT, F>(
        &self,
        task: TaskOption,
        id_in_job: usize,
        thread_pool: Rc<ThreadPool>,
    ) where
        F: SerFunc((TasKContext, Box<dyn Iterator<Item = T>>)) -> U,
        RT: Rdd<T> + 'static,
    {
        info!("inside submit task");
        let my_attempt_id = self.attempt_id.fetch_add(1, Ordering::SeqCst);
        let event_queues = self.event_queues.clone();
        let task = bincode::serialize(&task).unwrap();
        thread_pool.execute(move || {
            LocalScheduler::run_task::<T, U, RT, F>(event_queues, task, id_in_job, my_attempt_id)
        });
    }

    fn insert_into_stage_cache(&mut self, id: usize, stage: Stage) {
        self.id_to_stage.lock().insert(id, stage.clone());
    }

    fn register_shuffle(&mut self, shuffle_id: usize, num_maps: usize) {
        self.map_output_tracker
            .register_shuffle(shuffle_id, num_maps);
    }

    fn update_cache_locs(&self) {
        let mut locs = self.cache_locs.lock();
        *locs = env::env.cache_tracker.get_location_snapshot();
    }

    fn get_cache_locs(&self, rdd: Arc<dyn RddBase>) -> Option<Vec<Vec<Ipv4Addr>>> {
        let cache_locs = self.cache_locs.lock();
        let locs_opt = cache_locs.get(&rdd.get_rdd_id());
        match locs_opt {
            Some(locs) => Some(locs.clone()),
            None => None,
        }
    }

    fn get_next_job_id(&self) -> usize {
        self.next_job_id.fetch_add(1, Ordering::SeqCst)
    }

    fn get_next_stage_id(&self) -> usize {
        self.next_stage_id.fetch_add(1, Ordering::SeqCst)
    }

    fn get_next_task_id(&self) -> usize {
        self.next_task_id.fetch_add(1, Ordering::SeqCst)
    }

    fn get_missing_parent_stages(&self, stage: Stage) -> Vec<Stage> {
        info!("inside get missing parent stages");
        let mut missing: BTreeSet<Stage> = BTreeSet::new();
        let mut visited: BTreeSet<Arc<dyn RddBase>> = BTreeSet::new();
        self.visit_for_missing_parent_stages(&mut missing, &mut visited, stage.get_rdd());
        missing.into_iter().collect()
    }

    fn get_shuffle_map_stage(&self, shuf: Arc<dyn ShuffleDependencyTrait>) -> Stage {
        info!("inside get_shufflemap stage");
        let stage = match self.shuffle_to_map_stage.lock().get(&shuf.get_shuffle_id()) {
            Some(s) => Some(s.clone()),
            None => None,
        };
        match stage {
            Some(stage) => stage.clone(),
            None => {
                info!("inside get_shufflemap stage before");
                let stage = self.new_stage(shuf.get_rdd_base(), Some(shuf.clone()));
                self.shuffle_to_map_stage
                    .lock()
                    .insert(shuf.get_shuffle_id(), stage.clone());
                info!("inside get_shufflemap return");
                stage
            }
        }
    }

    fn num_threads(&self) -> usize {
        self.threads
    }
}
