use std::collections::{btree_set::BTreeSet, vec_deque::VecDeque, HashMap, HashSet};
use std::fmt::Debug;
use std::iter::FromIterator;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use crate::dependency::ShuffleDependencyTrait;
use crate::env;
use crate::error::{Error, NetworkError, Result};
use crate::map_output_tracker::MapOutputTracker;
use crate::partial::{ApproximateActionListener, ApproximateEvaluator, PartialResult};
use crate::rdd::{Rdd, RddBase};
use crate::scheduler::{
    listener::{JobEndListener, JobStartListener},
    CompletionEvent, EventQueue, Job, JobListener, JobTracker, LiveListenerBus, NativeScheduler,
    NoOpListener, ResultTask, Stage, TaskBase, TaskContext, TaskOption, TaskResult, TastEndReason,
};
use crate::serializable_traits::{AnyData, Data, SerFunc};
use crate::serialized_data_capnp::serialized_data;
use crate::shuffle::ShuffleMapTask;
use capnp::message::ReaderOptions;
use capnp_futures::serialize as capnp_serialize;
use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::net::TcpStream;
use tokio_util::compat::{Tokio02AsyncReadCompatExt, Tokio02AsyncWriteCompatExt};

const CAPNP_BUF_READ_OPTS: ReaderOptions = ReaderOptions {
    traversal_limit_in_words: std::u64::MAX,
    nesting_limit: 64,
};

// Just for now, creating an entire scheduler functions without dag scheduler trait.
// Later change it to extend from dag scheduler.
#[derive(Clone, Default)]
pub(crate) struct DistributedScheduler {
    max_failures: usize,
    attempt_id: Arc<AtomicUsize>,
    resubmit_timeout: u128,
    poll_timeout: u64,
    event_queues: EventQueue,
    next_job_id: Arc<AtomicUsize>,
    next_run_id: Arc<AtomicUsize>,
    next_task_id: Arc<AtomicUsize>,
    next_stage_id: Arc<AtomicUsize>,
    stage_cache: Arc<DashMap<usize, Stage>>,
    shuffle_to_map_stage: Arc<DashMap<usize, Stage>>,
    cache_locs: Arc<DashMap<usize, Vec<Vec<Ipv4Addr>>>>,
    master: bool,
    framework_name: String,
    is_registered: bool, // TODO: check if it is necessary
    active_jobs: HashMap<usize, Job>,
    active_job_queue: Vec<Job>,
    taskid_to_jobid: HashMap<String, usize>,
    taskid_to_slaveid: HashMap<String, String>,
    job_tasks: HashMap<usize, HashSet<String>>,
    slaves_with_executors: HashSet<String>,
    server_uris: Arc<Mutex<VecDeque<SocketAddrV4>>>,
    port: u16,
    map_output_tracker: MapOutputTracker,
    // TODO: fix proper locking mechanism
    scheduler_lock: Arc<Mutex<bool>>,
    live_listener_bus: LiveListenerBus,
}

impl DistributedScheduler {
    pub fn new(
        max_failures: usize,
        master: bool,
        servers: Option<Vec<SocketAddrV4>>,
        port: u16,
    ) -> Self {
        log::debug!(
            "starting distributed scheduler @ port {} (in master mode: {})",
            port,
            master,
        );
        let mut live_listener_bus = LiveListenerBus::new();
        live_listener_bus.start().unwrap();
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
            framework_name: "native_spark".to_string(),
            is_registered: true, // TODO: check if it is necessary
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
            live_listener_bus,
        }
    }

    /// Run an approximate job on the given RDD and pass all the results to an ApproximateEvaluator
    /// as they arrive. Returns a partial result object from the evaluator.
    pub fn run_approximate_job<T: Data, U: Data, R, F, E>(
        self: Arc<Self>,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        evaluator: E,
        timeout: Duration,
    ) -> Result<PartialResult<R>>
    where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
        E: ApproximateEvaluator<U, R> + Send + Sync + 'static,
        R: Clone + Debug + Send + Sync + 'static,
    {
        // acquiring lock so that only one job can run at same time this lock is just
        // a temporary patch for preventing multiple jobs to update cache locks which affects
        // construction of dag task graph. dag task graph construction needs to be altered
        let selfc = self.clone();
        let _lock = selfc.scheduler_lock.lock();
        env::Env::run_in_async_rt(|| -> Result<PartialResult<R>> {
            futures::executor::block_on(async move {
                let partitions: Vec<_> = (0..final_rdd.number_of_splits()).collect();
                let listener = ApproximateActionListener::new(evaluator, timeout, partitions.len());
                let jt = JobTracker::from_scheduler(
                    &*self,
                    func,
                    final_rdd.clone(),
                    partitions,
                    listener,
                )
                .await?;
                if final_rdd.number_of_splits() == 0 {
                    // Return immediately if the job is running 0 tasks
                    let time = Instant::now();
                    self.live_listener_bus.post(Box::new(JobStartListener {
                        job_id: jt.run_id,
                        time,
                        stage_infos: vec![],
                    }));
                    self.live_listener_bus.post(Box::new(JobEndListener {
                        job_id: jt.run_id,
                        time,
                        job_result: true,
                    }));
                    return Ok(PartialResult::new(
                        jt.listener.evaluator.lock().await.current_result(),
                        true,
                    ));
                }
                tokio::spawn(self.event_process_loop(false, jt.clone()));
                jt.listener.get_result().await
            })
        })
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
        let selfc = self.clone();
        let _lock = selfc.scheduler_lock.lock();
        env::Env::run_in_async_rt(|| -> Result<Vec<U>> {
            futures::executor::block_on(async move {
                let jt = JobTracker::from_scheduler(
                    &*self,
                    func,
                    final_rdd.clone(),
                    partitions,
                    NoOpListener,
                )
                .await?;
                self.event_process_loop(allow_local, jt).await
            })
        })
    }

    /// Start the event processing loop for a given job.
    async fn event_process_loop<T: Data, U: Data, F, L>(
        self: Arc<Self>,
        allow_local: bool,
        jt: Arc<JobTracker<F, U, T, L>>,
    ) -> Result<Vec<U>>
    where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
        L: JobListener,
    {
        // TODO: update cache

        if allow_local {
            if let Some(result) = DistributedScheduler::local_execution(jt.clone())? {
                return Ok(result);
            }
        }

        self.event_queues.insert(jt.run_id, VecDeque::new());

        let mut results: Vec<Option<U>> = (0..jt.num_output_parts).map(|_| None).collect();
        let mut fetch_failure_duration = Duration::new(0, 0);

        self.submit_stage(jt.final_stage.clone(), jt.clone())
            .await?;
        log::debug!(
            "pending stages and tasks: {:?}",
            jt.pending_tasks
                .lock()
                .await
                .iter()
                .map(|(k, v)| (k.id, v.iter().map(|x| x.get_task_id()).collect::<Vec<_>>()))
                .collect::<Vec<_>>()
        );

        let mut num_finished = 0;
        while num_finished != jt.num_output_parts {
            let event_option = self.wait_for_event(jt.run_id, self.poll_timeout);
            let start = Instant::now();

            if let Some(evt) = event_option {
                log::debug!("event starting");
                let stage = self
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
                    .await
                    .get_mut(&stage)
                    .unwrap()
                    .remove(&evt.task);
                use super::dag_scheduler::TastEndReason::*;
                match evt.reason {
                    Success => {
                        self.on_event_success(evt, &mut results, &mut num_finished, jt.clone())
                            .await?;
                    }
                    FetchFailed(failed_vals) => {
                        self.on_event_failure(jt.clone(), failed_vals, evt.task.get_stage_id())
                            .await;
                        fetch_failure_duration = start.elapsed();
                    }
                    Error(error) => panic!("{}", error),
                    OtherFailure(msg) => panic!("{}", msg),
                }
            }
        }

        if !jt.failed.lock().await.is_empty()
            && fetch_failure_duration.as_millis() > self.resubmit_timeout
        {
            self.update_cache_locs().await?;
            for stage in jt.failed.lock().await.iter() {
                self.submit_stage(stage.clone(), jt.clone()).await?;
            }
            jt.failed.lock().await.clear();
        }

        self.event_queues.remove(&jt.run_id);
        Ok(results
            .into_iter()
            .map(|s| match s {
                Some(v) => v,
                None => panic!("some results still missing"),
            })
            .collect())
    }

    async fn receive_results<T: Data, U: Data, F, R>(
        event_queues: Arc<DashMap<usize, VecDeque<CompletionEvent>>>,
        receiver: R,
        task: TaskOption,
        target_port: u16,
    ) where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
        R: futures::AsyncRead + std::marker::Unpin,
    {
        let result: TaskResult = {
            let message = capnp_futures::serialize::read_message(receiver, CAPNP_BUF_READ_OPTS)
                .await
                .unwrap()
                .ok_or_else(|| NetworkError::NoMessageReceived)
                .unwrap();
            let task_data = message.get_root::<serialized_data::Reader>().unwrap();
            log::debug!(
                "received task #{} result of {} bytes from executor @{}",
                task.get_task_id(),
                task_data.get_msg().unwrap().len(),
                target_port
            );
            bincode::deserialize(&task_data.get_msg().unwrap()).unwrap()
        };

        match task {
            TaskOption::ResultTask(tsk) => {
                let result = match result {
                    TaskResult::ResultTask(r) => r,
                    _ => panic!("wrong result type"),
                };
                if let Ok(task_final) = tsk.downcast::<ResultTask<T, U, F>>() {
                    let task_final = task_final as Box<dyn TaskBase>;
                    DistributedScheduler::task_ended(
                        event_queues,
                        task_final,
                        TastEndReason::Success,
                        // Can break in future. But actually not needed for distributed scheduler since task runs on different processes.
                        // Currently using this because local scheduler needs it. It can be solved by refactoring tasks differently for local and distributed scheduler
                        result.into_box(),
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
                        event_queues,
                        task_final,
                        TastEndReason::Success,
                        result.into_box(),
                    );
                }
            }
        };
    }

    fn task_ended(
        event_queues: Arc<DashMap<usize, VecDeque<CompletionEvent>>>,
        task: Box<dyn TaskBase>,
        reason: TastEndReason,
        result: Box<dyn AnyData>,
        // TODO: accumvalues needs to be done
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

#[async_trait::async_trait]
impl NativeScheduler for DistributedScheduler {
    fn submit_task<T: Data, U: Data, F>(
        &self,
        task: TaskOption,
        _id_in_job: usize,
        target_executor: SocketAddrV4,
    ) where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        if !env::Configuration::get().is_driver {
            return;
        }
        log::debug!("inside submit task");
        let event_queues_clone = self.event_queues.clone();
        tokio::spawn(async move {
            let mut num_retries = 0;
            loop {
                match TcpStream::connect(&target_executor).await {
                    Ok(mut stream) => {
                        let (reader, writer) = stream.split();
                        let reader = reader.compat();
                        let writer = writer.compat_write();
                        let task_bytes = bincode::serialize(&task).unwrap();
                        log::debug!(
                            "sending task #{} of {} bytes to exec @{},",
                            task.get_task_id(),
                            task_bytes.len(),
                            target_executor.port(),
                        );

                        // TODO: remove blocking call when possible
                        futures::executor::block_on(async {
                            let mut message = capnp::message::Builder::new_default();
                            let mut task_data = message.init_root::<serialized_data::Builder>();
                            task_data.set_msg(&task_bytes);
                            capnp_serialize::write_message(writer, message)
                                .await
                                .map_err(Error::CapnpDeserialization)
                                .unwrap();
                        });

                        log::debug!("sent data to exec @{}", target_executor.port());

                        // receive results back
                        DistributedScheduler::receive_results::<T, U, F, _>(
                            event_queues_clone,
                            reader,
                            task,
                            target_executor.port(),
                        )
                        .await;
                        break;
                    }
                    Err(_) => {
                        if num_retries > 5 {
                            panic!("executor @{} not initialized", target_executor.port());
                        }
                        tokio::time::delay_for(Duration::from_millis(20)).await;
                        num_retries += 1;
                        continue;
                    }
                }
            }
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
                servers.push_front(target_host);
                target_host
            } else {
                unreachable!()
            }
        }
    }

    async fn update_cache_locs(&self) -> Result<()> {
        self.cache_locs.clear();
        env::Env::get()
            .cache_tracker
            .get_location_snapshot()
            .await?
            .into_iter()
            .for_each(|(k, v)| {
                self.cache_locs.insert(k, v);
            });
        Ok(())
    }

    async fn get_shuffle_map_stage(&self, shuf: Arc<dyn ShuffleDependencyTrait>) -> Result<Stage> {
        log::debug!("getting shuffle map stage");
        let stage = self.shuffle_to_map_stage.get(&shuf.get_shuffle_id());
        match stage {
            Some(stage) => Ok(stage.clone()),
            None => {
                log::debug!("started creating shuffle map stage before");
                let stage = self
                    .new_stage(shuf.get_rdd_base(), Some(shuf.clone()))
                    .await?;
                self.shuffle_to_map_stage
                    .insert(shuf.get_shuffle_id(), stage.clone());
                log::debug!("finished inserting newly created shuffle stage");
                Ok(stage)
            }
        }
    }

    async fn get_missing_parent_stages(&'_ self, stage: Stage) -> Result<Vec<Stage>> {
        log::debug!("getting missing parent stages");
        let mut missing: BTreeSet<Stage> = BTreeSet::new();
        let mut visited: BTreeSet<Arc<dyn RddBase>> = BTreeSet::new();
        self.visit_for_missing_parent_stages(&mut missing, &mut visited, stage.get_rdd())
            .await?;
        Ok(missing.into_iter().collect())
    }

    impl_common_scheduler_funcs!();
}

impl Drop for DistributedScheduler {
    fn drop(&mut self) {
        self.live_listener_bus.stop().unwrap();
    }
}
