use super::*;
use crate::scheduler::Scheduler;

use std::any::Any;
use std::collections::btree_map::BTreeMap;
use std::collections::btree_set::BTreeSet;
use std::collections::vec_deque::VecDeque;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::net::{Ipv4Addr, TcpStream};
use std::option::Option;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time;
use std::time::{Duration, Instant};

use capnp::serialize_packed;
use parking_lot::Mutex;
use threadpool::ThreadPool;

//just for now, creating an entire scheduler functions without dag scheduler trait. Later change it to extend from dag scheduler
#[derive(Clone, Default)]
pub struct DistributedScheduler {
    threads: usize,
    max_failures: usize,
    attempt_id: Arc<AtomicUsize>,
    resubmit_timeout: u128,
    poll_timeout: u64,
    event_queues: Arc<Mutex<HashMap<usize, VecDeque<CompletionEvent>>>>,
    next_job_id: Arc<AtomicUsize>,
    next_run_id: Arc<AtomicUsize>,
    next_task_id: Arc<AtomicUsize>,
    next_stage_id: Arc<AtomicUsize>,
    stage_cache: Arc<Mutex<HashMap<usize, Stage>>>,
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
    server_uris: Arc<Mutex<VecDeque<(String, u16)>>>,
    port: u16,
    map_output_tracker: MapOutputTracker,
}

impl DistributedScheduler {
    pub fn new(
        threads: usize,
        max_failures: usize,
        master: bool,
        servers: Option<Vec<(String, u16)>>,
        port: u16,
    ) -> Self {
        info!(
            "starting distributed scheduler in client - {} {}",
            master, port
        );
        DistributedScheduler {
            //            threads,
            threads: 100,
            max_failures,
            attempt_id: Arc::new(AtomicUsize::new(0)),
            resubmit_timeout: 2000,
            poll_timeout: 500,
            event_queues: Arc::new(Mutex::new(HashMap::new())),
            next_job_id: Arc::new(AtomicUsize::new(0)),
            next_run_id: Arc::new(AtomicUsize::new(0)),
            next_task_id: Arc::new(AtomicUsize::new(0)),
            next_stage_id: Arc::new(AtomicUsize::new(0)),
            stage_cache: Arc::new(Mutex::new(HashMap::new())),
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
            server_uris: if let Some(servers) = servers {
                let mut vec = VecDeque::new();
                for (i, j) in servers {
                    vec.push_front((i, j));
                }
                Arc::new(Mutex::new(VecDeque::from_iter(vec)))
            } else {
                Arc::new(Mutex::new(VecDeque::new()))
            },
            port,
            map_output_tracker: env::Env::get().map_output_tracker.clone(),
        }
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
                //                    result: Some(Box::new(result)),
                result,
                accum_updates: HashMap::new(),
            });
        } else {
            info!("ignoring completion event for DAG Job");
        }
    }

    pub fn run_job<T: Data, U: Data, F>(
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
            "shuffle maanger in final rdd of run job {:?}",
            env::Env::get().shuffle_manager
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
            let start_time = Instant::now();

            if let Some(mut evt) = event_option {
                info!("event starting");
                let stage = self.stage_cache.lock()[&evt.task.get_stage_id()].clone();
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
                        fetch_failure_duration = start_time.elapsed();
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
}

impl NativeScheduler for DistributedScheduler {
    fn submit_task<T: Data, U: Data, F>(
        &self,
        task: TaskOption,
        id_in_job: usize,
        thread_pool: Rc<ThreadPool>,
    ) where
        F: SerFunc((TasKContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        info!("inside submit task");
        let my_attempt_id = self.attempt_id.fetch_add(1, Ordering::SeqCst);
        let event_queues = self.event_queues.clone();
        //        let ser_task = SerializeableTask { task };
        //        let ser_task = task;
        //
        //        let task_bytes = bincode::serialize(&ser_task).unwrap();
        //let server_port = self.port;
        //        server_port = server_port - (server_port % 1000);
        //        server_port = server_port + ser_task.get_task_id();
        //info!("server port number {}", server_port);
        //        let log_output = format!("task id {}", ser_task.get_task_id());
        //        env::log_file.lock().write(&log_output.as_bytes());
        if self.master {
            //            self.server_uris.lock().push_front(server.clone());
            //            let ten_millis = std::time::Duration::from_millis(1000);
            //            thread::sleep(ten_millis);
            let server_map = self.server_uris.lock().pop_back().unwrap();
            self.server_uris.lock().push_front(server_map.clone());
            let server_port = server_map.1;
            //            client_port = client_port - (client_port % 1000);
            //            client_port = client_port + ser_task.get_task_id();
            let server_address = server_map.0.clone();
            let event_queues_clone = event_queues.clone();
            thread_pool.execute(move || {
                while let Err(_) = TcpStream::connect(format!("{}:{}", server_address, server_port))
                {
                    continue;
                }
                let ser_task = task;

                let task_bytes = bincode::serialize(&ser_task).unwrap();
                info!(
                    "task in executor {} {:?} master",
                    server_port,
                    ser_task.get_task_id()
                );
                let mut stream =
                    TcpStream::connect(format!("{}:{}", server_address, server_port)).unwrap();
                info!(
                    "task in executor {} {} master task len",
                    server_port,
                    task_bytes.len()
                );
                let mut message = ::capnp::message::Builder::new_default();
                let mut task_data = message.init_root::<serialized_data::Builder>();
                info!("sending data to server");
                task_data.set_msg(&task_bytes);
                serialize_packed::write_message(&mut stream, &message);

                let r = ::capnp::message::ReaderOptions {
                    traversal_limit_in_words: std::u64::MAX,
                    nesting_limit: 64,
                };
                let mut stream_r = std::io::BufReader::new(&mut stream);
                let message_reader = serialize_packed::read_message(&mut stream_r, r).unwrap();
                let task_data = message_reader
                    .get_root::<serialized_data::Reader>()
                    .unwrap();
                info!(
                    "task in executor {} {} master task result len",
                    server_port,
                    task_data.get_msg().unwrap().len()
                );
                let result: TaskResult =
                    bincode::deserialize(&task_data.get_msg().unwrap()).unwrap();
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
            })
        }
    }

    impl_common_funcs!();
}
