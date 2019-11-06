use super::*;

use parking_lot::Mutex;
use std::any::Any;
use std::collections::btree_map::BTreeMap;
use std::collections::btree_set::BTreeSet;
use std::collections::vec_deque::VecDeque;
use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;
use std::option::Option;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time;
use std::time::{Duration, Instant};
use threadpool::ThreadPool;

#[derive(Clone, Default)]
pub struct LocalScheduler {
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
    //    cache_tracker: Arc<Mutex<CacheTracker>>,
}

impl LocalScheduler {
    pub fn new(
        threads: usize,
        max_failures: usize,
        master: bool,
        //        map_output_tracker: MapOutputTracker,
    ) -> Self {
        //        unimplemented!()
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
        //        l.update_cache_locs();
    }

    fn get_cache_locs(&self, rdd: Arc<dyn RddBase>) -> Option<Vec<Vec<Ipv4Addr>>> {
        let cache_locs = self.cache_locs.lock();
        let locs_opt = cache_locs.get(&rdd.get_rdd_id());
        match locs_opt {
            Some(locs) => Some(locs.clone()),
            None => None,
        }
        //        (self.cache_locs.lock().get(&rdd.get_rdd_id())).clone()
    }

    fn update_cache_locs(&self) {
        let mut locs = self.cache_locs.lock();
        *locs = env::env.cache_tracker.get_location_snapshot();
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

    fn get_shuffle_map_stage(&self, shuf: Arc<dyn ShuffleDependencyTrait>) -> Stage {
        info!("inside get_shufflemap stage");
        //        let log_output = format!("inside get_shufflemap stage");
        //        env::log_file.lock().write(&log_output.as_bytes());
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

    fn new_stage(
        &self,
        rdd_base: Arc<dyn RddBase>,
        shuffle_dependency: Option<Arc<dyn ShuffleDependencyTrait>>,
    ) -> Stage {
        info!("inside new stage");
        env::env
            .cache_tracker
            .register_rdd(rdd_base.get_rdd_id(), rdd_base.number_of_splits());
        if shuffle_dependency.is_some() {
            info!("shuffle dependcy and registering mapoutput tracker");
            self.map_output_tracker.register_shuffle(
                shuffle_dependency.clone().unwrap().get_shuffle_id(),
                rdd_base.number_of_splits(),
            );
            info!("new stage tracker after");
        }
        let id = self.next_stage_id.fetch_add(1, Ordering::SeqCst);
        info!("new stage id {}", id);
        let stage = Stage::new(
            id,
            rdd_base.clone(),
            shuffle_dependency,
            self.get_parent_stages(rdd_base),
        );
        self.id_to_stage.lock().insert(id, stage.clone());
        info!("new stage stage return");
        stage
    }

    fn visit_for_parent_stages(
        &self,
        parents: &mut BTreeSet<Stage>,
        visited: &mut BTreeSet<Arc<dyn RddBase>>,
        rdd: Arc<dyn RddBase>,
    ) {
        info!(
            "parent stages {:?}",
            parents.iter().map(|x| x.id).collect::<Vec<_>>()
        );
        info!(
            "visisted stages {:?}",
            visited.iter().map(|x| x.get_rdd_id()).collect::<Vec<_>>()
        );
        if !visited.contains(&rdd) {
            visited.insert(rdd.clone());
            env::env
                .cache_tracker
                .register_rdd(rdd.get_rdd_id(), rdd.number_of_splits());
            for dep in rdd.get_dependencies() {
                match dep {
                    Dependency::ShuffleDependency(shuf_dep) => {
                        parents.insert(self.get_shuffle_map_stage(shuf_dep.clone()));
                    }
                    Dependency::OneToOneDependency(oto_dep) => {
                        self.visit_for_parent_stages(parents, visited, oto_dep.get_rdd_base())
                    }
                    Dependency::NarrowDependency(nar_dep) => {
                        self.visit_for_parent_stages(parents, visited, nar_dep.get_rdd_base())
                    } //TODO finish range dependency
                }
            }
        }
    }

    fn get_parent_stages(&self, rdd: Arc<dyn RddBase>) -> Vec<Stage> {
        info!("inside get parent stages");
        let mut parents: BTreeSet<Stage> = BTreeSet::new();
        let mut visited: BTreeSet<Arc<dyn RddBase>> = BTreeSet::new();
        self.visit_for_parent_stages(&mut parents, &mut visited, rdd.clone());
        info!(
            "parent stages {:?}",
            parents.iter().map(|x| x.id).collect::<Vec<_>>()
        );
        parents.into_iter().collect()
    }

    fn visit_for_missing_parent_stages(
        &self,
        missing: &mut BTreeSet<Stage>,
        visited: &mut BTreeSet<Arc<dyn RddBase>>,
        rdd: Arc<dyn RddBase>,
    ) {
        info!(
            "missing stages {:?}",
            missing.iter().map(|x| x.id).collect::<Vec<_>>()
        );
        info!(
            "visisted stages {:?}",
            visited.iter().map(|x| x.get_rdd_id()).collect::<Vec<_>>()
        );
        if !visited.contains(&rdd) {
            visited.insert(rdd.clone());
            // TODO CacheTracker register
            for p in 0..rdd.number_of_splits() {
                let locs = self.get_cache_locs(rdd.clone());
                info!("cache locs {:?}", locs);
                if locs == None {
                    for dep in rdd.get_dependencies() {
                        info!("for dep in missing stages ");
                        match dep {
                            Dependency::ShuffleDependency(shuf_dep) => {
                                let stage = self.get_shuffle_map_stage(shuf_dep.clone());
                                info!("shuffle stage in missing stages {:?}", stage.id);
                                if !stage.is_available() {
                                    info!(
                                        "inserting shuffle stage in missing stages {:?}",
                                        stage.id
                                    );
                                    missing.insert(stage);
                                }
                            }
                            Dependency::NarrowDependency(nar_dep) => {
                                info!("narrow stage in missing stages ");
                                self.visit_for_missing_parent_stages(
                                    missing,
                                    visited,
                                    nar_dep.get_rdd_base(),
                                )
                            }
                            Dependency::OneToOneDependency(one_dep) => {
                                info!("one to one stage in missing stages ");
                                self.visit_for_missing_parent_stages(
                                    missing,
                                    visited,
                                    one_dep.get_rdd_base(),
                                )
                            } //TODO finish range dependency
                        }
                    }
                }
            }
        }
    }

    fn get_missing_parent_stages(&self, stage: Stage) -> Vec<Stage> {
        info!("inside get missing parent stages");
        let mut missing: BTreeSet<Stage> = BTreeSet::new();
        let mut visited: BTreeSet<Arc<dyn RddBase>> = BTreeSet::new();
        self.visit_for_missing_parent_stages(&mut missing, &mut visited, stage.get_rdd());
        missing.into_iter().collect()
    }

    pub fn run_job<T: Data, U: Data, F, RT>(
        &self,
        func: Arc<F>,
        final_rdd: Arc<RT>,
        partitions: Vec<usize>,
        allow_local: bool,
    ) -> Vec<U>
    where
        F: SerFunc((TasKContext, Box<dyn Iterator<Item = T>>)) -> U,
        RT: Rdd<T> + 'static,
    {
        info!(
            "shuffle maanger in final rdd of run job {:?}",
            env::env.shuffle_manager
        );
        let thread_pool = Arc::new(ThreadPool::new(self.threads));
        let run_id = self.next_run_id.fetch_add(1, Ordering::SeqCst);
        let output_parts = partitions;
        let num_output_parts = output_parts.len();
        let final_stage = self.new_stage(final_rdd.clone(), None);
        let mut results: Vec<Option<U>> = (0..num_output_parts).map(|_| None).collect();
        let mut finished: Vec<bool> = (0..num_output_parts).map(|_| false).collect();
        let mut num_finished = 0;
        let mut waiting: BTreeSet<Stage> = BTreeSet::new();
        let mut running: BTreeSet<Stage> = BTreeSet::new();
        let mut failed: BTreeSet<Stage> = BTreeSet::new();
        let mut pending_tasks: BTreeMap<Stage, BTreeSet<Box<dyn TaskBase>>> = BTreeMap::new();
        let mut fetch_failure_duration = Duration::new(0, 0);

        //TODO update cache
        //TODO logging

        if allow_local && final_stage.parents.is_empty() && (num_output_parts == 1) {
            let split = (final_rdd.splits()[output_parts[0]]).clone();
            let task_context = TasKContext::new(final_stage.id, output_parts[0], 0);
            return vec![func((task_context, final_rdd.iterator(split)))];
        }

        self.event_queues.lock().insert(run_id, VecDeque::new());

        self.submit_stage(
            final_stage.clone(),
            &mut waiting,
            &mut running,
            &mut finished,
            &mut pending_tasks,
            output_parts.clone(),
            num_output_parts,
            final_stage.clone(),
            func.clone(),
            final_rdd.clone(),
            run_id,
            thread_pool.clone(),
        );
        info!(
            "pending stages and tasks {:?}",
            pending_tasks
                .iter()
                .map(|(k, v)| (k.id, v.iter().map(|x| x.get_task_id()).collect::<Vec<_>>()))
                .collect::<Vec<_>>()
        );

        while num_finished != num_output_parts {
            let event_option = self.wait_for_event(run_id, self.poll_timeout);
            let start = Instant::now();

            if let Some(mut evt) = event_option {
                info!("event starting");
                let stage = self.id_to_stage.lock()[&evt.task.get_stage_id()].clone();
                info!(
                    "removing stage task from pending tasks {} {}",
                    stage.id,
                    evt.task.get_task_id()
                );
                pending_tasks.get_mut(&stage).unwrap().remove(&evt.task);
                use super::dag_scheduler::TastEndReason::*;
                match evt.reason {
                    Success => {
                        //                        println!("inside run job and inside event success");
                        //TODO logging
                        //TODO add to Accumulator

                        // ResultTask alone done now.
                        //                        if let Some(result) = evt.get_result::<U>();
                        let mut result_type = evt.task.downcast_ref::<ResultTask<T, U, RT, F>>().is_some();
                        if result_type {
                            if let Ok(rt) = evt.task.downcast::<ResultTask<T, U, RT, F>>() {
                                let result = evt
                                    .result
                                    .take()
                                    .unwrap()
                                    .downcast_ref::<U>()
                                    .unwrap()
                                    .clone();
                                //                                let result: Box<Any> =
                                //                                    evt.result.take().unwrap();
                                //                                //                                let result = result.into_any();
                                //                                let result: Box<U> =
                                //                                    Box::<Any>::downcast(result.into_any()).unwrap();
                                //                                //                                let result = result.downcast::<U>().unwrap();
                                //                                let result = *result;
                                //                                println!(
                                //                                    "result task result in master {} {:?}",
                                //                                    self.master, result
                                //                                );
                                results[rt.output_id] = Some(result);
                                finished[rt.output_id] = true;
                                num_finished += 1;
                            }
                        } else if let Ok(smt) = evt.task.downcast::<ShuffleMapTask>() {
                            let result = evt
                                .result
                                .take()
                                .unwrap()
                                .downcast_ref::<String>()
                                .unwrap()
                                .clone();
                            //                                let result = *result;
                            //                                let result: serde_traitobject::Box<serde_traitobject::Any> =
                            //                                    evt.result.take().unwrap();
                            //                                //                                let result = result.into_any();
                            //                                let result: Box<String> =
                            //                                    Box::<Any>::downcast(result.into_any()).unwrap();
                            //                                //                                let result = result.downcast::<String>().unwrap();
                            //                                let result = *result;
                            info!("result inside queue {:?}", result);
                            self.id_to_stage
                                .lock()
                                .get_mut(&smt.stage_id)
                                .unwrap()
                                .add_output_loc(smt.partition, result);
                            let stage = self.id_to_stage.lock().clone()[&smt.stage_id].clone();
                            info!(
                                "pending stages {:?}",
                                pending_tasks
                                    .iter()
                                    .map(|(x, y)| (
                                        x.id,
                                        y.iter().map(|k| k.get_task_id()).collect::<Vec<_>>()
                                    ))
                                    .collect::<Vec<_>>()
                            );
                            info!(
                                "pending tasks {:?}",
                                pending_tasks
                                    .get(&stage)
                                    .unwrap()
                                    .iter()
                                    .map(|x| x.get_task_id())
                                    .collect::<Vec<_>>()
                            );
                            info!(
                                "running {:?}",
                                running.iter().map(|x| x.id).collect::<Vec<_>>()
                            );
                            info!(
                                "waiting {:?}",
                                waiting.iter().map(|x| x.id).collect::<Vec<_>>()
                            );

                            if running.contains(&stage)
                                && pending_tasks.get(&stage).unwrap().is_empty()
                            {
                                info!("here before registering map outputs ");
                                //TODO logging
                                running.remove(&stage);
                                if stage.shuffle_dependency.is_some() {
                                    info!(
                                        "stage output locs before register mapoutput tracker {:?}",
                                        stage.output_locs
                                    );
                                    let locs = stage
                                        .output_locs
                                        .iter()
                                        .map(|x| match x.get(0) {
                                            Some(s) => Some(s.to_owned()),
                                            None => None,
                                        })
                                        .collect();
                                    info!(
                                        "locs for shuffle id {:?} {:?}",
                                        stage.clone().shuffle_dependency.unwrap().get_shuffle_id(),
                                        locs
                                    );
                                    self.map_output_tracker.register_map_outputs(
                                        stage.shuffle_dependency.unwrap().get_shuffle_id(),
                                        locs,
                                    );
                                    info!("here after registering map outputs ");
                                }
                                //TODO Cache
                                self.update_cache_locs();
                                let mut newly_runnable = Vec::new();
                                for stage in &waiting {
                                    info!(
                                        "waiting stage parent stages for stage {} are {:?}",
                                        stage.id,
                                        self.get_missing_parent_stages(stage.clone())
                                            .iter()
                                            .map(|x| x.id)
                                            .collect::<Vec<_>>()
                                    );
                                    if self.get_missing_parent_stages(stage.clone()).is_empty() {
                                        newly_runnable.push(stage.clone())
                                    }
                                }
                                for stage in &newly_runnable {
                                    waiting.remove(stage);
                                }
                                for stage in &newly_runnable {
                                    running.insert(stage.clone());
                                }
                                for stage in newly_runnable {
                                    self.submit_missing_tasks(
                                        stage,
                                        &mut finished,
                                        &mut pending_tasks,
                                        output_parts.clone(),
                                        num_output_parts,
                                        final_stage.clone(),
                                        func.clone(),
                                        final_rdd.clone(),
                                        run_id,
                                        thread_pool.clone(),
                                    );
                                }
                            }
                        }
                    }
                    FetchFailed(FetchFailedVals {
                        server_uri,
                        shuffle_id,
                        map_id,
                        reduce_id,
                    }) => {
                        //TODO mapoutput tacker needs to be finished for this
                        let failed_stage = self
                            .id_to_stage
                            .lock()
                            .get(&evt.task.get_stage_id())
                            .unwrap()
                            .clone();
                        running.remove(&failed_stage);
                        failed.insert(failed_stage);
                        //TODO logging
                        self.shuffle_to_map_stage
                            .lock()
                            .get_mut(&shuffle_id)
                            .unwrap()
                            .remove_output_loc(map_id, server_uri.clone());
                        self.map_output_tracker.unregister_map_output(
                            shuffle_id,
                            map_id,
                            server_uri.clone(),
                        );
                        //logging
                        failed.insert(
                            self.shuffle_to_map_stage
                                .lock()
                                .get(&shuffle_id)
                                .unwrap()
                                .clone(),
                        );
                        fetch_failure_duration = start.elapsed()
                    }
                    _ => {
                        //TODO error handling
                    }
                }
            }
            if !failed.is_empty() && fetch_failure_duration.as_millis() > self.resubmit_timeout {
                self.update_cache_locs();
                for stage in &failed {
                    self.submit_stage(
                        stage.clone(),
                        &mut waiting,
                        &mut running,
                        &mut finished,
                        &mut pending_tasks,
                        output_parts.clone(),
                        num_output_parts,
                        final_stage.clone(),
                        func.clone(),
                        final_rdd.clone(),
                        run_id,
                        thread_pool.clone(),
                    );
                }
                failed.clear();
            }
        }

        self.event_queues.lock().remove(&run_id);
        //        let dur = time::Duration::from_millis(20000);
        //        thread::sleep(dur);
        results
            .into_iter()
            .map(|s| match s {
                Some(v) => v,
                None => panic!("some results still missing"),
            })
            .collect()
    }

    fn submit_stage<T: Data, U: Data, F, RT>(
        &self,
        stage: Stage,
        waiting: &mut BTreeSet<Stage>,
        running: &mut BTreeSet<Stage>,
        finished: &mut Vec<bool>,
        pending_tasks: &mut BTreeMap<Stage, BTreeSet<Box<dyn TaskBase>>>,
        output_parts: Vec<usize>,
        num_output_parts: usize,
        final_stage: Stage,
        func: Arc<F>,
        final_rdd: Arc<RT>,
        run_id: usize,
        thread_pool: Arc<ThreadPool>,
    ) where
        F: SerFunc((TasKContext, Box<dyn Iterator<Item = T>>)) -> U,
        RT: Rdd<T> + 'static,
    {
        info!("submiting stage {}", stage.id);
        if !waiting.contains(&stage) && !running.contains(&stage) {
            let missing = self.get_missing_parent_stages(stage.clone());
            info!(
                "inside submit stage missing stages {:?}",
                missing.iter().map(|x| x.id).collect::<Vec<_>>()
            );
            if missing.is_empty() {
                self.submit_missing_tasks(
                    stage.clone(),
                    finished,
                    pending_tasks,
                    output_parts.clone(),
                    num_output_parts,
                    final_stage.clone(),
                    func,
                    final_rdd.clone(),
                    run_id,
                    thread_pool.clone(),
                );
                running.insert(stage.clone());
            } else {
                for parent in missing {
                    self.submit_stage(
                        parent,
                        waiting,
                        running,
                        finished,
                        pending_tasks,
                        output_parts.clone(),
                        num_output_parts,
                        final_stage.clone(),
                        func.clone(),
                        final_rdd.clone(),
                        run_id,
                        thread_pool.clone(),
                    );
                }
                waiting.insert(stage.clone());
            }
        }
    }

    fn submit_missing_tasks<T: Data, U: Data, F, RT>(
        &self,
        stage: Stage,
        finished: &mut Vec<bool>,
        pending_tasks: &mut BTreeMap<Stage, BTreeSet<Box<dyn TaskBase>>>,
        output_parts: Vec<usize>,
        num_output_parts: usize,
        final_stage: Stage,
        func: Arc<F>,
        final_rdd: Arc<RT>,
        run_id: usize,
        thread_pool: Arc<ThreadPool>,
    ) where
        F: SerFunc((TasKContext, Box<dyn Iterator<Item = T>>)) -> U,
        RT: Rdd<T> + 'static,
    {
        let my_pending = pending_tasks
            .entry(stage.clone())
            .or_insert_with(BTreeSet::new);
        if stage == final_stage {
            info!("final stage {}", stage.id);
            let mut id_in_job = 0;
            for (id, part) in output_parts.iter().enumerate().take(num_output_parts) {
                let locs = self.get_preferred_locs(final_rdd.clone() as Arc<dyn RddBase>, *part);
                let result_task = ResultTask::new(
                    self.next_task_id.fetch_add(1, Ordering::SeqCst),
                    run_id,
                    final_stage.id,
                    final_rdd.clone(),
                    func.clone(),
                    *part,
                    locs,
                    id,
                );
                my_pending.insert(Box::new(result_task.clone()));
                self.submit_task::<T, U, RT, F>(
                    TaskOption::ResultTask(Box::new(result_task)),
                    id_in_job,
                    thread_pool.clone(),
                );
                id_in_job += 1;
            }
        } else {
            let mut id_in_job = 0;
            for p in 0..stage.num_partitions {
                info!("shuffle_stage {}", stage.id);
                if stage.output_locs[p].is_empty() {
                    let locs = self.get_preferred_locs(stage.get_rdd(), p);
                    info!("creating task for {} partition  {}", stage.id, p);
                    let shuffle_map_task = ShuffleMapTask::new(
                        self.next_task_id.fetch_add(1, Ordering::SeqCst),
                        run_id,
                        stage.id,
                        stage.rdd.clone(),
                        stage.shuffle_dependency.clone().unwrap(),
                        p,
                        locs,
                    );
                    info!(
                        "creating task for {} partition  {} and shuffle id {}",
                        stage.id,
                        p,
                        shuffle_map_task.dep.get_shuffle_id()
                    );
                    my_pending.insert(Box::new(shuffle_map_task.clone()));
                    self.submit_task::<T, U, RT, F>(
                        TaskOption::ShuffleMapTask(Box::new(shuffle_map_task)),
                        id_in_job,
                        thread_pool.clone(),
                    );
                    id_in_job += 1;
                }
            }
        }
    }

    fn get_preferred_locs(&self, rdd: Arc<dyn RddBase>, partition: usize) -> Vec<Ipv4Addr> {
        //TODO have to implement this completely

        if let Some(cached) = self.get_cache_locs(rdd.clone()) {
            if let Some(cached) = cached.get(partition) {
                return cached.clone();
            }
        }
        let rdd_prefs = rdd.preferred_locations(rdd.splits()[partition].clone());
        if !rdd_prefs.is_empty() {
            return rdd_prefs;
        }
        for dep in rdd.get_dependencies().iter() {
            if let Dependency::NarrowDependency(nar_dep) = dep {
                for in_part in nar_dep.get_parents(partition) {
                    let locs = self.get_preferred_locs(nar_dep.get_rdd_base(), in_part);
                    if !locs.is_empty() {
                        return locs;
                    }
                }
            }
        }
        Vec::new()
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

    fn submit_task<T: Data, U: Data, RT, F>(
        &self,
        task: TaskOption,
        id_in_job: usize,
        thread_pool: Arc<ThreadPool>,
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

    fn run_task<T: Data, U: Data, RT, F>(
        event_queues: Arc<Mutex<HashMap<usize, VecDeque<CompletionEvent>>>>,
        //        task: Box<Task<U>>,
        task: Vec<u8>,
        id_in_job: usize,
        attempt_id: usize,
    ) where
        F: SerFunc((TasKContext, Box<dyn Iterator<Item = T>>)) -> U,
        RT: Rdd<T> + 'static,
    {
        let des_task: TaskOption = bincode::deserialize(&task).unwrap();
        let result = des_task.run(attempt_id);
        //        println!("result {:?}", result);
        match des_task {
            TaskOption::ResultTask(tsk) => {
                let result = match result {
                    TaskResult::ResultTask(r) => r,
                    _ => panic!("wrong result type"),
                };
                if let Ok(task_final) = tsk.downcast::<ResultTask<T, U, RT, F>>() {
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
}

//TODO Serialize and Deserialize
