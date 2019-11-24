use super::*;

use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;
use std::sync::Arc;

pub trait Scheduler {
    fn start(&self);
    fn wait_for_register(&self);
    fn run_job<T: Data, U: Data, RT, F>(
        &self,
        rdd: &RT,
        func: F,
        partitions: Vec<i64>,
        allow_local: bool,
    ) -> Vec<U>
    where
        Self: Sized,
        RT: Rdd<Item = T>,
        F: Fn(Box<dyn Iterator<Item = T>>) -> U;
    fn stop(&self);
    fn default_parallelism(&self) -> i64;
}

/// Functionality by the library built-in schedulers
pub(crate) trait SharedScheduler {
    /// Fast path for execution. Runs the DD in the driver main thread if possible.
    fn local_execution<T: Data, U: Data, F, RT>(
        jt: JobTracker<F, RT, U, T>,
    ) -> Result<Option<Vec<U>>>
    where
        F: SerFunc((TasKContext, Box<dyn Iterator<Item = T>>)) -> U,
        RT: Rdd<T> + 'static,
    {
        if jt.final_stage.parents.is_empty() && (jt.num_output_parts == 1) {
            let split = (jt.final_rdd.splits()[jt.output_parts[0]]).clone();
            let task_context = TasKContext::new(jt.final_stage.id, jt.output_parts[0], 0);
            Ok(Some(vec![(&jt.func)((
                task_context,
                jt.final_rdd.iterator(split)?,
            ))]))
        } else {
            Ok(None)
        }
    }

    fn new_stage(
        &mut self,
        rdd_base: Arc<dyn RddBase>,
        shuffle_dependency: Option<Arc<dyn ShuffleDependencyTrait>>,
    ) -> Stage {
        info!("inside new stage");
        env::env
            .cache_tracker
            .register_rdd(rdd_base.get_rdd_id(), rdd_base.number_of_splits());
        if shuffle_dependency.is_some() {
            info!("shuffle dependency and registering mapoutput tracker");
            self.register_shuffle(
                shuffle_dependency.clone().unwrap().get_shuffle_id(),
                rdd_base.number_of_splits(),
            );
            info!("new stage tracker after");
        }
        let id = self.get_next_stage_id();
        info!("new stage id {}", id);
        let stage = Stage::new(
            id,
            rdd_base.clone(),
            shuffle_dependency,
            self.get_parent_stages(rdd_base),
        );
        self.insert_into_stage_cache(id, stage.clone());
        info!("new stage stage return");
        stage
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

    fn on_event_failure<T: Data, U: Data, F, RT>(
        &self,
        jt: JobTracker<F, RT, U, T>,
        failed_vals: FetchFailedVals,
        stage_id: usize,
    ) where
        F: SerFunc((TasKContext, Box<dyn Iterator<Item = T>>)) -> U,
        RT: Rdd<T> + 'static,
    {
        let FetchFailedVals {
            server_uri,
            shuffle_id,
            map_id,
            reduce_id,
        } = failed_vals;

        //TODO mapoutput tracker needs to be finished for this
        let failed_stage = self.id_to_stage.lock().get(&stage_id).unwrap().clone();
        jt.running.borrow_mut().remove(&failed_stage);
        jt.failed.borrow_mut().insert(failed_stage);
        //TODO logging
        self.shuffle_to_map_stage
            .lock()
            .get_mut(&shuffle_id)
            .unwrap()
            .remove_output_loc(map_id, server_uri.clone());
        self.map_output_tracker
            .unregister_map_output(shuffle_id, map_id, server_uri.clone());
        //logging
        jt.failed.borrow_mut().insert(
            self.shuffle_to_map_stage
                .lock()
                .get(&shuffle_id)
                .unwrap()
                .clone(),
        );
    }

    fn on_event_success<T: Data, U: Data, F, RT>(
        &self,
        mut completed_event: CompletionEvent,
        results: &mut Vec<Option<U>>,
        num_finished: &mut usize,
        jt: JobTracker<F, RT, U, T>,
    ) where
        F: SerFunc((TasKContext, Box<dyn Iterator<Item = T>>)) -> U,
        RT: Rdd<T> + 'static,
    {
        //TODO logging
        //TODO add to Accumulator

        let mut result_type = completed_event
            .task
            .downcast_ref::<ResultTask<T, U, RT, F>>()
            .is_some();
        if result_type {
            if let Ok(rt) = completed_event.task.downcast::<ResultTask<T, U, RT, F>>() {
                let result = completed_event
                    .result
                    .take()
                    .unwrap()
                    .downcast_ref::<U>()
                    .unwrap()
                    .clone();

                results[rt.output_id] = Some(result);
                jt.finished.borrow_mut()[rt.output_id] = true;
                *num_finished += 1;
            }
        } else if let Ok(smt) = completed_event.task.downcast::<ShuffleMapTask>() {
            let result = completed_event
                .result
                .take()
                .unwrap()
                .downcast_ref::<String>()
                .unwrap()
                .clone();

            info!("result inside queue {:?}", result);
            self.id_to_stage
                .lock()
                .get_mut(&smt.stage_id)
                .unwrap()
                .add_output_loc(smt.partition, result);
            let stage = self.id_to_stage.lock().clone()[&smt.stage_id].clone();
            info!(
                "pending stages {:?}",
                jt.pending_tasks
                    .borrow()
                    .iter()
                    .map(|(x, y)| (x.id, y.iter().map(|k| k.get_task_id()).collect::<Vec<_>>()))
                    .collect::<Vec<_>>()
            );
            info!(
                "pending tasks {:?}",
                jt.pending_tasks
                    .borrow()
                    .get(&stage)
                    .unwrap()
                    .iter()
                    .map(|x| x.get_task_id())
                    .collect::<Vec<_>>()
            );
            info!(
                "running {:?}",
                jt.running.borrow().iter().map(|x| x.id).collect::<Vec<_>>()
            );
            info!(
                "waiting {:?}",
                jt.waiting.borrow().iter().map(|x| x.id).collect::<Vec<_>>()
            );

            if jt.running.borrow().contains(&stage)
                && jt.pending_tasks.borrow().get(&stage).unwrap().is_empty()
            {
                info!("here before registering map outputs ");
                //TODO logging
                jt.running.borrow_mut().remove(&stage);
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
                for stage in jt.waiting.borrow().iter() {
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
                    jt.waiting.borrow_mut().remove(stage);
                }
                for stage in &newly_runnable {
                    jt.running.borrow_mut().insert(stage.clone());
                }
                for stage in newly_runnable {
                    self.submit_missing_tasks(stage, jt.clone());
                }
            }
        }
    }

    fn submit_stage<T: Data, U: Data, F, RT>(&self, stage: Stage, jt: JobTracker<F, RT, U, T>)
    where
        F: SerFunc((TasKContext, Box<dyn Iterator<Item = T>>)) -> U,
        RT: Rdd<T> + 'static,
    {
        info!("submiting stage {}", stage.id);
        if !jt.waiting.borrow().contains(&stage) && !jt.running.borrow().contains(&stage) {
            let missing = self.get_missing_parent_stages(stage.clone());
            info!(
                "inside submit stage missing stages {:?}",
                missing.iter().map(|x| x.id).collect::<Vec<_>>()
            );
            if missing.is_empty() {
                self.submit_missing_tasks(stage.clone(), jt.clone());
                jt.running.borrow_mut().insert(stage.clone());
            } else {
                for parent in missing {
                    self.submit_stage(parent, jt.clone());
                }
                jt.waiting.borrow_mut().insert(stage.clone());
            }
        }
    }

    fn submit_missing_tasks<T: Data, U: Data, F, RT>(
        &self,
        stage: Stage,
        mut jt: JobTracker<F, RT, U, T>,
    ) where
        F: SerFunc((TasKContext, Box<dyn Iterator<Item = T>>)) -> U,
        RT: Rdd<T> + 'static,
    {
        let mut pending_tasks = jt.pending_tasks.borrow_mut();
        let my_pending = pending_tasks
            .entry(stage.clone())
            .or_insert_with(BTreeSet::new);
        if stage == jt.final_stage {
            info!("final stage {}", stage.id);
            let mut id_in_job = 0;
            for (id, part) in jt.output_parts.iter().enumerate().take(jt.num_output_parts) {
                let locs = self.get_preferred_locs(jt.final_rdd.clone() as Arc<dyn RddBase>, *part);
                let result_task = ResultTask::new(
                    self.get_next_task_id(),
                    jt.run_id,
                    jt.final_stage.id,
                    jt.final_rdd.clone(),
                    jt.func.clone(),
                    *part,
                    locs,
                    id,
                );
                my_pending.insert(Box::new(result_task.clone()));
                self.submit_task::<T, U, RT, F>(
                    TaskOption::ResultTask(Box::new(result_task)),
                    id_in_job,
                    jt.thread_pool.clone(),
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
                        self.get_next_task_id(),
                        jt.run_id,
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
                        jt.thread_pool.clone(),
                    );
                    id_in_job += 1;
                }
            }
        }
    }

    fn submit_task<T: Data, U: Data, RT, F>(
        &self,
        task: TaskOption,
        id_in_job: usize,
        thread_pool: Rc<ThreadPool>,
    ) where
        F: SerFunc((TasKContext, Box<dyn Iterator<Item = T>>)) -> U,
        RT: Rdd<T> + 'static;

    // setters:
    fn insert_into_stage_cache(&mut self, id: usize, stage: Stage);
    fn register_shuffle(&mut self, shuffle_id: usize, num_maps: usize);
    /// refreshes cache locations
    fn update_cache_locs(&self);

    // getters:
    fn get_cache_locs(&self, rdd: Arc<dyn RddBase>) -> Option<Vec<Vec<Ipv4Addr>>>;
    fn get_missing_parent_stages(&self, stage: Stage) -> Vec<Stage>;
    fn get_next_job_id(&self) -> usize;
    fn get_next_stage_id(&self) -> usize;
    fn get_next_task_id(&self) -> usize;

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

    fn get_shuffle_map_stage(&self, shuf: Arc<dyn ShuffleDependencyTrait>) -> Stage;

    fn num_threads(&self) -> usize {
        num_cpus::get()
    }
}
