use std::collections::{BTreeSet, VecDeque};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::dependency::{Dependency, ShuffleDependencyTrait};
use crate::env;
use crate::error::{Error, Result};
use crate::rdd::RddBase;
use crate::scheduler::{
    CompletionEvent, FetchFailedVals, JobListener, JobTracker, ResultTask, Stage, TaskBase,
    TaskContext, TaskOption,
};
use crate::serializable_traits::{Data, SerFunc};
use crate::shuffle::ShuffleMapTask;
use dashmap::DashMap;

pub(crate) type EventQueue = Arc<DashMap<usize, VecDeque<CompletionEvent>>>;

/// Functionality of the library built-in schedulers
#[async_trait::async_trait]
pub(crate) trait NativeScheduler: Send + Sync {
    /// Fast path for execution. Runs the DD in the driver main thread if possible.
    fn local_execution<T: Data, U: Data, F, L>(
        jt: Arc<JobTracker<F, U, T, L>>,
    ) -> Result<Option<Vec<U>>>
    where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
        L: JobListener,
    {
        if jt.final_stage.parents.is_empty() && (jt.num_output_parts == 1) {
            let split = (jt.final_rdd.splits()[jt.output_parts[0]]).clone();
            let task_context = TaskContext::new(jt.final_stage.id, jt.output_parts[0], 0);
            Ok(Some(vec![(&jt.func)((
                task_context,
                jt.final_rdd.iterator(split)?,
            ))]))
        } else {
            Ok(None)
        }
    }

    async fn new_stage(
        &self,
        rdd_base: Arc<dyn RddBase>,
        shuffle_dependency: Option<Arc<dyn ShuffleDependencyTrait>>,
    ) -> Result<Stage> {
        log::debug!("creating new stage");
        env::Env::get()
            .cache_tracker
            .register_rdd(rdd_base.get_rdd_id(), rdd_base.number_of_splits())
            .await?;
        if let Some(dep) = shuffle_dependency.clone() {
            log::debug!("shuffle dependency exists, registering to map output tracker");
            self.register_shuffle(dep.get_shuffle_id(), rdd_base.number_of_splits());
            log::debug!("new stage tracker after");
        }
        let id = self.get_next_stage_id();
        log::debug!("new stage id #{}", id);
        let stage = Stage::new(
            id,
            rdd_base.clone(),
            shuffle_dependency,
            self.get_parent_stages(rdd_base).await?,
        );
        self.insert_into_stage_cache(id, stage.clone());
        log::debug!("returning new stage #{}", id);
        Ok(stage)
    }

    async fn visit_for_missing_parent_stages<'s, 'a: 's>(
        &'s self,
        missing: &'a mut BTreeSet<Stage>,
        visited: &'a mut BTreeSet<Arc<dyn RddBase>>,
        rdd: Arc<dyn RddBase>,
    ) -> Result<()> {
        log::debug!(
            "missing stages: {:?}",
            missing.iter().map(|x| x.id).collect::<Vec<_>>()
        );
        log::debug!(
            "visited stages: {:?}",
            visited.iter().map(|x| x.get_rdd_id()).collect::<Vec<_>>()
        );
        if !visited.contains(&rdd) {
            visited.insert(rdd.clone());
            // TODO: CacheTracker register
            for _ in 0..rdd.number_of_splits() {
                let locs = self.get_cache_locs(rdd.clone());
                log::debug!("cache locs: {:?}", locs);
                if locs == None {
                    for dep in rdd.get_dependencies() {
                        log::debug!("for dep in missing stages ");
                        match dep {
                            Dependency::ShuffleDependency(shuf_dep) => {
                                let stage = self.get_shuffle_map_stage(shuf_dep.clone()).await?;
                                log::debug!("shuffle stage #{} in missing stages", stage.id);
                                if !stage.is_available() {
                                    log::debug!(
                                        "inserting shuffle stage #{} in missing stages",
                                        stage.id
                                    );
                                    missing.insert(stage);
                                }
                            }
                            Dependency::NarrowDependency(nar_dep) => {
                                log::debug!("narrow stage in missing stages");
                                self.visit_for_missing_parent_stages(
                                    missing,
                                    visited,
                                    nar_dep.get_rdd_base(),
                                )
                                .await?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn visit_for_parent_stages<'s, 'a: 's>(
        &'s self,
        parents: &'a mut BTreeSet<Stage>,
        visited: &'a mut BTreeSet<Arc<dyn RddBase>>,
        rdd: Arc<dyn RddBase>,
    ) -> Result<()> {
        log::debug!(
            "parent stages: {:?}",
            parents.iter().map(|x| x.id).collect::<Vec<_>>()
        );
        log::debug!(
            "visited stages: {:?}",
            visited.iter().map(|x| x.get_rdd_id()).collect::<Vec<_>>()
        );
        if !visited.contains(&rdd) {
            visited.insert(rdd.clone());
            env::Env::get()
                .cache_tracker
                .register_rdd(rdd.get_rdd_id(), rdd.number_of_splits())
                .await?;
            for dep in rdd.get_dependencies() {
                match dep {
                    Dependency::ShuffleDependency(shuf_dep) => {
                        parents.insert(self.get_shuffle_map_stage(shuf_dep.clone()).await?);
                    }
                    Dependency::NarrowDependency(nar_dep) => {
                        self.visit_for_parent_stages(parents, visited, nar_dep.get_rdd_base())
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn get_parent_stages(&self, rdd: Arc<dyn RddBase>) -> Result<Vec<Stage>> {
        log::debug!("inside get parent stages");
        let mut parents: BTreeSet<Stage> = BTreeSet::new();
        let mut visited: BTreeSet<Arc<dyn RddBase>> = BTreeSet::new();
        self.visit_for_parent_stages(&mut parents, &mut visited, rdd.clone())
            .await?;
        log::debug!(
            "parent stages: {:?}",
            parents.iter().map(|x| x.id).collect::<Vec<_>>()
        );
        Ok(parents.into_iter().collect())
    }

    async fn on_event_failure<T: Data, U: Data, F, L>(
        &self,
        jt: Arc<JobTracker<F, U, T, L>>,
        failed_vals: FetchFailedVals,
        stage_id: usize,
    ) where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
        L: JobListener,
    {
        let FetchFailedVals {
            server_uri,
            shuffle_id,
            map_id,
            ..
        } = failed_vals;

        // TODO: mapoutput tracker needs to be finished for this
        // let failed_stage = self.id_to_stage.lock().get(&stage_id).?.clone();
        let failed_stage = self.fetch_from_stage_cache(stage_id);
        jt.running.lock().await.remove(&failed_stage);
        jt.failed.lock().await.insert(failed_stage);
        // TODO: logging
        self.remove_output_loc_from_stage(shuffle_id, map_id, &server_uri);
        self.unregister_map_output(shuffle_id, map_id, server_uri);
        jt.failed
            .lock()
            .await
            .insert(self.fetch_from_shuffle_to_cache(shuffle_id));
    }

    async fn on_event_success<T: Data, U: Data, F, L>(
        &self,
        mut completed_event: CompletionEvent,
        results: &mut Vec<Option<U>>,
        num_finished: &mut usize,
        jt: Arc<JobTracker<F, U, T, L>>,
    ) -> Result<()>
    where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
        L: JobListener,
    {
        // TODO: logging
        // TODO: add to Accumulator

        let result_type = completed_event
            .task
            .downcast_ref::<ResultTask<T, U, F>>()
            .is_some();
        if result_type {
            if let Ok(rt) = completed_event.task.downcast::<ResultTask<T, U, F>>() {
                let any_result = completed_event.result.take().ok_or_else(|| Error::Other)?;
                jt.listener
                    .task_succeeded(rt.output_id, &*any_result)
                    .await?;
                let result = any_result
                    .as_any()
                    .downcast_ref::<U>()
                    .ok_or_else(|| {
                        Error::DowncastFailure("generic type U in scheduler on success")
                    })?
                    .clone();
                results[rt.output_id] = Some(result);
                jt.finished.lock().await[rt.output_id] = true;
                *num_finished += 1;
            }
        } else if let Ok(smt) = completed_event.task.downcast::<ShuffleMapTask>() {
            let shuffle_server_uri = completed_event
                .result
                .take()
                .ok_or_else(|| Error::Other)?
                .as_any()
                .downcast_ref::<String>()
                .ok_or_else(|| crate::Error::DowncastFailure("String"))?
                .clone();
            log::debug!(
                "completed shuffle task server uri: {:?}",
                shuffle_server_uri
            );
            self.add_output_loc_to_stage(smt.stage_id, smt.partition, shuffle_server_uri);

            let stage = self.fetch_from_stage_cache(smt.stage_id);
            log::debug!(
                "pending stages: {:?}",
                jt.pending_tasks
                    .lock()
                    .await
                    .iter()
                    .map(|(x, y)| (x.id, y.iter().map(|k| k.get_task_id()).collect::<Vec<_>>()))
                    .collect::<Vec<_>>()
            );
            log::debug!(
                "pending tasks: {:?}",
                jt.pending_tasks
                    .lock()
                    .await
                    .get(&stage)
                    .ok_or_else(|| Error::Other)?
                    .iter()
                    .map(|x| x.get_task_id())
                    .collect::<Vec<_>>()
            );
            log::debug!(
                "running stages: {:?}",
                jt.running
                    .lock()
                    .await
                    .iter()
                    .map(|x| x.id)
                    .collect::<Vec<_>>()
            );
            log::debug!(
                "waiting stages: {:?}",
                jt.waiting
                    .lock()
                    .await
                    .iter()
                    .map(|x| x.id)
                    .collect::<Vec<_>>()
            );

            if jt.running.lock().await.contains(&stage)
                && jt
                    .pending_tasks
                    .lock()
                    .await
                    .get(&stage)
                    .ok_or_else(|| Error::Other)?
                    .is_empty()
            {
                log::debug!("started registering map outputs");
                // TODO: logging
                jt.running.lock().await.remove(&stage);
                if let Some(dep) = stage.shuffle_dependency {
                    log::debug!(
                        "stage output locs before register mapoutput tracker: {:?}",
                        stage.output_locs
                    );
                    let locs = stage
                        .output_locs
                        .iter()
                        .map(|x| x.get(0).map(|s| s.to_owned()))
                        .collect();
                    log::debug!("locs for shuffle id #{}: {:?}", dep.get_shuffle_id(), locs);
                    self.register_map_outputs(dep.get_shuffle_id(), locs);
                    log::debug!("finished registering map outputs");
                }
                // TODO: Cache
                self.update_cache_locs().await?;
                let mut newly_runnable = Vec::new();
                let waiting_stages: Vec<_> = jt.waiting.lock().await.iter().cloned().collect();
                for stage in waiting_stages {
                    let missing_stages = self.get_missing_parent_stages(stage.clone()).await?;
                    log::debug!(
                        "waiting stage parent stages for stage #{} are: {:?}",
                        stage.id,
                        missing_stages.iter().map(|x| x.id).collect::<Vec<_>>()
                    );
                    if missing_stages.iter().next().is_none() {
                        newly_runnable.push(stage.clone())
                    }
                }
                for stage in &newly_runnable {
                    jt.waiting.lock().await.remove(stage);
                }
                for stage in &newly_runnable {
                    jt.running.lock().await.insert(stage.clone());
                }
                for stage in newly_runnable {
                    self.submit_missing_tasks(stage, jt.clone()).await?;
                }
            }
        }
        Ok(())
    }

    async fn submit_stage<T: Data, U: Data, F, L>(
        &self,
        stage: Stage,
        jt: Arc<JobTracker<F, U, T, L>>,
    ) -> Result<()>
    where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
        L: JobListener,
    {
        log::debug!("submitting stage #{}", stage.id);
        if !jt.waiting.lock().await.contains(&stage) && !jt.running.lock().await.contains(&stage) {
            let missing = self.get_missing_parent_stages(stage.clone()).await?;
            log::debug!(
                "while submitting stage #{}, missing stages: {:?}",
                stage.id,
                missing.iter().map(|x| x.id).collect::<Vec<_>>()
            );
            if missing.is_empty() {
                self.submit_missing_tasks(stage.clone(), jt.clone()).await?;
                jt.running.lock().await.insert(stage);
            } else {
                for parent in missing {
                    self.submit_stage(parent, jt.clone()).await?;
                }
                jt.waiting.lock().await.insert(stage);
            }
        }
        Ok(())
    }

    async fn submit_missing_tasks<T: Data, U: Data, F, L>(
        &self,
        stage: Stage,
        jt: Arc<JobTracker<F, U, T, L>>,
    ) -> Result<()>
    where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
        L: JobListener,
    {
        let mut pending_tasks = jt.pending_tasks.lock().await;
        let my_pending = pending_tasks
            .entry(stage.clone())
            .or_insert_with(BTreeSet::new);
        if stage == jt.final_stage {
            log::debug!("final stage #{}", stage.id);
            for (id_in_job, (id, part)) in jt
                .output_parts
                .iter()
                .enumerate()
                .take(jt.num_output_parts)
                .enumerate()
            {
                let locs = self.get_preferred_locs(jt.final_rdd.get_rdd_base(), *part);
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
                let task = Box::new(result_task.clone()) as Box<dyn TaskBase>;
                let executor = self.next_executor_server(&*task);
                my_pending.insert(task);
                self.submit_task::<T, U, F>(
                    TaskOption::ResultTask(Box::new(result_task)),
                    id_in_job,
                    executor,
                )
            }
        } else {
            for p in 0..stage.num_partitions {
                log::debug!("shuffle stage #{}", stage.id);
                if stage.output_locs[p].is_empty() {
                    let locs = self.get_preferred_locs(stage.get_rdd(), p);
                    log::debug!("creating task for stage #{} partition #{}", stage.id, p);
                    let shuffle_map_task = ShuffleMapTask::new(
                        self.get_next_task_id(),
                        jt.run_id,
                        stage.id,
                        stage.rdd.clone(),
                        stage
                            .shuffle_dependency
                            .clone()
                            .ok_or_else(|| Error::Other)?,
                        p,
                        locs,
                    );
                    log::debug!(
                        "creating task for stage #{}, partition #{} and shuffle id #{}",
                        stage.id,
                        p,
                        shuffle_map_task.dep.get_shuffle_id()
                    );
                    let task = Box::new(shuffle_map_task.clone()) as Box<dyn TaskBase>;
                    let executor = self.next_executor_server(&*task);
                    my_pending.insert(task);
                    self.submit_task::<T, U, F>(
                        TaskOption::ShuffleMapTask(Box::new(shuffle_map_task)),
                        p,
                        executor,
                    );
                }
            }
        }
        Ok(())
    }

    fn wait_for_event(&self, run_id: usize, timeout: u64) -> Option<CompletionEvent> {
        // TODO: make use of async to wait for events
        let end = Instant::now() + Duration::from_millis(timeout);
        while self.get_event_queue().get(&run_id)?.is_empty() {
            if Instant::now() > end {
                return None;
            } else {
                thread::sleep(end - Instant::now());
            }
        }
        self.get_event_queue().get_mut(&run_id)?.pop_front()
    }

    fn submit_task<T: Data, U: Data, F>(
        &self,
        task: TaskOption,
        id_in_job: usize,
        target_executor: SocketAddrV4,
    ) where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U;

    // mutators:
    fn add_output_loc_to_stage(&self, stage_id: usize, partition: usize, host: String);
    fn insert_into_stage_cache(&self, id: usize, stage: Stage);
    /// refreshes cache locations
    fn register_shuffle(&self, shuffle_id: usize, num_maps: usize);
    fn register_map_outputs(&self, shuffle_id: usize, locs: Vec<Option<String>>);
    fn remove_output_loc_from_stage(&self, shuffle_id: usize, map_id: usize, server_uri: &str);
    async fn update_cache_locs(&self) -> Result<()>;
    fn unregister_map_output(&self, shuffle_id: usize, map_id: usize, server_uri: String);

    // getters:
    fn fetch_from_stage_cache(&self, id: usize) -> Stage;
    fn fetch_from_shuffle_to_cache(&self, id: usize) -> Stage;
    fn get_cache_locs(&self, rdd: Arc<dyn RddBase>) -> Option<Vec<Vec<Ipv4Addr>>>;
    fn get_event_queue(&self) -> &Arc<DashMap<usize, VecDeque<CompletionEvent>>>;
    async fn get_missing_parent_stages<'a>(&'a self, stage: Stage) -> Result<Vec<Stage>>;
    fn get_next_job_id(&self) -> usize;
    fn get_next_stage_id(&self) -> usize;
    fn get_next_task_id(&self) -> usize;
    fn next_executor_server(&self, rdd: &dyn TaskBase) -> SocketAddrV4;

    fn get_preferred_locs(&self, rdd: Arc<dyn RddBase>, partition: usize) -> Vec<Ipv4Addr> {
        // TODO: have to implement this completely
        if let Some(cached) = self.get_cache_locs(rdd.clone()) {
            if let Some(cached) = cached.get(partition) {
                return cached.clone();
            }
        }
        let rdd_prefs = rdd.preferred_locations(rdd.splits()[partition].clone());
        if !rdd.is_pinned() {
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
        } else {
            // when pinned, is required that there is exactly one preferred location
            // for a given partition
            assert!(rdd_prefs.len() == 1);
            rdd_prefs
        }
    }

    async fn get_shuffle_map_stage(&self, shuf: Arc<dyn ShuffleDependencyTrait>) -> Result<Stage>;
}

macro_rules! impl_common_scheduler_funcs {
    () => {
        #[inline]
        fn add_output_loc_to_stage(&self, stage_id: usize, partition: usize, host: String) {
            self.stage_cache
                .get_mut(&stage_id)
                .unwrap()
                .add_output_loc(partition, host);
        }

        #[inline]
        fn insert_into_stage_cache(&self, id: usize, stage: Stage) {
            self.stage_cache.insert(id, stage.clone());
        }

        #[inline]
        fn fetch_from_stage_cache(&self, id: usize) -> Stage {
            self.stage_cache.get(&id).unwrap().clone()
        }

        #[inline]
        fn fetch_from_shuffle_to_cache(&self, id: usize) -> Stage {
            self.shuffle_to_map_stage.get(&id).unwrap().clone()
        }

        #[inline]
        fn unregister_map_output(&self, shuffle_id: usize, map_id: usize, server_uri: String) {
            self.map_output_tracker
                .unregister_map_output(shuffle_id, map_id, server_uri)
        }

        #[inline]
        fn register_shuffle(&self, shuffle_id: usize, num_maps: usize) {
            self.map_output_tracker
                .register_shuffle(shuffle_id, num_maps)
        }

        #[inline]
        fn register_map_outputs(&self, shuffle_id: usize, locs: Vec<Option<String>>) {
            self.map_output_tracker
                .register_map_outputs(shuffle_id, locs)
        }

        #[inline]
        fn remove_output_loc_from_stage(&self, shuffle_id: usize, map_id: usize, server_uri: &str) {
            self.shuffle_to_map_stage
                .get_mut(&shuffle_id)
                .unwrap()
                .remove_output_loc(map_id, server_uri);
        }

        #[inline]
        fn get_cache_locs(&self, rdd: Arc<dyn RddBase>) -> Option<Vec<Vec<Ipv4Addr>>> {
            let locs_opt = self.cache_locs.get(&rdd.get_rdd_id());
            locs_opt.map(|l| l.clone())
        }

        #[inline]
        fn get_event_queue(&self) -> &Arc<DashMap<usize, VecDeque<CompletionEvent>>> {
            &self.event_queues
        }

        #[inline]
        fn get_next_job_id(&self) -> usize {
            self.next_job_id.fetch_add(1, Ordering::SeqCst)
        }

        #[inline]
        fn get_next_stage_id(&self) -> usize {
            self.next_stage_id.fetch_add(1, Ordering::SeqCst)
        }

        #[inline]
        fn get_next_task_id(&self) -> usize {
            self.next_task_id.fetch_add(1, Ordering::SeqCst)
        }
    };
}
