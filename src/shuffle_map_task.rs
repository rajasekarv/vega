use super::*;
//use downcast_rs::Downcast;
//use std::collections::{HashMap, HashSet};
//use std::fs::File;
//use std::hash::Hash;
//use std::io::{BufWriter, Write};
//use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Serialize, Deserialize, Clone)]
pub struct ShuffleMapTask {
    pub task_id: usize,
    pub run_id: usize,
    pub stage_id: usize,
    #[serde(with = "serde_traitobject")]
    pub rdd: Arc<dyn RddBase>,
    #[serde(with = "serde_traitobject")]
    pub dep: Arc<dyn ShuffleDependencyTrait>,
    pub partition: usize,
    pub locs: Vec<String>,
}

impl ShuffleMapTask {
    pub fn new(
        task_id: usize,
        run_id: usize,
        stage_id: usize,
        rdd: Arc<dyn RddBase>,
        dep: Arc<dyn ShuffleDependencyTrait>,
        partition: usize,
        locs: Vec<String>,
    ) -> Self {
        ShuffleMapTask {
            task_id,
            run_id,
            stage_id,
            rdd,
            dep,
            partition,
            locs,
        }
    }

    pub fn to_string(&self) -> String {
        format!("ShuffleMapTask({:?}, {:?}", self.stage_id, self.partition)
    }
}

impl TaskBase for ShuffleMapTask {
    fn get_run_id(&self) -> usize {
        self.run_id
    }

    fn get_stage_id(&self) -> usize {
        self.stage_id
    }

    fn get_task_id(&self) -> usize {
        self.task_id
    }
    fn preferred_locations(&self) -> Vec<String> {
        self.locs.clone()
    }
    fn generation(&self) -> Option<i64> {
        //        let base = self.rdd.get_rdd_base();
        let context = self.rdd.get_context();
        Some(env::env.map_output_tracker.get_generation())
    }
}

impl Task for ShuffleMapTask {
    fn run(&self, id: usize) -> serde_traitobject::Box<dyn serde_traitobject::Any + Send + Sync> {
        serde_traitobject::Box::new(self.dep.do_shuffle_task(self.rdd.clone(), self.partition))
            as serde_traitobject::Box<dyn serde_traitobject::Any + Send + Sync>
    }
}
