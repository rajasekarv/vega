use super::*;
use std::fmt::{Display, Formatter, Result};
use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::sync::Arc;
#[derive(Serialize, Deserialize)]
pub struct ResultTask<T: Data, U: Data, F>
where
    F: Fn((TasKContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        + Serialize
        + Deserialize
        + Clone,
{
    pub task_id: usize,
    pub run_id: usize,
    pub stage_id: usize,
    #[serde(with = "serde_traitobject")]
    pub rdd: Arc<dyn Rdd<Item = T>>,
    pub func: Arc<F>,
    pub partition: usize,
    pub locs: Vec<Ipv4Addr>,
    pub output_id: usize,
    _marker: PhantomData<T>,
}

impl<T: Data, U: Data, F> Display for ResultTask<T, U, F>
where
    F: Fn((TasKContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        + Serialize
        + Deserialize
        + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ResultTask({}, {})", self.stage_id, self.partition)
    }
}

impl<T: Data, U: Data, F> ResultTask<T, U, F>
where
    F: Fn((TasKContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        + Serialize
        + Deserialize
        + Clone,
{
    pub fn clone(&self) -> Self {
        ResultTask {
            task_id: self.task_id,
            run_id: self.run_id,
            stage_id: self.stage_id,
            rdd: self.rdd.clone(),
            func: self.func.clone(),
            partition: self.partition,
            locs: self.locs.clone(),
            output_id: self.output_id,
            _marker: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> ResultTask<T, U, F>
where
    F: Fn((TasKContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        + Serialize
        + Deserialize
        + Clone,
{
    pub fn new(
        task_id: usize,
        run_id: usize,
        stage_id: usize,
        rdd: Arc<dyn Rdd<Item = T>>,
        func: Arc<F>,
        partition: usize,
        locs: Vec<Ipv4Addr>,
        output_id: usize,
    ) -> Self {
        ResultTask {
            task_id,
            run_id,
            stage_id,
            rdd,
            func,
            partition,
            locs,
            output_id,
            _marker: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> TaskBase for ResultTask<T, U, F>
where
    F: Fn((TasKContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        + Serialize
        + Deserialize
        + Clone,
{
    fn get_run_id(&self) -> usize {
        self.run_id
    }

    fn get_stage_id(&self) -> usize {
        self.stage_id
    }

    fn get_task_id(&self) -> usize {
        self.task_id
    }
    fn preferred_locations(&self) -> Vec<Ipv4Addr> {
        self.locs.clone()
    }
    fn generation(&self) -> Option<i64> {
        let base = self.rdd.get_rdd_base();
        let context = base.get_context();
        Some(env::Env::get().map_output_tracker.get_generation())
    }
}

impl<T: Data, U: Data, F> Task for ResultTask<T, U, F>
where
    F: Fn((TasKContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        + Serialize
        + Deserialize
        + Clone,
{
    fn run(&self, id: usize) -> serde_traitobject::Box<dyn serde_traitobject::Any + Send + Sync> {
        let split = self.rdd.splits()[self.partition].clone();
        let context = TasKContext::new(self.stage_id, self.partition, id);
        serde_traitobject::Box::new((self.func)((context, self.rdd.iterator(split).unwrap())))
            as serde_traitobject::Box<dyn serde_traitobject::Any + Send + Sync>
    }
}
