use super::*;
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct ResultTask<T: Data, U: Data, RT, F>
where
    RT: Rdd<T> + 'static,
    F: Fn((TasKContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        + PartialEq
        + Eq
        + Serialize
        + Deserialize
        + Clone,
{
    pub task_id: usize,
    pub run_id: usize,
    pub stage_id: usize,
    #[serde(with = "serde_traitobject")]
    pub rdd: Arc<RT>,
    pub func: Arc<F>,
    pub partition: usize,
    pub locs: Vec<String>,
    pub output_id: usize,
    _marker: PhantomData<T>,
}

impl<T: Data, U: Data, RT, F> ResultTask<T, U, RT, F>
where
    RT: Rdd<T> + 'static,
    F: Fn((TasKContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        + PartialEq
        + Eq
        + Serialize
        + Deserialize
        + Clone,
{
    pub fn new(
        task_id: usize,
        run_id: usize,
        stage_id: usize,
        rdd: Arc<RT>,
        func: Arc<F>,
        partition: usize,
        locs: Vec<String>,
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

    fn to_string(&self) -> String {
        format!("ResultTask({}, {})", self.stage_id, self.partition)
    }
}

impl<T: Data, U: Data, RT, F> TaskBase for ResultTask<T, U, RT, F>
where
    RT: Rdd<T> + 'static,
    F: Fn((TasKContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        + PartialEq
        + Eq
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
    fn preferred_locations(&self) -> Vec<String> {
        self.locs.clone()
    }
    fn generation(&self) -> Option<i64> {
        let base = self.rdd.get_rdd_base();
        let context = base.get_context();
        Some(env::env.map_output_tracker.get_generation())
    }
}

impl<T: Data, U: Data, RT, F> Task for ResultTask<T, U, RT, F>
where
    RT: Rdd<T> + 'static,
    F: Fn((TasKContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        + PartialEq
        + Eq
        + Serialize
        + Deserialize
        + Clone,
{
    fn run(&self, id: usize) -> serde_traitobject::Box<dyn serde_traitobject::Any + Send + Sync> {
        let split = self.rdd.splits()[self.partition].clone();
        let context = TasKContext::new(self.stage_id, self.partition, id);
        serde_traitobject::Box::new((self.func)((context, self.rdd.iterator(split))))
            as serde_traitobject::Box<dyn serde_traitobject::Any + Send + Sync>
    }
}
