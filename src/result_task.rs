use std::fmt::Display;
use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::sync::Arc;

use crate::env;
use crate::rdd::Rdd;
use crate::serializable_traits::Data;
use crate::task::{Task, TaskBase, TaskContext};
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Any as SerAny, Box as SerBox, Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(crate) struct ResultTask<T: Data, U: Data, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U
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
    pinned: bool,
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
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U
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
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U
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
            pinned: self.rdd.is_pinned(),
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
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U
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
            pinned: rdd.is_pinned(),
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
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U
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

    fn is_pinned(&self) -> bool {
        self.pinned
    }

    fn preferred_locations(&self) -> Vec<Ipv4Addr> {
        self.locs.clone()
    }

    fn generation(&self) -> Option<i64> {
        Some(env::Env::get().map_output_tracker.get_generation())
    }
}

#[async_trait::async_trait]
impl<T: Data, U: Data, F> Task for ResultTask<T, U, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        + Serialize
        + Deserialize
        + Clone,
{
    async fn run(&self, id: usize) -> SerBox<dyn SerAny + Send + Sync> {
        let split = self.rdd.splits()[self.partition].clone();
        let context = TaskContext::new(self.stage_id, self.partition, id);
        let result: Vec<T> = {
            let iterator = self.rdd.iterator(split).await.unwrap();
            let mut l = iterator.lock();
            l.into_iter().collect()
        };
        SerBox::new((self.func)((context, Box::new(result.into_iter()))))
    }
}
