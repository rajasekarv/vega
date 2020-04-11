use std::fmt::Display;
use std::net::Ipv4Addr;
use std::sync::Arc;

use crate::dependency::ShuffleDependencyTrait;
use crate::env;
use crate::rdd::RddBase;
use crate::task::{Task, TaskBase};
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Any as SerAny, Box as SerBox};

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct ShuffleMapTask {
    pub task_id: usize,
    pub run_id: usize,
    pub stage_id: usize,
    #[serde(with = "serde_traitobject")]
    pub rdd: Arc<dyn RddBase>,
    pinned: bool,
    #[serde(with = "serde_traitobject")]
    pub dep: Arc<dyn ShuffleDependencyTrait>,
    pub partition: usize,
    pub locs: Vec<Ipv4Addr>,
}

impl ShuffleMapTask {
    pub fn new(
        task_id: usize,
        run_id: usize,
        stage_id: usize,
        rdd: Arc<dyn RddBase>,
        dep: Arc<dyn ShuffleDependencyTrait>,
        partition: usize,
        locs: Vec<Ipv4Addr>,
    ) -> Self {
        ShuffleMapTask {
            task_id,
            run_id,
            stage_id,
            pinned: rdd.is_pinned(),
            rdd,
            dep,
            partition,
            locs,
        }
    }
}

impl Display for ShuffleMapTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ShuffleMapTask({:?}, {:?})",
            self.stage_id, self.partition
        )
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
impl Task for ShuffleMapTask {
    async fn run(&self, _id: usize) -> SerBox<dyn SerAny + Send + Sync> {
        SerBox::new(
            self.dep
                .do_shuffle_task(self.rdd.clone(), self.partition)
                .await,
        )
    }
}
