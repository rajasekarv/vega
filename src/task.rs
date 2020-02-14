use downcast_rs::Downcast;
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Any as SerAny, Arc as SerArc, Box as SerBox, Deserialize, Serialize};
use std::cmp::Ordering;
use std::net::Ipv4Addr;

pub struct TaskContext {
    pub stage_id: usize,
    pub split_id: usize,
    pub attempt_id: usize,
}

impl TaskContext {
    pub fn new(stage_id: usize, split_id: usize, attempt_id: usize) -> Self {
        TaskContext {
            stage_id,
            split_id,
            attempt_id,
        }
    }
}

pub trait TaskBase: Downcast + Send + Sync {
    fn get_run_id(&self) -> usize;
    fn get_stage_id(&self) -> usize;
    fn get_task_id(&self) -> usize;
    fn is_pinned(&self) -> bool {
        false
    }
    fn preferred_locations(&self) -> Vec<Ipv4Addr> {
        Vec::new()
    }
    fn generation(&self) -> Option<i64> {
        None
    }
}

impl_downcast!(TaskBase);

impl PartialOrd for dyn TaskBase {
    fn partial_cmp(&self, other: &dyn TaskBase) -> Option<Ordering> {
        Some(self.get_task_id().cmp(&other.get_task_id()))
    }
}

impl PartialEq for dyn TaskBase {
    fn eq(&self, other: &dyn TaskBase) -> bool {
        self.get_task_id() == other.get_task_id()
    }
}

impl Eq for dyn TaskBase {}

impl Ord for dyn TaskBase {
    fn cmp(&self, other: &dyn TaskBase) -> Ordering {
        self.get_task_id().cmp(&other.get_task_id())
    }
}

#[async_trait::async_trait]
pub trait Task: TaskBase + Send + Sync + Downcast {
    async fn run(&self, id: usize) -> SerBox<dyn SerAny + Send + Sync>;
}

impl_downcast!(Task);

pub trait TaskBox: Task + Serialize + Deserialize + 'static + Downcast {}
impl<K> TaskBox for K where K: Task + Serialize + Deserialize + 'static {}

impl_downcast!(TaskBox);

#[derive(Serialize, Deserialize)]
pub enum TaskOption {
    #[serde(with = "serde_traitobject")]
    ResultTask(Box<dyn TaskBox>),
    #[serde(with = "serde_traitobject")]
    ShuffleMapTask(Box<dyn TaskBox>),
}

#[derive(Serialize, Deserialize)]
pub enum TaskResult {
    ResultTask(SerBox<dyn SerAny + Send + Sync>),
    ShuffleTask(SerBox<dyn SerAny + Send + Sync>),
}

impl TaskOption {
    pub async fn run(&self, id: usize) -> TaskResult {
        match self {
            TaskOption::ResultTask(tsk) => TaskResult::ResultTask(tsk.run(id).await),
            TaskOption::ShuffleMapTask(tsk) => TaskResult::ShuffleTask(tsk.run(id).await),
        }
    }

    pub fn get_task_id(&self) -> usize {
        match self {
            TaskOption::ResultTask(tsk) => tsk.get_task_id(),
            TaskOption::ShuffleMapTask(tsk) => tsk.get_task_id(),
        }
    }

    pub fn get_run_id(&self) -> usize {
        match self {
            TaskOption::ResultTask(tsk) => tsk.get_run_id(),
            TaskOption::ShuffleMapTask(tsk) => tsk.get_run_id(),
        }
    }

    pub fn get_stage_id(&self) -> usize {
        match self {
            TaskOption::ResultTask(tsk) => tsk.get_stage_id(),
            TaskOption::ShuffleMapTask(tsk) => tsk.get_stage_id(),
        }
    }
}
