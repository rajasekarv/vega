use crate::rdd::Rdd;
use crate::serializable_traits::Data;

#[macro_use]
mod base_scheduler;
mod dag_scheduler;
mod distributed_scheduler;
mod job;
mod job_listener;
pub(self) mod listener;
mod live_listener_bus;
mod local_scheduler;
mod result_task;
mod stage;
mod task;

pub(self) use self::base_scheduler::EventQueue;
pub(self) use self::dag_scheduler::{CompletionEvent, FetchFailedVals, TastEndReason};
pub(self) use self::job::{Job, JobTracker};
pub(self) use self::job_listener::NoOpListener;
pub(self) use self::live_listener_bus::LiveListenerBus;
pub(self) use self::stage::Stage;

pub(crate) use self::base_scheduler::NativeScheduler;
pub(crate) use self::distributed_scheduler::DistributedScheduler;
pub(crate) use self::job_listener::JobListener;
pub(crate) use self::local_scheduler::LocalScheduler;
pub(crate) use self::result_task::ResultTask;
pub(crate) use self::task::TaskContext;
pub(crate) use self::task::{Task, TaskBase, TaskOption, TaskResult};

pub trait Scheduler {
    fn start(&self);
    fn wait_for_register(&self);
    fn run_job<T: Data, U: Data, F>(
        &self,
        rdd: &dyn Rdd<Item = T>,
        func: F,
        partitions: Vec<i64>,
        allow_local: bool,
    ) -> Vec<U>
    where
        Self: Sized,
        F: Fn(Box<dyn Iterator<Item = T>>) -> U;
    fn stop(&self);
    fn default_parallelism(&self) -> i64;
}
