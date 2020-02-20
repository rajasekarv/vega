use std::any::Any;
use std::cell::RefCell;
use std::clone::Clone;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::option::Option;
use std::sync::{
    atomic::{AtomicUsize, Ordering as OrdAtomic},
    Arc,
};
use std::thread;
use std::time;
use std::time::{Duration, Instant};

use crate::env;
use crate::rdd::Rdd;
use crate::scheduler::NativeScheduler;
use crate::serializable_traits::{Data, SerFunc};
use crate::stage::Stage;
use crate::task::{TaskBase, TaskContext};
use parking_lot::Mutex;

#[derive(Clone, Debug)]
pub struct Job {
    run_id: usize,
    job_id: usize,
}

impl Job {
    pub fn new(run_id: usize, job_id: usize) -> Self {
        Job { run_id, job_id }
    }
}

// manual ordering implemented because we want the jobs to sorted in reverse order
impl PartialOrd for Job {
    fn partial_cmp(&self, other: &Job) -> Option<Ordering> {
        Some(other.job_id.cmp(&self.job_id))
    }
}

impl PartialEq for Job {
    fn eq(&self, other: &Job) -> bool {
        self.job_id == other.job_id
    }
}

impl Eq for Job {}

impl Ord for Job {
    fn cmp(&self, other: &Job) -> Ordering {
        other.job_id.cmp(&self.job_id)
    }
}

type PendingTasks = BTreeMap<Stage, BTreeSet<Box<dyn TaskBase>>>;

/// Contains all the necessary types to run and track a job progress
pub(crate) struct JobTracker<F, U: Data, T: Data>
where
    F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
{
    pub output_parts: Vec<usize>,
    pub num_output_parts: usize,
    pub final_stage: Stage,
    pub func: Arc<F>,
    pub final_rdd: Arc<dyn Rdd<Item = T>>,
    pub run_id: usize,
    pub waiting: Arc<Mutex<BTreeSet<Stage>>>,
    pub running: Arc<Mutex<BTreeSet<Stage>>>,
    pub failed: Arc<Mutex<BTreeSet<Stage>>>,
    pub finished: Arc<Mutex<Vec<bool>>>,
    pub pending_tasks: Arc<Mutex<PendingTasks>>,
    _marker_t: PhantomData<T>,
    _marker_u: PhantomData<U>,
}

impl<F, U: Data, T: Data> JobTracker<F, U, T>
where
    F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
{
    pub fn from_scheduler<S>(
        scheduler: &S,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        output_parts: Vec<usize>,
    ) -> JobTracker<F, U, T>
    where
        S: NativeScheduler,
    {
        let run_id = scheduler.get_next_job_id();
        let final_stage = scheduler.new_stage(final_rdd.clone().get_rdd_base(), None);
        JobTracker::new(run_id, final_stage, func, final_rdd, output_parts)
    }

    fn new(
        run_id: usize,
        final_stage: Stage,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        output_parts: Vec<usize>,
    ) -> JobTracker<F, U, T> {
        let finished: Vec<bool> = (0..output_parts.len()).map(|_| false).collect();
        let mut pending_tasks: BTreeMap<Stage, BTreeSet<Box<dyn TaskBase>>> = BTreeMap::new();
        JobTracker {
            num_output_parts: output_parts.len(),
            output_parts,
            final_stage,
            func,
            final_rdd,
            run_id,
            waiting: Arc::new(Mutex::new(BTreeSet::new())),
            running: Arc::new(Mutex::new(BTreeSet::new())),
            failed: Arc::new(Mutex::new(BTreeSet::new())),
            finished: Arc::new(Mutex::new(finished)),
            pending_tasks: Arc::new(Mutex::new(pending_tasks)),
            _marker_t: PhantomData,
            _marker_u: PhantomData,
        }
    }
}

impl<F, U: Data, T: Data> Clone for JobTracker<F, U, T>
where
    F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
{
    fn clone(&self) -> Self {
        JobTracker {
            output_parts: self.output_parts.clone(),
            num_output_parts: self.num_output_parts,
            final_stage: self.final_stage.clone(),
            func: self.func.clone(),
            final_rdd: self.final_rdd.clone(),
            run_id: self.run_id,
            waiting: self.waiting.clone(),
            running: self.running.clone(),
            failed: self.running.clone(),
            finished: self.finished.clone(),
            pending_tasks: self.pending_tasks.clone(),
            _marker_t: PhantomData,
            _marker_u: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sort_job() {
        let mut jobs = vec![Job::new(1, 2), Job::new(1, 1), Job::new(1, 3)];
        println!("{:?}", jobs);
        jobs.sort();
        println!("{:?}", jobs);
        assert_eq!(jobs, vec![Job::new(1, 3), Job::new(1, 2), Job::new(1, 1),])
    }
}
