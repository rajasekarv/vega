use std::clone::Clone;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::option::Option;
use std::sync::Arc;

use crate::scheduler::{JobListener, NativeScheduler, Stage, TaskBase, TaskContext};
use crate::serializable_traits::{Data, SerFunc};
use crate::{Rdd, Result};
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
pub(crate) struct Job {
    run_id: usize,
    job_id: usize,
}

impl Job {
    pub fn new(run_id: usize, job_id: usize) -> Self {
        Job { run_id, job_id }
    }
}

// Manual ordering implemented because we want the jobs to be sorted in reverse order.
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
pub(crate) struct JobTracker<F, U: Data, T: Data, L>
where
    F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    L: JobListener,
{
    pub output_parts: Vec<usize>,
    pub num_output_parts: usize,
    pub final_stage: Stage,
    pub func: Arc<F>,
    pub final_rdd: Arc<dyn Rdd<Item = T>>,
    pub run_id: usize,
    pub waiting: Mutex<BTreeSet<Stage>>,
    pub running: Mutex<BTreeSet<Stage>>,
    pub failed: Mutex<BTreeSet<Stage>>,
    pub finished: Mutex<Vec<bool>>,
    pub pending_tasks: Mutex<PendingTasks>,
    pub listener: L,
    _marker_t: PhantomData<T>,
    _marker_u: PhantomData<U>,
}

impl<F, U: Data, T: Data, L> JobTracker<F, U, T, L>
where
    F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    L: JobListener,
{
    pub async fn from_scheduler<S>(
        scheduler: &S,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        output_parts: Vec<usize>,
        listener: L,
    ) -> Result<Arc<JobTracker<F, U, T, L>>>
    where
        S: NativeScheduler,
    {
        let run_id = scheduler.get_next_job_id();
        let final_stage = scheduler
            .new_stage(final_rdd.clone().get_rdd_base(), None)
            .await?;
        Ok(JobTracker::new(
            run_id,
            final_stage,
            func,
            final_rdd,
            output_parts,
            listener,
        ))
    }

    fn new(
        run_id: usize,
        final_stage: Stage,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        output_parts: Vec<usize>,
        listener: L,
    ) -> Arc<JobTracker<F, U, T, L>> {
        let finished: Vec<bool> = (0..output_parts.len()).map(|_| false).collect();
        let pending_tasks: BTreeMap<Stage, BTreeSet<Box<dyn TaskBase>>> = BTreeMap::new();
        Arc::new(JobTracker {
            num_output_parts: output_parts.len(),
            output_parts,
            final_stage,
            func,
            final_rdd,
            run_id,
            waiting: Mutex::new(BTreeSet::new()),
            running: Mutex::new(BTreeSet::new()),
            failed: Mutex::new(BTreeSet::new()),
            finished: Mutex::new(finished),
            pending_tasks: Mutex::new(pending_tasks),
            listener: listener,
            _marker_t: PhantomData,
            _marker_u: PhantomData,
        })
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
