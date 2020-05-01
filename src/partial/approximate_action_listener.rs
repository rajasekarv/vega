use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::partial::*;
use crate::scheduler::JobListener;
use crate::serializable_traits::AnyData;
use crate::{Error, Result};
use tokio::sync::Mutex;

/// A JobListener for an approximate single-result action, such as count() or non-parallel reduce().
/// This listener waits up to timeout milliseconds and will return a partial answer even if the
/// complete answer is not available by then.
///
/// This type assumes that the action is performed on an entire `Rdd<Item=T>` via a function that computes
/// a result of type U for each partition, and that the action returns a partial or complete result
/// of type R. Note that the type R must include any error bars on it (e.g. see BoundedInt).
pub(crate) struct ApproximateActionListener<U, R, E>
where
    E: ApproximateEvaluator<U, R>,
    R: Clone + Debug + Send + Sync + 'static,
{
    pub evaluator: Mutex<E>,
    start_time: Instant,
    timeout: Duration,
    failure: Mutex<Option<Error>>,
    total_tasks: usize,
    finished_tasks: AtomicUsize,
    /// Set if we've already returned a PartialResult
    result_object: Mutex<Option<PartialResult<R>>>,
    _marker_r: PhantomData<R>,
    _marker_u: PhantomData<U>,
}

impl<E, U: Debug, R> ApproximateActionListener<U, R, E>
where
    E: ApproximateEvaluator<U, R>,
    R: Clone + Debug + Send + Sync + 'static,
{
    pub fn new(evaluator: E, timeout: Duration, num_partitions: usize) -> Self {
        ApproximateActionListener {
            evaluator: Mutex::new(evaluator),
            start_time: Instant::now(),
            timeout,
            /// Set if the job has failed (permanently)
            failure: Mutex::new(None),
            total_tasks: num_partitions,
            finished_tasks: AtomicUsize::new(0),
            result_object: Mutex::new(None),
            _marker_r: PhantomData,
            _marker_u: PhantomData,
        }
    }

    /// Waits for up to timeout milliseconds since the listener was created and then returns a
    /// PartialResult with the result so far. This may be complete if the whole job is done.
    pub async fn get_result(&self) -> Result<PartialResult<R>> {
        let finish_time = self.start_time + self.timeout;
        while Instant::now() < finish_time {
            {
                let mut failure = self.failure.lock().await;
                if failure.is_some() {
                    return Err(failure.take().unwrap());
                }
            }
            if self.finished_tasks.load(Ordering::SeqCst) == self.total_tasks {
                return Ok(PartialResult::new(
                    self.evaluator.lock().await.current_result(),
                    true,
                ));
            }
            tokio::time::delay_for(self.timeout / 20).await;
        }
        // Ran out of time before full completion, return partial job
        let result = PartialResult::new(self.evaluator.lock().await.current_result(), false);
        let mut current_result = self.result_object.lock().await;
        *current_result = Some(result.clone());
        Ok(result)
    }
}

#[async_trait::async_trait]
impl<E, U, R> JobListener for ApproximateActionListener<U, R, E>
where
    E: ApproximateEvaluator<U, R> + Send + Sync,
    R: Clone + Debug + Send + Sync + 'static,
    U: Send + Sync + 'static,
{
    async fn task_succeeded(&self, index: usize, result: &dyn AnyData) -> Result<()> {
        let result = result.as_any().downcast_ref::<U>().ok_or_else(|| {
            Error::DowncastFailure(
                "failed converting to generic type param @ ApproximateActionListener",
            )
        })?;
        self.evaluator.lock().await.merge(index, result);
        let current_finished = self.finished_tasks.fetch_add(1, Ordering::SeqCst) + 1;
        if current_finished == self.total_tasks {
            // If we had already returned a PartialResult, set its final value
            if let Some(ref mut value) = *self.result_object.lock().await {
                value.set_final_value(self.evaluator.lock().await.current_result())?;
            }
        }
        Ok(())
    }

    async fn job_failed(&self, err: Error) {
        let mut failure = self.failure.lock().await;
        *failure = Some(err);
    }
}
