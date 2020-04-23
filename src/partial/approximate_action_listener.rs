use std::marker::PhantomData;
use std::time::{Duration, Instant};

use crate::job_listener::JobListener;
use crate::partial::*;
use crate::{Error, Result};

/// A JobListener for an approximate single-result action, such as count() or non-parallel reduce().
/// This listener waits up to timeout milliseconds and will return a partial answer even if the
/// complete answer is not available by then.
///
/// This type assumes that the action is performed on an entire RDD[T] via a function that computes
/// a result of type U for each partition, and that the action returns a partial or complete result
/// of type R. Note that the type R must *include///any error bars on it (e.g. see BoundedInt).
pub(crate) struct ApproximateActionListener<U, R, E>
where
    E: ApproximateEvaluator<U, R>,
{
    evaluator: E,
    start_time: Instant,
    timeout: Duration,
    failure: Option<Error>,
    total_tasks: usize,
    finished_tasks: usize,
    /// Set if we've already returned a PartialResult
    result_object: Option<PartialResult<R>>,
    _marker_r: PhantomData<R>,
    _marker_u: PhantomData<U>,
}

impl<E, U: std::fmt::Debug, R> ApproximateActionListener<U, R, E>
where
    E: ApproximateEvaluator<U, R>,
{
    pub fn new(evaluator: E, timeout: Duration) -> Self {
        ApproximateActionListener {
            evaluator,
            start_time: Instant::now(),
            timeout,
            /// Set if the job has failed (permanently)
            failure: None,
            total_tasks: 0,
            finished_tasks: 0,
            result_object: None,
            _marker_r: PhantomData,
            _marker_u: PhantomData,
        }
    }

    /// Waits for up to timeout milliseconds since the listener was created and then returns a
    /// PartialResult with the result so far. This may be complete if the whole job is done.
    pub async fn get_result(&mut self) -> Result<PartialResult<R>> {
        let finish_time = self.start_time + self.timeout;
        while Instant::now() < finish_time {
            if self.failure.is_some() {
                return Err(self.failure.take().unwrap());
            } else if self.finished_tasks >= self.total_tasks {
                return Ok(PartialResult::new(self.evaluator.current_result(), true));
            }
        }
        // Ran out of time before full completion, return partial job
        let result = PartialResult::new(self.evaluator.current_result(), false);
        // TODO: self.result_object = result.clone();
        Ok(result)
    }
}

#[async_trait::async_trait]
impl<E, U: Send, R> JobListener<R> for ApproximateActionListener<U, R, E>
where
    E: ApproximateEvaluator<U, R> + Send + Sync,
    R: Into<U> + Send,
{
    async fn task_succeeded(&mut self, index: usize, result: R) -> Result<()> {
        self.evaluator.merge(index, result.into());
        self.finished_tasks += 1;
        if self.finished_tasks == self.total_tasks {
            // If we had already returned a PartialResult, set its final value
            if let Some(ref mut value) = self.result_object {
                value
                    .set_final_value(self.evaluator.current_result())
                    .await?;
            }
        }
        Ok(())
    }

    async fn job_failed(&mut self, err: Error) {
        self.failure = Some(err);
    }
}
