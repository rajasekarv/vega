use crate::{Error, Result};

/// Interface used to listen for job completion or failure events after submitting a job to the
/// DAGScheduler. The listener is notified each time a task succeeds, as well as if the whole
/// job fails (and no further taskSucceeded events will happen).
#[async_trait::async_trait]
pub(crate) trait JobListener<R> {
    async fn task_succeeded(&mut self, index: usize, result: R) -> Result<()>;
    async fn job_failed(&mut self, err: Error);
}
