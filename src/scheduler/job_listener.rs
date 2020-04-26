use crate::serializable_traits::AnyData;
use crate::{Error, Result};

/// Interface used to listen for job completion or failure events after submitting a job to the
/// DAGScheduler. The listener is notified each time a task succeeds, as well as if the whole
/// job fails (and no further taskSucceeded events will happen).
#[async_trait::async_trait]
pub(crate) trait JobListener: Send + Sync {
    async fn task_succeeded(&self, _index: usize, _result: &dyn AnyData) -> Result<()> {
        Ok(())
    }
    async fn job_failed(&self, err: Error) {
        log::debug!("job failed with error: {}", err);
    }
}

/// A listener which produces no action whatsoever.
pub(super) struct NoOpListener;
impl JobListener for NoOpListener {}
