use std::fmt::Debug;
use std::sync::Arc;

use crate::partial::PartialJobError;
use crate::{Error, Result};
use parking_lot::Mutex;

type OnComplete<R> = Arc<
    dyn Fn(Arc<PartialResult<R>>, Box<dyn Fn(R) + Send + Sync>) -> Result<Arc<PartialResult<R>>>
        + Send
        + Sync,
>;
type OnFail<R> = Arc<
    dyn Fn(Arc<PartialResult<R>>, Box<dyn Fn(&Error) + Send + Sync>) -> Result<()> + Send + Sync,
>;

#[derive(Clone)]
pub struct PartialResult<R>
where
    R: Clone + Debug,
{
    final_value: Arc<Mutex<Option<R>>>,
    failure: Arc<Mutex<Option<Error>>>,
    completion_handler: Arc<Mutex<Option<Box<dyn Fn(R) + Send + Sync>>>>,
    failure_handler: Arc<Mutex<Option<Box<dyn Fn(&Error) + Send + Sync>>>>,
    /// The partial result initial value, which may be or not the final value.
    pub initial_value: R,
    /// Whether this is the final value or not.
    pub is_final: bool,
    // funcs:
    _get_final_value: Arc<dyn Fn(Arc<PartialResult<R>>) -> Result<R> + Send + Sync>,
    /// Set a handler to be called when this PartialResult completes. Only one completion handler
    /// is supported per PartialResult.
    on_complete: OnComplete<R>,
    /// Set a handler to be called if this PartialResult's job fails. Only one failure handler
    /// is supported per PartialResult
    on_fail: OnFail<R>,
}

#[inline(always)]
fn _internal_get_final_value<R>(this: Arc<PartialResult<R>>) -> Result<R>
where
    R: Clone + Debug + Send + Sync + 'static,
{
    while this.final_value.lock().is_none() && this.failure.lock().is_none() {
        // TODO: improve this with channels for notification
        std::thread::sleep_ms(5);
    }

    let final_value = &mut *this.final_value.lock();
    let failure = &mut *this.failure.lock();
    if final_value.is_some() {
        Ok(final_value.take().ok_or_else(|| PartialJobError::None)?)
    } else {
        Err(failure.take().ok_or_else(|| PartialJobError::None)?)
    }
}

#[inline(always)]
fn _internal_on_complete<R>(
    this: Arc<PartialResult<R>>,
    handler: Box<dyn Fn(R) + Send + Sync>,
) -> Result<Arc<PartialResult<R>>>
where
    R: Clone + Debug + Send + Sync + 'static,
{
    {
        let mut completion_handler = this.completion_handler.lock();
        if completion_handler.is_some() {
            return Err(PartialJobError::SetOnCompleteTwice.into());
        }
        if let Some(final_value) = this.final_value.lock().take() {
            // We already have a final value, so let's call the handler
            handler(final_value);
        }
        completion_handler.replace(handler);
    }
    Ok(this)
}

#[inline(always)]
fn _internal_on_fail<R>(
    this: Arc<PartialResult<R>>,
    handler: Box<dyn Fn(&Error) + Send + Sync>,
) -> Result<()>
where
    R: Clone + Debug + Send + Sync + 'static,
{
    {
        let mut failure_handler = this.failure_handler.lock();
        if failure_handler.is_some() {
            return Err(PartialJobError::SetOnFailTwice.into());
        }
        if let Some(ref failure) = *this.failure.lock() {
            // We already have a failure, so let's call the handler
            handler(failure);
        }
        failure_handler.replace(handler);
    }
    Ok(())
}

impl<R> PartialResult<R>
where
    R: Clone + Debug + Send + Sync + 'static,
{
    pub(crate) fn new(initial_value: R, is_final: bool) -> PartialResult<R> {
        let _get_final_value = Arc::new(|this: Arc<PartialResult<R>>| -> Result<R> {
            _internal_get_final_value(this)
        });

        let on_complete = Arc::new(
            |this: Arc<PartialResult<R>>,
             handler: Box<dyn Fn(R) + Send + Sync>|
             -> Result<Arc<PartialResult<R>>> { _internal_on_complete(this, handler) },
        );

        let on_fail = Arc::new(
            |this: Arc<PartialResult<R>>,
             handler: Box<dyn Fn(&Error) + Send + Sync>|
             -> Result<()> { _internal_on_fail(this, handler) },
        );

        let final_value = if is_final {
            Arc::new(Mutex::new(Some(initial_value.clone())))
        } else {
            Arc::new(Mutex::new(None))
        };

        PartialResult {
            final_value,
            failure: Arc::new(Mutex::new(None)),
            completion_handler: Arc::new(Mutex::new(None)),
            failure_handler: Arc::new(Mutex::new(None)),
            initial_value,
            is_final,
            _get_final_value,
            on_complete,
            on_fail,
        }
    }

    /// Blocking method to wait for and return the final value.
    pub fn get_final_value(self: Self) -> Result<R> {
        let selfc = Arc::new(self);
        (selfc._get_final_value)(selfc.clone())
    }

    pub(crate) fn set_final_value(&mut self, value: R) -> Result<()> {
        let final_value = &mut *self.final_value.lock();
        if final_value.is_some() {
            return Err(PartialJobError::SetFinalValTwice.into());
        }
        // Call the completion handler if it was set
        if let Some(ref handler) = *self.completion_handler.lock() {
            handler(value.clone());
        }
        *final_value = Some(value);
        Ok(())
    }

    /// Transform this PartialResult into a PartialResult of type T.
    pub(crate) fn map<T>(self: Arc<Self>) -> Result<PartialResult<T>>
    where
        T: From<R> + Debug + Send + Sync + Clone + 'static,
    {
        let initial_value = self.initial_value.clone().into();
        let mut new_partial_res: PartialResult<T> =
            PartialResult::new(initial_value, self.is_final);

        // override the current closures
        let sc = self.clone();
        new_partial_res._get_final_value =
            Arc::new(move |_this: Arc<PartialResult<T>>| -> Result<T> {
                let ori_val = (sc._get_final_value)(sc.clone())?;
                let transformed = ori_val.into();
                Ok(transformed)
            });

        let sc = self.clone();
        new_partial_res.on_fail = Arc::new(
            move |_this: Arc<PartialResult<T>>,
                  handler: Box<dyn Fn(&Error) + Send + Sync>|
                  -> Result<()> {
                (sc.on_fail)(sc.clone(), handler)?;
                Ok(())
            },
        );

        new_partial_res.on_complete = Arc::new(
            move |_this: Arc<PartialResult<T>>,
                  handler: Box<dyn Fn(T) + Send + Sync>|
                  -> Result<Arc<PartialResult<T>>> {
                let transformed_handler = Box::new(move |res: R| handler(res.into()));
                let res: Arc<PartialResult<R>> =
                    (self.on_complete)(self.clone(), transformed_handler)?;
                Ok(Arc::new(res.map::<T>()?))
            },
        );

        Ok(new_partial_res)
    }

    pub(crate) fn set_failure(&mut self, err: Error) -> Result<()> {
        let mut failure = self.failure.lock();
        if failure.is_some() {
            // We already have a failure, so let's call the handler
            return Err(PartialJobError::SetFailureValTwice.into());
        }
        // // Call the failure handler if it was set
        if let Some(ref handler) = *self.failure_handler.lock() {
            handler(&err);
        }
        failure.replace(err);
        Ok(())
    }
}

impl<R> Debug for PartialResult<R>
where
    R: Clone + Debug,
{
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self.final_value.lock() {
            Some(ref value) => write!(fmt, "PartialResult {{ final: {:?} }})", value),
            None => write!(
                fmt,
                "PartialResult {{ partial: {:?} }})",
                self.initial_value
            ),
        }
    }
}
