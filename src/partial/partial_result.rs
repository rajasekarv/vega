use crate::partial::PartialJobError;
use crate::{Error, Result};

pub(crate) struct PartialResult<R> {
    final_value: Option<R>,
    failure: Option<Error>,
    completion_handler: Option<Box<dyn Fn(&R) + Send>>,
    failure_handler: Option<Box<dyn Fn(Error) + Send>>,
    pub initial_value: R,
    pub is_final: bool,
}

impl<R> PartialResult<R> {
    pub fn new(initial_value: R, is_final: bool) -> Self {
        PartialResult {
            final_value: None,
            failure: None,
            completion_handler: None,
            failure_handler: None,
            initial_value,
            is_final,
        }
    }

    pub async fn set_final_value(&mut self, value: R) -> Result<()> {
        if self.final_value.is_some() {
            return Err(PartialJobError::SetFinalValTwice.into());
        }
        // Call the completion handler if it was set
        if let Some(handler) = &self.completion_handler {
            handler(&value);
        }
        self.final_value = Some(value);
        Ok(())
    }

    /// Blocking method to wait for and return the final value.
    pub async fn get_final_value(&mut self) -> Result<R> {
        while self.final_value.is_none() && self.failure.is_none() {
            // sleep
        }
        if self.final_value.is_some() {
            Ok(self
                .final_value
                .take()
                .ok_or_else(|| PartialJobError::None)?)
        } else {
            Err(self.failure.take().ok_or_else(|| PartialJobError::None)?)
        }
    }

    /// Set a handler to be called when this PartialResult completes. Only one completion handler
    /// is supported per PartialResult.
    pub fn on_complete(&self, handler: &dyn Fn(R)) -> PartialResult<R> {
        // if (completionHandler.isDefined) {
        //   throw new UnsupportedOperationException("onComplete cannot be called twice")
        // }
        // completionHandler = Some(handler)
        // if (finalValue.isDefined) {
        //   // We already have a final value, so let's call the handler
        //   handler(finalValue.get)
        // }
        // return this
        todo!()
    }

    /// Set a handler to be called if this PartialResult's job fails. Only one failure handler
    /// is supported per PartialResult.
    pub fn on_fail(&self, handler: &dyn Fn(Error)) {
        // if (failureHandler.isDefined) {
        //   throw new UnsupportedOperationException("onFail cannot be called twice")
        // }
        // failureHandler = Some(handler)
        // if (failure.isDefined) {
        //   // We already have a failure, so let's call the handler
        //   handler(failure.get)
        // }
        todo!()
    }

    /// Transform this PartialResult into a PartialResult of type T.
    pub fn map<T>(f: &dyn Fn(R) -> T) -> PartialResult<T> {
        // new PartialResult[T](f(initialVal), isFinal) {
        //    override def getFinalValue() : T = synchronized {
        //      f(PartialResult.this.getFinalValue())
        //    }
        //    override def onComplete(handler: T => Unit): PartialResult[T] = synchronized {
        //      PartialResult.this.onComplete(handler.compose(f)).map(f)
        //    }
        //   override def onFail(handler: Exception => Unit): Unit = {
        //     synchronized {
        //       PartialResult.this.onFail(handler)
        //     }
        //   }
        //   override def toString : String = synchronized {
        //     PartialResult.this.getFinalValueInternal() match {
        //       case Some(value) => "(final: " + f(value) + ")"
        //       case None => "(partial: " + initialValue + ")"
        //     }
        //   }
        //   def getFinalValueInternal(): Option[T] = PartialResult.this.getFinalValueInternal().map(f)
        // }
        todo!()
    }

    pub fn set_failure(&mut self, err: Error) {
        // if (failure.isDefined) {
        //   throw new UnsupportedOperationException("setFailure called twice on a PartialResult")
        // }
        // failure = Some(exception)
        // // Call the failure handler if it was set
        // failureHandler.foreach(h => h(exception))
        // // Notify any threads that may be calling getFinalValue()
        // this.notifyAll()
        todo!()
    }
}

impl<R> std::fmt::Display for PartialResult<R> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        // finalValue match {
        //   case Some(value) => "(final: " + value + ")"
        //   case None => "(partial: " + initialValue + ")"
        // }
        todo!()
    }
}
