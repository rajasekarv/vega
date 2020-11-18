//! Support for approximate results. This provides convenient API and also implementation for
//! approximate calculation.
use thiserror::Error;

mod approximate_action_listener;
pub(self) mod approximate_evaluator;
mod bounded_double;
mod count_evaluator;
mod grouped_count_evaluator;
mod partial_result;

pub(crate) use approximate_action_listener::ApproximateActionListener;
pub(crate) use approximate_evaluator::ApproximateEvaluator;
pub use bounded_double::BoundedDouble;
pub(crate) use count_evaluator::CountEvaluator;
pub(crate) use grouped_count_evaluator::GroupedCountEvaluator;
pub use partial_result::PartialResult;

#[derive(Debug, Error)]
pub enum PartialJobError {
    #[error("on_fail cannot be called twice")]
    SetOnFailTwice,

    #[error("set_failure called twice on a PartialResult")]
    SetFailureValTwice,

    #[error("set_final_value called twice on a PartialResult")]
    SetFinalValTwice,

    #[error("on_complete cannot be called twice")]
    SetOnCompleteTwice,

    #[error("unreachable")]
    None,
}
