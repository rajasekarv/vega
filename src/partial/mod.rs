//! Support for approximate results. This provides convenient API and also implementation for
//! approximate calculation.

mod approximate_action_listener;
pub(self) mod approximate_evaluator;
pub(self) mod bounded_double;
mod count_evaluator;

pub(crate) use approximate_action_listener::ApproximateActionListener;
pub(crate) use approximate_evaluator::ApproximateEvaluator;
pub(crate) use count_evaluator::CountEvaluator;
