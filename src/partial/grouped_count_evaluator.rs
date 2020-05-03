use std::collections::HashMap;
use std::hash::Hash;

use crate::partial::{
    approximate_evaluator::ApproximateEvaluator, bounded_double::BoundedDouble,
    count_evaluator::bound,
};
use crate::serializable_traits::Data;

/// An ApproximateEvaluator for counts by key. Returns a map of key to confidence interval.
pub(crate) struct GroupedCountEvaluator<T>
where
    T: Eq + Hash,
{
    total_outputs: usize,
    confidence: f64,
    outputs_merged: usize,
    sums: HashMap<T, usize>,
}

impl<T: Eq + Hash> GroupedCountEvaluator<T> {
    pub fn new(total_outputs: usize, confidence: f64) -> Self {
        GroupedCountEvaluator {
            total_outputs,
            confidence,
            outputs_merged: 0,
            sums: HashMap::new(),
        }
    }
}

impl<T: Data + Eq + Hash> ApproximateEvaluator<HashMap<T, usize>, HashMap<T, BoundedDouble>>
    for GroupedCountEvaluator<T>
{
    fn merge(&mut self, _output_id: usize, task_result: &HashMap<T, usize>) {
        self.outputs_merged += 1;
        task_result.iter().for_each(|(k, v)| {
            *self.sums.entry(k.clone()).or_insert(0) += v;
        });
    }

    fn current_result(&self) -> HashMap<T, BoundedDouble> {
        if self.outputs_merged == 0 {
            HashMap::new()
        } else if self.outputs_merged == self.total_outputs {
            self.sums
                .iter()
                .map(|(k, sum)| {
                    let sum = *sum as f64;
                    (k.clone(), BoundedDouble::from((sum, 1.0, sum, sum)))
                })
                .collect()
        } else {
            let p = self.outputs_merged as f64 / self.total_outputs as f64;
            self.sums
                .iter()
                .map(|(k, sum)| (k.clone(), bound(self.confidence, *sum as f64, p)))
                .collect()
        }
    }
}
