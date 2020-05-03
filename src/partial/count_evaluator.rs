use crate::partial::{approximate_evaluator::ApproximateEvaluator, bounded_double::BoundedDouble};
use statrs::{distribution::Poisson, statistics::Mean};

/// An ApproximateEvaluator for counts.
pub(crate) struct CountEvaluator {
    total_outputs: usize,
    confidence: f64,
    outputs_merged: usize,
    sum: usize,
}

impl CountEvaluator {
    pub fn new(total_outputs: usize, confidence: f64) -> Self {
        CountEvaluator {
            total_outputs,
            confidence,
            outputs_merged: 0,
            sum: 0,
        }
    }
}

impl ApproximateEvaluator<usize, BoundedDouble> for CountEvaluator {
    fn merge(&mut self, _output_id: usize, task_result: &usize) {
        self.outputs_merged += 1;
        self.sum += task_result;
    }

    fn current_result(&self) -> BoundedDouble {
        if self.outputs_merged == 0 || self.sum == 0 {
            BoundedDouble::from((0.0, 0.0, 0.0, f64::MAX))
        } else if self.outputs_merged == self.total_outputs {
            BoundedDouble::from((self.sum as f64, 1.0_f64, self.sum as f64, self.sum as f64))
        } else {
            let p = self.outputs_merged as f64 / self.total_outputs as f64;
            bound(self.confidence, self.sum as f64, p)
        }
    }
}

pub(super) fn bound(confidence: f64, sum: f64, p: f64) -> BoundedDouble {
    // "sum" elements have been observed having scanned a fraction
    // p of the data. This suggests data is counted at a rate of sum / p across the whole data
    // set. The total expected count from the rest is distributed as
    // (1-p) Poisson(sum / p) = Poisson(sum*(1-p)/p)
    let dist = Poisson::new(sum * (1.0f64 - p) / p).unwrap();

    // Not quite symmetric; calculate interval straight from discrete distribution
    // FIXME: (should be) val low_interval = dist.inverseCumulativeProbability((1 - confidence) / 2)
    // let lower_range = (1.0 - confidence) / 2.0;
    let low_interval = 0.0;
    // FIXME: (should be) val high = dist.inverseCumulativeProbability((1 + confidence) / 2)
    // let higher_range = (1.0 + confidence) / 2.0;
    let high_interval = 0.0;

    // Add 'sum' to each because distribution is just of remaining count, not observed
    BoundedDouble::from((
        sum + dist.mean(),
        confidence,
        sum + low_interval,
        sum + high_interval,
    ))
}
