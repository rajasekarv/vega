use crate::{Data, Deserialize, Serialize};
use std::any::Any;

use downcast_rs::Downcast;
use rand::Rng;
use rand_distr::{Bernoulli, Distribution, Poisson};
use rand_pcg::Pcg64;

/// Default maximum gap-sampling fraction.
/// For sampling fractions <= this value, the gap sampling optimization will be applied.
/// Above this value, it is assumed that "traditional" Bernoulli sampling is faster. The
/// optimal value for this will depend on the RNG.  More expensive RNGs will tend to make
/// the optimal value higher. The most reliable way to determine this value for a new RNG
/// is to experiment. When tuning for a new RNG, expect a value of 0.5 to be close in
/// most cases, as an initial guess.
const DEFAULT_MAX_GAP_SAMPLING_FRACTION: f64 = 0.4;

/// Default epsilon for floating point numbers sampled from the RNG.
/// The gap-sampling compute logic requires taking log(x), where x is sampled from an RNG.
/// To guard against errors from taking log(0), a positive epsilon lower bound is applied.
/// A good value for this parameter is at or near the minimum positive floating
/// point value returned by for the RNG being used.
// TODO: this is a straight port, it may not apply exactly to pcg64 rng;
// but should be mostly fine; double check
const RNG_EPSILON: f64 = 5e-11;

pub(crate) trait RandomSampler<T: Data>: Send + Sync + Serialize + Deserialize {
    fn get_sampler(&self) -> Box<dyn Any> {
        unimplemented!()
    }

    fn sample(&self, items: Box<dyn Iterator<Item = T>>) -> Box<dyn Iterator<Item = T>> {
        unimplemented!()
    }
}

pub(crate) fn get_default_rng() -> Pcg64 {
    rand_pcg::Pcg64::new(
        0xcafe_f00d_d15e_a5e5,
        0x0a02_bdbf_7bb3_c0a7_ac28_fa16_a64a_bf96,
    )
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct PoissonSampler {
    fraction: f64,
    use_gap_sampling_if_possible: bool,
    prob: f64,
}

impl PoissonSampler {
    pub fn new(fraction: f64) -> PoissonSampler {
        let prob = if fraction > 0.0 { fraction } else { 1.0 };

        PoissonSampler {
            fraction,
            use_gap_sampling_if_possible: true,
            prob,
        }
    }

    fn use_gap_sampling_if_possible(&mut self) {
        self.use_gap_sampling_if_possible = true;
    }
}

impl<T: Data> RandomSampler<T> for PoissonSampler {
    fn get_sampler(&self) -> Box<dyn Any> {
        let rng = Poisson::new(self.prob).unwrap();

        let f = |items: Box<dyn Iterator<Item = T>>| -> Vec<T> {
            if self.fraction <= 0.0 {
                vec![]
            } else {
                let use_gap_sampling = self.use_gap_sampling_if_possible
                    && self.fraction <= DEFAULT_MAX_GAP_SAMPLING_FRACTION;
                let mut gap_sampling_replacement =
                    GapSamplingReplacement::new(self.fraction, RNG_EPSILON);

                items
                    .filter_map(move |item| {
                        let count = if use_gap_sampling {
                            gap_sampling_replacement.sample()
                        } else {
                            unimplemented!()
                        };
                        if count == 0 {
                            None
                        } else {
                            Some(item)
                        }
                    })
                    .collect()
            }
        };

        unimplemented!()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct BernoulliSampler {
    fraction: f64,
}

impl BernoulliSampler {
    pub fn new(fraction: f64) -> BernoulliSampler {
        BernoulliSampler { fraction }
    }
}

impl<T: Data> RandomSampler<T> for BernoulliSampler {}

struct GapSamplingReplacement {
    fraction: f64,
    epsilon: f64,
    q: f64,
    rng: rand_pcg::Pcg64,
    count_for_dropping: u64,
}

impl GapSamplingReplacement {
    fn new(fraction: f64, epsilon: f64) -> GapSamplingReplacement {
        assert!(fraction > 0.0 && fraction < 1.0);
        assert!(epsilon > 0.0);

        let mut sampler = GapSamplingReplacement {
            q: (-fraction).exp(),
            fraction,
            epsilon,
            rng: get_default_rng(),
            count_for_dropping: 0,
        };
        // Advance to first sample as part of object construction.
        sampler.advance();
        sampler
    }

    fn sample(&mut self) -> u64 {
        if self.count_for_dropping > 0 {
            self.count_for_dropping -= 1;
            0
        } else {
            let r = self.poisson_ge1();
            self.advance();
            r
        }
    }

    /// Sample from Poisson distribution, conditioned such that the sampled value is >= 1.
    /// This is an adaptation from the algorithm for generating
    /// [Poisson distributed random variables](http://en.wikipedia.org/wiki/Poisson_distribution)
    fn poisson_ge1(&mut self) -> u64 {
        // simulate that the standard poisson sampling
        // gave us at least one iteration, for a sample of >= 1
        let mut pp: f64 = self.q + ((1.0 - self.q) * self.rng.gen::<f64>());
        let mut r = 1;

        // now continue with standard poisson sampling algorithm
        pp *= self.rng.gen::<f64>();
        while pp > self.q {
            r += 1;
            pp *= self.rng.gen::<f64>()
        }
        r
    }

    /// Skip elements with replication factor zero (i.e. elements that won't be sampled).
    /// Samples 'k' from geometric distribution P(k) = (1-q)(q)^k, where q = e^(-f), that is
    /// q is the probability of Poisson(0; f)
    fn advance(&mut self) {
        let u = self.epsilon.max(self.rng.gen::<f64>());
        self.count_for_dropping = (u.log(std::f64::consts::E) / (-self.fraction)) as u64;
    }
}

/// Returns a sampling rate that guarantees a sample of size greater than or equal to
/// `sample_size_lower_bound` 99.99% of the time.
///
/// How the sampling rate is determined:
///
/// Let p = num / total, where num is the sample size and total is the total number of
/// datapoints in the RDD. We're trying to compute q > p such that
///   - when sampling with replacement, we're drawing each datapoint with prob_i ~ Pois(q),
///     where we want to guarantee
///     Pr[s < num] < 0.0001 for s = sum(prob_i for i from 0 to total),
///     i.e. the failure rate of not having a sufficiently large sample < 0.0001.
///     Setting q = p + 5 /// sqrt(p/total) is sufficient to guarantee 0.9999 success rate for
///     num > 12, but we need a slightly larger q (9 empirically determined).
///   - when sampling without replacement, we're drawing each datapoint with prob_i
///     ~ Binomial(total, fraction) and our choice of q guarantees 1-delta, or 0.9999 success
///     rate, where success rate is defined the same as in sampling with replacement.
///
/// The smallest sampling rate supported is 1e-10 (in order to avoid running into the limit of the
/// RNG's resolution).
pub(crate) fn compute_fraction_for_sample_size(
    sample_size_lower_bound: u64,
    total: u64,
    with_replacement: bool,
) -> f64 {
    if (with_replacement) {
        poisson_bounds::get_upper_bound(sample_size_lower_bound as f64) / total as f64
    } else {
        let fraction = sample_size_lower_bound as f64 / total as f64;
        binomial_bounds::get_upper_bound(1e-4, total, fraction)
    }
}

mod poisson_bounds {
    /// Returns a lambda such that P[X < s] is very small, where X ~ Pois(lambda).
    pub(super) fn get_upper_bound(s: f64) -> f64 {
        (s + num_std(s) * s.sqrt()).max(1e-10)
    }

    #[inline(always)]
    fn num_std(s: f64) -> f64 {
        match s {
            v if v < 6.0 => 12.0,
            v if v < 16.0 => 9.0,
            _ => 6.0,
        }
    }
}

mod binomial_bounds {
    // Returns a threshold `p` such that if we conduct n Bernoulli trials with success rate = `p`,
    // it is very unlikely to have less than `fraction * n` successes.
    pub(super) fn get_upper_bound(delta: f64, n: u64, fraction: f64) -> f64 {
        let gamma = -delta.log(std::f64::consts::E) / n as f64;
        fraction + gamma - (gamma * gamma + 3.0 * gamma * fraction).sqrt()
    }
}
