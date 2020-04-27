use crate::serializable_traits::Data;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Poisson};
use rand_pcg::Pcg64;
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Deserialize, Serialize};

/// Default maximum gap-sampling fraction.
/// For sampling fractions <= this value, the gap sampling optimization will be applied.
/// Above this value, it is assumed that "traditional" Bernoulli sampling is faster. The
/// optimal value for this will depend on the RNG.  More expensive RNGs will tend to make
/// the optimal value higher. The most reliable way to determine this value for a new RNG
/// is to experiment. When tuning for a new RNG, expect a value of 0.5 to be close in
/// most cases, as an initial guess.
// TODO: tune for PCG64, performance is similar and around same order of magnitude
// of XORShift so shouldn't be too far off
const DEFAULT_MAX_GAP_SAMPLING_FRACTION: f64 = 0.4;

/// Default epsilon for floating point numbers sampled from the RNG.
/// The gap-sampling compute logic requires taking log(x), where x is sampled from an RNG.
/// To guard against errors from taking log(0), a positive epsilon lower bound is applied.
/// A good value for this parameter is at or near the minimum positive floating
/// point value returned by for the RNG being used.
// TODO: this is a straight port, it may not apply exactly to pcg64 rng but should be mostly fine;
// double check; Apache Spark(tm) uses XORShift by default
const RNG_EPSILON: f64 = 5e-11;

/// Sampling fraction arguments may be results of computation, and subject to floating
/// point jitter.  I check the arguments with this epsilon slop factor to prevent spurious
/// warnings for cases such as summing some numbers to get a sampling fraction of 1.000000001
const ROUNDING_EPSILON: f64 = 1e-6;

type RSamplerFunc<'a, T> =
    Box<dyn Fn(Box<dyn Iterator<Item = T>>) -> Box<dyn Iterator<Item = T>> + 'a>;

pub(crate) trait RandomSampler<T: Data>: Send + Sync + Serialize + Deserialize {
    /// Returns a function which returns random samples,
    /// the sampler is thread-safe as the RNG is seeded with random seeds per thread.
    fn get_sampler(&self, seed: Option<u64>) -> RSamplerFunc<T>;
}

pub(crate) fn get_default_rng() -> Pcg64 {
    Pcg64::new(
        0xcafe_f00d_d15e_a5e5,
        0x0a02_bdbf_7bb3_c0a7_ac28_fa16_a64a_bf96,
    )
}

pub(crate) fn get_default_rng_from_seed(seed: u64) -> Pcg64 {
    Pcg64::seed_from_u64(seed)
}

/// Get a new rng with random thread local random seed
fn get_rng_with_random_seed() -> Pcg64 {
    Pcg64::seed_from_u64(rand::random::<u64>())
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub(crate) struct PoissonSampler {
    fraction: f64,
    use_gap_sampling_if_possible: bool,
    prob: f64,
}

impl PoissonSampler {
    pub fn new(fraction: f64, use_gap_sampling_if_possible: bool) -> PoissonSampler {
        let prob = if fraction > 0.0 { fraction } else { 1.0 };

        PoissonSampler {
            fraction,
            use_gap_sampling_if_possible,
            prob,
        }
    }
}

impl<T: Data> RandomSampler<T> for PoissonSampler {
    fn get_sampler(&self, seed: Option<u64>) -> RSamplerFunc<T> {
        Box::new(
            move |items: Box<dyn Iterator<Item = T>>| -> Box<dyn Iterator<Item = T>> {
                if self.fraction <= 0.0 {
                    Box::new(std::iter::empty())
                } else {
                    let use_gap_sampling = self.use_gap_sampling_if_possible
                        && self.fraction <= DEFAULT_MAX_GAP_SAMPLING_FRACTION;

                    let mut gap_sampling = if use_gap_sampling {
                        // Initialize here and move to avoid constructing a new one each iteration
                        Some(GapSamplingReplacement::new(self.fraction, RNG_EPSILON))
                    } else {
                        None
                    };
                    let dist = Poisson::new(self.prob).unwrap();

                    let mut rng: Pcg64 = match seed {
                        Some(s) => get_default_rng_from_seed(s),
                        None => get_rng_with_random_seed(),
                    };

                    Box::new(items.flat_map(move |item| {
                        let count = if use_gap_sampling {
                            gap_sampling.as_mut().unwrap().sample()
                        } else {
                            dist.sample(&mut rng)
                        };
                        if count != 0 {
                            vec![item; count as usize]
                        } else {
                            vec![]
                        }
                    }))
                }
            },
        )
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub(crate) struct BernoulliSampler {
    fraction: f64,
}

impl BernoulliSampler {
    pub fn new(fraction: f64) -> BernoulliSampler {
        assert!(fraction >= (0.0 - ROUNDING_EPSILON) && fraction <= (1.0 + ROUNDING_EPSILON));
        BernoulliSampler { fraction }
    }

    fn sample(&self, gap_sampling: Option<&mut GapSamplingReplacement>, rng: &mut Pcg64) -> u64 {
        match self.fraction {
            v if v <= 0.0 => 0,
            v if v >= 1.0 => 1,
            v if v <= DEFAULT_MAX_GAP_SAMPLING_FRACTION => gap_sampling.unwrap().sample(),
            v if rng.gen::<f64>() <= v => 1,
            _ => 0,
        }
    }
}

impl<T: Data> RandomSampler<T> for BernoulliSampler {
    fn get_sampler(&self, seed: Option<u64>) -> RSamplerFunc<T> {
        Box::new(
            move |items: Box<dyn Iterator<Item = T>>| -> Box<dyn Iterator<Item = T>> {
                let mut gap_sampling = if self.fraction > 0.0 && self.fraction < 1.0 {
                    Some(GapSamplingReplacement::new(self.fraction, RNG_EPSILON))
                } else {
                    None
                };

                let mut rng: Pcg64 = match seed {
                    Some(s) => get_default_rng_from_seed(s),
                    None => get_rng_with_random_seed(),
                };

                let selfc = *self;
                Box::new(items.filter(move |_| selfc.sample(gap_sampling.as_mut(), &mut rng) > 0))
            },
        )
    }
}

/// A sampler based on Bernoulli trials for partitioning a data sequence.
#[derive(Clone, Copy, Serialize, Deserialize)]
pub(crate) struct BernoulliCellSampler {
    /// lower bound of the acceptance range
    lb: f64,
    /// upper bound of the acceptance range
    ub: f64,
    /// whether to use the complement of the range specified, default to false
    complement: bool,
}

impl BernoulliCellSampler {
    pub fn new(lb: f64, ub: f64, complement: bool) -> BernoulliCellSampler {
        // epsilon slop to avoid failure from floating point jitter.
        assert!(
            lb <= (ub + ROUNDING_EPSILON),
            format!("Lower bound {} must be <= upper bound {}", lb, ub)
        );
        assert!(
            lb >= (0.0 - ROUNDING_EPSILON),
            format!("Lower bound {} must be >= 0.0", lb)
        );
        assert!(
            ub <= (1.0 + ROUNDING_EPSILON),
            format!("Upper bound {} must be <= 1.0", ub)
        );

        BernoulliCellSampler { lb, ub, complement }
    }

    /// Whether to sample the next item or not.
    /// Return how many times the next item will be sampled. Return 0 if it is not sampled.
    pub fn sample(&self, rng: &mut Pcg64) -> i32 {
        if (self.ub - self.lb) <= 0.0 {
            if self.complement {
                1
            } else {
                0
            }
        } else {
            let x = rng.gen::<f64>();
            let n;
            if x >= self.lb && x < self.ub {
                n = 1;
            } else {
                n = 0;
            }

            if self.complement {
                1 - n
            } else {
                n
            }
        }
    }
}

impl<T: Data> RandomSampler<T> for BernoulliCellSampler {
    /// Take a random sample
    fn get_sampler(&self, seed: Option<u64>) -> RSamplerFunc<T> {
        Box::new(
            move |items: Box<dyn Iterator<Item = T>>| -> Box<dyn Iterator<Item = T>> {
                let mut rng: Pcg64 = match seed {
                    Some(s) => get_default_rng_from_seed(s),
                    None => get_rng_with_random_seed(),
                };

                let selfc = *self;
                Box::new(items.filter(move |_| selfc.sample(&mut rng) > 0))
            },
        )
    }
}

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
            rng: get_rng_with_random_seed(),
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
    if with_replacement {
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
    const MIN_SAMPLING_RATE: f64 = 1e-10;

    // Returns a threshold `p` such that if we conduct n Bernoulli trials with success rate = `p`,
    // it is very unlikely to have less than `fraction * n` successes.
    pub(super) fn get_upper_bound(delta: f64, n: u64, fraction: f64) -> f64 {
        let gamma = -delta.log(std::f64::consts::E) / n as f64;
        let max = MIN_SAMPLING_RATE
            .max(fraction + gamma + (gamma * gamma + 2.0 * gamma * fraction).sqrt());
        max.min(1.0)
    }
}
