use rand::{
    distributions::{Bernoulli, Distribution},
    Rng,
};

/// Shuffle the elements of a vec into a random order in place, modifying it.
pub(crate) fn randomize_in_place<T, R>(iter: &mut Vec<T>, rand: &mut R)
where
    R: Rng,
{
    for i in (1..(iter.len() - 1)).rev() {
        let idx = rand.gen_range(0, i + 1);
        unsafe {
            let tmp = &mut (&mut *(iter.as_mut_slice() as *mut [T]))[idx];
            let swap = &mut iter[i];
            std::mem::swap(tmp, swap);
        }
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

#[test]
#[cfg(test)]
fn test_randomize_in_place() {
    use rand::SeedableRng;
    let mut sample = vec![1_i64, 2, 3, 4, 5, 6, 7, 8, 9, 10];

    let mut randomized_samples = vec![];
    for seed in 0..10 {
        let mut rng = rand_pcg::Pcg64::seed_from_u64(seed);
        let mut copied_sample = sample.clone();
        randomize_in_place(&mut copied_sample, &mut rng);
        randomized_samples.push(copied_sample);
    }
    randomized_samples.push(sample);

    let equal: u8 = randomized_samples
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let cmp1 = &randomized_samples[0..i];
            let cmp2 = &randomized_samples[i + 1..];
            if cmp1.iter().any(|x| x == v) || cmp2.iter().any(|x| x == v) {
                1
            } else {
                0
            }
        })
        .sum();

    assert!(equal == 0);
}
