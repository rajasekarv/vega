use std::net::{Ipv4Addr, SocketAddr, TcpListener};

use crate::error;
use rand::Rng;

pub(crate) mod random;
#[cfg(test)]
pub(crate) mod test_utils;

/// Shuffle the elements of a vec into a random order in place, modifying it.
pub(crate) fn randomize_in_place<T, R>(iter: &mut Vec<T>, rand: &mut R)
where
    R: Rng,
{
    for i in (1..(iter.len() - 1)).rev() {
        let idx = rand.gen_range(0, i + 1);
        iter.swap(idx, i);
    }
}

pub(crate) fn get_free_connection(ip: Ipv4Addr) -> Result<(TcpListener, u16), error::NetworkError> {
    let mut port = 0;
    for _ in 0..100 {
        port = get_dynamic_port();
        let bind_addr = SocketAddr::from((ip, port));
        if let Ok(conn) = TcpListener::bind(bind_addr) {
            return Ok((conn, port));
        }
    }
    Err(error::NetworkError::FreePortNotFound(port, 100))
}

pub(crate) fn get_dynamic_port() -> u16 {
    const FIRST_DYNAMIC_PORT: u16 = 49152;
    const LAST_DYNAMIC_PORT: u16 = 65535;
    FIRST_DYNAMIC_PORT + rand::thread_rng().gen_range(0, LAST_DYNAMIC_PORT - FIRST_DYNAMIC_PORT)
}

#[test]
#[cfg(test)]
fn test_randomize_in_place() {
    use rand::SeedableRng;
    let sample = vec![1_i64, 2, 3, 4, 5, 6, 7, 8, 9, 10];

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
