use std::{collections::HashMap, env::temp_dir};
use std::path::PathBuf;
use std::time::Instant;
use vega::*;
use rand::Rng;
use serde_derive::{Deserialize, Serialize};

fn parse_vector(line: String) -> Vec<f64> {
    line.split(' ')
        .map(|number| number.parse::<f64>().unwrap())
        .collect()
}

fn squared_distance(p: &Vec<f64>, center: &Vec<f64>) -> f64 {
    assert_eq!(p.len(), center.len());
    let sum = p.iter().zip(center.iter()).fold(0 as f64, |acc, (x, y)| {
        let delta = *y - *x;
        acc + delta * delta
    });
    sum.sqrt()
}

fn closest_point(p: &Vec<f64>, centers: &Vec<Vec<f64>>) -> usize {
    let mut best_index = 0;
    let mut closest = f64::MAX;

    for (index, center) in centers.iter().enumerate() {
        let temp_dist = squared_distance(p, center);
        if temp_dist < closest {
            closest = temp_dist;
            best_index = index
        }
    }

    best_index
}

fn merge_results(a: (Vec<f64>, i32), b: (Vec<f64>, i32)) -> (Vec<f64>, i32) {
    (
        a.0.iter().zip(b.0.iter()).map(|(x, y)| x + y).collect(),
        a.1 + b.1,
    )
}

fn main() -> Result<()> {
    let sc = Context::new()?;
    let now = Instant::now();

    // TODO: need to change dir
    let dir = PathBuf::from("/opt/data/pt_km_50000_5");
    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        String::from_utf8(file)
            .unwrap()
            .lines()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    }));

    let lines = sc.read_source(LocalFsReaderConfig::new(dir).num_partitions_per_executor(1), deserializer);
    let data_rdd = lines.flat_map(Fn!(|lines: Vec<String>| {
        Box::new(lines.into_iter().map(|line| {
            parse_vector::<>(line)
        })) as Box<dyn Iterator<Item = _>>
    }));
    //data_rdd.cache();

    let k = 10;
    let converge_dist = 0.3;
    let mut iter = 0;
    let mut k_points = data_rdd.take(k).unwrap();
    let mut temp_dist = 100.0;
    while temp_dist > converge_dist && iter < 5 {
        let k_points_c = k_points.clone();
        let closest = data_rdd.map(
            Fn!(move |p| {
                (closest_point(&p, &k_points_c), (p, 1))
            }));
        let point_stats = closest.reduce_by_key(Fn!(|(a, b)| merge_results(a, b)), 1);
        let new_points = point_stats.map(
            Fn!(|pair: (usize, (Vec<f64>, i32))|
                (pair.0, pair.1.0.iter().map(|x| x * 1.0 / pair.1.1 as f64).collect::<Vec<_>>())
            ), 
        ).collect().unwrap();
        println!("new_points = {:?}", new_points);
        let new_points = new_points.into_iter().collect::<HashMap<usize, Vec<f64>>>();
        temp_dist = 0.0;
        for i in 0..k as usize {
            temp_dist += squared_distance(&k_points[i], &new_points[&i]);
        }
    
        for (idx, point) in new_points {
            k_points[idx] = point;
        }
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("temp_dist = {:?}, time at iter {:?}: {:?}", temp_dist, iter, dur);

        iter += 1;
    }
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("total time {:?} s, k_points = {:?}", dur, k_points);
    Ok(())
}