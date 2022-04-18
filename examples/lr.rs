use std::time::Instant;
use std::path::PathBuf;
use vega::*;
use rand::Rng;
use rand_distr::{Normal, Distribution};
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct Point {
    x: Vec<f32>,
    y: f32,
}

fn main() -> Result<()> {
    let sc = Context::new()?;

    let mut rng = rand::thread_rng();
    let dim = 5;
    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Point>>(&file).unwrap()  //Item = Point
    }));

    let dir = PathBuf::from("/opt/data/pt_lr_1");
    let mut points_rdd = sc.read_source(LocalFsReaderConfig::new(dir).num_partitions_per_executor(1), deserializer)
        .flat_map(Fn!(|v: Vec<Point>| Box::new(v.into_iter()) as Box<dyn Iterator<Item = _>>));
    let mut w = (0..dim).map(|_| rng.gen::<f32>()).collect::<Vec<_>>();  
    let iter_num = 3;
    let now = Instant::now();
    for i in 0..iter_num {
        let w_c = w.clone();
        let g = points_rdd.map(Fn!(move |p: Point| {
                let y = p.y;    
                p.x.iter().zip(w.iter())
                    .map(|(&x, &w): (&f32, &f32)| x * (1f32/(1f32+(-y * (w * x)).exp())-1f32) * y)
                    .collect::<Vec<_>>()
            }))
        .reduce(Fn!(|x: Vec<f32>, y: Vec<f32>| x.into_iter()
            .zip(y.into_iter())
            .map(|(x, y)| x + y)
            .collect::<Vec<_>>()))
        .unwrap();
        w = w_c.into_iter()
            .zip(g.unwrap().into_iter())
            .map(|(x, y)| x-y)
            .collect::<Vec<_>>();
        println!("{:?}: w = {:?}", i, w);
    } 
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("total time {:?} s", dur);
    println!("w = {:?}", w);

    Ok(())
}
