use std::time::{Duration, Instant};
use vega::*;

fn main() -> Result<()> {
    let sc = Context::new()?;
    let col = sc.make_rdd((0..10000), 2);
    let start = Instant::now();
    for i in 0..100 {
        let top = col.top(1000);
    }
    let duration = start.elapsed();
    println!("Time elapsed in top() is: {:?}", duration);
    let start = Instant::now();
    for i in 0..100 {
        let top = col.top_iter(1000);
    }
    let duration = start.elapsed();
    println!("Time elapsed in top_iter() is: {:?}", duration);
    let start = Instant::now();
    for i in 0..100 {
        let top = col.top(1000);
    }
    let duration = start.elapsed();
    println!("Time elapsed in top() is: {:?}", duration);
    let start = Instant::now();
    for i in 0..100 {
        let top = col.top_iter(1000);
    }
    let duration = start.elapsed();
    println!("Time elapsed in top_iter() is: {:?}", duration);
    Ok(())
}
