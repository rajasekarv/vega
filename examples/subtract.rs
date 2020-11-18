use std::sync::Arc;
use vega::*;

fn main() -> Result<()> {
    let sc = Context::new()?;
    let col1 = vec![1, 2, 3, 4, 5, 10, 12, 13, 19, 0];

    let col2 = vec![3, 4, 5, 6, 7, 8, 11, 13];

    let first = sc.parallelize(col1, 4);
    let second = sc.parallelize(col2, 4);
    let ans = first.subtract(Arc::new(second));

    for elem in ans.collect().iter() {
        println!("{:?}", elem);
    }

    Ok(())
}
