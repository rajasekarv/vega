use vega::*;

fn main() -> Result<()> {
    let sc = Context::new()?;
    let col = sc.make_rdd((0..10).collect::<Vec<_>>(), 32);
    //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.
    let vec_iter = col.map(Fn!(|i| (0..i).collect::<Vec<_>>()));
    let res = vec_iter.collect().unwrap();
    println!("result: {:?}", res);
    Ok(())
}
