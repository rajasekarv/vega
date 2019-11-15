#![allow(where_clauses_object_safety)]
use native_spark::*;
#[macro_use]
extern crate serde_closure;

fn get_mode() -> String {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.get(0) {
        Some(val) if val == "distributed" => val.to_owned(),
        _ => "local".to_owned(),
    }
}

fn main() -> Result<()> {
    let sc = Context::new(&get_mode())?;
    let col = sc.make_rdd((0..10).collect::<Vec<_>>(), 32);
    //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.
    let vec_iter = col.map(Fn!(|i| (0..i).collect::<Vec<_>>()));
    let res = vec_iter.collect().unwrap();
    println!("result: {:?}", res);
    Ok(())
}
