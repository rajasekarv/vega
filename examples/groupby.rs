#![allow(where_clauses_object_safety)]
use native_spark::*;
extern crate serde_closure;

fn main() {
    let sc = Context::new("local");
    let vec = vec![
        ("x".to_string(), 1),
        ("x".to_string(), 2),
        ("x".to_string(), 3),
        ("x".to_string(), 4),
        ("x".to_string(), 5),
        ("x".to_string(), 6),
        ("x".to_string(), 7),
        ("y".to_string(), 1),
        ("y".to_string(), 2),
        ("y".to_string(), 3),
        ("y".to_string(), 4),
        ("y".to_string(), 5),
        ("y".to_string(), 6),
        ("y".to_string(), 7),
        ("y".to_string(), 8),
    ];
    let r = sc.make_rdd(vec, 4);
    let g = r.group_by_key(4);
    let res = g.collect();
    println!("res {:?}", res);
    sc.drop_executors();
}
