#![allow(where_clauses_object_safety)]
use native_spark::*;

fn get_mode() -> String {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.get(0) {
        Some(val) if val == "distributed" => val.to_owned(),
        _ => "local".to_owned(),
    }
}

fn main() -> Result<()> {
    let sc = Context::new(&get_mode())?;
    let col1 = vec![
        (1, ("A".to_string(), "B".to_string())),
        (2, ("C".to_string(), "D".to_string())),
        (3, ("E".to_string(), "F".to_string())),
        (4, ("G".to_string(), "H".to_string())),
    ];
    let col1 = sc.parallelize(col1, 4);
    let col2 = vec![
        (1, "A1".to_string()),
        (1, "A2".to_string()),
        (2, "B1".to_string()),
        (2, "B2".to_string()),
        (3, "C1".to_string()),
        (3, "C2".to_string()),
    ];
    let col2 = sc.parallelize(col2, 4);
    let inner_joined_rdd = col2.join(col1.clone(), 4);
    let res = inner_joined_rdd.collect().unwrap();
    println!("res {:?}", res);
    Ok(())
}
