use native_spark::*;

use std::sync::Arc;

extern crate serde_closure;
use once_cell::sync::Lazy;

static CONTEXT: Lazy<Arc<Context>> = Lazy::new(|| Context::new().unwrap());

#[test]
fn test_group_by() {
    let sc = CONTEXT.clone();
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
    let r = sc.clone().make_rdd(vec, 4);
    let g = r.group_by_key(4);
    let mut res = g.collect().unwrap();
    res.sort();
    println!("res {:?}", res);

    let expected = vec![
        ("x".to_string(), vec![1, 2, 3, 4, 5, 6, 7]),
        ("y".to_string(), vec![1, 2, 3, 4, 5, 6, 7, 8]),
    ];
    assert_eq!(expected, res);
}

#[test]
fn test_join() {
    let sc = CONTEXT.clone();
    let col1 = vec![
        (1, ("A".to_string(), "B".to_string())),
        (2, ("C".to_string(), "D".to_string())),
        (3, ("E".to_string(), "F".to_string())),
        (4, ("G".to_string(), "H".to_string())),
    ];
    let col1 = sc.clone().parallelize(col1, 4);
    let col2 = vec![
        (1, "A1".to_string()),
        (1, "A2".to_string()),
        (2, "B1".to_string()),
        (2, "B2".to_string()),
        (3, "C1".to_string()),
        (3, "C2".to_string()),
    ];
    let col2 = sc.clone().parallelize(col2, 4);
    let inner_joined_rdd = col2.join(col1.clone(), 4);
    let mut res = inner_joined_rdd.collect().unwrap();
    println!("res {:?}", res);
    res.sort();

    let expected = vec![
        (1, "A1", "A", "B"),
        (1, "A2", "A", "B"),
        (2, "B1", "C", "D"),
        (2, "B2", "C", "D"),
        (3, "C1", "E", "F"),
        (3, "C2", "E", "F"),
    ]
    .iter()
    .map(|tuple| {
        (
            tuple.0,
            (
                tuple.1.to_string(),
                (tuple.2.to_string(), tuple.3.to_string()),
            ),
        )
    })
    .collect::<Vec<_>>();
    assert_eq!(expected, res);
}
