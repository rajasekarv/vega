use native_spark::*;
extern crate serde_closure;

#[test]
fn test_group_by() {
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
    let mut res = g.collect();
    res.sort();
    println!("res {:?}", res);
    sc.drop_executors();

    let expected = vec![
        ("x".to_string(), vec![1, 2, 3, 4, 5, 6, 7]),
        ("y".to_string(), vec![1, 2, 3, 4, 5, 6, 7, 8])];
    assert_eq!(expected, res);
}

#[test]
fn test_join() {
    let sc = Context::new("local");
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
    let mut res = inner_joined_rdd.collect();
    println!("res {:?}", res);
    res.sort();
    sc.drop_executors();

    let expected = vec![
        (1, "A1", "A", "B"),
        (1, "A2", "A", "B"),
        (2, "B1", "C", "D"),
        (2, "B2", "C", "D"),
        (3, "C1", "E", "F"),
        (3, "C2", "E", "F")]
        .iter()
        .map(|tuple| (tuple.0, (tuple.1.to_string(), (tuple.2.to_string(), tuple.3.to_string()))))
        .collect::<Vec<_>>();
    assert_eq!(expected, res);
}