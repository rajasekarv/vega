use fast_spark::*;
#[macro_use]
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
