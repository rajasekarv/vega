use fast_spark::*;
#[macro_use]
extern crate serde_closure;

#[test]
fn test_make_rdd() {
    // for distributed mode, use Context::new("distributed")
    let sc = Context::new("local");
    let col = sc.make_rdd((0..10).collect::<Vec<_>>(), 32);
    //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.
    let vec_iter = col.map(Fn!(|i| (0..i).collect::<Vec<_>>()));
    let res = vec_iter.collect();
    sc.drop_executors();

    let expected = (0..10).map(|i| (0..i).collect::<Vec<_>>()).collect::<Vec<_>>();
    println!("{:?}", res);
    assert_eq!(expected, res);
}
