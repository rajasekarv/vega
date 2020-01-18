use native_spark::*;

use std::sync::Arc;

extern crate serde_closure;
use once_cell::sync::Lazy;
use native_spark::rdd::CoGroupedRdd;
use native_spark::partitioner::HashPartitioner;
use serde_traitobject::Arc as SerArc;

static CONTEXT: Lazy<Arc<Context>> = Lazy::new(|| Context::new().unwrap());

#[test]
fn test_union() {
    let sc = CONTEXT.clone();
    let join = || {
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
        col2.join(col1.clone(), 4)
    };
    let join1 = join();
    let join2 = join();
    let res = join1.union(join2.get_rdd()).unwrap().collect().unwrap();
    assert_eq!(res.len(), 12);
}

#[test]
fn test_union_with_unique_partitioner() {
    let sc = CONTEXT.clone();
    let partitioner = HashPartitioner::<i32>::new(2);
    let co_grouped = || {
        let rdd = vec![
            (1i32, "A".to_string()),
            (2, "B".to_string()),
            (3, "C".to_string()),
            (4, "D".to_string()),
        ];
        let rdd0 = SerArc::new(sc.parallelize(rdd.clone(), 2))
            as SerArc<dyn Rdd<Item = (i32, String)>>;
        let rdd1 =
            SerArc::new(sc.parallelize(rdd, 2)) as SerArc<dyn Rdd<Item = (i32, String)>>;
        CoGroupedRdd::<i32>::new(
            vec![rdd0.get_rdd_base().into(), rdd1.get_rdd_base().into()],
            Box::new(partitioner.clone()),
        )
    };
    let rdd0 = co_grouped();
    let rdd1 = co_grouped();
    let res = rdd0.union(rdd1.get_rdd()).unwrap().collect().unwrap();
    assert_eq!(res.len(), 8);
}
