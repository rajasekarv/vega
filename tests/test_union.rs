use native_spark::io::*;
use native_spark::*;

use std::fs::{create_dir_all, remove_dir_all, File};
use std::io::prelude::*;
use std::sync::Arc;

#[macro_use]
extern crate serde_closure;
use once_cell::sync::Lazy;

static CONTEXT: Lazy<Arc<Context>> = Lazy::new(|| Context::new().unwrap());

#[test]
fn test_error() {
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
