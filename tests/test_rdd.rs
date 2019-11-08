use std::fs::{create_dir_all, remove_dir_all, File};
use std::io::prelude::*;

use native_spark::io::*;
use native_spark::*;

#[macro_use]
extern crate serde_closure;
use lazy_static::*;

lazy_static! {
    static ref WORK_DIR: std::path::PathBuf = std::env::temp_dir();
}
const TEST_DIR: &str = "ns_test_dir";

fn set_up(file_name: &str) {
    let temp_dir = WORK_DIR.join(TEST_DIR);
    println!("Creating tests in dir: {}", (&temp_dir).to_str().unwrap());
    create_dir_all(&temp_dir).unwrap();

    let fixture =
        b"This is some textual test data.\nCan be converted to strings and there are two lines.";

    let mut f = File::create(temp_dir.join(file_name)).unwrap();
    f.write_all(fixture).unwrap();
}

fn tear_down() {
    // Clean up files
    let temp_dir = WORK_DIR.join(TEST_DIR);
    remove_dir_all(temp_dir).unwrap();
}

fn test_runner<T>(test: T)
where
    T: FnOnce() -> () + std::panic::UnwindSafe,
{
    let result = std::panic::catch_unwind(|| test());
    tear_down();
    assert!(result.is_ok())
}

#[test]
fn test_make_rdd() {
    // for distributed mode, use Context::new("distributed")
    let sc = Context::new("local").unwrap();
    let col = sc.make_rdd((0..10).collect::<Vec<_>>(), 32);
    //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.
    let vec_iter = col.map(Fn!(|i| (0..i).collect::<Vec<_>>()));
    let res = vec_iter.collect();

    let expected = (0..10)
        .map(|i| (0..i).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    println!("{:?}", res);
    assert_eq!(expected, res);
}

#[test]
fn test_take() {
    let sc = Context::new("local").unwrap();
    let col1 = vec![1, 2, 3, 4, 5, 6];
    let col1_rdd = sc.parallelize(col1, 4);

    let taken_1 = col1_rdd.take(1);
    assert_eq!(taken_1.len(), 1);

    let taken_3 = col1_rdd.take(3);
    assert_eq!(taken_3.len(), 3);

    let taken_7 = col1_rdd.take(7);
    assert_eq!(taken_7.len(), 6);

    let col2: Vec<i32> = vec![];
    let col2_rdd = sc.parallelize(col2, 4);
    let taken_0 = col2_rdd.take(1);
    assert!(taken_0.is_empty());
}

#[test]
fn test_first() {
    let sc = Context::new("local").unwrap();
    let col1 = vec![1, 2, 3, 4];
    let col1_rdd = sc.parallelize(col1, 4);

    let taken_1 = col1_rdd.first();
    assert!(taken_1.is_ok());

    // TODO: uncomment when it returns a proper error instead of panicking
    // let col2: Vec<i32> = vec![];
    // let col2_rdd = sc.parallelize(col2, 4);
    // let taken_0 = col2_rdd.first();
    // assert!(taken_0.is_err());
}

#[test]
fn test_read_files() {
    // Single file test
    let file_name = "test_file_01";
    let file_path = WORK_DIR.join(TEST_DIR).join(file_name);
    set_up(file_name);

    let processor = Fn!(|reader: DistributedLocalReader| {
        let mut files: Vec<_> = reader.into_iter().collect();
        assert_eq!(files.len(), 1);

        // do stuff with the read files ...
        let parsed: Vec<_> = String::from_utf8(files.pop().unwrap())
            .unwrap()
            .lines()
            .map(|s| s.to_string())
            .collect();

        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0], "This is some textual test data.");

        // return parsed stuff
        parsed
    });

    test_runner(|| {
        let sc = Context::new("local").unwrap();
        let result = sc
            .read_files(LocalFsReaderConfig::new(file_path), processor)
            .collect();
        assert_eq!(result[0].len(), 2);
    });

    // Multiple files test
    let _multi_files: Vec<_> = (0..10)
        .map(|idx| {
            let f_name = format!("test_file_{}", idx);
            let path = WORK_DIR.join(TEST_DIR).join(f_name.as_str());
            set_up(path.as_path().to_str().unwrap());
        })
        .collect::<Vec<_>>();

    let processor = Fn!(|reader: DistributedLocalReader| {
        let files: Vec<_> = reader.into_iter().collect();

        // do stuff with the read files ...
        let parsed: Vec<_> = files
            .into_iter()
            .map(|f| String::from_utf8(f).unwrap())
            .flat_map(|s| s.lines().map(|l| l.to_owned()).collect::<Vec<_>>())
            .collect();

        // return parsed stuff
        parsed
    });

    test_runner(|| {
        let sc = Context::new("local").unwrap();
        let files = sc.read_files(LocalFsReaderConfig::new(WORK_DIR.join(TEST_DIR)), processor);
        let result: Vec<_> = files.collect().into_iter().flatten().collect();
        assert_eq!(result.len(), 20);
    });
}

#[test]
fn test_distinct() {
    use std::collections::HashSet;
    let sc = Context::new("local").unwrap();
    let rdd = sc.parallelize(vec![1, 2, 2, 2, 3, 3, 3, 4, 4, 5], 3);
    assert!(rdd.distinct().collect().len() == 5);
    assert!(
        rdd.distinct().collect().into_iter().collect::<HashSet<_>>()
            == rdd.distinct().collect().into_iter().collect::<HashSet<_>>()
    );
    assert!(
        rdd.distinct_with_num_partitions(2)
            .collect()
            .into_iter()
            .collect::<HashSet<_>>()
            == rdd.distinct().collect().into_iter().collect::<HashSet<_>>()
    );
    assert!(
        rdd.distinct_with_num_partitions(10)
            .collect()
            .into_iter()
            .collect::<HashSet<_>>()
            == rdd.distinct().collect().into_iter().collect::<HashSet<_>>()
    );
}

#[test]
fn test_partition_wise_sampling() {
    let sc = Context::new("local").unwrap();
    // w/o replace & num < sample
    {
        let rdd = sc.parallelize(vec![1, 2, 3, 4, 5], 6);
        let result = rdd.take_sample(false, 6, Some(123));
        assert!(result.len() == 5);
        // guaranteed with this seed:
        assert!(result[0] > result[1]);
    }

    // replace & Poisson & no-GapSampling
    {
        // high enough samples param to guarantee drawing >1 times w/ replacement
        let rdd = sc.parallelize((0_i32..100).collect::<Vec<_>>(), 5);
        let result = rdd.take_sample(true, 80, None);
        assert!(result.len() == 80);
    }

    // no replace & Bernoulli + GapSampling
    {
        let rdd = sc.parallelize((0_i32..100).collect::<Vec<_>>(), 5);
        let result = rdd.take_sample(false, 10, None);
        assert!(result.len() == 10);
    }
}
