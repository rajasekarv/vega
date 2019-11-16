use native_spark::io::*;
use native_spark::*;

use std::fs::{create_dir_all, remove_dir_all, File};
use std::io::prelude::*;
use std::sync::Arc;

#[macro_use]
extern crate serde_closure;
use once_cell::sync::Lazy;

static CONTEXT: Lazy<Arc<Context>> = Lazy::new(|| Context::new().unwrap());
static WORK_DIR: Lazy<std::path::PathBuf> = Lazy::new(std::env::temp_dir);
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
fn test_make_rdd() -> Result<()> {
    // for distributed mode, use Context::new("distributed")
    let sc = CONTEXT.clone();
    let col = sc.clone().make_rdd((0..10).collect::<Vec<_>>(), 32);
    //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.
    let vec_iter = col.map(Fn!(|i| (0..i).collect::<Vec<_>>()));
    col.for_each(Fn!(|i| println!("{:?}", i)))?;
    col.for_each_partition(Fn!(|i: Box<Iterator<Item = i64>>| println!(
        "{:?}",
        i.collect::<Vec<_>>()
    )))?;
    let res = vec_iter.collect()?;

    let expected = (0..10)
        .map(|i| (0..i).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    println!("{:?}", res);
    assert_eq!(expected, res);
    Ok(())
}

#[test]
fn test_map_partitions() -> Result<()> {
    let sc = CONTEXT.clone();
    let rdd = sc.clone().make_rdd(vec![1, 2, 3, 4], 2);
    let partition_sums = rdd
        .map_partitions(Fn!(
            |iter: Box<dyn Iterator<Item = i64>>| Box::new(std::iter::once(iter.sum::<i64>()))
                as Box<dyn Iterator<Item = i64>>
        ))
        .collect()?;
    assert_eq!(partition_sums, vec![3, 7]);
    assert_eq!(rdd.glom().collect()?, vec![vec![1, 2], vec![3, 4]]);
    Ok(())
}

#[test]
fn test_fold() {
    let sc = CONTEXT.clone();
    let rdd = sc.make_rdd((-1000..1000).collect::<Vec<_>>(), 10);
    let f = Fn!(|c, x| c + x);
    // def op: (Int, Int) => Int = (c: Int, x: Int) => c + x
    let sum = rdd.fold(0, f).unwrap();
    assert_eq!(sum, -1000)
}

#[test]
fn test_fold_with_modifying_initial_value() {
    let sc = CONTEXT.clone();
    let rdd = sc
        .make_rdd((-1000..1000).collect::<Vec<i32>>(), 10)
        .map(Fn!(|x| vec![x]));
    let f = Fn!(|mut c: Vec<i32>, x: Vec<i32>| {
        c[0] += x[0];
        c
    });
    let sum = rdd.fold(vec![0], f).unwrap();
    assert_eq!(sum[0], -1000)
}

#[test]
fn test_aggregate() {
    let sc = CONTEXT.clone();
    let pairs = sc.make_rdd(
        vec![
            ("a".to_owned(), 1_i32),
            ("b".to_owned(), 2),
            ("a".to_owned(), 2),
            ("c".to_owned(), 5),
            ("a".to_owned(), 3),
        ],
        2,
    );
    use std::collections::{HashMap, HashSet};
    type StringMap = HashMap<String, i32>;
    let empty_map = StringMap::new();
    let merge_element = Fn!(|mut map: StringMap, pair: (String, i32)| {
        *map.entry(pair.0).or_insert(0) += pair.1;
        map
    });
    let merge_maps = Fn!(|mut map1: StringMap, map2: StringMap| {
        for (key, value) in map2 {
            *map1.entry(key).or_insert(0) += value;
        }
        map1
    });
    let result = pairs
        .aggregate(empty_map, merge_element, merge_maps)
        .unwrap();
    assert_eq!(
        result.into_iter().collect::<HashSet<_>>(),
        vec![
            ("a".to_owned(), 6),
            ("b".to_owned(), 2),
            ("c".to_owned(), 5)
        ]
        .into_iter()
        .collect()
    )
}

#[test]
fn test_take() -> Result<()> {
    let sc = CONTEXT.clone();
    let col1 = vec![1, 2, 3, 4, 5, 6];
    let col1_rdd = sc.clone().parallelize(col1, 4);

    let taken_1 = col1_rdd.take(1)?;
    assert_eq!(taken_1.len(), 1);

    let taken_3 = col1_rdd.take(3)?;
    assert_eq!(taken_3.len(), 3);

    let taken_7 = col1_rdd.take(7)?;
    assert_eq!(taken_7.len(), 6);

    let col2: Vec<i32> = vec![];
    let col2_rdd = sc.clone().parallelize(col2, 4);
    let taken_0 = col2_rdd.take(1)?;
    assert!(taken_0.is_empty());
    Ok(())
}

#[test]
fn test_first() {
    let sc = CONTEXT.clone();
    let col1 = vec![1, 2, 3, 4];
    let col1_rdd = sc.clone().parallelize(col1, 4);

    let taken_1 = col1_rdd.first();
    assert!(taken_1.is_ok());

    let col2: Vec<i32> = vec![];
    let col2_rdd = sc.parallelize(col2, 4);
    let taken_0 = col2_rdd.first();
    assert!(taken_0.is_err());
}

#[test]
fn test_read_files() -> Result<()> {
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
        let sc = CONTEXT.clone();
        let result = sc
            .clone()
            .read_files(LocalFsReaderConfig::new(file_path), processor)
            .collect()
            .unwrap();
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
        let sc = CONTEXT.clone();
        let files = sc
            .clone()
            .read_files(LocalFsReaderConfig::new(WORK_DIR.join(TEST_DIR)), processor);
        let result: Vec<_> = files.collect().unwrap().into_iter().flatten().collect();
        assert_eq!(result.len(), 20);
    });
    Ok(())
}

#[test]
fn test_distinct() -> Result<()> {
    use std::collections::HashSet;
    let sc = CONTEXT.clone();
    let rdd = sc
        .clone()
        .parallelize(vec![1, 2, 2, 2, 3, 3, 3, 4, 4, 5], 3);
    assert!(rdd.distinct().collect()?.len() == 5);
    assert!(
        rdd.distinct()
            .collect()?
            .into_iter()
            .collect::<HashSet<_>>()
            == rdd
                .distinct()
                .collect()?
                .into_iter()
                .collect::<HashSet<_>>()
    );
    assert!(
        rdd.distinct_with_num_partitions(2)
            .collect()?
            .into_iter()
            .collect::<HashSet<_>>()
            == rdd
                .distinct()
                .collect()?
                .into_iter()
                .collect::<HashSet<_>>()
    );
    assert!(
        rdd.distinct_with_num_partitions(10)
            .collect()?
            .into_iter()
            .collect::<HashSet<_>>()
            == rdd
                .distinct()
                .collect()?
                .into_iter()
                .collect::<HashSet<_>>()
    );
    Ok(())
}

#[test]
fn test_partition_wise_sampling() -> Result<()> {
    let sc = CONTEXT.clone();
    // w/o replace & num < sample
    {
        let rdd = sc.clone().parallelize(vec![1, 2, 3, 4, 5], 6);
        let result = rdd.take_sample(false, 6, Some(123))?;
        assert!(result.len() == 5);
        // guaranteed with this seed:
        assert!(result[0] > result[1]);
    }

    // replace & Poisson & no-GapSampling
    {
        // high enough samples param to guarantee drawing >1 times w/ replacement
        let rdd = sc.clone().parallelize((0_i32..100).collect::<Vec<_>>(), 5);
        let result = rdd.take_sample(true, 80, None)?;
        assert!(result.len() == 80);
    }

    // no replace & Bernoulli + GapSampling
    {
        let rdd = sc.clone().parallelize((0_i32..100).collect::<Vec<_>>(), 5);
        let result = rdd.take_sample(false, 10, None)?;
        assert!(result.len() == 10);
    }
    Ok(())
}

#[test]
fn test_union() -> Result<()> {
    let sc = CONTEXT.clone();
    let rdd0 = sc.parallelize(vec![1i32, 2, 3, 4], 2);
    let rdd1 = sc.parallelize(vec![5i32, 6, 7, 8], 2);
    let res = rdd0.union(rdd1)?;
    assert_eq!(res.collect()?.len(), 8);
    Ok(())
}

#[test]
fn test_cartesian() -> Result<()> {
    let sc = CONTEXT.clone();
    let rdd1 = sc.parallelize((0..2).collect::<Vec<_>>(), 2);
    let rdd2 = sc.parallelize("αβ".chars().collect::<Vec<_>>(), 2);

    let res = rdd1.cartesian(rdd2).collect()?;
    itertools::assert_equal(res, vec![(0, 'α'), (0, 'β'), (1, 'α'), (1, 'β')]);
    Ok(())
}
