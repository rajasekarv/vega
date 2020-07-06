use std::collections::HashSet;
use std::fs::{create_dir_all, File};
use std::io::prelude::*;
use std::sync::Arc;

use once_cell::sync::Lazy;
use vega::io::*;
use vega::partitioner::HashPartitioner;
use vega::rdd::CoGroupedRdd;
use vega::*;

static CONTEXT: Lazy<Arc<Context>> = Lazy::new(|| Context::new().unwrap());
//static AGGREGATOR: Lazy<Arc<Aggregator<data,dat1,dat2>>> = Lazy::new(|| Aggregator::new().unwrap());
static WORK_DIR: Lazy<std::path::PathBuf> = Lazy::new(std::env::temp_dir);
const TEST_DIR: &str = "vega_test_dir";

#[allow(unused_must_use)]
fn set_up(file_name: &str) {
    let temp_dir = WORK_DIR.join(TEST_DIR);
    println!("Creating tests in dir: {}", (&temp_dir).to_str().unwrap());
    create_dir_all(&temp_dir);

    let fixture =
        b"This is some textual test data.\nCan be converted to strings and there are two lines.";

    if !std::path::Path::new(file_name).exists() {
        let mut f = File::create(temp_dir.join(file_name)).unwrap();
        f.write_all(fixture).unwrap();
    }
}

#[test]
fn test_make_rdd() -> Result<()> {
    let sc = CONTEXT.clone();
    let col = sc.make_rdd((0..10).collect::<Vec<_>>(), 32);
    let vec_iter = col.map(Fn!(|i| (0..i).collect::<Vec<_>>()));
    let res = vec_iter.collect()?;
    let expected = (0..10)
        .map(|i| (0..i).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(expected, res);
    Ok(())
}

#[test]
fn test_basic_operations() -> Result<()> {
    let sc = CONTEXT.clone();
    let nums = sc.make_rdd(vec![1i32, 2, 3, 4], 2);
    assert_eq!(nums.number_of_splits(), 2);
    assert_eq!(nums.collect()?, vec![1i32, 2, 3, 4]);
    //  assert_eq!(nums.iterator()?, vec![1i32,2,3,4]);
    let dups = sc.make_rdd(vec![1i32, 1, 2, 2, 3, 3, 4, 4], 2);
    assert_eq!(dups.distinct().count()?, 4);
    assert_eq!(nums.reduce(Fn!(|x: i32, y: i32| x + y))?, Some(10));
    assert_eq!(nums.fold(0, Fn!(|x: i32, y: i32| x + y))?, 10);
    assert_eq!(
        nums.map(Fn!(|x: i32| x.to_string())).collect()?,
        vec![
            "1".to_string(),
            "2".to_string(),
            "3".to_string(),
            "4".to_string()
        ]
    );
    //assert_eq!(nums.filter(Fn!(|x:i32| x>2)).collect(),vec![3i32,4])
    assert_eq!(
        nums.flat_map(Fn!(
            |x: i32| Box::new((1..(1 + x))) as Box<dyn Iterator<Item = _>>
        ))
        .collect()?,
        vec![1i32, 1, 2, 1, 2, 3, 1, 2, 3, 4]
    );
    assert_eq!(
        nums.union(nums.get_rdd())?.collect()?,
        vec![1i32, 2, 3, 4, 1, 2, 3, 4]
    );
    assert_eq!(nums.glom().collect()?, vec![vec![1i32, 2], vec![3i32, 4]]);
    // assert_eq!(nums.collect(Fn!(|x:i32| x>=3 )).collect()?, vec![3i32,4]); --method not implemented
    // assert_eq!(nums.gr) -- no keyby implementation
    //assert_eq!(!nums.isEmpty()) -- no isEmpty
    // no min, max functions

    Ok(())
}

#[test]
fn test_filter() {
    let sc = CONTEXT.clone();
    let col1 = vec![13, 28, 3, 4, 51, 103, 12, 113, 19];

    let rdd = sc.parallelize(col1, 2);

    let predicate = Fn!(|&i: &usize| i > 100);

    assert_eq!(rdd.filter(predicate).collect().unwrap(), vec![103, 113]);
}

#[test]
fn test_map_partitions() -> Result<()> {
    let sc = CONTEXT.clone();
    let rdd = sc.make_rdd(vec![1, 2, 3, 4], 2);
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
    let col1_rdd = sc.parallelize(col1, 4);

    let taken_1 = col1_rdd.take(1)?;
    assert_eq!(taken_1.len(), 1);

    let taken_3 = col1_rdd.take(3)?;
    assert_eq!(taken_3.len(), 3);

    let taken_7 = col1_rdd.take(7)?;
    assert_eq!(taken_7.len(), 6);

    let col2: Vec<i32> = vec![];
    let col2_rdd = sc.parallelize(col2, 4);
    let taken_0 = col2_rdd.take(1)?;
    assert!(taken_0.is_empty());
    Ok(())
}

#[test]
fn test_first() {
    let sc = CONTEXT.clone();
    let col1 = vec![1, 2, 3, 4];
    let col1_rdd = sc.parallelize(col1, 4);

    let taken_1 = col1_rdd.first();
    assert!(taken_1.is_ok());

    let col2: Vec<i32> = vec![];
    let col2_rdd = sc.parallelize(col2, 4);
    let taken_0 = col2_rdd.first();
    assert!(taken_0.is_err());
}

#[test]
fn test_read_files_bytes() -> Result<()> {
    let deserializer = Fn!(|file: Vec<u8>| -> Vec<String> {
        // do stuff with the read files ...
        let parsed: Vec<_> = String::from_utf8(file)
            .unwrap()
            .lines()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0], "This is some textual test data.");
        // return lines
        parsed
    });

    // Single file test
    let file_name = "test_file_1";
    let file_path = WORK_DIR.join(TEST_DIR).join(file_name);
    set_up(file_name);

    let context = CONTEXT.clone();
    let result = context
        .read_source(LocalFsReaderConfig::new(file_path), deserializer)
        .collect()
        .unwrap();
    assert_eq!(result[0].len(), 2);

    // Multiple files test
    (0..10).for_each(|idx| {
        let f_name = format!("test_file_{}", idx);
        let path = WORK_DIR.join(TEST_DIR).join(f_name.as_str());
        set_up(path.as_path().to_str().unwrap());
    });

    let sc = CONTEXT.clone();
    let files = sc.read_source(
        LocalFsReaderConfig::new(WORK_DIR.join(TEST_DIR)),
        deserializer,
    );
    let result: Vec<_> = files.collect().unwrap().into_iter().flatten().collect();
    assert_eq!(result.len(), 20);

    Ok(())
}

#[test]
fn test_read_files() -> Result<()> {
    let deserializer = Fn!(|file: std::path::PathBuf| {
        let mut file = File::open(file).unwrap();
        let mut content = String::new();
        file.read_to_string(&mut content).unwrap();
        let parsed: Vec<_> = content.lines().map(|s| s.to_string()).collect();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0], "This is some textual test data.");
        parsed
    });

    let file_name = "test_file_1";
    let file_path = WORK_DIR.join(TEST_DIR).join(file_name);
    set_up(file_name);
    let context = CONTEXT.clone();
    let result = context
        .read_source(LocalFsReaderConfig::new(file_path), deserializer)
        .collect()
        .unwrap();
    assert_eq!(result[0].len(), 2);

    Ok(())
}

#[test]
fn test_distinct() -> Result<()> {
    use std::collections::HashSet;
    let sc = CONTEXT.clone();
    let rdd = sc.parallelize(vec![1, 2, 2, 2, 3, 3, 3, 4, 4, 5], 3);
    assert_eq!(rdd.distinct().collect()?.len(), 5);
    assert_eq!(
        rdd.distinct()
            .collect()?
            .into_iter()
            .collect::<HashSet<_>>(),
        rdd.distinct()
            .collect()?
            .into_iter()
            .collect::<HashSet<_>>()
    );
    assert_eq!(
        rdd.distinct_with_num_partitions(2)
            .collect()?
            .into_iter()
            .collect::<HashSet<_>>(),
        rdd.distinct()
            .collect()?
            .into_iter()
            .collect::<HashSet<_>>()
    );
    assert_eq!(
        rdd.distinct_with_num_partitions(10)
            .collect()?
            .into_iter()
            .collect::<HashSet<_>>(),
        rdd.distinct()
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
        let rdd = sc.parallelize(vec![1, 2, 3, 4, 5], 6);
        let result = rdd.take_sample(false, 6, Some(123))?;
        assert!(result.len() == 5);
        // guaranteed with this seed:
        assert!(result[0] > result[1]);
    }

    // replace & Poisson & no-GapSampling
    {
        // high enough samples param to guarantee drawing >1 times w/ replacement
        let rdd = sc.parallelize((0_i32..100).collect::<Vec<_>>(), 5);
        let result = rdd.take_sample(true, 80, None)?;
        assert!(result.len() == 80);
    }

    // no replace & Bernoulli + GapSampling
    {
        let rdd = sc.parallelize((0_i32..100).collect::<Vec<_>>(), 5);
        let result = rdd.take_sample(false, 10, None)?;
        assert!(result.len() == 10);
    }
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

#[test]
fn test_coalesced() -> Result<()> {
    let sc = CONTEXT.clone();

    // do not shuffle
    {
        let rdd = sc.parallelize(vec![1; 101], 101);
        let res = rdd.coalesce(5, false).glom().collect()?;
        assert_eq!(res.len(), 5);
        assert_eq!(res[0].iter().sum::<u8>(), 20);
        assert_eq!(res[4].iter().sum::<u8>(), 21);
    }

    // shuffle and increase num partitions
    {
        let rdd = sc.parallelize(vec![1; 100], 20);
        let res = rdd.repartition(100).glom().collect()?;
        assert_eq!(res.len(), 100);
    }

    Ok(())
}

#[test]
fn test_union() -> Result<()> {
    let sc = CONTEXT.clone();

    let rdd0 = sc.parallelize(vec![1i32, 2, 3, 4], 2);
    let rdd1 = sc.parallelize(vec![5i32, 6, 7, 8], 2);
    let res = rdd0.union(rdd1.get_rdd())?;
    assert_eq!(res.collect()?.len(), 8);

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
    let res = join1.union(join2.get_rdd())?.collect()?;
    assert_eq!(res.len(), 12);

    let nums = sc.make_rdd(vec![1i32, 2, 3, 4], 2);
    assert_eq!(
        Context::union(&[nums.get_rdd()])?.collect()?,
        vec![1i32, 2, 3, 4]
    );
    assert_eq!(
        Context::union(&[nums.get_rdd(), nums.get_rdd()])?.collect()?,
        vec![1i32, 2, 3, 4, 1, 2, 3, 4]
    );

    Ok(())
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
        let rdd0 = SerArc::new(sc.parallelize(rdd.clone(), 2));
        let rdd1 = SerArc::new(sc.parallelize(rdd, 2));
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

#[test]
fn test_zip() {
    let sc = CONTEXT.clone();
    let col1 = vec![1, 2, 3, 4, 5];
    let col2 = vec![
        "5a".to_string(),
        "4b".to_string(),
        "3c".to_string(),
        "2d".to_string(),
        "1a".to_string(),
    ];

    let first = sc.parallelize(col1, 3);
    let second = sc.parallelize(col2, 3);
    let res = first.zip(Arc::new(second)).collect().unwrap();

    let expected = vec![
        (1, "5a".to_string()),
        (2, "4b".to_string()),
        (3, "3c".to_string()),
        (4, "2d".to_string()),
        (5, "1a".to_string()),
    ];
    assert_eq!(res, expected);
}

#[test]
fn test_intersection_with_num_partitions() {
    let sc = CONTEXT.clone();

    let col1 = vec![1, 2, 3, 4, 5, 10, 12, 13, 19, 0];

    let col2 = vec![3, 4, 5, 6, 7, 8, 11, 13];

    let first = sc.parallelize(col1, 2);
    let second = sc.parallelize(col2, 4);
    let mut res = first
        .intersection_with_num_partitions(Arc::new(second), 3)
        .collect()
        .unwrap();

    res.sort();

    let expected = vec![3, 4, 5, 13];
    assert_eq!(res, expected);
}

#[test]
fn test_intersection() {
    let sc = CONTEXT.clone();

    let col1 = vec![1, 2, 3, 4, 5, 10, 12, 13, 19, 0];

    let col2 = vec![3, 4, 5, 6, 7, 8, 11, 13];

    let first = sc.parallelize(col1, 2);
    let second = sc.parallelize(col2, 4);
    let mut res = first.intersection(Arc::new(second)).collect().unwrap();

    res.sort();

    let expected = vec![3, 4, 5, 13];
    assert_eq!(res, expected);
}

#[test]
fn test_range() {
    let sc = CONTEXT.clone();
    let rdd = sc.range(1, 12, 1, 3);
    let res = rdd.collect().unwrap();

    let expected: Vec<_> = (1..=12).collect();
    assert_eq!(res, expected);
}

#[test]
fn count_aprox() -> Result<()> {
    let sc = CONTEXT.clone();

    // this should complete  and return the final value, so confidence should be 100%
    let time_out = std::time::Duration::from_nanos(100);
    let count = sc
        .range(1, 10_000, 1, 100)
        .count_approx(time_out, Some(0.9))?;
    assert_eq!(
        count.get_final_value()?,
        BoundedDouble::from((10_000.0, 1.0, 10_000.0, 10_000.0))
    );

    // no results
    let count = sc
        .make_rdd(Vec::<i32>::new(), 1)
        .count_approx(time_out, Some(0.9))?;
    assert_eq!(
        count.get_final_value()?,
        BoundedDouble::from((0.0, 0.0, 0.0, f64::MAX))
    );

    // only check the partial result
    let time_out = std::time::Duration::from_secs(2);
    let count = sc
        .make_rdd(vec![0i32; 10_000], 10)
        .count_approx(time_out, Some(0.9))?;
    let confidence = count.initial_value.confidence;
    let count = count.initial_value.mean;
    eprintln!("count: {}, confidence: {}", count, confidence);
    // assert!(confidence != 1.0 && confidence != 0.0);

    Ok(())
}

#[test]
fn count_by_value_aprox() -> Result<()> {
    let sc = CONTEXT.clone();

    // this should complete  and return the final value, so confidence should be 100%
    let time_out = std::time::Duration::from_nanos(100);
    let mut res: Vec<_> = sc
        .make_rdd(vec![1i32, 2, 2, 3, 3, 3], 6)
        .count_by_value_aprox(time_out, Some(0.9))?
        .get_final_value()?
        .into_iter()
        .map(|(k, v)| (k, v.mean))
        .collect();
    res.sort_by(|e1, e2| e1.0.cmp(&e2.0));

    let expected = vec![(1i32, 1.0f64), (2, 2.0), (3, 3.0)];
    assert_eq!(res, expected);
    Ok(())
}

#[test]
fn test_is_empty() {
    let sc = CONTEXT.clone();
    let v: Vec<usize> = Vec::new();
    let rdd = sc.parallelize(v, 1);

    assert_eq!(rdd.is_empty(), true);
}

#[test]
fn test_max_min() {
    let sc = CONTEXT.clone();

    let col1 = vec![13, 28, 3, 4, 51, 103, 12, 113, 19];

    let rdd = sc.parallelize(col1, 2);

    assert_eq!(rdd.max().unwrap().unwrap(), 113);
    assert_eq!(rdd.min().unwrap().unwrap(), 3);
}

#[test]
fn test_key_by() {
    let sc = CONTEXT.clone();

    let col1 = vec![3, 4, 5];
    let rdd = sc.parallelize(col1, 2);

    let res = rdd.key_by(Fn!(|&i: &usize| i * 10)).collect().unwrap();

    assert_eq!(res, vec![(3, 30), (4, 40), (5, 50)]);
}

#[test]
fn test_random_split() {
    let sc = CONTEXT.clone();

    let rdd = sc.range(1, 600, 1, 3);

    let weights: Vec<f64> = vec![1.0, 2.0, 3.0];
    let rdds: Vec<Vec<u64>> = rdd
        .random_split(weights, None)
        .iter()
        .map(|rdd| rdd.collect().unwrap())
        .collect();
    let rdd_lengths: Vec<i64> = rdds.iter().map(|v| v.len() as i64).collect();

    // Total number of splited RDDs should be same as the length of weights.
    assert_eq!(rdds.len(), 3);

    // Total count of of elements of all splited RDDs shall be equal to
    // the total count of the original RDD.
    assert_eq!(rdd_lengths.iter().sum::<i64>(), 600);

    // The count of elements in each splitted RDD shall match its assigned weight.
    assert!((rdd_lengths[0] as i64 - 100).abs() < 50);
    assert!((rdd_lengths[1] as i64 - 200).abs() < 50);
    assert!((rdd_lengths[2] as i64 - 300).abs() < 50);

    // The splitted RDDs shall be disjoint sets
    assert!(rdds[0].iter().all(|i| !rdds[1].contains(i)));
    assert!(rdds[0].iter().all(|i| !rdds[2].contains(i)));
    assert!(rdds[1].iter().all(|i| !rdds[2].contains(i)));
}

#[test]
fn test_top() {
    let sc = CONTEXT.clone();

    let col1 = vec![13, 28, 3, 4, 51, 108, 12, 113, 19];
    let rdd = sc.parallelize(col1, 4);
    let res: Vec<usize> = rdd.top(3).unwrap();
    assert_eq!(res, vec![113, 108, 51]);
}

#[test]
fn test_take_ordered() {
    let sc = CONTEXT.clone();

    let col1 = vec![13, 28, 3, 4, 51, 108, 12, 113, 19];
    let rdd = sc.parallelize(col1, 4);

    let res: Vec<usize> = rdd.take_ordered(3).unwrap();
    assert_eq!(res, vec![3, 4, 12]);
}

#[test]
fn test_subtract() {
    let sc = CONTEXT.clone();
    let col1 = vec![1, 2, 3, 4, 5, 10, 12, 13, 19, 0];

    let col2 = vec![3, 4, 5, 6, 7, 8, 11, 13];

    let first = sc.parallelize(col1, 4);
    let second = sc.parallelize(col2, 4);
    let ans = first.subtract(Arc::new(second));
    // assert_eq!(HashSet::from_iter(ans.collect().unwrap().iter().cloned()), HashSet::from_iter(vec![19, 12, 10, 1, 0, 2].iter().cloned()));

    let mut expected_vec = vec![19, 12, 10, 1, 0, 2];
    expected_vec.sort();
    let mut actual = ans.collect().unwrap();
    actual.sort();

    

    println!("{:?}",expected_vec);
    println!("{:?}",actual);
        
    assert_eq!(actual,expected_vec)

}
