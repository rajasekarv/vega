#![allow(where_clauses_object_safety)]
use native_spark::*;
#[macro_use]
extern crate serde_closure;
use chrono::prelude::*;

use std::fs;
use std::io::{BufRead, BufReader};

fn main() -> Result<()> {
    let sc = Context::new()?;
    let files = fs::read_dir("csv_folder")
        .unwrap()
        .map(|x| x.unwrap().path().to_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    let len = files.len();
    let files = sc.make_rdd(files, len);
    let lines = files.flat_map(Fn!(|file| {
        let f = fs::File::open(file).expect("unable to create file");
        let f = BufReader::new(f);
        Box::new(f.lines().map(|line| line.unwrap())) as Box<dyn Iterator<Item = String>>
    }));
    let line = lines.map(Fn!(|line: String| {
        let line = line.split(' ').collect::<Vec<_>>();
        let mut time: i64 = line[8].parse::<i64>().unwrap();
        time /= 1000;
        let time = Utc.timestamp(time, 0).hour();
        (
            (line[0].to_string(), line[1].to_string(), time),
            (line[7].parse::<i64>().unwrap(), 1.0),
        )
    }));
    let sum = line.reduce_by_key(Fn!(|((vl, cl), (vr, cr))| (vl + vr, cl + cr)), 1);
    let avg = sum.map(Fn!(|(k, (v, c))| (k, v as f64 / c)));
    let res = avg.collect().unwrap();
    println!("{:?}", &res[0]);
    Ok(())
}
