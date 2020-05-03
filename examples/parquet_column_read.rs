#![allow(where_clauses_object_safety, clippy::single_component_path_imports)]
use chrono::prelude::*;
use itertools::izip;
use parquet::column::reader::get_typed_column_reader;
use parquet::data_type::{ByteArrayType, Int32Type, Int64Type};
use parquet::file::reader::{FileReader, SerializedFileReader};
use vega::*;

use std::fs::File;
use std::path::PathBuf;

fn main() -> Result<()> {
    let context = Context::new()?;
    let deserializer = Fn!(|file: PathBuf| read(file));
    let files = context
        .read_source(LocalFsReaderConfig::new("./parquet_file_dir"), deserializer)
        .flat_map(Fn!(
            |iter: Vec<((i32, String, i64), (i64, f64))>| Box::new(iter.into_iter())
                as Box<dyn Iterator<Item = _>>
        ));
    let sum = files.reduce_by_key(Fn!(|((vl, cl), (vr, cr))| (vl + vr, cl + cr)), 1);
    let avg = sum.map(Fn!(|(k, (v, c))| (k, v as f64 / c)));
    let res = avg.collect().unwrap();
    println!("result: {:?}", &res[0]);
    Ok(())
}

fn read(file: PathBuf) -> Vec<((i32, String, i64), (i64, f64))> {
    let file = File::open(file).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    let batch_size = 500_000 as usize;
    let iter = (0..metadata.num_row_groups()).flat_map(move |i| {
        let row_group_reader = reader.get_row_group(i).unwrap();
        let mut first_reader =
            get_typed_column_reader::<Int32Type>(row_group_reader.get_column_reader(0).unwrap());
        let mut second_reader = get_typed_column_reader::<ByteArrayType>(
            row_group_reader.get_column_reader(1).unwrap(),
        );
        let mut bytes_reader =
            get_typed_column_reader::<Int64Type>(row_group_reader.get_column_reader(7).unwrap());
        let mut time_reader =
            get_typed_column_reader::<Int64Type>(row_group_reader.get_column_reader(8).unwrap());
        let num_rows = metadata.row_group(i).num_rows() as usize;
        println!("num group rows: {}", num_rows);
        let mut chunks = vec![];
        let mut batch_count = 0 as usize;
        while batch_count < num_rows {
            let begin = batch_count;
            let mut end = batch_count + batch_size;
            if end > num_rows {
                end = num_rows as usize;
            }
            chunks.push((begin, end));
            batch_count = end;
        }
        println!("total rows: {}, chunks: {:?}", num_rows, chunks);
        chunks.into_iter().flat_map(move |(begin, end)| {
            let end = end as usize;
            let begin = begin as usize;
            let mut first = vec![Default::default(); end - begin];
            let mut second = vec![Default::default(); end - begin];
            let mut time = vec![Default::default(); end - begin];
            let mut bytes = vec![Default::default(); end - begin];
            first_reader
                .read_batch(batch_size, None, None, &mut first)
                .unwrap();
            second_reader
                .read_batch(batch_size, None, None, &mut second)
                .unwrap();
            time_reader
                .read_batch(batch_size, None, None, &mut time)
                .unwrap();
            bytes_reader
                .read_batch(batch_size, None, None, &mut bytes)
                .unwrap();
            let first = first.into_iter();
            let second = second
                .into_iter()
                .map(|x| unsafe { String::from_utf8_unchecked(x.data().to_vec()) });
            let time = time.into_iter().map(|t| {
                let t = t / 1000;
                i64::from(Utc.timestamp(t, 0).hour())
            });
            let bytes = bytes.into_iter().map(|b| (b, 1.0));
            let key = izip!(first, second, time);
            let value = bytes;
            key.zip(value)
        })
    });
    iter.collect()
}
