use std::collections::{HashMap, BTreeMap};
use std::path::PathBuf;
use std::time::Instant;
use vega::*;

fn main() -> Result<()> {
    let sc = Context::new()?;
    let now = Instant::now();

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        String::from_utf8(file)
            .unwrap()
            .lines()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    }));

    let iters = 1; //7 causes core dump, why? some hints: converge when 6
    let dir = PathBuf::from("/opt/data/pt_pr");
    let lines = sc.read_source(LocalFsReaderConfig::new(dir).num_partitions_per_executor(1), deserializer);
    let links = lines.flat_map(Fn!(|lines: Vec<String>| {
            Box::new(lines.into_iter().map(|line| {
                let parts = line.split(" ")
                    .collect::<Vec<_>>();
                (parts[0].to_string(), parts[1].to_string())
            })) as Box<dyn Iterator<Item = _>>
        })).distinct_with_num_partitions(1)
        .group_by_key(1);
    //links.cache();
    let mut ranks = links.map_values(Fn!(|_| 1.0));

    for _ in 0..iters {
        let contribs = links.join(ranks, 1)
            .map(Fn!(|(k, v)| v))
            .flat_map(Fn!(|(urls, rank): (Vec<String>, f64)| {
                let size = urls.len() as f64;
                Box::new(urls.into_iter().map(move |url| (url, rank / size))) 
                    as Box<dyn Iterator<Item = _>>
                })
            );
        ranks = contribs.reduce_by_key(Fn!(|(x, y)| x + y), 1)
            .map_values(Fn!(|v| 0.15 + 0.85 * v));
    }

    let output = ranks.reduce(Fn!(|x: (String, f64), y: (String, f64)| {
        if x.1 > y.1 {
            x
        } else {
            y
        }
    })).unwrap();
    let output = output.unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("{:?} rank first with {:?}, total time = {:?}", output.0, output.1, dur);

    Ok(())
}
