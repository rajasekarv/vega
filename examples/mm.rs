use std::collections::{HashMap, BTreeMap};
use std::path::PathBuf;
use std::time::Instant;
use vega::*;

// unsecure mode
fn main() -> Result<()> {
    let sc = Context::new()?;
    let now = Instant::now();

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<((u32, u32), f64)>>(&file).unwrap()  //Item = ((u32, u32), f64)
    }));

    let dir_a = PathBuf::from("/opt/data/pt_mm_a_100");
    let dir_b = PathBuf::from("/opt/data/pt_mm_b_100");
    let ma = sc.read_source(LocalFsReaderConfig::new(dir_a).num_partitions_per_executor(1), deserializer.clone())
        .flat_map(Fn!(|va: Vec<((u32, u32), f64)>| {
            Box::new(va.into_iter().map(|a| 
                (a.0.1, (a.0.0, a.1))
            )) as Box<dyn Iterator<Item = _>>
        }));
    let mb = sc.read_source(LocalFsReaderConfig::new(dir_b).num_partitions_per_executor(1), deserializer)
        .flat_map(Fn!(|vb: Vec<((u32, u32), f64)>| {
            Box::new(vb.into_iter().map(|b|
                (b.0.0, (b.0.1, b.1))
            )) as Box<dyn Iterator<Item = _>>
        }));

    let temp = ma.join(mb, 1)
        .map(Fn!(|n: (u32, ((u32, f64), (u32, f64)))| ((n.1.0.0, n.1.1.0), n.1.0.1 * n.1.1.1)));

    let mc = temp.reduce_by_key(Fn!(|(x, y)| x + y), 1);

    let output = mc.count().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("count = {:?}, total time = {:?}", output, dur);
    Ok(())
}