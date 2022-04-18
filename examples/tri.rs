use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;
use vega::*;
use rand::Rng;

fn main() -> Result<()> {
    let sc = Context::new()?;
    let now = Instant::now();

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<(u32, u32)>>(&file).unwrap()  //Item = (u32, u32)
    }));

    let dir = PathBuf::from("/opt/data/pt_tc_7");
    let graph = sc.read_source(LocalFsReaderConfig::new(dir).num_partitions_per_executor(1), deserializer)
        .flat_map(Fn!(|v: Vec<(u32, u32)>| Box::new(v.into_iter()) as Box<dyn Iterator<Item = _>>))
        .filter(Fn!(|edge: &(u32, u32)| edge.0 != edge.1))
        .map(Fn!(|edge: (u32, u32)| match edge.0 < edge.1 {
            true => edge,
            false => (edge.1, edge.0)
        })).distinct();
    //graph.cache();
    let count = graph.join(graph.clone(), 1)
        .key_by(Fn!(|item: &(u32, (u32, u32))| item.1))
        .join(graph.clone()
            .map(Fn!(|edge| (edge, 1 as i32))),
            1
        ).count().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("total time {:?}s, count = {:?}", dur, count);
    Ok(())
}