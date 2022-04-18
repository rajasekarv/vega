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

    let dir = PathBuf::from("/opt/data/pt_tc_2");
    let mut tc = sc.read_source(LocalFsReaderConfig::new(dir).num_partitions_per_executor(1), deserializer)
        .flat_map(Fn!(|v: Vec<(u32, u32)>| Box::new(v.into_iter()) as Box<dyn Iterator<Item = _>>));
    //tc.cache();
    let edges = tc.map(Fn!(|x: (u32, u32)| (x.1, x.0)));
    
    // This join is iterated until a fixed point is reached.
    let mut old_count = 0;
    let mut next_count = tc.count().unwrap();
    let mut iter = 0;
    while next_count != old_count && iter < 5 {
        old_count = next_count;
        tc = tc.union(
            tc.join(edges.clone(), 1)
                .map(Fn!(|x: (u32, (u32, u32))| (x.1.1, x.1.0)))
                .into()
        ).unwrap().distinct_with_num_partitions(1);
        //tc.cache();
        next_count = tc.count().unwrap();
        iter += 1;
        println!("next_count = {:?}", next_count);
    }
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("total time {:?}s", dur);
    Ok(())
}