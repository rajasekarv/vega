use super::*;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
//use std::io::prelude::*;
//use std::io::prelude::*;
//use std::io::Write;
use std::sync::Arc;

pub struct Env {
    pub map_output_tracker: MapOutputTracker,
    pub shuffle_manager: ShuffleManager,
    pub shuffle_fetcher: ShuffleFetcher,
    pub cache_tracker: CacheTracker,
}

#[derive(Deserialize)]
struct Hosts {
    master: String,
    slaves: Vec<String>,
}

impl Env {
    pub fn new(master_ip: String, master_port: i64) -> Self {
        Env {
            map_output_tracker: MapOutputTracker::new(*is_master, master_ip.clone(), master_port),
            shuffle_manager: ShuffleManager::new(),
            shuffle_fetcher: ShuffleFetcher,
            cache_tracker: CacheTracker::new(*is_master, master_ip, master_port, &the_cache),
        }
    }
}

lazy_static! {
    pub static ref shuffle_cache: Arc<RwLock<HashMap<(usize, usize, usize), Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
    // Too lazy to choose a proper logger. Currently using a static log file to log the whole process. Just a crude version of logger.
    pub static ref is_master: bool = {
        let args = std::env::args().skip(1).collect::<Vec<_>>();
        match args.get(0) {
            Some(slave_string) => false,
            _ => true,
        }
    };
    pub static ref the_cache: BoundedMemoryCache = { BoundedMemoryCache::new() };
    pub static ref env: Env = {
        let host_path = std::env::home_dir().unwrap().join("hosts.conf");
        let mut host_file = File::open(host_path).expect("Unable to open the file");
        let mut hosts = String::new();
        host_file
            .read_to_string(&mut hosts)
            .expect("Unable to read the file");
        let hosts: Hosts = toml::from_str(&hosts).expect("unable to process the hosts.conf file");
        let master_address = hosts.master.clone();
        let master_ip = master_address.split(":").collect::<Vec<_>>()[0];
        let master_port = master_address.split(":").collect::<Vec<_>>()[1]
            .parse()
            .unwrap();
        Env::new(master_ip.to_string(), master_port)
    };
}
