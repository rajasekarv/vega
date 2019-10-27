use super::*;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::net::{Ipv4Addr, SocketAddr};
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
    master: SocketAddr,
    //slaves: Vec<SocketAddr>,
}

impl Hosts {
    fn load_from<R: Read>(mut reader: R) -> std::io::Result<Self> {
        let mut data = String::new();
        reader.read_to_string(&mut data)?;
        Ok(toml::from_str(&data).expect("unable to process the hosts.conf file"))
    }
}

impl Env {
    pub fn new(master_addr: SocketAddr) -> Self {
        Env {
            map_output_tracker: MapOutputTracker::new(*is_master, master_addr.clone()),
            shuffle_manager: ShuffleManager::new(),
            shuffle_fetcher: ShuffleFetcher,
            cache_tracker: CacheTracker::new(*is_master, master_addr, &the_cache),
        }
    }
}

lazy_static! {
    pub static ref shuffle_cache: Arc<RwLock<HashMap<(usize, usize, usize), Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
    // Too lazy to choose a proper logger. Currently using a static log file to log the whole process. Just a crude version of logger.
    pub static ref is_master: bool = {
        let args = std::env::args().skip(1).collect::<Vec<_>>();
        match args.get(0).as_ref().map(|arg| &arg[..]) {
            Some("slave") => false,
            _ => true,
        }
    };
    pub static ref the_cache: BoundedMemoryCache = { BoundedMemoryCache::new() };
    pub static ref env: Env = {
        let host_path = std::env::home_dir().unwrap().join("hosts.conf");
        let host_path_string = host_path.to_string_lossy();
        let mut host_file = File::open(&host_path).expect(&format!("Unable to open the file: {}", host_path_string));
        let mut hosts = String::new();
        host_file
            .read_to_string(&mut hosts)
            .expect(&format!("Unable to read the file: {}", host_path_string));
        let hosts: Hosts = toml::from_str(&hosts).expect(&format!("Unable to process the {} file in env", host_path_string));
        Env::new(hosts.master)
    };

    pub static ref local_ip: Ipv4Addr = std::env::var("SPARK_LOCAL_IP")
        .expect("You must set the SPARK_LOCAL_IP environment variable")
        .parse()
        .unwrap();
}
