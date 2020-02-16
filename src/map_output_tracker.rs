use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time;

use crate::env;
use crate::serialized_data_capnp::serialized_data;
use capnp::serialize_packed;
use log::info;
use parking_lot::{Mutex, RwLock};
use serde_derive::{Deserialize, Serialize};

pub enum MapOutputTrackerMessage {
    //contains shuffle_id
    GetMapOutputLocations(i64),
    StopMapOutputTracker,
}

// starts the server in master node and client in slave nodes. Similar to cache tracker
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct MapOutputTracker {
    is_master: bool,
    pub server_uris: Arc<RwLock<HashMap<usize, Vec<Option<String>>>>>,
    fetching: Arc<RwLock<HashSet<usize>>>,
    generation: Arc<Mutex<i64>>,
    master_addr: SocketAddr,
}

// Only master_addr doesn't have a default.
impl Default for MapOutputTracker {
    fn default() -> Self {
        MapOutputTracker {
            is_master: Default::default(),
            server_uris: Default::default(),
            fetching: Default::default(),
            generation: Default::default(),
            master_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0),
        }
    }
}

impl MapOutputTracker {
    pub fn new(is_master: bool, master_addr: SocketAddr) -> Self {
        let m = MapOutputTracker {
            is_master,
            server_uris: Arc::new(RwLock::new(HashMap::new())),
            fetching: Arc::new(RwLock::new(HashSet::new())),
            generation: Arc::new(Mutex::new(0)),
            master_addr,
        };
        m.server();
        m
    }

    fn client(&self, shuffle_id: usize) -> Vec<String> {
        //        if !self.is_master {

        while let Err(_) = TcpStream::connect(self.master_addr) {
            continue;
        }
        let mut stream = TcpStream::connect(self.master_addr).unwrap();
        let shuffle_id_bytes = bincode::serialize(&shuffle_id).unwrap();
        let mut message = ::capnp::message::Builder::new_default();
        let mut shuffle_data = message.init_root::<serialized_data::Builder>();
        shuffle_data.set_msg(&shuffle_id_bytes);
        serialize_packed::write_message(&mut stream, &message);

        let r = ::capnp::message::ReaderOptions {
            traversal_limit_in_words: std::u64::MAX,
            nesting_limit: 64,
        };
        let mut stream_r = std::io::BufReader::new(&mut stream);
        let message_reader = serialize_packed::read_message(&mut stream_r, r).unwrap();
        let shuffle_data = message_reader
            .get_root::<serialized_data::Reader>()
            .unwrap();
        let locs: Vec<String> = bincode::deserialize(&shuffle_data.get_msg().unwrap()).unwrap();
        locs
        //        }
        //        else {
        //
        //            let locs = self
        //                .server_uris
        //                .read()
        //                .unwrap()
        //                .get(&shuffle_id)
        //                .unwrap_or(&Vec::new())
        //                .clone();
        //            let locs = locs.into_iter().map(|x| x.unwrap()).collect::<Vec<_>>();
        //            return locs;
        //        }
    }

    fn server(&self) {
        if self.is_master {
            log::debug!("mapoutput tracker server starting");
            let master_addr = self.master_addr;
            let server_uris = self.server_uris.clone();
            thread::Builder::new()
                .name(format!("{}_map_output_tracker", env::THREAD_PREFIX))
            .spawn(move || {
                let listener = TcpListener::bind(master_addr).unwrap();
                log::debug!("mapoutput tracker server started");
                for stream in listener.incoming() {
                    match stream {
                        Err(_) => continue,
                        Ok(mut stream) => {
                            let server_uris_clone = server_uris.clone();
                            thread::spawn(move || {
                                //reading
                                let r = ::capnp::message::ReaderOptions {
                                    traversal_limit_in_words: std::u64::MAX,
                                    nesting_limit: 64,
                                };
                                let mut stream_r = std::io::BufReader::new(&mut stream);
                                let message_reader =
                                    match serialize_packed::read_message(&mut stream_r, r) {
                                        Ok(s) => s,
                                        Err(_) => return,
                                    };
                                let data = message_reader
                                    .get_root::<serialized_data::Reader>()
                                    .unwrap();
                                let shuffle_id: usize =
                                    bincode::deserialize(data.get_msg().unwrap()).unwrap();
                                while server_uris_clone
                                    .read()
                                    .get(&shuffle_id)
                                    .unwrap()
                                    .iter()
                                    .filter(|x| !x.is_none())
                                    .count()
                                    == 0
                                {
                                    //check whether this will hurt the performance or not
                                    let wait = time::Duration::from_millis(1);
                                    thread::sleep(wait);
                                }
                                let locs = server_uris_clone
                                    .read()
                                    .get(&shuffle_id)
                                    .unwrap_or(&Vec::new())
                                    .clone();
                                log::debug!("locs inside mapoutput tracker server before unwrapping for shuffle id {:?} {:?}",shuffle_id,locs);
                                let locs = locs.into_iter().map(|x| x.unwrap()).collect::<Vec<_>>();
                                log::debug!("locs inside mapoutput tracker server after unwrapping for shuffle id {:?} {:?} ", shuffle_id, locs);

                                //writing
                                let result = bincode::serialize(&locs).unwrap();
                                let mut message = ::capnp::message::Builder::new_default();
                                let mut locs_data = message.init_root::<serialized_data::Builder>();
                                locs_data.set_msg(&result);
                                serialize_packed::write_message(&mut stream, &message);
                            });
                        }
                    }
                }
            });
        }
    }

    pub fn register_shuffle(&self, shuffle_id: usize, num_maps: usize) {
        log::debug!("inside register shuffle");
        if self.server_uris.read().get(&shuffle_id).is_some() {
            //TODO error handling
            log::debug!("map tracker register shuffle none");
            return;
        }
        let mut server_uris = self.server_uris.write();
        server_uris.insert(shuffle_id, vec![None; num_maps]);
        log::debug!("server_uris after register_shuffle {:?}", server_uris);
    }

    pub fn register_map_output(&self, shuffle_id: usize, map_id: usize, server_uri: String) {
        //        if !self.is_master {
        //            return;
        //        }
        let mut array = self.server_uris.write();
        array.get_mut(&shuffle_id).unwrap()[map_id] = Some(server_uri);
    }

    pub fn register_map_outputs(&self, shuffle_id: usize, locs: Vec<Option<String>>) {
        //        if !self.is_master {
        //            let fetched = self.client(shuffle_id);
        //            println!("fetched locs from client {:?}", fetched);
        //            self.server_uris.write().insert(
        //                shuffle_id,
        //                fetched.iter().map(|x| Some(x.clone())).collect(),
        //            );
        //            return;
        //        }
        log::debug!(
            "registering map outputs inside map output tracker for shuffle id {} {:?}",
            shuffle_id,
            locs
        );
        self.server_uris.write().insert(shuffle_id, locs);
        //        .insert(shuffle_id, locs.into_iter().map(|x| Some(x)).collect());
    }

    pub fn unregister_map_output(&self, shuffle_id: usize, map_id: usize, server_uri: String) {
        //        if !self.is_master {
        //            return;
        //        }
        let array = self.server_uris.read();
        let array = array.get(&shuffle_id);
        if let Some(arr) = array {
            if arr.get(map_id).unwrap() == &Some(server_uri) {
                self.server_uris
                    .write()
                    .get_mut(&shuffle_id)
                    .unwrap()
                    .insert(map_id, None)
            }
            self.increment_generation();
        } else {
            //TODO error logging
        }
    }

    pub fn get_server_uris(&self, shuffle_id: usize) -> Vec<String> {
        log::debug!(
            "server uris inside get_server_uris method {:?}",
            self.server_uris
        );
        if self
            .server_uris
            .read()
            .get(&shuffle_id)
            .unwrap()
            .iter()
            .filter(|x| !x.is_none())
            .map(|x| x.clone().unwrap())
            .next()
            .is_none()
        {
            // if self.server_uris.read().get(&shuffle_id).is_empty(){
            if self.fetching.read().contains(&shuffle_id) {
                while self.fetching.read().contains(&shuffle_id) {
                    //check whether this will hurt the performance or not
                    let wait = time::Duration::from_millis(1);
                    thread::sleep(wait);
                }
                log::debug!(
                    "returning after fetching done {:?}",
                    self.server_uris
                        .read()
                        .get(&shuffle_id)
                        .unwrap()
                        .iter()
                        .filter(|x| !x.is_none())
                        .map(|x| x.clone().unwrap())
                        .collect::<Vec<_>>()
                );
                return self
                    .server_uris
                    .read()
                    .get(&shuffle_id)
                    .unwrap()
                    .iter()
                    .filter(|x| !x.is_none())
                    .map(|x| x.clone().unwrap())
                    .collect();
            } else {
                log::debug!("adding to fetching queue");
                self.fetching.write().insert(shuffle_id);
            }
            // TODO logging
            let fetched = self.client(shuffle_id);
            log::debug!("fetched locs from client {:?}", fetched);
            self.server_uris.write().insert(
                shuffle_id,
                fetched.iter().map(|x| Some(x.clone())).collect(),
            );
            log::debug!("wriiten to server_uris after fetching");
            self.fetching.write().remove(&shuffle_id);
            log::debug!("returning from get server uri");

            fetched
        } else {
            self.server_uris
                .read()
                .get(&shuffle_id)
                .unwrap()
                .iter()
                .filter(|x| !x.is_none())
                .map(|x| x.clone().unwrap())
                .collect()
        }
    }

    pub fn increment_generation(&self) {
        *self.generation.lock() += 1;
    }

    pub fn get_generation(&self) -> i64 {
        *self.generation.lock()
    }

    pub fn update_generation(&mut self, new_gen: i64) {
        if new_gen > *self.generation.lock() {
            self.server_uris = Arc::new(RwLock::new(HashMap::new()));
            *self.generation.lock() = new_gen;
        }
    }
}
