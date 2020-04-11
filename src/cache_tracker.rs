use std::collections::LinkedList;
use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time;

use crate::cache::{BoundedMemoryCache, CachePutResponse, KeySpace};
use crate::env;
use crate::rdd::Rdd;
use crate::serializable_traits::Data;
use crate::serialized_data_capnp::serialized_data;
use crate::split::Split;
use capnp::serialize_packed;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde_derive::{Deserialize, Serialize};

// Cache tracker works by creating a server in master node and slave nodes acting as clients
#[derive(Serialize, Deserialize)]
pub enum CacheTrackerMessage {
    AddedToCache {
        rdd_id: usize,
        partition: usize,
        host: Ipv4Addr,
        size: usize,
    },
    DroppedFromCache {
        rdd_id: usize,
        partition: usize,
        host: Ipv4Addr,
        size: usize,
    },
    MemoryCacheLost {
        host: Ipv4Addr,
    },
    RegisterRdd {
        rdd_id: usize,
        num_partitions: usize,
    },
    SlaveCacheStarted {
        host: Ipv4Addr,
        size: usize,
    },
    GetCacheStatus,
    GetCacheLocations,
    StopCacheTracker,
}

#[derive(Serialize, Deserialize)]
pub enum CacheTrackerMessageReply {
    CacheLocations(HashMap<usize, Vec<LinkedList<Ipv4Addr>>>),
    CacheStatus(Vec<(Ipv4Addr, usize, usize)>),
    Ok,
}

#[derive(Clone, Debug)]
pub(crate) struct CacheTracker {
    is_master: bool,
    locs: Arc<DashMap<usize, Vec<LinkedList<Ipv4Addr>>>>,
    slave_capacity: Arc<DashMap<Ipv4Addr, usize>>,
    slave_usage: Arc<DashMap<Ipv4Addr, usize>>,
    registered_rdd_ids: Arc<RwLock<HashSet<usize>>>,
    loading: Arc<RwLock<HashSet<(usize, usize)>>>,
    cache: KeySpace<'static>,
    master_addr: SocketAddr,
}

impl CacheTracker {
    pub fn new(
        is_master: bool,
        master_addr: SocketAddr,
        local_ip: Ipv4Addr,
        the_cache: &'static BoundedMemoryCache,
    ) -> Self {
        let m = CacheTracker {
            is_master,
            locs: Arc::new(DashMap::new()),
            slave_capacity: Arc::new(DashMap::new()),
            slave_usage: Arc::new(DashMap::new()),
            registered_rdd_ids: Arc::new(RwLock::new(HashSet::new())),
            loading: Arc::new(RwLock::new(HashSet::new())),
            cache: the_cache.new_key_space(),
            master_addr: SocketAddr::new(master_addr.ip(), master_addr.port() + 1),
        };
        m.server();
        m.client(CacheTrackerMessage::SlaveCacheStarted {
            host: local_ip,
            size: m.cache.get_capacity(),
        });
        m
    }

    // Slave node will ask master node for cache locs
    fn client(&self, message: CacheTrackerMessage) -> CacheTrackerMessageReply {
        while let Err(_) = TcpStream::connect(self.master_addr) {
            continue;
        }
        let mut stream = TcpStream::connect(self.master_addr).unwrap();
        let shuffle_id_bytes = bincode::serialize(&message).unwrap();
        let mut message = capnp::message::Builder::new_default();
        let mut shuffle_data = message.init_root::<serialized_data::Builder>();
        shuffle_data.set_msg(&shuffle_id_bytes);
        serialize_packed::write_message(&mut stream, &message).unwrap();

        let r = capnp::message::ReaderOptions {
            traversal_limit_in_words: std::u64::MAX,
            nesting_limit: 64,
        };
        let mut stream_r = std::io::BufReader::new(&mut stream);
        let message_reader = serialize_packed::read_message(&mut stream_r, r).unwrap();
        let shuffle_data = message_reader
            .get_root::<serialized_data::Reader>()
            .unwrap();
        let reply: CacheTrackerMessageReply =
            bincode::deserialize(&shuffle_data.get_msg().unwrap()).unwrap();
        reply
    }

    // This will start only in master node and will server all the slave nodes
    fn server(&self) {
        if self.is_master {
            let locs = self.locs.clone();
            let slave_capacity = self.slave_capacity.clone();
            let slave_usage = self.slave_usage.clone();
            let master_addr = self.master_addr;
            thread::spawn(move || {
                // TODO: make this use async rt
                let listener = TcpListener::bind(master_addr).unwrap();
                for stream in listener.incoming() {
                    match stream {
                        Err(_) => continue,
                        Ok(mut stream) => {
                            let locs = locs.clone();
                            let slave_capacity = slave_capacity.clone();
                            let slave_usage = slave_usage.clone();
                            thread::spawn(move || {
                                //reading
                                let r = capnp::message::ReaderOptions {
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
                                let message: CacheTrackerMessage =
                                    bincode::deserialize(data.get_msg().unwrap()).unwrap();
                                //TODO logging
                                let reply = match message {
                                    CacheTrackerMessage::SlaveCacheStarted { host, size } => {
                                        slave_capacity.insert(host.clone(), size);
                                        slave_usage.insert(host, 0);
                                        CacheTrackerMessageReply::Ok
                                    }
                                    CacheTrackerMessage::RegisterRdd {
                                        rdd_id,
                                        num_partitions,
                                    } => {
                                        locs.insert(
                                            rdd_id,
                                            (0..num_partitions)
                                                .map(|_| LinkedList::new())
                                                .collect(),
                                        );
                                        CacheTrackerMessageReply::Ok
                                    }
                                    CacheTrackerMessage::AddedToCache {
                                        rdd_id,
                                        partition,
                                        host,
                                        size,
                                    } => {
                                        if size > 0 {
                                            slave_usage.insert(
                                                host.clone(),
                                                CacheTracker::get_cache_usage(
                                                    slave_usage.clone(),
                                                    host,
                                                ) + size,
                                            );
                                        } else {
                                            //TODO logging
                                        }
                                        if let Some(mut locs_rdd) = locs.get_mut(&rdd_id) {
                                            if let Some(locs_rdd_p) = locs_rdd.get_mut(partition) {
                                                locs_rdd_p.push_front(host);
                                            }
                                        }
                                        CacheTrackerMessageReply::Ok
                                    }
                                    CacheTrackerMessage::DroppedFromCache {
                                        rdd_id,
                                        partition,
                                        host,
                                        size,
                                    } => {
                                        if size > 0 {
                                            let remaining = CacheTracker::get_cache_usage(
                                                slave_usage.clone(),
                                                host,
                                            ) - size;
                                            slave_usage.insert(host.clone(), remaining);
                                        }
                                        let remaining_locs = locs
                                            .get(&rdd_id)
                                            .unwrap()
                                            .get(partition)
                                            .unwrap()
                                            .iter()
                                            .filter(|x| *x == &host)
                                            .copied()
                                            .collect();
                                        if let Some(mut locs_r) = locs.get_mut(&rdd_id) {
                                            if let Some(locs_p) = locs_r.get_mut(partition) {
                                                *locs_p = remaining_locs;
                                            }
                                        }
                                        CacheTrackerMessageReply::Ok
                                    }
                                    //TODO memory cache lost needs to be implemented
                                    CacheTrackerMessage::GetCacheLocations => {
                                        let locs_clone = locs
                                            .iter()
                                            .map(|kv| {
                                                let (k, v) = (kv.key(), kv.value());
                                                (*k, v.clone())
                                            })
                                            .collect();
                                        CacheTrackerMessageReply::CacheLocations(locs_clone)
                                    }
                                    CacheTrackerMessage::GetCacheStatus => {
                                        let status = slave_capacity
                                            .iter()
                                            .map(|kv| {
                                                let (host, capacity) = (kv.key(), kv.value());
                                                (
                                                    *host,
                                                    *capacity,
                                                    CacheTracker::get_cache_usage(
                                                        slave_usage.clone(),
                                                        *host,
                                                    ),
                                                )
                                            })
                                            .collect();
                                        CacheTrackerMessageReply::CacheStatus(status)
                                    }
                                    _ => CacheTrackerMessageReply::Ok,
                                };
                                let result = bincode::serialize(&reply).unwrap();
                                let mut message = capnp::message::Builder::new_default();
                                let mut locs_data = message.init_root::<serialized_data::Builder>();
                                locs_data.set_msg(&result);
                                serialize_packed::write_message(&mut stream, &message).unwrap();
                            });
                        }
                    }
                }
            });
        }
    }

    pub fn get_cache_usage(slave_usage: Arc<DashMap<Ipv4Addr, usize>>, host: Ipv4Addr) -> usize {
        match slave_usage.get(&host) {
            Some(s) => *s,
            None => 0,
        }
    }

    pub fn get_cache_capacity(
        slave_capacity: Arc<DashMap<Ipv4Addr, usize>>,
        host: Ipv4Addr,
    ) -> usize {
        match slave_capacity.get(&host) {
            Some(s) => *s,
            None => 0,
        }
    }

    pub fn register_rdd(&self, rdd_id: usize, num_partitions: usize) {
        if !self.registered_rdd_ids.read().contains(&rdd_id) {
            //TODO logging
            self.registered_rdd_ids.write().insert(rdd_id);
            self.client(CacheTrackerMessage::RegisterRdd {
                rdd_id,
                num_partitions,
            });
        }
    }

    pub fn get_location_snapshot(&self) -> HashMap<usize, Vec<Vec<Ipv4Addr>>> {
        match self.client(CacheTrackerMessage::GetCacheLocations) {
            CacheTrackerMessageReply::CacheLocations(s) => s
                .into_iter()
                .map(|(k, v)| {
                    let v = v
                        .into_iter()
                        .map(|x| x.into_iter().map(|x| x).collect())
                        .collect();
                    (k, v)
                })
                .collect(),
            _ => panic!("wrong type from cache tracker"),
        }
    }

    pub fn get_cache_status(&self) -> Vec<(Ipv4Addr, usize, usize)> {
        match self.client(CacheTrackerMessage::GetCacheStatus) {
            CacheTrackerMessageReply::CacheStatus(s) => s,
            _ => panic!("wrong type from cache tracker"),
        }
    }

    pub async fn get_or_compute<T: Data>(
        &self,
        rdd: Arc<dyn Rdd<Item = T>>,
        split: Box<dyn Split>,
    ) -> Box<dyn Iterator<Item = T>> {
        if let Some(cached_val) = self.cache.get(rdd.get_rdd_id(), split.get_index()) {
            let res: Vec<T> = bincode::deserialize(&cached_val).unwrap();
            Box::new(res.into_iter())
        } else {
            let key = (rdd.get_rdd_id(), split.get_index());
            while self.loading.read().contains(&key) {
                let dur = time::Duration::from_millis(1);
                thread::sleep(dur);
            }
            if let Some(cached_val) = self.cache.get(rdd.get_rdd_id(), split.get_index()) {
                let res: Vec<T> = bincode::deserialize(&cached_val).unwrap();
                return Box::new(res.into_iter());
            }
            self.loading.write().insert(key);

            let mut lock = self.loading.write();
            let res: Vec<_> = rdd
                .compute(split.clone())
                .await
                .unwrap()
                .lock()
                .into_iter()
                .collect();
            let res_bytes = bincode::serialize(&res).unwrap();
            let put_response = self
                .cache
                .put(rdd.get_rdd_id(), split.get_index(), res_bytes);
            lock.remove(&key);

            if let CachePutResponse::CachePutSuccess(size) = put_response {
                self.client(CacheTrackerMessage::AddedToCache {
                    rdd_id: rdd.get_rdd_id(),
                    partition: split.get_index(),
                    host: env::Configuration::get().local_ip,
                    size,
                });
            }
            Box::new(res.into_iter())
        }
    }

    // TODO drop_entry needs to be implemented
}
