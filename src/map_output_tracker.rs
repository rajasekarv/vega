use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use crate::serialized_data_capnp::serialized_data;
use crate::{Error, NetworkError, Result};
use capnp::message::{Builder as MsgBuilder, ReaderOptions};
use capnp_futures::serialize as capnp_serialize;
use dashmap::{DashMap, DashSet};
use parking_lot::Mutex;
use thiserror::Error;
use tokio::{
    net::{TcpListener, TcpStream},
    stream::StreamExt,
};
use tokio_util::compat::{Tokio02AsyncReadCompatExt, Tokio02AsyncWriteCompatExt};

const CAPNP_BUF_READ_OPTS: ReaderOptions = ReaderOptions {
    traversal_limit_in_words: std::u64::MAX,
    nesting_limit: 64,
};

pub(crate) enum MapOutputTrackerMessage {
    // Contains shuffle_id
    GetMapOutputLocations(i64),
    StopMapOutputTracker,
}

/// The key is the shuffle_id
pub type ServerUris = Arc<DashMap<usize, Vec<Option<String>>>>;

// Starts the server in master node and client in slave nodes. Similar to cache tracker.
#[derive(Clone, Debug)]
pub(crate) struct MapOutputTracker {
    is_master: bool,
    pub server_uris: ServerUris,
    fetching: Arc<DashSet<usize>>,
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
        let output_tracker = MapOutputTracker {
            is_master,
            server_uris: Arc::new(DashMap::new()),
            fetching: Arc::new(DashSet::new()),
            generation: Arc::new(Mutex::new(0)),
            master_addr,
        };
        output_tracker.server();
        output_tracker
    }

    async fn client(&self, shuffle_id: usize) -> Result<Vec<String>> {
        let mut stream = loop {
            match TcpStream::connect(self.master_addr).await {
                Ok(stream) => break stream,
                Err(_) => continue,
            }
        };
        let (reader, writer) = stream.split();
        let reader = reader.compat();
        let mut writer = writer.compat_write();
        log::debug!(
            "connected to master to fetch shuffle task #{} data hosts",
            shuffle_id
        );
        let shuffle_id_bytes = bincode::serialize(&shuffle_id)?;
        let mut message = MsgBuilder::new_default();
        let mut shuffle_data = message.init_root::<serialized_data::Builder>();
        shuffle_data.set_msg(&shuffle_id_bytes);
        capnp_serialize::write_message(&mut writer, &message).await?;
        let message_reader = capnp_serialize::read_message(reader, CAPNP_BUF_READ_OPTS)
            .await?
            .ok_or_else(|| NetworkError::NoMessageReceived)?;
        let shuffle_data = message_reader.get_root::<serialized_data::Reader>()?;
        let locs: Vec<String> = bincode::deserialize(&shuffle_data.get_msg()?)?;
        Ok(locs)
    }

    fn server(&self) {
        if !self.is_master {
            return;
        }
        log::debug!("map output tracker server starting");
        let master_addr = self.master_addr;
        let server_uris = self.server_uris.clone();
        tokio::spawn(async move {
            let mut listener = TcpListener::bind(master_addr)
                .await
                .map_err(NetworkError::TcpListener)?;
            log::debug!("map output tracker server started");
            while let Some(Ok(mut stream)) = listener.incoming().next().await {
                let server_uris_clone = server_uris.clone();
                tokio::spawn(async move {
                    let (reader, writer) = stream.split();
                    let reader = reader.compat();
                    let writer = writer.compat_write();

                    // reading
                    let message_reader = capnp_serialize::read_message(reader, CAPNP_BUF_READ_OPTS)
                        .await?
                        .ok_or_else(|| NetworkError::NoMessageReceived)?;
                    let shuffle_id = {
                        let data = message_reader.get_root::<serialized_data::Reader>()?;
                        bincode::deserialize(data.get_msg()?)?
                    };
                    while server_uris_clone
                        .get(&shuffle_id)
                        .ok_or_else(|| MapOutputError::ShuffleIdNotFound(shuffle_id))?
                        .iter()
                        .filter(|x| !x.is_none())
                        .count()
                        == 0
                    {
                        //check whether this will hurt the performance or not
                        tokio::time::delay_for(Duration::from_millis(1)).await;
                    }
                    let locs = server_uris_clone
                        .get(&shuffle_id)
                        .map(|kv| {
                            kv.value()
                                .iter()
                                .cloned()
                                .map(|x| x.unwrap())
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    log::debug!(
                        "locs inside map output tracker server for shuffle id #{}: {:?}",
                        shuffle_id,
                        locs
                    );

                    // writting response
                    let result = bincode::serialize(&locs)?;
                    let mut message = MsgBuilder::new_default();
                    let mut locs_data = message.init_root::<serialized_data::Builder>();
                    locs_data.set_msg(&result);
                    // TODO: remove blocking call when possible
                    futures::executor::block_on(async {
                        capnp_futures::serialize::write_message(writer, message)
                            .await
                            .map_err(Error::CapnpDeserialization)?;
                        Ok::<_, Error>(())
                    })?;
                    Ok::<_, Error>(())
                });
            }
            Err::<(), _>(Error::ExecutorShutdown)
        });
    }

    pub fn register_shuffle(&self, shuffle_id: usize, num_maps: usize) {
        log::debug!("inside register shuffle");
        if self.server_uris.get(&shuffle_id).is_some() {
            // TODO: error handling
            log::debug!("map tracker register shuffle none");
            return;
        }
        self.server_uris.insert(shuffle_id, vec![None; num_maps]);
        log::debug!("server_uris after register_shuffle {:?}", self.server_uris);
    }

    pub fn register_map_output(&self, shuffle_id: usize, map_id: usize, server_uri: String) {
        log::debug!(
            "registering map output from shuffle task #{} with map id #{} at server: {}",
            shuffle_id,
            map_id,
            server_uri
        );
        self.server_uris.get_mut(&shuffle_id).unwrap()[map_id] = Some(server_uri);
    }

    pub fn register_map_outputs(&self, shuffle_id: usize, locs: Vec<Option<String>>) {
        log::debug!(
            "registering map outputs inside map output tracker for shuffle id #{}: {:?}",
            shuffle_id,
            locs
        );
        self.server_uris.insert(shuffle_id, locs);
    }

    pub fn unregister_map_output(&self, shuffle_id: usize, map_id: usize, server_uri: String) {
        let array = self.server_uris.get(&shuffle_id);
        if let Some(arr) = array {
            if arr.get(map_id).unwrap() == &Some(server_uri) {
                self.server_uris
                    .get_mut(&shuffle_id)
                    .unwrap()
                    .insert(map_id, None)
            }
            self.increment_generation();
        } else {
            // TODO: error logging
        }
    }

    pub async fn get_server_uris(&self, shuffle_id: usize) -> Result<Vec<String>> {
        log::debug!(
            "trying to get uri for shuffle task #{}, current server uris: {:?}",
            shuffle_id,
            self.server_uris
        );

        if self
            .server_uris
            .get(&shuffle_id)
            .map(|some| some.iter().filter_map(|x| x.clone()).next())
            .flatten()
            .is_none()
        {
            if self.fetching.contains(&shuffle_id) {
                while self.fetching.contains(&shuffle_id) {
                    // TODO: check whether this will hurt the performance or not
                    tokio::time::delay_for(Duration::from_millis(1)).await;
                }
                let servers = self
                    .server_uris
                    .get(&shuffle_id)
                    .ok_or_else(|| MapOutputError::ShuffleIdNotFound(shuffle_id))?
                    .iter()
                    .filter(|x| !x.is_none())
                    .map(|x| x.clone().unwrap())
                    .collect::<Vec<_>>();
                log::debug!("returning after fetching done, return: {:?}", servers);
                return Ok(servers);
            } else {
                log::debug!("adding to fetching queue");
                self.fetching.insert(shuffle_id);
            }
            let fetched = self.client(shuffle_id).await?;
            log::debug!("fetched locs from client: {:?}", fetched);
            self.server_uris.insert(
                shuffle_id,
                fetched.iter().map(|x| Some(x.clone())).collect(),
            );
            log::debug!("added locs to server uris after fetching");
            self.fetching.remove(&shuffle_id);
            Ok(fetched)
        } else {
            Ok(self
                .server_uris
                .get(&shuffle_id)
                .ok_or_else(|| MapOutputError::ShuffleIdNotFound(shuffle_id))?
                .iter()
                .filter(|x| !x.is_none())
                .map(|x| x.clone().unwrap())
                .collect())
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
            self.server_uris = Arc::new(DashMap::new());
            *self.generation.lock() = new_gen;
        }
    }
}

#[derive(Debug, Error)]
pub enum MapOutputError {
    #[error("Shuffle id output #{0} not found in the map")]
    ShuffleIdNotFound(usize),
}
