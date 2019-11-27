use super::*;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
//use toml::ser::Error::KeyNewline;

#[derive(Debug, Serialize, Deserialize)]
pub enum CachePutResponse {
    CachePutSuccess(usize),
    CachePutFailure,
}

// Despite the name, it is currently unbounded cache. Once done with LRU iterator, have to make this bounded.
// Since we are storing everything as serialized objects, size estimation is as simple as getting the length of byte vector
#[derive(Debug, Clone)]
pub struct BoundedMemoryCache {
    max_bytes: usize,
    next_key_space_id: Arc<AtomicUsize>,
    current_bytes: usize,
    map: Arc<Mutex<HashMap<((usize, usize), usize), (Vec<u8>, usize)>>>,
}

//TODO remove all hardcoded values
impl BoundedMemoryCache {
    pub fn new() -> Self {
        BoundedMemoryCache {
            max_bytes: 2000, // in MB
            next_key_space_id: Arc::new(AtomicUsize::new(0)),
            current_bytes: 0,
            map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn new_key_space_id(&self) -> usize {
        self.next_key_space_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn new_key_space(&self) -> KeySpace {
        KeySpace::new(self, self.new_key_space_id())
    }

    fn get(&self, dataset_id: (usize, usize), partition: usize) -> Option<Vec<u8>> {
        match self.map.lock().get(&(dataset_id, partition)) {
            Some(entry) => Some(entry.0.clone()),
            None => None,
        }
    }

    fn put(
        &self,
        dataset_id: (usize, usize),
        partition: usize,
        value: Vec<u8>,
    ) -> CachePutResponse {
        let key = (dataset_id, partition);
        //TODO logging
        let size = value.len() * 8 + 2 * 8; //this number of MB
        if size as f64 / (1000.0 * 1000.0) > self.max_bytes as f64 {
            CachePutResponse::CachePutFailure
        } else {
            //TODO ensure free space needs to be done and this needs to be modified
            self.map.lock().insert(key, (value, size));
            CachePutResponse::CachePutSuccess(size)
        }
    }

    fn ensure_free_space(&self, dataset_id: u64, space: u64) -> bool {
        //TODO logging
        //        let iter =
        unimplemented!()
    }

    fn report_entry_dropped(data_set_id: usize, partition: usize, entry: (Vec<u8>, usize)) {
        //TODO loggging
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub struct KeySpace<'a> {
    pub cache: &'a BoundedMemoryCache,
    pub key_space_id: usize,
}

impl<'a> KeySpace<'a> {
    fn new(cache: &'a BoundedMemoryCache, key_space_id: usize) -> Self {
        KeySpace {
            cache,
            key_space_id,
        }
    }

    pub fn get(&self, dataset_id: usize, partition: usize) -> Option<Vec<u8>> {
        self.cache.get((self.key_space_id, dataset_id), partition)
    }
    pub fn put(&self, dataset_id: usize, partition: usize, value: Vec<u8>) -> CachePutResponse {
        self.cache
            .put((self.key_space_id, dataset_id), partition, value)
    }
    pub fn get_capacity(&self) -> usize {
        self.cache.max_bytes
    }
}
