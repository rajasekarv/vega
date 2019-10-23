use super::*;
//use parking_lot::Mutex;
use std::marker::PhantomData;
//use std::sync::Arc;
//use std::sync::Mutex;

// check once where C have to satisfy Data trait

// Aggregator for shuffle tasks
// Presently Mutexes are being used just to avoid the lifetime issues associated with mutable references.
// It has significant cost associated with it due to lot of locking requirements
// In future change everything to plain mutable references.
#[derive(Serialize, Deserialize)]
pub struct Aggregator<K: Data, V: Data, C: Data> {
    #[serde(with = "serde_traitobject")]
    pub create_combiner: Box<dyn serde_traitobject::Fn(V) -> C + Send + Sync>,
    #[serde(with = "serde_traitobject")]
    pub merge_value: Box<dyn serde_traitobject::Fn((C, V)) -> C + Send + Sync>,
    #[serde(with = "serde_traitobject")]
    pub merge_combiners: Box<dyn serde_traitobject::Fn((C, C)) -> C + Send + Sync>,
    _marker: PhantomData<K>,
}

impl<K: Data, V: Data, C: Data> Aggregator<K, V, C> {
    pub fn new(
        create_combiner: Box<dyn serde_traitobject::Fn(V) -> C + Send + Sync>,
        merge_value: Box<dyn serde_traitobject::Fn((C, V)) -> C + Send + Sync>,
        merge_combiners: Box<dyn serde_traitobject::Fn((C, C)) -> C + Send + Sync>,
    ) -> Self {
        Aggregator {
            create_combiner,
            merge_value,
            merge_combiners,
            _marker: PhantomData,
        }
    }
}
