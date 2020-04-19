use crate::serializable_traits::Data;
use crate::Fn;
use serde_derive::{Deserialize, Serialize};
use std::marker::PhantomData;

// Aggregator for shuffle tasks.
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

impl<K: Data, V: Data> Default for Aggregator<K, V, Vec<V>> {
    fn default() -> Self {
        let merge_value = Box::new(Fn!(|mv: (Vec<V>, V)| {
            let (mut buf, v) = mv;
            buf.push(v);
            buf
        }));
        let create_combiner = Box::new(Fn!(|v: V| vec![v]));
        let merge_combiners = Box::new(Fn!(|mc: (Vec<V>, Vec<V>)| {
            let (mut b1, mut b2) = mc;
            b1.append(&mut b2);
            b1
        }));
        Aggregator {
            create_combiner,
            merge_value,
            merge_combiners,
            _marker: PhantomData,
        }
    }
}
