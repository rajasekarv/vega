use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use crate::aggregator::Aggregator;
use crate::env;
use crate::partitioner::Partitioner;
use crate::rdd::RddBase;
use crate::serializable_traits::{AnyData, Data};
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Deserialize, Serialize};

// Revise if enum is good choice. Considering enum since down casting one trait object to another trait object is difficult.
#[derive(Clone, Serialize, Deserialize)]
pub enum Dependency {
    #[serde(with = "serde_traitobject")]
    NarrowDependency(Arc<dyn NarrowDependencyTrait>),
    #[serde(with = "serde_traitobject")]
    ShuffleDependency(Arc<dyn ShuffleDependencyTrait>),
}

pub trait NarrowDependencyTrait: Serialize + Deserialize + Send + Sync {
    fn get_parents(&self, partition_id: usize) -> Vec<usize>;
    fn get_rdd_base(&self) -> Arc<dyn RddBase>;
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct OneToOneDependency {
    #[serde(with = "serde_traitobject")]
    rdd_base: Arc<dyn RddBase>,
}

impl OneToOneDependency {
    pub fn new(rdd_base: Arc<dyn RddBase>) -> Self {
        OneToOneDependency { rdd_base }
    }
}

impl NarrowDependencyTrait for OneToOneDependency {
    fn get_parents(&self, partition_id: usize) -> Vec<usize> {
        vec![partition_id]
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        self.rdd_base.clone()
    }
}

/// Represents a one-to-one dependency between ranges of partitions in the parent and child RDDs.
#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct RangeDependency {
    #[serde(with = "serde_traitobject")]
    rdd_base: Arc<dyn RddBase>,
    /// the start of the range in the child RDD
    out_start: usize,
    /// the start of the range in the parent RDD
    in_start: usize,
    /// the length of the range
    length: usize,
}

impl RangeDependency {
    pub fn new(
        rdd_base: Arc<dyn RddBase>,
        in_start: usize,
        out_start: usize,
        length: usize,
    ) -> Self {
        RangeDependency {
            rdd_base,
            in_start,
            out_start,
            length,
        }
    }
}

impl NarrowDependencyTrait for RangeDependency {
    fn get_parents(&self, partition_id: usize) -> Vec<usize> {
        if partition_id >= self.out_start && partition_id < self.out_start + self.length {
            vec![partition_id - self.out_start + self.in_start]
        } else {
            Vec::new()
        }
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        self.rdd_base.clone()
    }
}

#[async_trait::async_trait]
pub trait ShuffleDependencyTrait: Serialize + Deserialize + Send + Sync {
    fn get_shuffle_id(&self) -> usize;
    fn get_rdd_base(&self) -> Arc<dyn RddBase>;
    fn is_shuffle(&self) -> bool;
    async fn do_shuffle_task(&self, rdd_base: Arc<dyn RddBase>, partition: usize) -> String;
}

impl PartialOrd for dyn ShuffleDependencyTrait {
    fn partial_cmp(&self, other: &dyn ShuffleDependencyTrait) -> Option<Ordering> {
        Some(self.get_shuffle_id().cmp(&other.get_shuffle_id()))
    }
}

impl PartialEq for dyn ShuffleDependencyTrait {
    fn eq(&self, other: &dyn ShuffleDependencyTrait) -> bool {
        self.get_shuffle_id() == other.get_shuffle_id()
    }
}

impl Eq for dyn ShuffleDependencyTrait {}

impl Ord for dyn ShuffleDependencyTrait {
    fn cmp(&self, other: &dyn ShuffleDependencyTrait) -> Ordering {
        self.get_shuffle_id().cmp(&other.get_shuffle_id())
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct ShuffleDependency<K: Data, V: Data, C: Data> {
    pub shuffle_id: usize,
    pub is_cogroup: bool,
    #[serde(with = "serde_traitobject")]
    pub rdd_base: Arc<dyn RddBase>,
    #[serde(with = "serde_traitobject")]
    pub aggregator: Arc<Aggregator<K, V, C>>,
    #[serde(with = "serde_traitobject")]
    pub partitioner: Box<dyn Partitioner>,
    is_shuffle: bool,
}

impl<K: Data, V: Data, C: Data> ShuffleDependency<K, V, C> {
    pub fn new(
        shuffle_id: usize,
        is_cogroup: bool,
        rdd_base: Arc<dyn RddBase>,
        aggregator: Arc<Aggregator<K, V, C>>,
        partitioner: Box<dyn Partitioner>,
    ) -> Self {
        ShuffleDependency {
            shuffle_id,
            is_cogroup,
            rdd_base,
            aggregator,
            partitioner,
            is_shuffle: true,
        }
    }
}

#[async_trait::async_trait]
impl<K: Data + Eq + Hash, V: Data, C: Data> ShuffleDependencyTrait for ShuffleDependency<K, V, C> {
    fn get_shuffle_id(&self) -> usize {
        self.shuffle_id
    }

    fn is_shuffle(&self) -> bool {
        self.is_shuffle
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        self.rdd_base.clone()
    }

    async fn do_shuffle_task(&self, rdd_base: Arc<dyn RddBase>, partition: usize) -> String {
        log::debug!(
            "executing shuffle task #{} for partition #{}",
            self.shuffle_id,
            partition
        );
        let split = rdd_base.splits()[partition].clone();
        let aggregator = self.aggregator.clone();
        let num_output_splits = self.partitioner.get_num_of_partitions();
        log::debug!("is cogroup rdd: {}", self.is_cogroup);
        log::debug!("number of output splits: {}", num_output_splits);
        let partitioner = self.partitioner.clone();
        let mut buckets: Vec<HashMap<K, C>> = (0..num_output_splits)
            .map(|_| HashMap::new())
            .collect::<Vec<_>>();
        log::debug!(
            "before iterating while executing shuffle map task for partition #{}",
            partition
        );
        log::debug!("split index: {}", split.get_index());

        let mut func = |iter: &mut dyn Iterator<Item = Box<dyn AnyData>>| {
            for (count, i) in iter.enumerate() {
                let b = i.into_any().downcast::<(K, V)>().unwrap();
                let (k, v) = *b;
                if count == 0 {
                    log::debug!(
                        "iterating inside dependency map task after downcasting: key: {:?}, value: {:?}",
                        k,
                        v
                    );
                }
                let bucket_id = partitioner.get_partition(&k);
                let bucket = &mut buckets[bucket_id];
                if let Some(old_v) = bucket.get_mut(&k) {
                    let input = ((old_v.clone(), v),);
                    let output = aggregator.merge_value.call(input);
                    *old_v = output;
                } else {
                    bucket.insert(k, aggregator.create_combiner.call((v,)));
                }
            }
        };

        if self.is_cogroup {
            let iter = rdd_base.cogroup_iterator_any(split).await.unwrap();
            func(&mut *iter.lock());
        } else {
            let iter = rdd_base.iterator_any(split).await.unwrap();
            func(&mut *iter.lock());
        }

        for (i, bucket) in buckets.into_iter().enumerate() {
            let set: Vec<(K, C)> = bucket.into_iter().collect();
            let ser_bytes = bincode::serialize(&set).unwrap();
            log::debug!(
                "shuffle dependency map task set from bucket #{} in shuffle id #{}, partition #{}: {:?}",
                i,
                self.shuffle_id,
                partition,
                set.get(0)
            );
            env::SHUFFLE_CACHE.insert((self.shuffle_id, partition, i), ser_bytes);
        }
        log::debug!(
            "returning shuffle address for shuffle task #{}",
            self.shuffle_id
        );
        env::Env::get().shuffle_manager.get_server_uri()
    }
}
