use super::*;
//use downcast_rs::Downcast;
use std::cmp::Ordering;
use std::collections::HashMap;
//use std::fs::File;
use std::hash::Hash;
//use std::io::prelude::*;
//use std::io::{BufWriter, Write};
//use std::marker::PhantomData;
use std::sync::Arc;
//use serde_traitobject::Any;

// Revise if enum is good choice. Considering enum since down casting one trait object to another trait object is difficult.
#[derive(Clone, Serialize, Deserialize)]
pub enum Dependency {
    #[serde(with = "serde_traitobject")]
    NarrowDependency(Arc<dyn NarrowDependencyTrait>),
    #[serde(with = "serde_traitobject")]
    OneToOneDependency(Arc<dyn OneToOneDependencyTrait>),
    #[serde(with = "serde_traitobject")]
    ShuffleDependency(Arc<dyn ShuffleDependencyTrait>),
}

pub trait NarrowDependencyTrait: Serialize + Deserialize + Send + Sync {
    fn get_parents(&self, partition_id: usize) -> Vec<usize>;
    fn get_rdd_base(&self) -> Arc<dyn RddBase>;
}

#[derive(Serialize, Deserialize, Clone)]
pub struct OneToOneDependencyVals {
    //    #[serde(with = "serde_traitobject")]
    //    rdd: Arc<RT>,
    #[serde(with = "serde_traitobject")]
    rdd_base: Arc<dyn RddBase>,
    is_shuffle: bool,
    //    _marker: PhantomData<T>,
}

impl OneToOneDependencyVals {
    pub fn new(rdd_base: Arc<dyn RddBase>) -> Self {
        OneToOneDependencyVals {
            rdd_base,
            is_shuffle: false,
        }
    }
}
pub trait OneToOneDependencyTrait: Serialize + Deserialize + Send + Sync {
    fn get_parents(&self, partition_id: i64) -> Vec<i64>;
    fn get_rdd_base(&self) -> Arc<dyn RddBase>;
}

impl OneToOneDependencyTrait for OneToOneDependencyVals {
    fn get_parents(&self, partition_id: i64) -> Vec<i64> {
        vec![partition_id]
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        self.rdd_base.clone()
    }
}

pub trait ShuffleDependencyTrait: Serialize + Deserialize + Send + Sync {
    fn get_shuffle_id(&self) -> usize;
    //    fn get_partitioner(&self) -> &dyn PartitionerBox;
    fn is_shuffle(&self) -> bool;
    fn get_rdd_base(&self) -> Arc<dyn RddBase>;
    fn do_shuffle_task(&self, rdd_base: Arc<dyn RddBase>, partition: usize) -> String;
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
//impl_downcast!(ShuffleDependencyTrait);

#[derive(Serialize, Deserialize)]
pub struct ShuffleDependency<K: Data, V: Data, C: Data> {
    pub shuffle_id: usize,
    //    #[serde(with = "serde_traitobject")]
    pub is_cogroup: bool,
    #[serde(with = "serde_traitobject")]
    pub rdd_base: Arc<dyn RddBase>,
    #[serde(with = "serde_traitobject")]
    pub aggregator: Arc<Aggregator<K, V, C>>,
    #[serde(with = "serde_traitobject")]
    pub partitioner: Box<dyn Partitioner>,
    is_shuffle: bool,
}
impl<
        K: Data,
        V: Data,
        C: Data,
        //        RT: Rdd<(K, V)> + 'static,
    > ShuffleDependency<K, V, C>
{
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

impl<K: Data + Eq + Hash, V: Data, C: Data> ShuffleDependencyTrait for ShuffleDependency<K, V, C> {
    fn get_shuffle_id(&self) -> usize {
        self.shuffle_id
    }
    //    fn get_partitioner(&self) -> &dyn PartitionerBox {
    //        &*self.partitioner
    //    }
    fn is_shuffle(&self) -> bool {
        self.is_shuffle
    }
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        self.rdd_base.clone()
    }

    fn do_shuffle_task(&self, rdd_base: Arc<dyn RddBase>, partition: usize) -> String {
        info!("doing shuffle_task for partition {}", partition);
        let split = rdd_base.splits()[partition].clone();
        let aggregator = self.aggregator.clone();
        let num_output_splits = self.partitioner.get_num_of_partitions();
        info!("is cogroup rdd{}", self.is_cogroup);
        info!("num of output splits{}", num_output_splits);
        let partitioner = self.partitioner.clone();
        let mut buckets = (0..num_output_splits)
            .map(|_| HashMap::new())
            .collect::<Vec<_>>();
        info!(
            "before rdd base iterator in shuffle map task for partition {}",
            partition
        );
        info!("split index {}", split.get_index());

        let iter = if self.is_cogroup {
            rdd_base.cogroup_iterator_any(split)
        } else {
            rdd_base.iterator_any(split.clone())
        };

        for (count, i) in iter.unwrap().enumerate() {
            //            if count % 30000 == 0 {
            //                info!(
            //                    "inside rdd base iterator in shuffle map task  count for partition {} {}",
            //                    partition, count
            //                );
            //            }
            //
            //            if count == 0 {
            //                info!(
            //                    "iterator inside dependency map task before downcasting {:?} ",
            //                    i,
            //                );
            //                info!(
            //                    "type of K and V is {:?} {:?} ",
            //                    std::intrinsics::type_name::<K>(),
            //                    std::intrinsics::type_name::<V>()
            //                );
            //            }

            let b = i.into_any().downcast::<(K, V)>().unwrap();
            let (k, v) = *b;
            if count == 0 {
                info!(
                    "iterator inside dependency map task after downcasting {:?} {:?}",
                    k, v
                );
            }
            let bucket_id = partitioner.get_partition(&k);
            let bucket = &mut buckets[bucket_id];
            let old_v = bucket.get_mut(&k);
            if old_v.is_none() {
                bucket.insert(k, Some(aggregator.create_combiner.call((v,))));
            } else {
                let old_v = old_v.unwrap();
                let old = old_v.take().unwrap();
                let input = ((old, v),);
                let output = aggregator.merge_value.call(input);
                *old_v = Some(output);
            }
            //            if count < 5 {
            //                info!(
            //                    "bucket inside shuffle dependency after count {:?} {:?} ",
            //                    count, buckets
            //                );
            //            }
        }

        for (i, bucket) in buckets.into_iter().enumerate() {
            //            let mut file = File::create(file_path.clone()).unwrap();
            //            let mut contents = String::new();
            //            file.read_to_string(&mut contents)
            //                .expect("not able to read");
            //            println!("file before {:?}", contents);
            //            let mut file = BufWriter::new(file);
            let set: Vec<(K, C)> = bucket.into_iter().map(|(k, v)| (k, v.unwrap())).collect();
            //            println!("{:?}", set);
            let ser_bytes = bincode::serialize(&set).unwrap();
            //            file.write_all(&ser_bytes[..])
            //                .expect("not able to write to file");
            // currently shuffle cache is unbounded. File write is disabled. This is just for testing and have to revert back to file write as in previous commits.
            info!(
                "shuffle dependency map task set in shuffle id, partition,i  {:?} {:?} {:?} {:?} ",
                set.get(0),
                self.shuffle_id,
                partition,
                i
            );
            env::shuffle_cache
                .write()
                .insert((self.shuffle_id, partition, i), ser_bytes);
            //            let mut contents = String::new();
            //            file.read_to_string(&mut contents)
            //                .expect("not able to read");
            //            println!("file after {:?}", contents);
            //            println!("written to file {:?}", file_path);
        }
        env::Env::get().shuffle_manager.get_server_uri()
    }
}

//impl<K: Data, V: Data, C: Data, RT: Rdd<(K, V)> + 'static, P: PartitionerBox + Clone>
//    DependencyTrait for ShuffleDependency<K, V, C, RT, P>
//{
//}

//TODO add RangeDependency
//pub trait Dependency: objekt::Clone {}
//objekt::clone_trait_object!(Dependency);
