use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::aggregator::Aggregator;
use crate::context::Context;
use crate::dependency::{Dependency, OneToOneDependency};
use crate::error::Result;
use crate::partitioner::{HashPartitioner, Partitioner};
use crate::rdd::{
    co_grouped_rdd::CoGroupedRdd, shuffled_rdd::ShuffledRdd, AnyDataStream, ComputeResult, Rdd,
    RddBase, RddVals,
};
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::split::Split;
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Arc as SerArc, Deserialize, Serialize};

// Trait containing pair rdd methods. No need of implicit conversion like in Spark version
pub trait PairRdd<K: Data + Eq + Hash, V: Data>: Rdd<Item = (K, V)> + Send + Sync {
    fn combine_by_key<C: Data>(
        &self,
        aggregator: Aggregator<K, V, C>,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn Rdd<Item = (K, C)>>
    where
        Self: Sized + Serialize + Deserialize + 'static,
    {
        SerArc::new(ShuffledRdd::new(
            self.get_rdd(),
            Arc::new(aggregator),
            partitioner,
        ))
    }

    fn group_by_key(&self, num_splits: usize) -> SerArc<dyn Rdd<Item = (K, Vec<V>)>>
    where
        Self: Sized + Serialize + Deserialize + 'static,
    {
        self.group_by_key_using_partitioner(
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>
        )
    }

    fn group_by_key_using_partitioner(
        &self,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn Rdd<Item = (K, Vec<V>)>>
    where
        Self: Sized + Serialize + Deserialize + 'static,
    {
        self.combine_by_key(Aggregator::<K, V, _>::default(), partitioner)
    }

    fn reduce_by_key<F>(&self, func: F, num_splits: usize) -> SerArc<dyn Rdd<Item = (K, V)>>
    where
        F: SerFunc((V, V)) -> V,
        Self: Sized + Serialize + Deserialize + 'static,
    {
        self.reduce_by_key_using_partitioner(
            func,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>,
        )
    }

    fn reduce_by_key_using_partitioner<F>(
        &self,
        func: F,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn Rdd<Item = (K, V)>>
    where
        F: SerFunc((V, V)) -> V,
        Self: Sized + Serialize + Deserialize + 'static,
    {
        let create_combiner = Box::new(Fn!(|v: V| v));
        let f_clone = func.clone();
        let merge_value = Box::new(Fn!(move |(buf, v)| { (f_clone)((buf, v)) }));
        let merge_combiners = Box::new(Fn!(move |(b1, b2)| { (func)((b1, b2)) }));
        let aggregator = Aggregator::new(create_combiner, merge_value, merge_combiners);
        self.combine_by_key(aggregator, partitioner)
    }

    fn map_values<U: Data, F: SerFunc(V) -> U + Clone>(
        &self,
        f: F,
    ) -> SerArc<dyn Rdd<Item = (K, U)>>
    where
        F: SerFunc(V) -> U + Clone,
        Self: Sized,
    {
        SerArc::new(MappedValuesRdd::new(self.get_rdd(), f))
    }

    fn flat_map_values<U: Data, F: SerFunc(V) -> Box<dyn Iterator<Item = U>> + Clone>(
        &self,
        f: F,
    ) -> SerArc<dyn Rdd<Item = (K, U)>>
    where
        F: SerFunc(V) -> Box<dyn Iterator<Item = U>> + Clone,
        Self: Sized,
    {
        SerArc::new(FlatMappedValuesRdd::new(self.get_rdd(), f))
    }

    fn join<W: Data>(
        &self,
        other: serde_traitobject::Arc<dyn Rdd<Item = (K, W)>>,
        num_splits: usize,
    ) -> SerArc<dyn Rdd<Item = (K, (V, W))>> {
        let f = Fn!(|v: (Vec<V>, Vec<W>)| {
            let (vs, ws) = v;
            let combine = vs
                .into_iter()
                .flat_map(move |v| ws.clone().into_iter().map(move |w| (v.clone(), w)));
            Box::new(combine) as Box<dyn Iterator<Item = (V, W)>>
        });
        self.cogroup(
            other,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>,
        )
        .flat_map_values(Box::new(f))
    }

    fn cogroup<W: Data>(
        &self,
        other: serde_traitobject::Arc<dyn Rdd<Item = (K, W)>>,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn Rdd<Item = (K, (Vec<V>, Vec<W>))>> {
        let rdds: Vec<serde_traitobject::Arc<dyn RddBase>> = vec![
            serde_traitobject::Arc::from(self.get_rdd_base()),
            serde_traitobject::Arc::from(other.get_rdd_base()),
        ];
        let cg_rdd = CoGroupedRdd::<K>::new(rdds, partitioner);
        let f = Fn!(|v: Vec<Vec<Box<dyn AnyData>>>| -> (Vec<V>, Vec<W>) {
            let mut count = 0;
            let mut vs: Vec<V> = Vec::new();
            let mut ws: Vec<W> = Vec::new();
            for v in v.into_iter() {
                if count >= 2 {
                    break;
                }
                if count == 0 {
                    for i in v {
                        vs.push(*(i.into_any().downcast::<V>().unwrap()))
                    }
                } else if count == 1 {
                    for i in v {
                        ws.push(*(i.into_any().downcast::<W>().unwrap()))
                    }
                }
                count += 1;
            }
            (vs, ws)
        });
        cg_rdd.map_values(Box::new(f))
    }

    fn partition_by_key(&self, partitioner: Box<dyn Partitioner>) -> SerArc<dyn Rdd<Item = V>> {
        // Guarantee the number of partitions by introducing a shuffle phase
        let shuffle_steep = ShuffledRdd::new(
            self.get_rdd(),
            Arc::new(Aggregator::<K, V, _>::default()),
            partitioner,
        );
        // Flatten the results of the combined partitions
        let flattener = Fn!(|grouped: (K, Vec<V>)| {
            let (key, values) = grouped;
            let iter: Box<dyn Iterator<Item = _>> = Box::new(values.into_iter());
            iter
        });
        shuffle_steep.flat_map(flattener)
    }
}

// Implementing the PairRdd trait for all types which implements Rdd
impl<K: Data + Eq + Hash, V: Data, T> PairRdd<K, V> for T where T: Rdd<Item = (K, V)> {}
impl<K: Data + Eq + Hash, V: Data, T> PairRdd<K, V> for SerArc<T> where T: Rdd<Item = (K, V)> {}

#[derive(Serialize, Deserialize)]
pub struct MappedValuesRdd<K: Data, V: Data, U: Data, F>
where
    F: Func(V) -> U + Clone,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<dyn Rdd<Item = (K, V)>>,
    vals: Arc<RddVals>,
    f: F,
    _marker_t: PhantomData<K>, // phantom data is necessary because of type parameter T
    _marker_v: PhantomData<V>,
    _marker_u: PhantomData<U>,
}

impl<K: Data, V: Data, U: Data, F> Clone for MappedValuesRdd<K, V, U, F>
where
    F: Func(V) -> U + Clone,
{
    fn clone(&self) -> Self {
        MappedValuesRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            _marker_t: PhantomData,
            _marker_v: PhantomData,
            _marker_u: PhantomData,
        }
    }
}

impl<K: Data, V: Data, U: Data, F> MappedValuesRdd<K, V, U, F>
where
    F: Func(V) -> U + Clone,
{
    fn new(prev: Arc<dyn Rdd<Item = (K, V)>>, f: F) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev.get_rdd_base()),
            )));
        let vals = Arc::new(vals);
        MappedValuesRdd {
            prev,
            vals,
            f,
            _marker_t: PhantomData,
            _marker_v: PhantomData,
            _marker_u: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<K: Data, V: Data, U: Data, F> RddBase for MappedValuesRdd<K, V, U, F>
where
    F: SerFunc(V) -> U,
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        self.vals.dependencies.clone()
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        self.prev.splits()
    }

    fn number_of_splits(&self) -> usize {
        self.prev.number_of_splits()
    }

    // TODO Analyze the possible error in invariance here
    async fn iterator_any(&self, split: Box<dyn Split>) -> Result<AnyDataStream> {
        log::debug!("inside iterator_any mapvaluesrdd",);
        super::iterator_any_tuple(self, split).await
    }

    async fn cogroup_iterator_any(&self, split: Box<dyn Split>) -> Result<AnyDataStream> {
        log::debug!("inside iterator_any mapvaluesrdd",);
        super::cogroup_iterator_any(self, split).await
    }
}

#[async_trait::async_trait()]
impl<K: Data, V: Data, U: Data, F> Rdd for MappedValuesRdd<K, V, U, F>
where
    F: SerFunc(V) -> U,
{
    type Item = (K, U);

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    async fn compute(&self, split: Box<dyn Split>) -> Result<ComputeResult<Self::Item>> {
        let prev_iter = self.prev.iterator(split).await?;
        let f = self.f.clone();
        let mut prev_iter = prev_iter.lock();
        Ok(Arc::new(Mutex::new(
            prev_iter
                .into_iter()
                .map(move |(k, v)| (k, f(v)))
                .collect::<Vec<_>>()
                .into_iter(),
        )))
    }
}

#[derive(Serialize, Deserialize)]
pub struct FlatMappedValuesRdd<K: Data, V: Data, U: Data, F>
where
    F: Func(V) -> Box<dyn Iterator<Item = U>> + Clone,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<dyn Rdd<Item = (K, V)>>,
    vals: Arc<RddVals>,
    f: F,
    _marker_t: PhantomData<K>, // phantom data is necessary because of type parameter T
    _marker_v: PhantomData<V>,
    _marker_u: PhantomData<U>,
}

impl<K: Data, V: Data, U: Data, F> Clone for FlatMappedValuesRdd<K, V, U, F>
where
    F: Func(V) -> Box<dyn Iterator<Item = U>> + Clone,
{
    fn clone(&self) -> Self {
        FlatMappedValuesRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            _marker_t: PhantomData,
            _marker_v: PhantomData,
            _marker_u: PhantomData,
        }
    }
}

impl<K: Data, V: Data, U: Data, F> FlatMappedValuesRdd<K, V, U, F>
where
    F: Func(V) -> Box<dyn Iterator<Item = U>> + Clone,
{
    fn new(prev: Arc<dyn Rdd<Item = (K, V)>>, f: F) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev.get_rdd_base()),
            )));
        let vals = Arc::new(vals);
        FlatMappedValuesRdd {
            prev,
            vals,
            f,
            _marker_t: PhantomData,
            _marker_v: PhantomData,
            _marker_u: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<K: Data, V: Data, U: Data, F> RddBase for FlatMappedValuesRdd<K, V, U, F>
where
    F: SerFunc(V) -> Box<dyn Iterator<Item = U>>,
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        self.vals.dependencies.clone()
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        self.prev.splits()
    }

    fn number_of_splits(&self) -> usize {
        self.prev.number_of_splits()
    }

    // TODO Analyze the possible error in invariance here
    async fn iterator_any(&self, split: Box<dyn Split>) -> Result<AnyDataStream> {
        log::debug!("inside iterator_any flatmapvaluesrdd",);
        super::iterator_any_tuple(self, split).await
    }

    async fn cogroup_iterator_any(&self, split: Box<dyn Split>) -> Result<AnyDataStream> {
        log::debug!("inside iterator_any flatmapvaluesrdd",);
        super::cogroup_iterator_any(self, split).await
    }
}

#[async_trait::async_trait]
impl<K: Data, V: Data, U: Data, F> Rdd for FlatMappedValuesRdd<K, V, U, F>
where
    F: SerFunc(V) -> Box<dyn Iterator<Item = U>>,
{
    type Item = (K, U);

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    async fn compute(&self, split: Box<dyn Split>) -> Result<ComputeResult<Self::Item>> {
        let prev_iter = self.prev.iterator(split).await?;
        let func = self.f.clone();
        let mut prev_iter = prev_iter.lock();
        Ok(Arc::new(Mutex::new(
            prev_iter
                .into_iter()
                .map(move |(k, v)| func(v).map(move |x| (k.clone(), x)))
                .flatten()
                .collect::<Vec<_>>()
                .into_iter(),
        )))
    }
}
