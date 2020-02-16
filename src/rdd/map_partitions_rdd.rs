use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::pin::Pin;
use std::sync::{atomic::AtomicBool, atomic::Ordering::SeqCst, Arc};

use crate::context::Context;
use crate::dependency::{Dependency, OneToOneDependency};
use crate::error::{Error, Result};
use crate::rdd::{ComputeResult, DataIter, Rdd, RddBase, RddVals};
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::split::Split;
use log::info;
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::Arc as SerArc;

type ComputeIterator<T> = Box<dyn Iterator<Item = T> + Send>;

/// An RDD that applies the provided function to every partition of the parent RDD.
#[derive(Serialize, Deserialize)]
pub struct MapPartitionsRdd<T: Data, U: Data, F>
where
    F: Func(usize, ComputeIterator<T>) -> ComputeIterator<U> + Clone,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    func: F,
    pinned: AtomicBool,
    _marker_t: PhantomData<T>,
}

impl<T: Data, U: Data, F> Clone for MapPartitionsRdd<T, U, F>
where
    F: Func(usize, ComputeIterator<T>) -> ComputeIterator<U> + Clone,
{
    fn clone(&self) -> Self {
        MapPartitionsRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            func: self.func.clone(),
            pinned: AtomicBool::new(self.pinned.load(SeqCst)),
            _marker_t: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> MapPartitionsRdd<T, U, F>
where
    F: SerFunc(usize, ComputeIterator<T>) -> ComputeIterator<U>,
{
    pub(crate) fn new(prev: Arc<dyn Rdd<Item = T>>, func: F) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev.get_rdd_base()),
            )));
        let vals = Arc::new(vals);
        MapPartitionsRdd {
            prev,
            vals,
            func,
            pinned: AtomicBool::new(false),
            _marker_t: PhantomData,
        }
    }

    pub(crate) fn pin(self) -> Self {
        self.pinned.store(true, SeqCst);
        self
    }
}

impl<T: Data, U: Data, F> RddBase for MapPartitionsRdd<T, U, F>
where
    F: SerFunc(usize, ComputeIterator<T>) -> ComputeIterator<U>,
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.clone()
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        self.vals.dependencies.clone()
    }

    fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        self.prev.preferred_locations(split)
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        self.prev.splits()
    }

    fn number_of_splits(&self) -> usize {
        self.prev.number_of_splits()
    }

    default fn cogroup_iterator_any(&self, split: Box<dyn Split>) -> DataIter {
        self.iterator_any(split)
    }

    fn iterator_any(&self, split: Box<dyn Split>) -> DataIter {
        log::debug!("inside iterator_any map_partitions_rdd",);
        super::_iterator_any(self.get_rdd(), split)
    }

    fn is_pinned(&self) -> bool {
        self.pinned.load(SeqCst)
    }
}

impl<T: Data, V: Data, U: Data, F> RddBase for MapPartitionsRdd<T, (V, U), F>
where
    F: SerFunc(usize, ComputeIterator<T>) -> ComputeIterator<(V, U)>,
{
    fn cogroup_iterator_any(&self, split: Box<dyn Split>) -> DataIter {
        log::debug!("inside iterator_any map_partitions_rdd",);
        super::_cogroup_iterator_any(self.get_rdd(), split)
    }
}

#[async_trait::async_trait]
impl<T: Data, U: Data, F: 'static> Rdd for MapPartitionsRdd<T, U, F>
where
    F: SerFunc(usize, ComputeIterator<T>) -> ComputeIterator<U>,
{
    type Item = U;

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    async fn compute(&self, split: Box<dyn Split>) -> Result<ComputeResult<Self::Item>> {
        let prev_res = self.prev.iterator(split.clone()).await?;
        let this_result = self.func.call((split.get_index(), Box::new(prev_res)));
        Ok(this_result)
    }
}
