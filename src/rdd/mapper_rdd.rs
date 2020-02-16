use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as SyncOrd};
use std::sync::Arc;

use crate::context::Context;
use crate::dependency::{Dependency, OneToOneDependency};
use crate::error::{Error, Result};
use crate::rdd::{ComputeResult, DataIter, Rdd, RddBase, RddVals};
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::split::Split;
use crate::utils;
use futures::{FutureExt, StreamExt};
use parking_lot::Mutex;
use rand::Rng;
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Arc as SerArc, Box as SerBox, Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct MapperRdd<T: Data, U: Data, F>
where
    F: Func(T) -> U + Clone,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    f: F,
    pinned: AtomicBool,
    _marker_t: PhantomData<T>, // phantom data is necessary because of type parameter T
}

// Can't derive clone automatically
impl<T: Data, U: Data, F> Clone for MapperRdd<T, U, F>
where
    F: SerFunc(T) -> U + Clone,
{
    fn clone(&self) -> Self {
        MapperRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            pinned: AtomicBool::new(self.pinned.load(SyncOrd::SeqCst)),
            _marker_t: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> MapperRdd<T, U, F>
where
    F: SerFunc(T) -> U,
{
    pub(crate) fn new(prev: Arc<dyn Rdd<Item = T>>, f: F) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev.get_rdd_base()),
            )));
        let vals = Arc::new(vals);
        MapperRdd {
            prev,
            vals,
            f,
            pinned: AtomicBool::new(false),
            _marker_t: PhantomData,
        }
    }

    pub(crate) fn pin(self) -> Self {
        self.pinned.store(true, SyncOrd::SeqCst);
        self
    }
}

impl<T: Data, U: Data, F> RddBase for MapperRdd<T, U, F>
where
    F: SerFunc(T) -> U,
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
        log::debug!("inside iterator_any maprdd",);
        super::_iterator_any(self.get_rdd(), split)
    }

    fn is_pinned(&self) -> bool {
        self.pinned.load(SyncOrd::SeqCst)
    }
}

impl<T: Data, V: Data, U: Data, F> RddBase for MapperRdd<T, (V, U), F>
where
    F: SerFunc(T) -> (V, U),
{
    fn cogroup_iterator_any(&self, split: Box<dyn Split>) -> DataIter {
        log::debug!("inside iterator_any maprdd",);
        super::_cogroup_iterator_any(self.get_rdd(), split)
    }
}

#[async_trait::async_trait]
impl<T: Data, U: Data, F: 'static> Rdd for MapperRdd<T, U, F>
where
    F: SerFunc(T) -> U,
{
    type Item = U;
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    async fn compute(&self, split: Box<dyn Split>) -> Result<ComputeResult<Self::Item>> {
        let func = self.f.clone();
        let mut prev_iter = self.prev.iterator(split).await?;
        let this_iter = Box::new(prev_iter.map(move |e| func(e)));
        Ok(this_iter)
    }
}
