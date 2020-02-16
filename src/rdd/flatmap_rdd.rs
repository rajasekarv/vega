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
use futures::stream::StreamExt;
use parking_lot::Mutex;
use rand::Rng;
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Arc as SerArc, Box as SerBox, Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct FlatMapperRdd<T: Data, U: Data, F>
where
    F: Func(T) -> Box<dyn Iterator<Item = U> + Send> + Clone,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    func: F,
}

impl<T: Data, U: Data, F> Clone for FlatMapperRdd<T, U, F>
where
    F: Func(T) -> Box<dyn Iterator<Item = U> + Send> + Clone,
{
    fn clone(&self) -> Self {
        FlatMapperRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            func: self.func.clone(),
        }
    }
}

impl<T: Data, U: Data, F> FlatMapperRdd<T, U, F>
where
    F: SerFunc(T) -> Box<dyn Iterator<Item = U> + Send>,
{
    pub(crate) fn new(prev: Arc<dyn Rdd<Item = T>>, func: F) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev.get_rdd_base()),
            )));
        let vals = Arc::new(vals);
        FlatMapperRdd { prev, vals, func }
    }
}

impl<T: Data, U: Data, F> RddBase for FlatMapperRdd<T, U, F>
where
    F: SerFunc(T) -> Box<dyn Iterator<Item = U> + Send>,
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
        log::debug!("inside iterator_any flatmaprdd",);
        super::_iterator_any(self.get_rdd(), split)
    }
}

impl<T: Data, V: Data, U: Data, F: 'static> RddBase for FlatMapperRdd<T, (V, U), F>
where
    F: SerFunc(T) -> Box<dyn Iterator<Item = (V, U)> + Send>,
{
    fn cogroup_iterator_any(&self, split: Box<dyn Split>) -> DataIter {
        log::debug!("inside iterator_any flatmaprdd",);
        super::_cogroup_iterator_any(self.get_rdd(), split)
    }
}

#[async_trait::async_trait]
impl<T: Data, U: Data, F: 'static> Rdd for FlatMapperRdd<T, U, F>
where
    F: SerFunc(T) -> Box<dyn Iterator<Item = U> + Send>,
{
    type Item = U;

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    async fn compute(&self, split: Box<dyn Split>) -> Result<ComputeResult<Self::Item>> {
        let func = self.func.clone();
        let prev_res = self.prev.iterator(split).await?;
        let this_iter = prev_res.map(move |e| func(e)).flatten();
        Ok(Box::new(this_iter))
    }
}
