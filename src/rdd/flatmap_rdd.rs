use std::marker::PhantomData;
use std::sync::Arc;

use crate::context::Context;
use crate::dependency::{Dependency, OneToOneDependency};
use crate::error::Result;
use crate::rdd::{AnyDataStream, ComputeResult, Rdd, RddBase, RddVals};
use crate::serializable_traits::{Data, Func, SerFunc};
use crate::split::Split;
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct FlatMapperRdd<T: Data, U: Data, F>
where
    F: Func(T) -> Box<dyn Iterator<Item = U>> + Clone,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    func: F,
    _marker_t: PhantomData<T>, // phantom data is necessary because of type parameter T
}

impl<T: Data, U: Data, F> Clone for FlatMapperRdd<T, U, F>
where
    F: Func(T) -> Box<dyn Iterator<Item = U>> + Clone,
{
    fn clone(&self) -> Self {
        FlatMapperRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            func: self.func.clone(),
            _marker_t: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> FlatMapperRdd<T, U, F>
where
    F: SerFunc(T) -> Box<dyn Iterator<Item = U>>,
{
    pub(crate) fn new(prev: Arc<dyn Rdd<Item = T>>, func: F) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev.get_rdd_base()),
            )));
        let vals = Arc::new(vals);
        FlatMapperRdd {
            prev,
            vals,
            func,
            _marker_t: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<T: Data, U: Data, F> RddBase for FlatMapperRdd<T, U, F>
where
    F: SerFunc(T) -> Box<dyn Iterator<Item = U>>,
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

    async fn cogroup_iterator_any(&self, split: Box<dyn Split>) -> Result<AnyDataStream> {
        self.iterator_any(split).await
    }

    async fn iterator_any(&self, split: Box<dyn Split>) -> Result<AnyDataStream> {
        log::debug!("inside iterator_any flatmaprdd",);
        super::iterator_any(self, split).await
    }
}

#[async_trait::async_trait]
impl<T: Data, U: Data, F: 'static> Rdd for FlatMapperRdd<T, U, F>
where
    F: SerFunc(T) -> Box<dyn Iterator<Item = U>>,
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
        let mut prev_res = prev_res.lock();
        let this_iter = prev_res
            .into_iter()
            .map(move |e| func(e))
            .flatten()
            .collect::<Vec<_>>();
        Ok(Arc::new(Mutex::new(this_iter.into_iter())))
    }
}
