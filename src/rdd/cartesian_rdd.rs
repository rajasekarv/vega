use std::marker::PhantomData;
use std::sync::Arc;

use crate::context::Context;
use crate::dependency::Dependency;
use crate::error::{Error, Result};
use crate::rdd::{AnyDataStream, ComputeResult, Rdd, RddBase, RddVals};
use crate::serializable_traits::Data;
use crate::split::Split;
use itertools::{iproduct, Itertools};
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct CartesianSplit {
    idx: usize,
    s1_idx: usize,
    s2_idx: usize,
    #[serde(with = "serde_traitobject")]
    s1: Box<dyn Split>,
    #[serde(with = "serde_traitobject")]
    s2: Box<dyn Split>,
}

impl Split for CartesianSplit {
    fn get_index(&self) -> usize {
        self.idx
    }
}

#[derive(Serialize, Deserialize)]
pub struct CartesianRdd<T: Data, U: Data> {
    vals: Arc<RddVals>,
    #[serde(with = "serde_traitobject")]
    rdd1: Arc<dyn Rdd<Item = T>>,
    #[serde(with = "serde_traitobject")]
    rdd2: Arc<dyn Rdd<Item = U>>,
    num_partitions_in_rdd2: usize,
    _marker_t: PhantomData<T>,
    _market_u: PhantomData<U>,
}

impl<T: Data, U: Data> CartesianRdd<T, U> {
    pub(crate) fn new(
        rdd1: Arc<dyn Rdd<Item = T>>,
        rdd2: Arc<dyn Rdd<Item = U>>,
    ) -> CartesianRdd<T, U> {
        let vals = Arc::new(RddVals::new(rdd1.get_context()));
        let num_partitions_in_rdd2 = rdd2.number_of_splits();
        CartesianRdd {
            vals,
            rdd1,
            rdd2,
            num_partitions_in_rdd2,
            _marker_t: PhantomData,
            _market_u: PhantomData,
        }
    }
}

impl<T: Data, U: Data> Clone for CartesianRdd<T, U> {
    fn clone(&self) -> Self {
        CartesianRdd {
            vals: self.vals.clone(),
            rdd1: self.rdd1.clone(),
            rdd2: self.rdd2.clone(),
            num_partitions_in_rdd2: self.num_partitions_in_rdd2,
            _marker_t: PhantomData,
            _market_u: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<T: Data, U: Data> RddBase for CartesianRdd<T, U> {
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
        // create the cross product split
        let mut array =
            Vec::with_capacity(self.rdd1.number_of_splits() + self.rdd2.number_of_splits());
        for (s1, s2) in iproduct!(self.rdd1.splits().iter(), self.rdd2.splits().iter()) {
            let s1_idx = s1.get_index();
            let s2_idx = s2.get_index();
            let idx = s1_idx * self.num_partitions_in_rdd2 + s2_idx;
            array.push(Box::new(CartesianSplit {
                idx,
                s1_idx,
                s2_idx,
                s1: s1.clone(),
                s2: s2.clone(),
            }) as Box<dyn Split>);
        }
        array
    }

    async fn iterator_any(&self, split: Box<dyn Split>) -> Result<AnyDataStream> {
        super::iterator_any(self, split).await
    }
}

#[async_trait::async_trait]
impl<T: Data, U: Data> Rdd for CartesianRdd<T, U> {
    type Item = (T, U);
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>>
    where
        Self: Sized,
    {
        Arc::new(self.clone())
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    async fn compute(&self, split: Box<dyn Split>) -> Result<ComputeResult<Self::Item>> {
        let current_split = split
            .downcast::<CartesianSplit>()
            .or(Err(Error::DowncastFailure("CartesianSplit")))?;
        let iter1: Vec<_> = self
            .rdd1
            .iterator(current_split.s1)
            .await?
            .lock()
            .into_iter()
            .collect();
        let iter2: Vec<_> = self
            .rdd2
            .iterator(current_split.s2)
            .await?
            .lock()
            .into_iter()
            .collect();
        Ok(Arc::new(Mutex::new(
            iter1.into_iter().cartesian_product(iter2.into_iter()),
        )))
    }
}
