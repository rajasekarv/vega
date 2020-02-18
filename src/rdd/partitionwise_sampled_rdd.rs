use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use crate::context::Context;
use crate::dependency::{Dependency, OneToOneDependency};
use crate::error::Result;
use crate::partitioner::Partitioner;
use crate::rdd::{ComputeResult, DataIter, Rdd, RddBase, RddVals};
use crate::serializable_traits::{AnyData, Data};
use crate::split::Split;
use crate::utils::random::RandomSampler;
use log::info;
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct PartitionwiseSampledRdd<T: Data> {
    #[serde(with = "serde_traitobject")]
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    #[serde(with = "serde_traitobject")]
    sampler: Arc<dyn RandomSampler<T>>,
    preserves_partitioning: bool,
    _marker_t: PhantomData<T>,
}

impl<T: Data> PartitionwiseSampledRdd<T> {
    pub(crate) fn new(
        prev: Arc<dyn Rdd<Item = T>>,
        sampler: Arc<dyn RandomSampler<T>>,
        preserves_partitioning: bool,
    ) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev.get_rdd_base()),
            )));
        let vals = Arc::new(vals);

        PartitionwiseSampledRdd {
            prev,
            vals,
            sampler,
            preserves_partitioning,
            _marker_t: PhantomData,
        }
    }
}

impl<T: Data> Clone for PartitionwiseSampledRdd<T> {
    fn clone(&self) -> Self {
        PartitionwiseSampledRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            sampler: self.sampler.clone(),
            preserves_partitioning: self.preserves_partitioning,
            _marker_t: PhantomData,
        }
    }
}

impl<T: Data> RddBase for PartitionwiseSampledRdd<T> {
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

    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        if self.preserves_partitioning {
            self.prev.partitioner()
        } else {
            None
        }
    }

    default fn cogroup_iterator_any(&self, split: Box<dyn Split>) -> DataIter {
        self.iterator_any(split)
    }

    fn iterator_any(&self, split: Box<dyn Split>) -> DataIter {
        log::debug!("inside PartitionwiseSampledRdd iterator_any");
        super::_iterator_any(self.get_rdd(), split)
    }
}

impl<T: Data, V: Data> RddBase for PartitionwiseSampledRdd<(T, V)> {
    fn cogroup_iterator_any(&self, split: Box<dyn Split>) -> DataIter {
        log::debug!("inside iterator_any maprdd",);
        super::_cogroup_iterator_any(self.get_rdd(), split)
    }
}

#[async_trait::async_trait]
impl<T: Data> Rdd for PartitionwiseSampledRdd<T> {
    type Item = T;
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    async fn compute(&self, split: Box<dyn Split>) -> Result<ComputeResult<Self::Item>> {
        let prev_res = self.prev.iterator(split).await?;
        let sampler_func = self.sampler.get_sampler();
        let this_iter = Box::new(sampler_func(Box::new(prev_res)).into_iter());
        Ok(this_iter)
    }
}
