//! This module implements parallel collection RDD for dividing the input collection for parallel processing.
use std::sync::{Arc, Weak};

use crate::context::Context;
use crate::dependency::Dependency;
use crate::error::Result;
use crate::rdd::{AnyDataStream, ComputeResult, Rdd, RddBase, RddVals};
use crate::serializable_traits::Data;
use crate::split::Split;
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};

/// A collection of objects which can be sliced into partitions with a partitioning function.
pub trait Chunkable<D>
where
    D: Data,
{
    fn slice_with_set_parts(self, parts: usize) -> Vec<Arc<Vec<D>>>;

    fn slice(self) -> Vec<Arc<Vec<D>>>
    where
        Self: Sized,
    {
        let as_many_parts_as_cpus = num_cpus::get();
        self.slice_with_set_parts(as_many_parts_as_cpus)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ParallelCollectionSplit<T> {
    rdd_id: i64,
    index: usize,
    values: Arc<Vec<T>>,
}

impl<T: Data> Split for ParallelCollectionSplit<T> {
    fn get_index(&self) -> usize {
        self.index
    }
}

impl<T: Data> ParallelCollectionSplit<T> {
    fn new(rdd_id: i64, index: usize, values: Arc<Vec<T>>) -> Self {
        ParallelCollectionSplit {
            rdd_id,
            index,
            values,
        }
    }

    fn iterator(&self) -> Box<dyn Iterator<Item = T>> {
        let data = self.values.clone();
        let len = data.len();
        Box::new((0..len).map(move |i| data[i].clone()))
    }
}

#[derive(Serialize, Deserialize)]
pub struct ParallelCollectionVals<T> {
    vals: Arc<RddVals>,
    #[serde(skip_serializing, skip_deserializing)]
    context: Weak<Context>,
    splits_: Vec<Arc<Vec<T>>>,
    num_slices: usize,
}

#[derive(Serialize, Deserialize)]
pub struct ParallelCollection<T> {
    rdd_vals: Arc<ParallelCollectionVals<T>>,
}

impl<T: Data> Clone for ParallelCollection<T> {
    fn clone(&self) -> Self {
        ParallelCollection {
            rdd_vals: self.rdd_vals.clone(),
        }
    }
}

impl<T: Data> ParallelCollection<T> {
    pub fn new<I>(context: Arc<Context>, data: I, num_slices: usize) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        ParallelCollection {
            rdd_vals: Arc::new(ParallelCollectionVals {
                context: Arc::downgrade(&context),
                vals: Arc::new(RddVals::new(context.clone())),
                splits_: ParallelCollection::slice(data, num_slices),
                num_slices,
            }),
        }
    }

    pub fn from_chunkable<C>(context: Arc<Context>, data: C) -> Self
    where
        C: Chunkable<T>,
    {
        let splits_ = data.slice();
        let rdd_vals = ParallelCollectionVals {
            context: Arc::downgrade(&context),
            vals: Arc::new(RddVals::new(context.clone())),
            num_slices: splits_.len(),
            splits_,
        };
        ParallelCollection {
            rdd_vals: Arc::new(rdd_vals),
        }
    }

    fn slice<I>(data: I, num_slices: usize) -> Vec<Arc<Vec<T>>>
    where
        I: IntoIterator<Item = T>,
    {
        if num_slices < 1 {
            panic!("Number of slices should be greater than or equal to 1");
        } else {
            let mut slice_count = 0;
            let data: Vec<_> = data.into_iter().collect();
            let data_len = data.len();
            let mut end = ((slice_count + 1) * data_len) / num_slices;
            let mut output = Vec::new();
            let mut tmp = Vec::new();
            let mut iter_count = 0;
            for i in data {
                if iter_count < end {
                    tmp.push(i);
                    iter_count += 1;
                } else {
                    slice_count += 1;
                    end = ((slice_count + 1) * data_len) / num_slices;
                    output.push(Arc::new(tmp.drain(..).collect::<Vec<_>>()));
                    tmp.push(i);
                    iter_count += 1;
                }
            }
            output.push(Arc::new(tmp.drain(..).collect::<Vec<_>>()));
            output
        }
    }
}

// impl<K: Data, V: Data> RddBase for ParallelCollection<(K, V)> {
//     fn cogroup_iterator_any(&self, split: Box<dyn Split>) -> Result<AnyDataStream> {
//         log::debug!("inside iterator_any parallel collection",);
//         super::cogroup_iterator_any(self, split)
//     }
// }

#[async_trait::async_trait]
impl<T: Data> RddBase for ParallelCollection<T> {
    fn get_rdd_id(&self) -> usize {
        self.rdd_vals.vals.id
    }
    fn get_context(&self) -> Arc<Context> {
        self.rdd_vals.vals.context.upgrade().unwrap()
    }
    fn get_dependencies(&self) -> Vec<Dependency> {
        self.rdd_vals.vals.dependencies.clone()
    }
    fn splits(&self) -> Vec<Box<dyn Split>> {
        (0..self.rdd_vals.splits_.len())
            .map(|i| {
                Box::new(ParallelCollectionSplit::new(
                    self.rdd_vals.vals.id as i64,
                    i,
                    self.rdd_vals.splits_[i as usize].clone(),
                )) as Box<dyn Split>
            })
            .collect::<Vec<Box<dyn Split>>>()
    }
    fn number_of_splits(&self) -> usize {
        self.rdd_vals.splits_.len()
    }

    async fn cogroup_iterator_any(&self, split: Box<dyn Split>) -> Result<AnyDataStream> {
        self.iterator_any(split).await
    }

    async fn iterator_any(&self, split: Box<dyn Split>) -> Result<AnyDataStream> {
        log::debug!("inside iterator_any parallel collection",);
        crate::rdd::iterator_any(self, split).await
    }
}

#[async_trait::async_trait]
impl<T: Data> Rdd for ParallelCollection<T> {
    type Item = T;
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(ParallelCollection {
            rdd_vals: self.rdd_vals.clone(),
        })
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    async fn compute(&self, split: Box<dyn Split>) -> Result<ComputeResult<Self::Item>> {
        if let Some(split) = split.downcast_ref::<ParallelCollectionSplit<T>>() {
            Ok(Arc::new(Mutex::new(split.iterator())))
        } else {
            panic!(
                "Got split object from different concrete type other than ParallelCollectionSplit"
            )
        }
    }
}
