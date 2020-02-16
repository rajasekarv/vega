use std::any::Any;
use std::collections::HashMap;
use std::hash::Hash;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use crate::aggregator::Aggregator;
use crate::context::Context;
use crate::dependency::{
    Dependency, NarrowDependencyTrait, OneToOneDependency, ShuffleDependency,
    ShuffleDependencyTrait,
};
use crate::error::{Error, Result};
use crate::partitioner::Partitioner;
use crate::rdd::{ComputeResult, DataIter, Rdd, RddBase, RddVals};
use crate::serializable_traits::{AnyData, Data};
use crate::shuffle::{ShuffleError, ShuffleFetcher};
use crate::split::Split;
use futures::{future, FutureExt, TryFutureExt};
use log::info;
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};
use CoGroupSplitDep::{NarrowCoGroupSplitDep, ShuffleCoGroupSplitDep};

#[derive(Clone, Serialize, Deserialize)]
enum CoGroupSplitDep {
    NarrowCoGroupSplitDep {
        #[serde(with = "serde_traitobject")]
        rdd: Arc<dyn RddBase>,
        #[serde(with = "serde_traitobject")]
        split: Box<dyn Split>,
    },
    ShuffleCoGroupSplitDep {
        shuffle_id: usize,
    },
}

#[derive(Clone, Serialize, Deserialize)]
struct CoGroupSplit {
    index: usize,
    deps: Vec<CoGroupSplitDep>,
}

impl CoGroupSplit {
    fn new(index: usize, deps: Vec<CoGroupSplitDep>) -> Self {
        CoGroupSplit { index, deps }
    }
}

impl Hasher for CoGroupSplit {
    fn finish(&self) -> u64 {
        self.index as u64
    }

    fn write(&mut self, bytes: &[u8]) {
        for i in bytes {
            self.write_u8(*i);
        }
    }
}

impl Split for CoGroupSplit {
    fn get_index(&self) -> usize {
        self.index
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct CoGroupedRdd<K: Data> {
    pub(crate) vals: Arc<RddVals>,
    pub(crate) rdds: Vec<serde_traitobject::Arc<dyn RddBase>>,
    #[serde(with = "serde_traitobject")]
    pub(crate) part: Box<dyn Partitioner>,
    _marker: PhantomData<K>,
}

impl<K: Data + Eq + Hash> CoGroupedRdd<K> {
    pub fn new(rdds: Vec<serde_traitobject::Arc<dyn RddBase>>, part: Box<dyn Partitioner>) -> Self {
        let context = rdds[0].get_context();
        let mut vals = RddVals::new(context.clone());
        let create_combiner = Box::new(Fn!(|v: Box<dyn AnyData>| vec![v]));
        fn merge_value(
            mut buf: Vec<Box<dyn AnyData>>,
            v: Box<dyn AnyData>,
        ) -> Vec<Box<dyn AnyData>> {
            buf.push(v);
            buf
        }
        let merge_value = Box::new(Fn!(|(buf, v)| merge_value(buf, v)));
        fn merge_combiners(
            mut b1: Vec<Box<dyn AnyData>>,
            mut b2: Vec<Box<dyn AnyData>>,
        ) -> Vec<Box<dyn AnyData>> {
            b1.append(&mut b2);
            b1
        }
        let merge_combiners = Box::new(Fn!(|(b1, b2)| merge_combiners(b1, b2)));
        trait AnyDataWithEq: AnyData + PartialEq {}
        impl<T: AnyData + PartialEq> AnyDataWithEq for T {}
        let aggr = Arc::new(
            Aggregator::<K, Box<dyn AnyData>, Vec<Box<dyn AnyData>>>::new(
                create_combiner,
                merge_value,
                merge_combiners,
            ),
        );
        let mut deps = Vec::new();
        for (index, rdd) in rdds.iter().enumerate() {
            let part = part.clone();
            if rdd
                .partitioner()
                .map_or(false, |p| p.equals(&part as &dyn Any))
            {
                let rdd_base = rdd.clone().into();
                deps.push(Dependency::NarrowDependency(
                    Arc::new(OneToOneDependency::new(rdd_base)) as Arc<dyn NarrowDependencyTrait>,
                ))
            } else {
                let rdd_base = rdd.clone().into();
                log::debug!("creating aggregator inside cogrouprdd");
                deps.push(Dependency::ShuffleDependency(
                    Arc::new(ShuffleDependency::new(
                        context.new_shuffle_id(),
                        true,
                        rdd_base,
                        aggr.clone(),
                        part,
                    )) as Arc<dyn ShuffleDependencyTrait>,
                ))
            }
        }
        vals.dependencies = deps;
        let vals = Arc::new(vals);
        CoGroupedRdd {
            vals,
            rdds,
            part,
            _marker: PhantomData,
        }
    }
}

impl<K: Data + Eq + Hash> RddBase for CoGroupedRdd<K> {
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
        let first_rdd = self.rdds[0].clone();
        let mut splits = Vec::new();
        for i in 0..self.part.get_num_of_partitions() {
            splits.push(Box::new(CoGroupSplit::new(
                i,
                self.rdds
                    .iter()
                    .enumerate()
                    .map(|(i, r)| match &self.get_dependencies()[i] {
                        Dependency::ShuffleDependency(s) => ShuffleCoGroupSplitDep {
                            shuffle_id: s.get_shuffle_id(),
                        },
                        _ => NarrowCoGroupSplitDep {
                            rdd: r.clone().into(),
                            split: r.splits()[i].clone(),
                        },
                    })
                    .collect(),
            )) as Box<dyn Split>)
        }
        splits
    }

    fn number_of_splits(&self) -> usize {
        self.part.get_num_of_partitions()
    }

    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        let part = self.part.clone() as Box<dyn Partitioner>;
        Some(part)
    }

    fn iterator_any(&self, split: Box<dyn Split>) -> DataIter {
        super::_cogroup_iterator_any(self.get_rdd(), split)
    }
}

#[async_trait::async_trait]
impl<K: Data + Eq + Hash> Rdd for CoGroupedRdd<K> {
    type Item = (K, Vec<Vec<Box<dyn AnyData>>>);
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    async fn compute(&self, split: Box<dyn Split>) -> Result<ComputeResult<Self::Item>> {
        type CoGroupedByKey<K> = Arc<Mutex<HashMap<K, Vec<Vec<Box<dyn AnyData>>>>>>;
        let mut agg: CoGroupedByKey<K> = Arc::new(Mutex::new(HashMap::new()));
        let split = split
            .downcast::<CoGroupSplit>()
            .or(Err(Error::DowncastFailure("CoGroupSplit")))?;
        {
            let num_rdds = self.rdds.len();

            let agg_clone = agg.clone();
            let downcast_fn = move |dep_num: usize, i: Box<dyn AnyData>| -> Result<()> {
                log::debug!("inside iterator cogrouprdd narrow dep iterator any {:?}", i);
                let (k, v) = *i
                    .into_any()
                    .downcast::<(Box<dyn AnyData>, Box<dyn AnyData>)>()
                    .or(Err(Error::DowncastFailure("unknowable")))?;
                let k = *(k
                    .into_any()
                    .downcast::<K>()
                    .or(Err(Error::DowncastFailure("unknowable")))?);
                let mut t = agg_clone.lock();
                t.entry(k).or_insert_with(|| vec![Vec::new(); num_rdds])[dep_num].push(v);
                Ok(())
            };

            let split_idx = split.get_index();
            let mut tasks = Vec::with_capacity(split.deps.len());
            for (dep_num, dep) in split.deps.into_iter().enumerate() {
                match dep {
                    NarrowCoGroupSplitDep { rdd, split } => {
                        let downcast_fn = downcast_fn.clone();
                        let dep_num = dep_num;
                        let iter =
                            tokio::spawn(rdd.iterator_any(split).map(move |iter| {
                                iter.map(move |element| -> Result<()> {
                                    downcast_fn(dep_num, element)?;
                                    Ok(())
                                })
                                .fold(Ok(()), |curr, res| if res.is_err() { res } else { curr })
                            }));
                        tasks.push(iter);
                    }
                    ShuffleCoGroupSplitDep { shuffle_id } => {
                        log::debug!("inside iterator cogrouprdd  shuffle dep agg {:?}", agg);
                        let merge_pair = |(k, c): (K, Vec<Box<dyn AnyData>>)| {
                            let mut temp = agg.lock();
                            let temp = temp
                                .entry(k)
                                .or_insert_with(|| vec![Vec::new(); self.rdds.len()]);
                            for v in c {
                                temp[dep_num].push(v);
                            }
                        };
                        ShuffleFetcher::fetch(shuffle_id, split_idx, merge_pair).await?;
                    }
                }
            }
            let task_results = future::join_all(tasks.into_iter())
                .await
                .into_iter()
                .flatten()
                .collect::<Result<Vec<_>>>()?;
            // guarantee that extra refs to agg are dropped here
        }
        let mut agg = Arc::try_unwrap(agg).unwrap().into_inner();
        Ok(Box::new(agg.into_iter().map(|(k, v)| (k, v))))
    }
}
