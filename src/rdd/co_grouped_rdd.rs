use std::any::Any;
use std::hash::Hash;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::aggregator::Aggregator;
use crate::context::Context;
use crate::dependency::{
    Dependency, NarrowDependencyTrait, OneToOneDependency, ShuffleDependency,
    ShuffleDependencyTrait,
};
use crate::error::Result;
use crate::partitioner::Partitioner;
use crate::rdd::{AnyDataStream, ComputeResult, Rdd, RddBase, RddVals};
use crate::serializable_traits::{AnyData, Data};
use crate::shuffle::ShuffleFetcher;
use crate::split::Split;
use dashmap::DashMap;
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};

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
        for (_index, rdd) in rdds.iter().enumerate() {
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

#[async_trait::async_trait]
impl<K: Data + Eq + Hash> RddBase for CoGroupedRdd<K> {
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
        let mut splits = Vec::new();
        for i in 0..self.part.get_num_of_partitions() {
            splits.push(Box::new(CoGroupSplit::new(
                i,
                self.rdds
                    .iter()
                    .enumerate()
                    .map(|(i, r)| match &self.get_dependencies()[i] {
                        Dependency::ShuffleDependency(s) => {
                            CoGroupSplitDep::ShuffleCoGroupSplitDep {
                                shuffle_id: s.get_shuffle_id(),
                            }
                        }
                        _ => CoGroupSplitDep::NarrowCoGroupSplitDep {
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

    async fn iterator_any(&self, split: Box<dyn Split>) -> Result<AnyDataStream> {
        super::cogroup_iterator_any(self, split).await
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
        if let Ok(split) = split.downcast::<CoGroupSplit>() {
            let agg: Arc<DashMap<K, Vec<Vec<Box<dyn AnyData>>>>> = Arc::new(DashMap::new());
            for (dep_num, dep) in split.clone().deps.into_iter().enumerate() {
                match dep {
                    CoGroupSplitDep::NarrowCoGroupSplitDep { rdd, split } => {
                        log::debug!("inside iterator cogrouprdd  narrow dep");
                        rdd.iterator_any(split)
                            .await
                            .unwrap()
                            .lock()
                            .into_iter()
                            .for_each(|i: Box<dyn AnyData>| {
                                log::debug!(
                                    "inside iterator cogrouprdd  narrow dep iterator any {:?}",
                                    i
                                );
                                let b = i
                                    .into_any()
                                    .downcast::<(Box<dyn AnyData>, Box<dyn AnyData>)>()
                                    .unwrap();
                                let (k, v) = *b;
                                let k = *(k.into_any().downcast::<K>().unwrap());
                                agg.entry(k)
                                    .or_insert_with(|| vec![Vec::new(); self.rdds.len()])[dep_num]
                                    .push(v)
                            });
                    }
                    CoGroupSplitDep::ShuffleCoGroupSplitDep { shuffle_id } => {
                        log::debug!("inside iterator CoGroupedRdd shuffle dep, agg: {:?}", agg);
                        let num_rdds = self.rdds.len();
                        let agg_clone = agg.clone();
                        let merge_pair = move |(k, c): (K, Vec<Box<dyn AnyData>>)| {
                            let mut temp = agg_clone
                                .entry(k)
                                .or_insert_with(|| vec![Vec::new(); num_rdds]);
                            for v in c {
                                temp[dep_num].push(v);
                            }
                        };
                        ShuffleFetcher::fetch(shuffle_id, split.get_index(), merge_pair).await?;
                    }
                }
            }
            let agg = Arc::try_unwrap(agg).unwrap();
            Ok(Arc::new(Mutex::new(agg.into_iter().map(|(k, v)| (k, v)))))
        } else {
            panic!("Got split object from different concrete type other than CoGroupSplit")
        }
    }
}
