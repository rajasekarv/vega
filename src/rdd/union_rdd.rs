use std::any::Any;

use serde_traitobject::{Arc as SerArc, Box as SerBox};

use crate::rdd::*;
use UnionVariants::*;

#[derive(Clone, Serialize, Deserialize)]
struct UnionSplit {
    /// index of the partition
    idx: usize,
    /// the parent RDD this partition refers to
    rdd: SerArc<dyn RddBase>,
    /// index of the parent RDD this partition refers to
    parent_rdd_index: usize,
    /// index of the partition within the parent RDD this partition refers to
    parent_rdd_split_index: usize,
}

impl UnionSplit {
    fn parent_partition(&self) -> Box<dyn Split> {
        self.rdd.splits()[self.parent_rdd_split_index].clone()
    }
}

impl Split for UnionSplit {
    fn get_index(&self) -> usize {
        self.idx
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct PartitionerAwareUnionSplit {
    idx: usize,
}

impl PartitionerAwareUnionSplit {
    fn parents<'a>(
        &'a self,
        rdds: &'a [SerArc<dyn RddBase>],
    ) -> impl Iterator<Item = Box<dyn Split>> + 'a {
        rdds.iter().map(move |rdd| rdd.splits()[self.idx].clone())
    }
}

impl Split for PartitionerAwareUnionSplit {
    fn get_index(&self) -> usize {
        self.idx
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum UnionVariants<T: Data> {
    NonUniquePartitioner {
        rdds: Vec<SerArc<dyn RddBase>>,
        vals: Arc<RddVals>,
        _marker: PhantomData<T>,
    },
    /// An RDD that can take multiple RDDs partitioned by the same partitioner and
    /// unify them into a single RDD while preserving the partitioner. So m RDDs with p partitions each
    /// will be unified to a single RDD with p partitions and the same partitioner.
    // TODO: The preferred location for each partition of the unified RDD will be the most common
    // preferred location of the corresponding partitions of the parent RDDs. For example,
    // location of partition 0 of the unified RDD will be where most of partition 0 of the
    // parent RDDs are located.
    PartitionerAware {
        rdds: Vec<SerArc<dyn RddBase>>,
        vals: Arc<RddVals>,
        #[serde(with = "serde_traitobject")]
        part: Box<dyn Partitioner>,
        _marker: PhantomData<T>,
    },
}

impl<T: Data> UnionVariants<T> {
    pub(crate) fn new(rdds: &[Arc<dyn Rdd<T>>]) -> Result<Self> {
        let context = rdds[0].get_context();
        let vals = Arc::new(RddVals::new(context.clone()));
        let final_rdds: Vec<_> = rdds
            .iter()
            .map(|rdd| SerArc::from(rdd.get_rdd_base()))
            .collect();
        if !UnionVariants::has_unique_partitioner(&rdds) {
            Ok(NonUniquePartitioner {
                rdds: final_rdds,
                vals,
                _marker: PhantomData,
            })
        } else {
            let part = rdds[0].partitioner().ok_or(Error::LackingPartitioner)?;
            Ok(PartitionerAware {
                rdds: final_rdds,
                vals,
                part,
                _marker: PhantomData,
            })
        }
    }

    fn has_unique_partitioner(rdds: &[Arc<dyn Rdd<T>>]) -> bool {
        rdds.iter()
            .map(|p| p.partitioner())
            .try_fold(None, |prev: Option<Box<dyn Partitioner>>, p| {
                if let Some(partitioner) = p {
                    if let Some(prev_partitioner) = prev {
                        if prev_partitioner.equals((&*partitioner).as_any()) {
                            // only continue in case both partitioners are the same
                            Ok(Some(partitioner))
                        } else {
                            Err(())
                        }
                    } else {
                        // first element
                        Ok(Some(partitioner))
                    }
                } else {
                    Err(())
                }
            })
            .is_ok()
    }
}

impl<T: Data> RddBase for UnionVariants<T> {
    fn get_rdd_id(&self) -> usize {
        match self {
            NonUniquePartitioner { vals, .. } => vals.id,
            PartitionerAware { vals, .. } => vals.id,
        }
    }

    fn get_context(&self) -> Arc<Context> {
        match self {
            NonUniquePartitioner { vals, .. } => vals.context.clone(),
            PartitionerAware { vals, .. } => vals.context.clone(),
        }
    }

    fn get_dependencies(&self) -> &[Dependency] {
        match self {
            NonUniquePartitioner { vals, .. } => &vals.dependencies,
            PartitionerAware { vals, .. } => &vals.dependencies,
        }
    }

    fn number_of_splits(&self) -> usize {
        match self {
            NonUniquePartitioner { rdds, .. } => {
                rdds.iter().fold(0, |l, rdd| l + rdd.number_of_splits())
            }
            PartitionerAware { rdds, .. } => {
                rdds.iter().fold(0, |l, rdd| l + rdd.number_of_splits())
            }
        }
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        match self {
            NonUniquePartitioner { rdds, .. } => rdds
                .iter()
                .enumerate()
                .flat_map(|(rdd_idx, rdd)| {
                    rdd.splits()
                        .into_iter()
                        .enumerate()
                        .map(move |(split_idx, _split)| (rdd_idx, rdd, split_idx))
                })
                .enumerate()
                .map(|(idx, (rdd_idx, rdd, s_idx))| {
                    Box::new(UnionSplit {
                        idx,
                        rdd: rdd.clone(),
                        parent_rdd_index: rdd_idx,
                        parent_rdd_split_index: s_idx,
                    }) as Box<dyn Split>
                })
                .collect(),
            PartitionerAware { rdds, part, .. } => {
                let num_partitions = part.get_num_of_partitions();
                (0..num_partitions)
                    .map(|idx| Box::new(PartitionerAwareUnionSplit { idx }) as Box<dyn Split>)
                    .collect()
            }
        }
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        info!("inside iterator_any union_rdd",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|x| Box::new(x) as Box<dyn AnyData>),
        ))
    }

    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        match self {
            NonUniquePartitioner { .. } => None,
            PartitionerAware { part, .. } => Some(part.clone()),
        }
    }
}

impl<T: Data> Rdd<Box<dyn AnyData>> for UnionVariants<T> {
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<Self> {
        Arc::new(self.clone())
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        let context = self.get_context();
        match self {
            NonUniquePartitioner { rdds, .. } => {
                let part = &*split
                    .downcast::<UnionSplit>()
                    .or(Err(Error::SplitDowncast("UnionSplit")))?;
                let parent = (&rdds[part.parent_rdd_index]);
                parent.iterator_any(part.parent_partition())
            }
            PartitionerAware { rdds, .. } => {
                let split = split
                    .downcast::<PartitionerAwareUnionSplit>()
                    .or(Err(Error::SplitDowncast("PartitionerAwareUnionSplit")))?;
                let iter: Result<Vec<_>> = rdds
                    .iter()
                    .zip(split.parents(&rdds))
                    .map(|(rdd, p)| rdd.iterator_any(p.clone()))
                    .collect();
                Ok(Box::new(iter?.into_iter().flatten()))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::partitioner::HashPartitioner;

    #[test]
    fn test_union() -> Result<()> {
        let sc = Context::new("local")?;
        // does not have unique partitioner:
        {
            // let rdd0 = sc.parallelize(vec![1i32, 2, 3, 4], 2);
            // let rdd1 = sc.parallelize(vec![5i32, 6, 7, 8], 2);
            // let res = rdd0.union(rdd1)?;
            // assert_eq!(res.collect()?.len(), 8);
        }
        // has a unique partitioner:
        {
            let partitioner = HashPartitioner::<i32>::new(2);
            let co_grouped = || {
                let rdd = vec![
                    (1i32, "A".to_string()),
                    (2, "B".to_string()),
                    (3, "C".to_string()),
                    (4, "D".to_string()),
                ];
                let rdd0 = SerArc::new(sc.parallelize(rdd.clone(), 2)) as SerArc<dyn RddBase>;
                let rdd1 = SerArc::new(sc.parallelize(rdd, 2)) as SerArc<dyn RddBase>;
                CoGroupedRdd::<i32>::new(vec![rdd0, rdd1], Box::new(partitioner.clone()))
            };
            let rdd0 = co_grouped();
            let rdd1 = co_grouped();
            let res = rdd0.union(rdd1)?.collect()?;
            assert_eq!(res.len(), 8);
        }

        Ok(())
    }
}
