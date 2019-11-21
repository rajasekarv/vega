use crate::error::*;
use crate::rdd::*;

#[derive(Serialize, Deserialize)]
pub struct PartitionwiseSampledRdd<RT, T: Data>
where
    RT: 'static + Rdd<T>,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<RT>,
    vals: Arc<RddVals>,
    #[serde(with = "serde_traitobject")]
    sampler: Arc<dyn RandomSampler<T>>,
    preserves_partitioning: bool,
    _marker_t: PhantomData<T>,
}

impl<RT: 'static, T: Data> PartitionwiseSampledRdd<RT, T>
where
    RT: Rdd<T>,
{
    pub(crate) fn new(
        prev: Arc<RT>,
        sampler: Arc<dyn RandomSampler<T>>,
        preserves_partitioning: bool,
    ) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::OneToOneDependency(Arc::new(
                OneToOneDependencyVals::new(prev.get_rdd()),
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

impl<RT: 'static, T: Data> Clone for PartitionwiseSampledRdd<RT, T>
where
    RT: Rdd<T>,
{
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

impl<RT: 'static, T: Data> RddBase for PartitionwiseSampledRdd<RT, T>
where
    RT: Rdd<T>,
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.clone()
    }

    fn get_dependencies(&self) -> &[Dependency] {
        &self.vals.dependencies
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

    default fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        self.iterator_any(split)
    }

    default fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        info!("inside PartitionwiseSampledRdd iterator_any");
        Ok(Box::new(
            self.iterator(split)?
                .map(|x| Box::new(x) as Box<dyn AnyData>),
        ))
    }
}

impl<RT: 'static, T: Data, V: Data> RddBase for PartitionwiseSampledRdd<RT, (T, V)>
where
    RT: Rdd<(T, V)>,
{
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        info!("inside iterator_any maprdd",);
        Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
            Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>
        })))
    }
}

impl<RT: 'static, T: Data> Rdd<T> for PartitionwiseSampledRdd<RT, T>
where
    RT: Rdd<T>,
{
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<Self> {
        Arc::new(self.clone())
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = T>>> {
        let sampler_func = self.sampler.get_sampler();
        let iter = self.prev.iterator(split)?;
        Ok(Box::new(sampler_func(iter).into_iter()) as Box<dyn Iterator<Item = T>>)
    }
}
