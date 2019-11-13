use crate::error::*;
use crate::rdd::*;

/// An RDD that applies the provided function to every partition of the parent RDD.
#[derive(Serialize, Deserialize)]
pub struct MapPartitionsRdd<RT, T: Data, U: Data, F>
where
    F: Func(Box<dyn Iterator<Item = T>>) -> Box<dyn Iterator<Item = U>> + Clone,
    RT: Rdd<T> + 'static,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<RT>,
    vals: Arc<RddVals>,
    f: F,
    _marker_t: PhantomData<T>,
}

impl<RT: 'static, T: Data, U: Data, F> Clone for MapPartitionsRdd<RT, T, U, F>
where
    F: Func(Box<dyn Iterator<Item = T>>) -> Box<dyn Iterator<Item = U>> + Clone,
    RT: Rdd<T>,
{
    fn clone(&self) -> Self {
        MapPartitionsRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            _marker_t: PhantomData,
        }
    }
}

impl<RT, T: Data, U: Data, F> MapPartitionsRdd<RT, T, U, F>
where
    F: SerFunc(Box<dyn Iterator<Item = T>>) -> Box<dyn Iterator<Item = U>>,
    RT: Rdd<T> + 'static,
{
    pub fn new(prev: Arc<RT>, f: F) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::OneToOneDependency(Arc::new(
                OneToOneDependencyVals::new(prev.get_rdd_base()),
            )));
        let vals = Arc::new(vals);
        MapPartitionsRdd {
            prev,
            vals,
            f,
            _marker_t: PhantomData,
        }
    }
}

impl<RT: 'static, T: Data, U: Data, F> RddBase for MapPartitionsRdd<RT, T, U, F>
where
    F: SerFunc(Box<dyn Iterator<Item = T>>) -> Box<dyn Iterator<Item = U>>,
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
        info!("inside iterator_any map_partitions_rdd",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|x| Box::new(x) as Box<dyn AnyData>),
        ))
    }
}

impl<RT: 'static, T: Data, V: Data, U: Data, F> RddBase for MapPartitionsRdd<RT, T, (V, U), F>
where
    F: SerFunc(Box<dyn Iterator<Item = T>>) -> Box<dyn Iterator<Item = (V, U)>>,
    RT: Rdd<T>,
{
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        info!("inside iterator_any map_partitions_rdd",);
        Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
            Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>
        })))
    }
}

impl<RT: 'static, T: Data, U: Data, F: 'static> Rdd<U> for MapPartitionsRdd<RT, T, U, F>
where
    F: SerFunc(Box<dyn Iterator<Item = T>>) -> Box<dyn Iterator<Item = U>>,
    RT: Rdd<T>,
{
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }
    fn get_rdd(&self) -> Arc<Self> {
        Arc::new(self.clone())
    }
    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = U>>> {
        Ok(Box::new(self.f.clone()(self.prev.iterator(split)?)))
    }
}
