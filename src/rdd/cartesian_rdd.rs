use itertools::{iproduct, Itertools};

use crate::rdd::*;

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
pub struct CartesianRdd<T: Data, U: Data, RT, RU>
where
    RT: 'static + Rdd<T>,
    RU: 'static + Rdd<U>,
{
    vals: Arc<RddVals>,
    #[serde(with = "serde_traitobject")]
    rdd1: Arc<RT>,
    #[serde(with = "serde_traitobject")]
    rdd2: Arc<RU>,
    num_partitions_in_rdd2: usize,
    _marker_t: PhantomData<T>,
    _market_u: PhantomData<U>,
}

impl<T: Data, U: Data, RT, RU> CartesianRdd<T, U, RT, RU>
where
    RT: 'static + Rdd<T>,
    RU: 'static + Rdd<U>,
{
    pub(crate) fn new(rdd1: Arc<RT>, rdd2: Arc<RU>) -> CartesianRdd<T, U, RT, RU> {
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

impl<T: Data, U: Data, RT, RU> Clone for CartesianRdd<T, U, RT, RU>
where
    RT: 'static + Rdd<T>,
    RU: 'static + Rdd<U>,
{
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

impl<T: Data, U: Data, RT, RU> RddBase for CartesianRdd<T, U, RT, RU>
where
    RT: 'static + Rdd<T>,
    RU: 'static + Rdd<U>,
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

    default fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        Ok(Box::new(
            self.iterator(split)?
                .map(|x| Box::new(x) as Box<dyn AnyData>),
        ))
    }
}

impl<T: Data, U: Data, RT, RU> Rdd<(T, U)> for CartesianRdd<T, U, RT, RU>
where
    RT: 'static + Rdd<T>,
    RU: 'static + Rdd<U>,
{
    fn get_rdd(&self) -> Arc<Self>
    where
        Self: Sized,
    {
        Arc::new(self.clone())
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = (T, U)>>> {
        let current_split = split
            .downcast::<CartesianSplit>()
            .or(Err(Error::UnsupportedOperation("CartesianSplit")))?;

        let iter1 = self.rdd1.iterator(current_split.s1)?;
        // required because iter2 must be clonable:
        let iter2: Vec<_> = self.rdd2.iterator(current_split.s2)?.collect();
        Ok(Box::new(iter1.cartesian_product(iter2.into_iter())))
    }
}
