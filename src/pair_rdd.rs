use super::*;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
//use std::sync::Mutex;
//use parking_lot::Mutex;

// Trait containing pair rdd methods. No need of implicit conversion like in Spark version
pub trait PairRdd<K: Data + Eq + Hash, V: Data>: Rdd<(K, V)> + Send + Sync {
    //    fn iterator_any_in_pair_rdd(&self, split: Box<dyn Split>) -> Box<dyn Iterator<Item = Box<dyn AnyData>>> {
    //        let log_output = format!("inside iterator_any pair rdd",);
    //        env::log_file.lock().write(&log_output.as_bytes());
    //        Box::new(self.iterator(split).map(|(k, v)| {
    //            Box::new((
    //                k,
    //                Box::new(v) as Box<dyn AnyData>,
    //            )) as Box<dyn AnyData>
    //        }))
    //    }

    fn combine_by_key<C: Data>(
        &self,
        create_combiner: Box<dyn serde_traitobject::Fn(V) -> C + Send + Sync>,
        merge_value: Box<dyn serde_traitobject::Fn((C, V)) -> C + Send + Sync>,
        merge_combiners: Box<dyn serde_traitobject::Fn((C, C)) -> C + Send + Sync>,
        partitioner: Box<dyn Partitioner>,
        //    ) -> Arc<RddBox<(K, C)>>
    ) -> ShuffledRdd<K, V, C, Self>
    where
        //        RT: RddBox<(K, V)>,
        Self: Sized + Serialize + Deserialize + 'static,
    {
        let aggregator = Arc::new(Aggregator::<K, V, C>::new(
            create_combiner,
            merge_value,
            merge_combiners,
        ));
        ShuffledRdd::new(self.get_rdd(), aggregator, partitioner)
    }

    fn group_by_key(&self, num_splits: usize) -> ShuffledRdd<K, V, Vec<V>, Self>
    where
        Self: Sized + Serialize + Deserialize + 'static,
    {
        self.group_by_key_using_partitioner(
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>
        )
    }

    fn group_by_key_using_partitioner(
        &self,
        partitioner: Box<dyn Partitioner>,
    ) -> ShuffledRdd<K, V, Vec<V>, Self>
    where
        Self: Sized + Serialize + Deserialize + 'static,
    {
        let create_combiner = Box::new(Fn!(|v: V| vec![v]));
        fn merge_value<V: Data>(mut buf: Vec<V>, v: V) -> Vec<V> {
            buf.push(v);
            buf
            //            buf
        }
        let merge_value = Box::new(Fn!(|(buf, v)| merge_value::<V>(buf, v)));
        fn merge_combiners<V: Data>(mut b1: Vec<V>, mut b2: Vec<V>) -> Vec<V> {
            b1.append(&mut b2);
            b1
            //            b1
        }
        let merge_combiners = Box::new(Fn!(|(b1, b2)| merge_combiners::<V>(b1, b2)));
        self.combine_by_key(create_combiner, merge_value, merge_combiners, partitioner)
        //        unimplemented!()
    }
    fn reduce_by_key<F>(&self, func: F, num_splits: usize) -> ShuffledRdd<K, V, V, Self>
    where
        F: Fn((V, V)) -> V
            + PartialEq
            + Eq
            + Send
            + Sync
            + std::clone::Clone
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + 'static,
        Self: Sized + Serialize + Deserialize + 'static,
    {
        self.reduce_by_key_using_partitioner(
            func,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>,
        )
    }

    fn reduce_by_key_using_partitioner<F>(
        &self,
        func: F,
        partitioner: Box<dyn Partitioner>,
    ) -> ShuffledRdd<K, V, V, Self>
    where
        F: Fn((V, V)) -> V
            + PartialEq
            + Eq
            + Send
            + Sync
            + std::clone::Clone
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + 'static,
        Self: Sized + Serialize + Deserialize + 'static,
    {
        let create_combiner = Box::new(Fn!(|v: V| v));
        fn merge_value<V: Data, F>(buf: V, v: V, func: F) -> V
        where
            F: Fn((V, V)) -> V
                + PartialEq
                + Eq
                + Send
                + Sync
                + Clone
                + serde::ser::Serialize
                + serde::de::DeserializeOwned
                + 'static,
        {
            let p = buf;
            func((p, v))
        }
        let func_clone = func.clone();
        let merge_value = Box::new(
            Fn!([func_clone] move | (buf, v) | merge_value::<V, F>(buf, v, func_clone.clone())),
        );
        fn merge_combiners<V: Data, F>(b1: V, b2: V, func: F) -> V
        where
            F: Fn((V, V)) -> V
                + PartialEq
                + Eq
                + Send
                + Sync
                + Clone
                + serde::ser::Serialize
                + serde::de::DeserializeOwned
                + 'static,
        {
            let p = b1;
            let res = func((p, b2));
            //            *b1.lock() = func(p, b2)
            //            b1
            res
        }
        let func_clone = func.clone();
        let merge_combiners = Box::new(
            Fn!([func_clone] move | (b1, b2) | merge_combiners::<V, F>(b1, b2, func_clone.clone())),
        );
        let bufs = self.combine_by_key(create_combiner, merge_value, merge_combiners, partitioner);
        bufs
    }

    fn map_values<U: Data>(&self, f: Arc<dyn Func(V) -> U>) -> MappedValuesRdd<Self, K, V, U>
    where
        Self: Sized,
    {
        MappedValuesRdd::new(self.get_rdd(), f)
    }

    fn flat_map_values<U: Data>(
        &self,
        f: Arc<dyn Func(V) -> Box<dyn Iterator<Item = U>>>,
    ) -> FlatMappedValuesRdd<Self, K, V, U>
    where
        Self: Sized,
    {
        FlatMappedValuesRdd::new(self.get_rdd(), f)
    }

    fn join<W: Data, RT: Rdd<(K, W)>>(
        &self,
        other: RT,
        num_splits: usize,
    ) -> FlatMappedValuesRdd<
        MappedValuesRdd<CoGroupedRdd<K>, K, Vec<Vec<Box<dyn AnyData>>>, (Vec<V>, Vec<W>)>,
        K,
        (Vec<V>, Vec<W>),
        (V, W),
    > {
        let f = Fn!(|v: (Vec<V>, Vec<W>)| {
            let (vs, ws) = v;
            let combine = vs
                .into_iter()
                .flat_map(move |v| ws.clone().into_iter().map(move |w| (v.clone(), w)));
            Box::new(combine) as Box<dyn Iterator<Item = (V, W)>>
        });
        self.cogroup(
            other,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>,
        )
        .flat_map_values(Arc::new(f))
    }

    fn cogroup<W: Data, RT: Rdd<(K, W)>>(
        &self,
        other: RT,
        partitioner: Box<dyn Partitioner>,
    ) -> MappedValuesRdd<CoGroupedRdd<K>, K, Vec<Vec<Box<dyn AnyData>>>, (Vec<V>, Vec<W>)> {
        let rdds: Vec<serde_traitobject::Arc<dyn RddBase>> = vec![
            serde_traitobject::Arc::from(self.get_rdd_base()),
            serde_traitobject::Arc::from(other.get_rdd_base()),
        ];
        let cg_rdd = CoGroupedRdd::<K>::new(rdds, partitioner);
        let f = Fn!(|v: Vec<Vec<Box<dyn AnyData>>>| -> (Vec<V>, Vec<W>) {
            let mut count = 0;
            let mut vs: Vec<V> = Vec::new();
            let mut ws: Vec<W> = Vec::new();
            for v in v.into_iter() {
                if count >= 2 {
                    break;
                }
                if count == 0 {
                    for i in v {
                        vs.push(*(i.into_any().downcast::<V>().unwrap()))
                    }
                } else if count == 1 {
                    for i in v {
                        ws.push(*(i.into_any().downcast::<W>().unwrap()))
                    }
                }
                count += 1;
            }
            (vs, ws)
        });
        cg_rdd.map_values(Arc::new(f))
    }

    //        fn map_values<U:Data,F>(&self, f: F) -> MappedValuesRdd<Self, K, V, U, F> where
    //            F: Fn(V) -> U + 'static + Send + Sync + PartialEq + Eq + Clone + serde::ser::Serialize + serde::de::DeserializeOwned,
    //            Self: Sized,
    //        {
    //            MappedValuesRdd::new(self.get_rdd(), f)
    //        }
}

// Implementing the PairRdd trait for all types which implements Rdd
impl<K: Data + Eq + Hash, V: Data, T> PairRdd<K, V> for T where T: Rdd<(K, V)> {}

#[derive(Serialize, Deserialize)]
pub struct MappedValuesRdd<RT: 'static, K: Data, V: Data, U: Data>
where
    RT: Rdd<(K, V)>,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<RT>,
    vals: Arc<RddVals>,
    #[serde(with = "serde_traitobject")]
    f: Arc<dyn Func(V) -> U>,
    _marker_t: PhantomData<K>, // phantom data is necessary because of type parameter T
    _marker_v: PhantomData<V>,
    _marker_u: PhantomData<U>,
}

impl<RT: 'static, K: Data, V: Data, U: Data> Clone for MappedValuesRdd<RT, K, V, U>
where
    RT: Rdd<(K, V)>,
{
    fn clone(&self) -> Self {
        MappedValuesRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            _marker_t: PhantomData,
            _marker_v: PhantomData,
            _marker_u: PhantomData,
        }
    }
}

impl<RT: 'static, K: Data, V: Data, U: Data> MappedValuesRdd<RT, K, V, U>
where
    RT: Rdd<(K, V)>,
{
    fn new(prev: Arc<RT>, f: Arc<dyn Func(V) -> U>) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::OneToOneDependency(Arc::new(
                //                OneToOneDependencyVals::new(prev.get_rdd(), prev.get_rdd()),
                OneToOneDependencyVals::new(prev.get_rdd_base()),
            )));
        let vals = Arc::new(vals);
        MappedValuesRdd {
            prev,
            vals,
            f,
            _marker_t: PhantomData,
            _marker_v: PhantomData,
            _marker_u: PhantomData,
        }
    }
}

impl<RT: 'static, K: Data, V: Data, U: Data> RddBase for MappedValuesRdd<RT, K, V, U>
where
    RT: Rdd<(K, V)>,
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }
    fn get_context(&self) -> Context {
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
    fn iterator_any(&self, split: Box<dyn Split>) -> Box<dyn Iterator<Item = Box<dyn AnyData>>> {
        info!("inside iterator_any mapvaluesrdd",);
        Box::new(
            self.iterator(split)
                .map(|(k, v)| Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>),
        )
    }
}

impl<RT: 'static, K: Data, V: Data, U: Data> Rdd<(K, U)> for MappedValuesRdd<RT, K, V, U>
where
    RT: Rdd<(K, V)>,
{
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }
    fn get_rdd(&self) -> Arc<Self> {
        Arc::new(self.clone())
    }
    fn compute(&self, split: Box<dyn Split>) -> Box<dyn Iterator<Item = (K, U)>> {
        let f = self.f.clone();
        Box::new(self.prev.iterator(split).map(move |(k, v)| (k, f(v))))
        //        let res = res.collect::<Vec<_>>();
        //        let log_output = format!("inside iterator maprdd values {:?}", res.get(0));
        //        env::log_file.lock().write(&log_output.as_bytes());
        //        Box::new(res.into_iter()) as Box<dyn Iterator<Item = (K, U)>>
    }
}

#[derive(Serialize, Deserialize)]
pub struct FlatMappedValuesRdd<RT: 'static, K: Data, V: Data, U: Data>
where
    RT: Rdd<(K, V)>,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<RT>,
    vals: Arc<RddVals>,
    #[serde(with = "serde_traitobject")]
    f: Arc<dyn Func(V) -> Box<dyn Iterator<Item = U>>>,
    _marker_t: PhantomData<K>, // phantom data is necessary because of type parameter T
    _marker_v: PhantomData<V>,
    _marker_u: PhantomData<U>,
}

impl<RT: 'static, K: Data, V: Data, U: Data> Clone for FlatMappedValuesRdd<RT, K, V, U>
where
    RT: Rdd<(K, V)>,
{
    fn clone(&self) -> Self {
        FlatMappedValuesRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            _marker_t: PhantomData,
            _marker_v: PhantomData,
            _marker_u: PhantomData,
        }
    }
}

impl<RT: 'static, K: Data, V: Data, U: Data> FlatMappedValuesRdd<RT, K, V, U>
where
    RT: Rdd<(K, V)>,
{
    fn new(prev: Arc<RT>, f: Arc<dyn Func(V) -> Box<dyn Iterator<Item = U>>>) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::OneToOneDependency(Arc::new(
                //                OneToOneDependencyVals::new(prev.get_rdd(), prev.get_rdd()),
                OneToOneDependencyVals::new(prev.get_rdd_base()),
            )));
        let vals = Arc::new(vals);
        FlatMappedValuesRdd {
            prev,
            vals,
            f,
            _marker_t: PhantomData,
            _marker_v: PhantomData,
            _marker_u: PhantomData,
        }
    }
}

impl<RT: 'static, K: Data, V: Data, U: Data> RddBase for FlatMappedValuesRdd<RT, K, V, U>
where
    RT: Rdd<(K, V)>,
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }
    fn get_context(&self) -> Context {
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
    fn iterator_any(&self, split: Box<dyn Split>) -> Box<dyn Iterator<Item = Box<dyn AnyData>>> {
        info!("inside iterator_any flatmapvaluesrdd",);
        Box::new(
            self.iterator(split)
                .map(|(k, v)| Box::new((k, v)) as Box<dyn AnyData>),
        )
    }
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Box<dyn Iterator<Item = Box<dyn AnyData>>> {
        info!("inside cogroup iterator_any flatmapvaluesrdd",);
        Box::new(
            self.iterator(split)
                .map(|(k, v)| Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>),
        )
    }
}

impl<RT: 'static, K: Data, V: Data, U: Data> Rdd<(K, U)> for FlatMappedValuesRdd<RT, K, V, U>
where
    RT: Rdd<(K, V)>,
{
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }
    fn get_rdd(&self) -> Arc<Self> {
        Arc::new(self.clone())
    }
    fn compute(&self, split: Box<dyn Split>) -> Box<dyn Iterator<Item = (K, U)>> {
        let f = self.f.clone();
        Box::new(
            self.prev
                .iterator(split)
                .flat_map( move |(k,v)| f(v).map(move |x| (k.clone(),x)))
//                .collect::<Vec<_>>()
//                .into_iter(),
        )
        //        let res = res.collect::<Vec<_>>();
        //        let log_output = format!("inside iterator flatmaprdd values {:?}", res.get(0));
        //        env::log_file.lock().write(&log_output.as_bytes());
        //        Box::new(res.into_iter()) as Box<dyn Iterator<Item = (K, U)>>
        //        Box::new(self.prev.iterator(split).map(move |(k,v)| (k, f(v))))
    }
}
