use super::*;
use std::path::Path;
//use objekt::Clone;
//use chrono::format::Item;
use std::cmp::Ordering;
use std::fs;
use std::hash::Hash;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::sync::Arc;
//use std::any::Any;

pub trait Reduce<T> {
    fn reduce<F>(self, f: F) -> Option<T>
    where
        Self: Sized,
        F: FnMut(T, T) -> T;
}

impl<T, I> Reduce<T> for I
where
    I: Iterator<Item = T>,
{
    #[inline]
    fn reduce<F>(mut self, f: F) -> Option<T>
    where
        Self: Sized,
        F: FnMut(T, T) -> T,
    {
        self.next().map(|first| self.fold(first, f))
    }
}

// Values which are needed for all RDDs
#[derive(Serialize, Deserialize)]
pub struct RddVals {
    pub id: usize,
    pub dependencies: Vec<Dependency>,
    should_cache: bool,
    #[serde(skip_serializing, skip_deserializing)]
    pub context: Context,
}

impl RddVals {
    pub fn new(sc: Context) -> Self {
        RddVals {
            id: sc.new_rdd_id(),
            dependencies: Vec::new(),
            should_cache: false,
            context: sc.clone(),
        }
    }

    fn cache(mut self) -> Self {
        self.should_cache = true;
        self
    }
}

// Due to the lack of HKTs in Rust, it is difficult to have collection of generic data with different types.
// Required for storing multiple RDDs inside dependencies and other places like Tasks, etc.,
// Refactored RDD trait into two traits one having RddBase trait which contains only non generic methods which provide information for dependency lists
// Another separate Rdd containing generic methods like map, etc.,
pub trait RddBase: Send + Sync + Serialize + Deserialize {
    fn get_rdd_id(&self) -> usize;
    fn get_context(&self) -> Context;
    fn get_dependencies(&self) -> &[Dependency];
    fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        Vec::new()
    }
    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        None
    }
    fn splits(&self) -> Vec<Box<dyn Split>>;
    fn number_of_splits(&self) -> usize {
        self.splits().len()
    }
    // Analyse whether this is required or not. It requires downcasting while executing tasks which could hurt performance.
    fn iterator_any(&self, split: Box<dyn Split>) -> Box<dyn Iterator<Item = Box<dyn AnyData>>>;
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Box<dyn Iterator<Item = Box<dyn AnyData>>> {
        self.iterator_any(split)
    }
}

//pub trait RddBaseBox: RddBase + Serialize + Deserialize {}
//impl<T> RddBaseBox for T where T: RddBase + Serialize + Deserialize {}

impl PartialOrd for dyn RddBase {
    fn partial_cmp(&self, other: &dyn RddBase) -> Option<Ordering> {
        Some(self.get_rdd_id().cmp(&other.get_rdd_id()))
    }
}

impl PartialEq for dyn RddBase {
    fn eq(&self, other: &dyn RddBase) -> bool {
        self.get_rdd_id() == other.get_rdd_id()
    }
}

impl Eq for dyn RddBase {}

impl Ord for dyn RddBase {
    fn cmp(&self, other: &dyn RddBase) -> Ordering {
        self.get_rdd_id().cmp(&other.get_rdd_id())
    }
}

// Rdd containing methods associated with processing
pub trait Rdd<T: Data>: RddBase + Send + Sync + Serialize + Deserialize {
    fn get_rdd(&self) -> Arc<Self>
    where
        Self: Sized;
    fn get_rdd_base(&self) -> Arc<dyn RddBase>;
    fn iterator(&self, split: Box<dyn Split>) -> Box<dyn Iterator<Item = T>> {
        self.compute(split)
    }
    fn compute(&self, split: Box<dyn Split>) -> Box<dyn Iterator<Item = T>>;
    //    fn partitioner<P: PartialEq<Any>>(&self) -> Option<Arc<P>>
    //    where
    //        Self: Sized,
    //    {
    //        None
    //    }
    fn map<U: Data, F>(&self, f: F) -> MapperRdd<Self, T, U, F>
    where
        F: SerFunc(T) -> U,
        Self: Sized + 'static,
    {
        MapperRdd::new(self.get_rdd(), f)
    }

    fn flat_map<U: Data, F>(&self, f: F) -> FlatMapperRdd<Self, T, U, F>
    where
        F: SerFunc(T) -> Box<dyn Iterator<Item = U>>,
        Self: Sized + 'static,
    {
        FlatMapperRdd::new(self.get_rdd(), f)
    }

    fn save_as_text_file(&self, path: String)
    where
        Self: Sized + 'static,
    {
        fn save<R: Data>(ctx: TasKContext, iter: Box<dyn Iterator<Item = R>>, path: String) {
            //            let path = "/tmp/";
            fs::create_dir_all(&path);
            let id = ctx.split_id;
            let file_path = Path::new(&path).join(format!("part-{}", id));
            let f = fs::File::create(file_path).expect("unable to create file");
            let mut f = BufWriter::new(f);
            for item in iter {
                let line = format!("{:?}", item);
                f.write_all(line.as_bytes())
                    .expect("error while writing to file");
            }
        }
        let cl = Fn!([path] move |(ctx, iter)| save::<T>(ctx, iter, path.to_string()));
        self.get_context().run_job_with_context(self.get_rdd(), cl);
    }

    fn reduce<F>(&self, f: F) -> Option<T>
    where
        Self: Sized + 'static,
        F: SerFunc(T, T) -> T,
    {
        // cloned cause we will use `f` later.
        let cf = f.clone();
        let reduce_partition = Fn!([cf] move |iter: Box<dyn Iterator<Item = T>>| {
        let acc = iter.reduce(cf);
        match acc {
            None => vec![],
            Some(e) => vec![e],
        }

        });
        let results = self.get_context().run_job(self.get_rdd(), reduce_partition);
        results.into_iter().flatten().reduce(f)
    }

    fn collect(&self) -> Vec<T>
    where
        Self: Sized + 'static,
    {
        let cl = Fn!(|iter: Box<dyn Iterator<Item = T>>| iter.collect::<Vec<T>>());
        let results = self.get_context().run_job(self.get_rdd(), cl);
        let size = results.iter().fold(0, |a, b: &Vec<T>| a + b.len());
        results
            .into_iter()
            .fold(Vec::with_capacity(size), |mut acc, v| {
                acc.extend(v);
                acc
            })
    }

    /// return a new Rdd containing the distinct elements in this rdd
    // since impl trait is not possible inside Rdd, we have to explicity mention the return type. Extremely ugly but it's ok consideting it is required very less number of times.
    fn distinct_with_num_partitions(
        &self,
        num_partitions: usize,
    ) 
    -> MapperRdd<
        ShuffledRdd<
            Option<T>,
            Option<T>,
            Option<T>,
            MapperRdd<Self, T, (Option<T>, Option<T>), Box<dyn Func(T) -> (Option<T>, Option<T>)>>,
        >,
        (Option<T>, Option<T>),
        T,
        Box<dyn Func((Option<T>, Option<T>)) -> T>,
    >
    where
        Self: Sized + 'static,
        T: Data + Eq + Hash,
    {
        self.map(Box::new(Fn!(|x| (Some(x), None))) as Box<dyn Func(T) -> (Option<T>, Option<T>)>)
            .reduce_by_key(Box::new(Fn!(|(x, y)| y)), num_partitions)
            .map(Box::new(Fn!(|x: (Option<T>, Option<T>)| {
                let (x, y) = x;
                x.unwrap()
            })))
    }

    /// return a new Rdd containing the distinct elements in this rdd
    fn distinct(
        &self,
    ) 
    -> MapperRdd<
        ShuffledRdd<
            Option<T>,
            Option<T>,
            Option<T>,
            MapperRdd<Self, T, (Option<T>, Option<T>), Box<dyn Func(T) -> (Option<T>, Option<T>)>>,
        >,
        (Option<T>, Option<T>),
        T,
        Box<dyn Func((Option<T>, Option<T>)) -> T>,
    >
    where
        Self: Sized + 'static,
        T: Data + Eq + Hash,
    {
        self.distinct_with_num_partitions(self.number_of_splits())
    }

    /// Return the first element in this RDD.
    fn first(&self) -> Result<T, Box<dyn std::error::Error>>
    where
        Self: Sized + 'static,
    {
        if let Some(result) = self.take(1).into_iter().next() {
            Ok(result)
        } else {
            //FIXME: return a proper error when we add return errors
            panic!("empty collection")
        }
    }

    /// Take the first num elements of the RDD. It works by first scanning one partition, and use the
    /// results from that partition to estimate the number of additional partitions needed to satisfy
    /// the limit.
    ///
    /// This method should only be used if the resulting array is expected to be small, as
    /// all the data is loaded into the driver's memory.
    fn take(&self, num: usize) -> Vec<T>
    where
        Self: 'static + Sized,
    {
        //TODO: in original spark this is configurable; see rdd/RDD.scala:1397
        // Math.max(conf.get(RDD_LIMIT_SCALE_UP_FACTOR), 2)
        const scale_up_factor: f64 = 2.0;
        if num == 0 {
            return vec![];
        }
        let mut buf = vec![];
        let total_parts = self.number_of_splits() as u32;
        let mut parts_scanned = 0_u32;
        while (buf.len() < num && parts_scanned < total_parts) {
            // The number of partitions to try in this iteration. It is ok for this number to be
            // greater than total_parts because we actually cap it at total_parts in run_job.
            let mut num_parts_to_try = 1u32;
            let left = num - buf.len();
            if (parts_scanned > 0) {
                // If we didn't find any rows after the previous iteration, quadruple and retry.
                // Otherwise, interpolate the number of partitions we need to try, but overestimate
                // it by 50%. We also cap the estimation in the end.
                let parts_scanned = f64::from(parts_scanned);
                num_parts_to_try = if buf.is_empty() {
                    (parts_scanned * scale_up_factor).ceil() as u32
                } else {
                    let num_parts_to_try =
                        (1.5 * left as f64 * parts_scanned / (buf.len() as f64)).ceil();
                    num_parts_to_try.min(parts_scanned * scale_up_factor) as u32
                };
            }

            let partitions: Vec<_> = (parts_scanned as usize
                ..total_parts.min(parts_scanned + num_parts_to_try) as usize)
                .collect();
            let num_partitions = partitions.len() as u32;
            let take_from_partion = Fn!([left] move | iter: Box<dyn Iterator<Item = T>> | {
                iter.take(*left).collect::<Vec<T>>()
            });

            let res = self.get_context().run_job_on_partitions(
                self.get_rdd(),
                take_from_partion,
                partitions,
            );

            res.into_iter().for_each(|r| {
                let take = num - buf.len();
                buf.extend(r.into_iter().take(take));
            });

            parts_scanned += num_partitions;
        }

        buf
    }
}

//pub trait RddBox<T: Data>: Rdd<T> + Serialize + Deserialize {}

//impl<K, T: Data> RddBox<T> for K where K: Rdd<T> + Serialize + Deserialize {}

// Lot of visual noise here due to the generic implementation of RddValues.
// Have to refactor a bit by converting repetitive traits into separate trait like Data trait
#[derive(Serialize, Deserialize)]
pub struct MapperRdd<RT: 'static, T: Data, U: Data, F>
where
    F: Func(T) -> U + Clone,
    RT: Rdd<T>,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<RT>,
    vals: Arc<RddVals>,
    f: F,
    _marker_t: PhantomData<T>, // phantom data is necessary because of type parameter T
}

// Can't derive clone automatically
impl<RT: 'static, T: Data, U: Data, F> Clone for MapperRdd<RT, T, U, F>
where
    F: Func(T) -> U + Clone,
    RT: Rdd<T>,
{
    fn clone(&self) -> Self {
        MapperRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            _marker_t: PhantomData,
        }
    }
}

impl<RT: 'static, T: Data, U: Data, F> MapperRdd<RT, T, U, F>
where
    F: SerFunc(T) -> U,
    RT: Rdd<T>,
{
    fn new(prev: Arc<RT>, f: F) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::OneToOneDependency(Arc::new(
                //                OneToOneDependencyVals::new(prev.get_rdd(), prev.get_rdd()),
                OneToOneDependencyVals::new(prev.get_rdd_base()),
            )));
        let vals = Arc::new(vals);
        MapperRdd {
            prev,
            vals,
            f,
            _marker_t: PhantomData,
            //            _marker_u: PhantomData,
        }
    }
}

impl<RT: 'static, T: Data, U: Data, F> RddBase for MapperRdd<RT, T, U, F>
where
    F: SerFunc(T) -> U,
    RT: Rdd<T>,
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

    default fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Box<dyn Iterator<Item = Box<dyn AnyData>>> {
        self.iterator_any(split)
    }
    default fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Box<dyn Iterator<Item = Box<dyn AnyData>>> {
        info!("inside iterator_any maprdd",);
        Box::new(
            self.iterator(split)
                .map(|x| Box::new(x) as Box<dyn AnyData>),
        )
    }
}

impl<RT: 'static, T: Data, V: Data, U: Data, F> RddBase for MapperRdd<RT, T, (V, U), F>
where
    F: SerFunc(T) -> (V, U),
    RT: Rdd<T>,
{
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Box<dyn Iterator<Item = Box<dyn AnyData>>> {
        info!("inside iterator_any maprdd",);
        Box::new(
            self.iterator(split)
                .map(|(k, v)| Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>),
        )
    }
}

impl<RT: 'static, T: Data, U: Data, F: 'static> Rdd<U> for MapperRdd<RT, T, U, F>
where
    F: SerFunc(T) -> U,
    RT: Rdd<T>,
{
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }
    fn get_rdd(&self) -> Arc<Self> {
        Arc::new(self.clone())
    }
    fn compute(&self, split: Box<dyn Split>) -> Box<dyn Iterator<Item = U>> {
        //        let res = Box::new(self.prev.iterator(split).map((*self.f).clone()));
        Box::new(self.prev.iterator(split).map(self.f.clone()))

        //        let res = res.collect::<Vec<_>>();
        //        let log_output = format!("inside iterator maprdd {:?}", res.get(0));
        //        env::log_file.lock().write(&log_output.as_bytes());
        //        Box::new(res.into_iter()) as Box<dyn Iterator<Item = U>>
        //        let f = (**self.f).clone();
    }
}

#[derive(Serialize, Deserialize)]
pub struct FlatMapperRdd<RT: 'static, T: Data, U: Data, F>
where
    F: Func(T) -> Box<dyn Iterator<Item = U>> + Clone,
    RT: Rdd<T>,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<RT>,
    vals: Arc<RddVals>,
    f: F,
    _marker_t: PhantomData<T>, // phantom data is necessary because of type parameter T
}

impl<RT: 'static, T: Data, U: Data, F> Clone for FlatMapperRdd<RT, T, U, F>
where
    F: Func(T) -> Box<dyn Iterator<Item = U>> + Clone,
    RT: Rdd<T>,
{
    fn clone(&self) -> Self {
        FlatMapperRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            _marker_t: PhantomData,
        }
    }
}

impl<RT: 'static, T: Data, U: Data, F> FlatMapperRdd<RT, T, U, F>
where
    F: SerFunc(T) -> Box<dyn Iterator<Item = U>>,
    RT: Rdd<T>,
{
    fn new(prev: Arc<RT>, f: F) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::OneToOneDependency(Arc::new(
                OneToOneDependencyVals::new(prev.get_rdd()),
            )));
        let vals = Arc::new(vals);
        FlatMapperRdd {
            prev,
            vals,
            f,
            _marker_t: PhantomData,
        }
    }
}

impl<RT: 'static, T: Data, U: Data, F> RddBase for FlatMapperRdd<RT, T, U, F>
where
    F: SerFunc(T) -> Box<dyn Iterator<Item = U>>,
    RT: Rdd<T>,
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

    default fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Box<dyn Iterator<Item = Box<dyn AnyData>>> {
        self.iterator_any(split)
    }

    default fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Box<dyn Iterator<Item = Box<dyn AnyData>>> {
        info!("inside iterator_any flatmaprdd",);
        Box::new(
            self.iterator(split)
                .map(|x| Box::new(x) as Box<dyn AnyData>),
        )
    }
}

impl<RT: 'static, T: Data, V: Data, U: Data, F: 'static> RddBase for FlatMapperRdd<RT, T, (V, U), F>
where
    F: SerFunc(T) -> Box<dyn Iterator<Item = (V, U)>>,
    RT: Rdd<T>,
{
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Box<dyn Iterator<Item = Box<dyn AnyData>>> {
        info!("inside iterator_any flatmaprdd",);
        Box::new(
            self.iterator(split)
                .map(|(k, v)| Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>),
        )
    }
}

impl<RT: 'static, T: Data, U: Data, F: 'static> Rdd<U> for FlatMapperRdd<RT, T, U, F>
where
    F: SerFunc(T) -> Box<dyn Iterator<Item = U>>,
    RT: Rdd<T>,
{
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }
    fn get_rdd(&self) -> Arc<Self> {
        Arc::new(self.clone())
    }
    fn compute(&self, split: Box<dyn Split>) -> Box<dyn Iterator<Item = U>> {
        let f = self.f.clone();
        Box::new(
            self.prev
                .iterator(split)
                .flat_map(f)
//                .collect::<Vec<_>>()
//                .into_iter(),
        )

        //        let res = res.collect::<Vec<_>>();
        //        let log_output = format!("inside iterator flatmaprdd {:?}", res.get(0));
        //        env::log_file.lock().write(&log_output.as_bytes());
        //        Box::new(res.into_iter()) as Box<dyn Iterator<Item = U>>
    }
}
