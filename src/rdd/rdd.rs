use std::cmp::Ordering;
use std::fs;
use std::hash::Hash;
use std::io::{BufWriter, Write};
use std::net::Ipv4Addr;
use std::path::Path;
use std::sync::{Arc, Weak};

use crate::context::Context;
use crate::dependency::Dependency;
use crate::error::{Error, Result};
use crate::partitioner::{HashPartitioner, Partitioner};
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::split::Split;
use crate::task::TaskContext;
use crate::utils;
use crate::utils::random::{BernoulliSampler, PoissonSampler, RandomSampler};
use fasthash::MetroHasher;
use rand::{Rng, SeedableRng};
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Arc as SerArc, Deserialize, Serialize};

pub mod parallel_collection_rdd;
pub use parallel_collection_rdd::*;
pub mod cartesian_rdd;
pub use cartesian_rdd::*;
pub mod co_grouped_rdd;
pub use co_grouped_rdd::*;
pub mod coalesced_rdd;
pub use coalesced_rdd::*;
pub mod mapper_rdd;
pub use mapper_rdd::*;
pub mod flatmap_rdd;
pub use flatmap_rdd::*;
pub mod pair_rdd;
pub use pair_rdd::*;
pub mod partitionwise_sampled_rdd;
pub use partitionwise_sampled_rdd::*;
pub mod shuffled_rdd;
pub use shuffled_rdd::*;
pub mod map_partitions_rdd;
pub use map_partitions_rdd::*;
pub mod zip_rdd;
pub use zip_rdd::*;
pub mod union_rdd;
pub use union_rdd::*;

// Values which are needed for all RDDs
#[derive(Serialize, Deserialize)]
pub(crate) struct RddVals {
    pub id: usize,
    pub dependencies: Vec<Dependency>,
    should_cache: bool,
    #[serde(skip_serializing, skip_deserializing)]
    pub context: Weak<Context>,
}

impl RddVals {
    pub fn new(sc: Arc<Context>) -> Self {
        RddVals {
            id: sc.new_rdd_id(),
            dependencies: Vec::new(),
            should_cache: false,
            context: Arc::downgrade(&sc),
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
    fn get_context(&self) -> Arc<Context>;
    fn get_dependencies(&self) -> Vec<Dependency>;
    fn preferred_locations(&self, _split: Box<dyn Split>) -> Vec<Ipv4Addr> {
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
    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>>;
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        self.iterator_any(split)
    }
    fn is_pinned(&self) -> bool {
        false
    }
}

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

impl<I: Rdd + ?Sized> RddBase for serde_traitobject::Arc<I> {
    fn get_rdd_id(&self) -> usize {
        (**self).get_rdd_base().get_rdd_id()
    }
    fn get_context(&self) -> Arc<Context> {
        (**self).get_rdd_base().get_context()
    }
    fn get_dependencies(&self) -> Vec<Dependency> {
        (**self).get_rdd_base().get_dependencies()
    }
    fn splits(&self) -> Vec<Box<dyn Split>> {
        (**self).get_rdd_base().splits()
    }
    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        (**self).get_rdd_base().iterator_any(split)
    }
}

impl<I: Rdd + ?Sized> Rdd for serde_traitobject::Arc<I> {
    type Item = I::Item;
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        (**self).get_rdd()
    }
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        (**self).get_rdd_base()
    }
    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        (**self).compute(split)
    }
}

// Rdd containing methods associated with processing
pub trait Rdd: RddBase + 'static {
    type Item: Data;
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>>;

    fn get_rdd_base(&self) -> Arc<dyn RddBase>;

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>>;

    fn iterator(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        self.compute(split)
    }

    fn map<U: Data, F>(&self, f: F) -> SerArc<dyn Rdd<Item = U>>
    where
        F: SerFunc(Self::Item) -> U,
        Self: Sized,
    {
        SerArc::new(MapperRdd::new(self.get_rdd(), f))
    }

    fn flat_map<U: Data, F>(&self, f: F) -> SerArc<dyn Rdd<Item = U>>
    where
        F: SerFunc(Self::Item) -> Box<dyn Iterator<Item = U>>,
        Self: Sized,
    {
        SerArc::new(FlatMapperRdd::new(self.get_rdd(), f))
    }

    /// Return a new RDD by applying a function to each partition of this RDD.
    fn map_partitions<U: Data, F>(&self, func: F) -> SerArc<dyn Rdd<Item = U>>
    where
        F: SerFunc(Box<dyn Iterator<Item = Self::Item>>) -> Box<dyn Iterator<Item = U>>,
        Self: Sized,
    {
        let ignore_idx = Fn!(move |_index: usize,
                                   items: Box<dyn Iterator<Item = Self::Item>>|
              -> Box<dyn Iterator<Item = _>> { (func)(items) });
        SerArc::new(MapPartitionsRdd::new(self.get_rdd(), ignore_idx))
    }

    /// Return a new RDD by applying a function to each partition of this RDD,
    /// while tracking the index of the original partition.
    fn map_partitions_with_index<U: Data, F>(&self, f: F) -> SerArc<dyn Rdd<Item = U>>
    where
        F: SerFunc(usize, Box<dyn Iterator<Item = Self::Item>>) -> Box<dyn Iterator<Item = U>>,
        Self: Sized,
    {
        SerArc::new(MapPartitionsRdd::new(self.get_rdd(), f))
    }

    /// Return an RDD created by coalescing all elements within each partition into an array.
    #[allow(clippy::type_complexity)]
    fn glom(&self) -> SerArc<dyn Rdd<Item = Vec<Self::Item>>>
    where
        Self: Sized,
    {
        let func = Fn!(
            |_index: usize, iter: Box<dyn Iterator<Item = Self::Item>>| Box::new(std::iter::once(
                iter.collect::<Vec<_>>()
            ))
                as Box<Iterator<Item = Vec<Self::Item>>>
        );
        SerArc::new(MapPartitionsRdd::new(self.get_rdd(), Box::new(func)))
    }

    fn save_as_text_file(&self, path: String) -> Result<Vec<()>>
    where
        Self: Sized,
    {
        fn save<R: Data>(ctx: TaskContext, iter: Box<dyn Iterator<Item = R>>, path: String) {
            fs::create_dir_all(&path).unwrap();
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
        let cl = Fn!(move |(ctx, iter)| save::<Self::Item>(ctx, iter, path.to_string()));
        self.get_context().run_job_with_context(self.get_rdd(), cl)
    }

    fn reduce<F>(&self, f: F) -> Result<Option<Self::Item>>
    where
        Self: Sized,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
    {
        // cloned cause we will use `f` later.
        let cf = f.clone();
        let reduce_partition = Fn!(move |iter: Box<dyn Iterator<Item = Self::Item>>| {
            let acc = iter.reduce(&cf);
            match acc {
                None => vec![],
                Some(e) => vec![e],
            }
        });
        let results = self.get_context().run_job(self.get_rdd(), reduce_partition);
        Ok(results?.into_iter().flatten().reduce(f))
    }

    /// Aggregate the elements of each partition, and then the results for all the partitions, using a
    /// given associative function and a neutral "initial value". The function
    /// Fn(t1, t2) is allowed to modify t1 and return it as its result value to avoid object
    /// allocation; however, it should not modify t2.
    ///
    /// This behaves somewhat differently from fold operations implemented for non-distributed
    /// collections. This fold operation may be applied to partitions individually, and then fold
    /// those results into the final result, rather than apply the fold to each element sequentially
    /// in some defined ordering. For functions that are not commutative, the result may differ from
    /// that of a fold applied to a non-distributed collection.
    ///
    /// # Arguments
    ///
    /// * `init` - an initial value for the accumulated result of each partition for the `op`
    ///                  operator, and also the initial value for the combine results from different
    ///                  partitions for the `f` function - this will typically be the neutral
    ///                  element (e.g. `0` for summation)
    /// * `f` - a function used to both accumulate results within a partition and combine results
    ///                  from different partitions
    fn fold<F>(&self, init: Self::Item, f: F) -> Result<Self::Item>
    where
        Self: Sized,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
    {
        let cf = f.clone();
        let zero = init.clone();
        let reduce_partition =
            Fn!(move |iter: Box<dyn Iterator<Item = Self::Item>>| iter.fold(zero.clone(), &cf));
        let results = self.get_context().run_job(self.get_rdd(), reduce_partition);
        Ok(results?.into_iter().fold(init, f))
    }

    /// Aggregate the elements of each partition, and then the results for all the partitions, using
    /// given combine functions and a neutral "initial value". This function can return a different result
    /// type, U, than the type of this RDD, T. Thus, we need one operation for merging a T into an U
    /// and one operation for merging two U's, as in Rust Iterator fold method. Both of these functions are
    /// allowed to modify and return their first argument instead of creating a new U to avoid memory
    /// allocation.
    ///
    /// # Arguments
    ///
    /// * `init` - an initial value for the accumulated result of each partition for the `seq_fn` function,
    ///                  and also the initial value for the combine results from
    ///                  different partitions for the `comb_fn` function - this will typically be the
    ///                  neutral element (e.g. `vec![]` for vector aggregation or `0` for summation)
    /// * `seq_fn` - a function used to accumulate results within a partition
    /// * `comb_fn` - an associative function used to combine results from different partitions
    fn aggregate<U: Data, SF, CF>(&self, init: U, seq_fn: SF, comb_fn: CF) -> Result<U>
    where
        Self: Sized,
        SF: SerFunc(U, Self::Item) -> U,
        CF: SerFunc(U, U) -> U,
    {
        let zero = init.clone();
        let reduce_partition =
            Fn!(move |iter: Box<dyn Iterator<Item = Self::Item>>| iter.fold(zero.clone(), &seq_fn));
        let results = self.get_context().run_job(self.get_rdd(), reduce_partition);
        Ok(results?.into_iter().fold(init, comb_fn))
    }

    /// Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of
    /// elements (a, b) where a is in `this` and b is in `other`.
    fn cartesian<U: Data>(
        &self,
        other: serde_traitobject::Arc<dyn Rdd<Item = U>>,
    ) -> SerArc<dyn Rdd<Item = (Self::Item, U)>>
    where
        Self: Sized,
    {
        SerArc::new(CartesianRdd::new(self.get_rdd(), other.into()))
    }

    /// Return a new RDD that is reduced into `num_partitions` partitions.
    ///
    /// This results in a narrow dependency, e.g. if you go from 1000 partitions
    /// to 100 partitions, there will not be a shuffle, instead each of the 100
    /// new partitions will claim 10 of the current partitions. If a larger number
    /// of partitions is requested, it will stay at the current number of partitions.
    ///
    /// However, if you're doing a drastic coalesce, e.g. to num_partitions = 1,
    /// this may result in your computation taking place on fewer nodes than
    /// you like (e.g. one node in the case of num_partitions = 1). To avoid this,
    /// you can pass shuffle = true. This will add a shuffle step, but means the
    /// current upstream partitions will be executed in parallel (per whatever
    /// the current partitioning is).
    ///
    /// ## Notes
    ///
    /// With shuffle = true, you can actually coalesce to a larger number
    /// of partitions. This is useful if you have a small number of partitions,
    /// say 100, potentially with a few partitions being abnormally large. Calling
    /// coalesce(1000, shuffle = true) will result in 1000 partitions with the
    /// data distributed using a hash partitioner. The optional partition coalescer
    /// passed in must be serializable.
    fn coalesce(&self, num_partitions: usize, shuffle: bool) -> SerArc<dyn Rdd<Item = Self::Item>>
    where
        Self: Sized,
    {
        if shuffle {
            // Distributes elements evenly across output partitions, starting from a random partition.
            use std::hash::Hasher;
            let distributed_partition = Fn!(
                move |index: usize, items: Box<dyn Iterator<Item = Self::Item>>| {
                    let mut hasher = MetroHasher::default();
                    index.hash(&mut hasher);
                    let mut rand = utils::random::get_default_rng_from_seed(hasher.finish());
                    let mut position = rand.gen_range(0, num_partitions);
                    Box::new(items.map(move |t| {
                        // Note that the hash code of the key will just be the key itself.
                        // The HashPartitioner will mod it with the number of total partitions.
                        position += 1;
                        (position, t)
                    })) as Box<dyn Iterator<Item = (usize, Self::Item)>>
                }
            );

            let map_steep: SerArc<dyn Rdd<Item = (usize, Self::Item)>> =
                SerArc::new(MapPartitionsRdd::new(self.get_rdd(), distributed_partition));
            let partitioner = Box::new(HashPartitioner::<usize>::new(num_partitions));
            SerArc::new(CoalescedRdd::new(
                Arc::new(map_steep.partition_by_key(partitioner)),
                num_partitions,
            ))
        } else {
            SerArc::new(CoalescedRdd::new(self.get_rdd(), num_partitions))
        }
    }

    fn collect(&self) -> Result<Vec<Self::Item>>
    where
        Self: Sized,
    {
        let cl =
            Fn!(|iter: Box<dyn Iterator<Item = Self::Item>>| iter.collect::<Vec<Self::Item>>());
        let results = self.get_context().run_job(self.get_rdd(), cl)?;
        let size = results.iter().fold(0, |a, b: &Vec<Self::Item>| a + b.len());
        Ok(results
            .into_iter()
            .fold(Vec::with_capacity(size), |mut acc, v| {
                acc.extend(v);
                acc
            }))
    }

    fn count(&self) -> Result<u64>
    where
        Self: Sized,
    {
        let context = self.get_context();
        let counting_func =
            Fn!(|iter: Box<dyn Iterator<Item = Self::Item>>| { iter.count() as u64 });
        Ok(context
            .run_job(self.get_rdd(), counting_func)?
            .into_iter()
            .sum())
    }

    /// Return the count of each unique value in this RDD as a dictionary of (value, count) pairs.	
    fn count_by_value(&self) -> SerArc<dyn Rdd<Item = (Self::Item, u64)>>
    where
        Self: Sized,
        Self::Item: Data + Eq + Hash,
    {
        self.map(Fn!(|x| (x, 1u64)))
        .reduce_by_key(Box::new(Fn!(|(x, y)| x + y))
            as Box<
                dyn Func((u64, u64)) -> u64,
            >, self.number_of_splits())
    }

    /// Return a new RDD containing the distinct elements in this RDD.
    fn distinct_with_num_partitions(
        &self,
        num_partitions: usize,
    ) -> SerArc<dyn Rdd<Item = Self::Item>>
    where
        Self: Sized,
        Self::Item: Data + Eq + Hash,
    {
        self.map(Box::new(Fn!(|x| (Some(x), None)))
            as Box<
                dyn Func(Self::Item) -> (Option<Self::Item>, Option<Self::Item>),
            >)
        .reduce_by_key(Box::new(Fn!(|(x, y)| y)), num_partitions)
        .map(Box::new(Fn!(|x: (
            Option<Self::Item>,
            Option<Self::Item>
        )| {
            let (x, y) = x;
            x.unwrap()
        })))
    }

    /// Return a new RDD containing the distinct elements in this RDD.
    fn distinct(&self) -> SerArc<dyn Rdd<Item = Self::Item>>
    where
        Self: Sized,
        Self::Item: Data + Eq + Hash,
    {
        self.distinct_with_num_partitions(self.number_of_splits())
    }

    /// Return the first element in this RDD.
    fn first(&self) -> Result<Self::Item>
    where
        Self: Sized,
    {
        if let Some(result) = self.take(1)?.into_iter().next() {
            Ok(result)
        } else {
            Err(Error::UnsupportedOperation("empty collection"))
        }
    }

    /// Return a new RDD that has exactly num_partitions partitions.
    ///
    /// Can increase or decrease the level of parallelism in this RDD. Internally, this uses
    /// a shuffle to redistribute data.
    ///
    /// If you are decreasing the number of partitions in this RDD, consider using `coalesce`,
    /// which can avoid performing a shuffle.
    fn repartition(&self, num_partitions: usize) -> SerArc<dyn Rdd<Item = Self::Item>>
    where
        Self: Sized,
    {
        self.coalesce(num_partitions, true)
    }

    /// Take the first num elements of the RDD. It works by first scanning one partition, and use the
    /// results from that partition to estimate the number of additional partitions needed to satisfy
    /// the limit.
    ///
    /// This method should only be used if the resulting array is expected to be small, as
    /// all the data is loaded into the driver's memory.
    fn take(&self, num: usize) -> Result<Vec<Self::Item>>
    where
        Self: Sized,
    {
        //TODO: in original spark this is configurable; see rdd/RDD.scala:1397
        // Math.max(conf.get(RDD_LIMIT_SCALE_UP_FACTOR), 2)
        const SCALE_UP_FACTOR: f64 = 2.0;
        if num == 0 {
            return Ok(vec![]);
        }
        let mut buf = vec![];
        let total_parts = self.number_of_splits() as u32;
        let mut parts_scanned = 0_u32;
        while buf.len() < num && parts_scanned < total_parts {
            // The number of partitions to try in this iteration. It is ok for this number to be
            // greater than total_parts because we actually cap it at total_parts in run_job.
            let mut num_parts_to_try = 1u32;
            let left = num - buf.len();
            if parts_scanned > 0 {
                // If we didn't find any rows after the previous iteration, quadruple and retry.
                // Otherwise, interpolate the number of partitions we need to try, but overestimate
                // it by 50%. We also cap the estimation in the end.
                let parts_scanned = f64::from(parts_scanned);
                num_parts_to_try = if buf.is_empty() {
                    (parts_scanned * SCALE_UP_FACTOR).ceil() as u32
                } else {
                    let num_parts_to_try =
                        (1.5 * left as f64 * parts_scanned / (buf.len() as f64)).ceil();
                    num_parts_to_try.min(parts_scanned * SCALE_UP_FACTOR) as u32
                };
            }

            let partitions: Vec<_> = (parts_scanned as usize
                ..total_parts.min(parts_scanned + num_parts_to_try) as usize)
                .collect();
            let num_partitions = partitions.len() as u32;
            let take_from_partion = Fn!(move |iter: Box<dyn Iterator<Item = Self::Item>>| {
                iter.take(left).collect::<Vec<Self::Item>>()
            });

            let res = self.get_context().run_job_with_partitions(
                self.get_rdd(),
                take_from_partion,
                partitions,
            )?;

            res.into_iter().for_each(|r| {
                let take = num - buf.len();
                buf.extend(r.into_iter().take(take));
            });

            parts_scanned += num_partitions;
        }

        Ok(buf)
    }

    /// Return a sampled subset of this RDD.
    ///
    /// # Arguments
    ///
    /// * `with_replacement` - can elements be sampled multiple times (replaced when sampled out)
    /// * `fraction` - expected size of the sample as a fraction of this RDD's size
    /// ** if without replacement: probability that each element is chosen; fraction must be [0, 1]
    /// ** if with replacement: expected number of times each element is chosen; fraction must be greater than or equal to 0
    /// * seed for the random number generator
    ///
    /// # Notes
    ///
    /// This is NOT guaranteed to provide exactly the fraction of the count of the given RDD.
    ///
    /// Replacement requires extra allocations due to the nature of the used sampler (Poisson distribution).
    /// This implies a performance penalty but should be negligible unless fraction and the dataset are rather large.
    fn sample(&self, with_replacement: bool, fraction: f64) -> SerArc<dyn Rdd<Item = Self::Item>>
    where
        Self: Sized,
    {
        assert!(fraction >= 0.0);

        let sampler = if with_replacement {
            Arc::new(PoissonSampler::new(fraction, true)) as Arc<dyn RandomSampler<Self::Item>>
        } else {
            Arc::new(BernoulliSampler::new(fraction)) as Arc<dyn RandomSampler<Self::Item>>
        };
        SerArc::new(PartitionwiseSampledRdd::new(self.get_rdd(), sampler, true))
    }

    /// Return a fixed-size sampled subset of this RDD in an array.
    ///
    /// # Arguments
    ///
    /// `with_replacement` - can elements be sampled multiple times (replaced when sampled out)
    ///
    /// # Notes
    ///
    /// This method should only be used if the resulting array is expected to be small,
    /// as all the data is loaded into the driver's memory.
    ///
    /// Replacement requires extra allocations due to the nature of the used sampler (Poisson distribution).
    /// This implies a performance penalty but should be negligible unless fraction and the dataset are rather large.
    fn take_sample(
        &self,
        with_replacement: bool,
        num: u64,
        seed: Option<u64>,
    ) -> Result<Vec<Self::Item>>
    where
        Self: Sized,
    {
        const NUM_STD_DEV: f64 = 10.0f64;
        const REPETITION_GUARD: u8 = 100;
        //TODO: this could be const eval when the support is there for the necessary functions
        let max_sample_size = std::u64::MAX - (NUM_STD_DEV * (std::u64::MAX as f64).sqrt()) as u64;
        assert!(num <= max_sample_size);

        if num == 0 {
            return Ok(vec![]);
        }

        let initial_count = self.count()?;
        if initial_count == 0 {
            return Ok(vec![]);
        }

        // The original implementation uses java.util.Random which is a LCG pseudorng,
        // not cryptographically secure and some problems;
        // Here we choose Pcg64, which is a proven good performant pseudorng although without
        // strong cryptographic guarantees, which ain't necessary here.
        let mut rng = if let Some(seed) = seed {
            rand_pcg::Pcg64::seed_from_u64(seed)
        } else {
            // PCG with default specification state and stream params
            utils::random::get_default_rng()
        };

        if !with_replacement && num >= initial_count {
            let mut sample = self.collect()?;
            utils::randomize_in_place(&mut sample, &mut rng);
            Ok(sample)
        } else {
            let fraction = utils::random::compute_fraction_for_sample_size(
                num,
                initial_count,
                with_replacement,
            );
            let mut samples = self.sample(with_replacement, fraction).collect()?;

            // If the first sample didn't turn out large enough, keep trying to take samples;
            // this shouldn't happen often because we use a big multiplier for the initial size.
            let mut num_iters = 0;
            while samples.len() < num as usize && num_iters < REPETITION_GUARD {
                log::warn!(
                    "Needed to re-sample due to insufficient sample size. Repeat #{}",
                    num_iters
                );
                samples = self.sample(with_replacement, fraction).collect()?;
                num_iters += 1;
            }

            if num_iters >= REPETITION_GUARD {
                panic!("Repeated sampling {} times; aborting", REPETITION_GUARD)
            }

            utils::randomize_in_place(&mut samples, &mut rng);
            Ok(samples.into_iter().take(num as usize).collect::<Vec<_>>())
        }
    }

    /// Applies a function f to all elements of this RDD.
    fn for_each<F>(&self, func: F) -> Result<Vec<()>>
    where
        F: SerFunc(Self::Item),
        Self: Sized,
    {
        let func = Fn!(move |iter: Box<dyn Iterator<Item = Self::Item>>| iter.for_each(&func));
        self.get_context().run_job(self.get_rdd(), func)
    }

    /// Applies a function f to each partition of this RDD.
    fn for_each_partition<F>(&self, func: F) -> Result<Vec<()>>
    where
        F: SerFunc(Box<dyn Iterator<Item = Self::Item>>),
        Self: Sized + 'static,
    {
        let func = Fn!(move |iter: Box<dyn Iterator<Item = Self::Item>>| (&func)(iter));
        self.get_context().run_job(self.get_rdd(), func)
    }

    fn union(
        &self,
        other: Arc<dyn Rdd<Item = Self::Item>>,
    ) -> Result<SerArc<dyn Rdd<Item = Self::Item>>>
    where
        Self: Clone,
    {
        Ok(SerArc::new(Context::union(&[
            Arc::new(self.clone()) as Arc<dyn Rdd<Item = Self::Item>>,
            other,
        ])?))
    }

    fn zip<S: Data>(
        &self,
        second: Arc<dyn Rdd<Item = S>>,
    ) -> SerArc<dyn Rdd<Item = (Self::Item, S)>>
    where
        Self: Clone,
    {
        SerArc::new(ZippedPartitionsRdd::<Self::Item, S>::new(
            Arc::new(self.clone()) as Arc<dyn Rdd<Item = Self::Item>>,
            second,
        ))
    }

    fn intersection<T>(&self, other: Arc<T>) -> SerArc<dyn Rdd<Item = Self::Item>>
    where
        Self: Clone,
        Self::Item: Data + Eq + Hash,
        T: Rdd<Item = Self::Item> + Sized,
    {
        self.intersection_with_num_partitions(other, self.number_of_splits())
    }

    fn intersection_with_num_partitions<T>(
        &self,
        other: Arc<T>,
        num_splits: usize,
    ) -> SerArc<dyn Rdd<Item = Self::Item>>
    where
        Self: Clone,
        Self::Item: Data + Eq + Hash,
        T: Rdd<Item = Self::Item> + Sized,
    {
        let other = other
            .map(Box::new(Fn!(
                |x: Self::Item| -> (Self::Item, Option<Self::Item>) { (x, None) }
            )))
            .clone();
        self.map(
            Box::new(Fn!(
                    |x| -> (Self::Item, Option<Self::Item>){
                        (x, None)
                    }
                )
            )
        ).cogroup(
            other,
            Box::new(HashPartitioner::<Self::Item>::new(num_splits)) as Box<dyn Partitioner>
        ).map(
            Box::new(
                Fn!(
                    |(x, (v1, v2)): (Self::Item, (Vec::<Option<Self::Item>>, Vec::<Option<Self::Item>>))| -> Option<Self::Item> {
                        if v1.len() >= 1 && v2.len() >= 1 {
                            Some(x)
                        } else {
                            None
                        }
                    }
                )
            )
        ).map_partitions(
            Box::new(
                Fn!(
                    |iter: Box<dyn Iterator<Item=Option<Self::Item>>>| -> Box<dyn Iterator<Item=Self::Item>> {
                        Box::new(
                            iter.filter(|x| x.is_some()).map(|x| x.unwrap())
                        ) as Box<dyn Iterator<Item=Self::Item>>
                    }
                )
            )
        )
    }
}

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
