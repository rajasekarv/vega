#![allow(clippy::module_inception)]

use std::cmp::Ordering;
use std::fs;
use std::hash::Hash;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::path::Path;
use std::sync::Arc;

use log::info;
use rand::{RngCore, SeedableRng};
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::Arc as SerArc;
use serde_traitobject::{Deserialize, Serialize};

pub mod cartesian_rdd;
pub use cartesian_rdd::CartesianRdd;
pub mod co_grouped_rdd;
pub use co_grouped_rdd::CoGroupedRdd;
pub mod pair_rdd;
pub use pair_rdd::PairRdd;
pub mod partitionwise_sampled_rdd;
pub use partitionwise_sampled_rdd::PartitionwiseSampledRdd;
pub mod shuffled_rdd;
pub use shuffled_rdd::ShuffledRdd;
pub mod map_partitions_rdd;
pub use map_partitions_rdd::MapPartitionsRdd;
pub mod rdd;
pub use rdd::*;
mod union_rdd;
pub use union_rdd::UnionVariants;

use crate::aggregator::Aggregator;
use crate::context::Context;
use crate::dependency::{
    Dependency, OneToOneDependencyTrait, OneToOneDependencyVals, ShuffleDependency,
    ShuffleDependencyTrait,
};
use crate::error::*;
use crate::partitioner::{HashPartitioner, Partitioner};
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::shuffle_fetcher::ShuffleFetcher;
use crate::split::Split;
use crate::task::TasKContext;
use crate::utils;
use crate::utils::random::{BernoulliSampler, PoissonSampler, RandomSampler};
