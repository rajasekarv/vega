#![feature(
    coerce_unsized,
    core_intrinsics,
    fn_traits,
    specialization,
    unboxed_closures,
    unsize
)]
#![allow(
    dead_code,
    unused,
    where_clauses_object_safety,
    non_upper_case_globals,
    deprecated
)]
#[macro_use]
extern crate downcast_rs;
#[macro_use]
extern crate serde_closure;
#[macro_use]
extern crate lazy_static;
extern crate capnp;
use log::{error, info};
use std::io::prelude::*;
pub mod serialized_data_capnp {
    include!(concat!(env!("OUT_DIR"), "/capnp/serialized_data_capnp.rs"));
}
//use serde_closure::Fn;
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Deserialize, Serialize};
use serialized_data_capnp::serialized_data;

pub mod context;
pub use context::*;

mod executor;
use executor::*;

pub mod partitioner;
pub use partitioner::*;

pub mod rdd;
pub use rdd::*;

pub mod pair_rdd;
pub use pair_rdd::*;

pub mod io;

mod dependency;
use dependency::*;

mod shuffled_rdd;
use shuffled_rdd::*;

mod split;
use split::*;

mod parallel_collection;
use parallel_collection::*;

mod co_grouped_rdd;
use co_grouped_rdd::*;

mod cache_tracker;
use cache_tracker::*;

mod cache;
use cache::*;

mod shuffle_fetcher;
use shuffle_fetcher::*;

mod shuffle_manager;
use shuffle_manager::*;

mod shuffle_map_task;
use shuffle_map_task::*;

mod scheduler;
use scheduler::*;

mod dag_scheduler;
use dag_scheduler::*;

mod task;
use task::*;

mod local_scheduler;
use local_scheduler::*;

mod distributed_scheduler;
use distributed_scheduler::*;

mod stage;
use stage::*;

mod aggregator;
use aggregator::*;

mod map_output_tracker;
use map_output_tracker::*;

mod result_task;
use result_task::*;

mod job;
use job::*;

mod serializable_traits;
use serializable_traits::{AnyData, Data, Func, SerFunc};

mod env;
//use env::*;

pub mod error;
pub use error::{Error, Result};

mod hosts;
use hosts::Hosts;
