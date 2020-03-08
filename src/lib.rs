#![feature(
    arbitrary_self_types,
    coerce_unsized,
    core_intrinsics,
    fn_traits,
    never_type,
    specialization,
    unboxed_closures,
    unsize
)]
#![allow(dead_code, where_clauses_object_safety, deprecated)]
#![allow(clippy::single_component_path_imports)]

#[macro_use]
extern crate downcast_rs;
#[macro_use]
extern crate serde_closure;

pub mod serialized_data_capnp {
    include!(concat!(env!("OUT_DIR"), "/capnp/serialized_data_capnp.rs"));
}

pub mod context;
pub use context::Context;
mod executor;
pub mod partitioner;
mod shuffle;
pub use partitioner::*;
#[path = "rdd/rdd.rs"]
pub mod rdd;
pub use rdd::*;
pub mod io;
pub use io::*;
mod dependency;
pub use dependency::*;
pub mod split;
pub use split::*;
mod cache;
mod cache_tracker;
#[macro_use]
mod scheduler;
pub mod aggregator;
mod dag_scheduler;
mod distributed_scheduler;
mod local_scheduler;
mod stage;
mod task;
pub use aggregator::*;
mod env;
mod job;
mod map_output_tracker;
mod result_task;
pub mod serializable_traits;
pub use env::DeploymentMode;
pub mod error;
pub use error::*;
pub mod fs;
mod hosts;
pub mod utils;
