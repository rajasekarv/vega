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

mod serialized_data_capnp {
    include!(concat!(env!("OUT_DIR"), "/capnp/serialized_data_capnp.rs"));
}

mod context;
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
mod split;
pub use split::*;
mod cache;
mod cache_tracker;
#[macro_use]
mod scheduler;
mod aggregator;
mod dag_scheduler;
mod distributed_scheduler;
mod local_scheduler;
mod stage;
mod task;
pub use aggregator::*;
mod env;
mod job;
mod map_output_tracker;
mod partial;
mod result_task;
mod serializable_traits;
pub use env::DeploymentMode;
mod error;
pub use error::*;
pub mod fs;
mod hosts;
mod utils;

// Import global external types and macros:
pub use serde_closure::Fn;
use serde_traitobject::{Arc as SerArc, Box as SerBox};
