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

mod aggregator;
mod cache;
mod cache_tracker;
mod context;
mod dependency;
mod env;
mod executor;
pub mod io;
mod map_output_tracker;
mod partial;
pub mod partitioner;
#[path = "rdd/rdd.rs"]
pub mod rdd;
mod scheduler;
mod serializable_traits;
mod shuffle;
mod split;
pub use env::DeploymentMode;
mod error;
pub mod fs;
mod hosts;
mod utils;

// Import global external types and macros:
pub use serde_closure::Fn;
use serde_traitobject::{Arc as SerArc, Box as SerBox};

// Re-exports:
pub use context::Context;
pub use error::*;
pub use io::LocalFsReaderConfig;
pub use rdd::{PairRdd, Rdd};
