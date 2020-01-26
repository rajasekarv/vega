use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;

use crate::context::Context;
use crate::rdd::{Rdd, RddBase};
use crate::serializable_traits::Data;
use downcast_rs::Downcast;
use serde_traitobject::Arc as SerArc;

mod local_file_reader;
pub use local_file_reader::{LocalFsReader, LocalFsReaderConfig};

pub trait ReaderConfiguration<T: Data> {
    fn make_reader(self, context: Arc<Context>) -> SerArc<dyn Rdd<Item = T>>;
}
