use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;

use downcast_rs::Downcast;
use serde_traitobject::Arc as SerArc;

mod local_file_reader;
use crate::*;
pub use local_file_reader::{LocalFsReader, LocalFsReaderConfig, LocalFsReaderSplit};

pub trait ReaderConfiguration {
    fn make_reader(self, context: Arc<Context>) -> SerArc<dyn Rdd<Item = Vec<u8>>>;
}
