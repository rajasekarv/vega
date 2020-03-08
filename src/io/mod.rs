use std::sync::Arc;

use crate::context::Context;
use crate::rdd::Rdd;
use crate::serializable_traits::{Data, SerFunc};
use serde_traitobject::Arc as SerArc;

mod local_file_reader;
pub use local_file_reader::{LocalFsReader, LocalFsReaderConfig};

pub trait ReaderConfiguration<I: Data> {
    fn make_reader<F, O>(self, context: Arc<Context>, decoder: F) -> SerArc<dyn Rdd<Item = O>>
    where
        O: Data,
        F: SerFunc(I) -> O;
}
