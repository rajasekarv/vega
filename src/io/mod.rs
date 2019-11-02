mod file_reader;

use crate::*;

pub use file_reader::{LocalFsReader, LocalFsReaderConfig};

pub trait ReaderConfiguration<D>
where
    D: Data,
{
    type Reader: Chunkable<D>;
    fn build(self) -> Self::Reader;
}

pub trait DistributedReader: Data {
    /// When the different copies of the reader have been distributed
    /// to executors consume self and return a proper executor-local read.
    fn get_reading_handler(self) -> Box<dyn Read>;
}
