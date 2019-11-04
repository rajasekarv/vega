mod file_reader;

use crate::*;

pub use file_reader::{DistributedLocalReader, LocalFsReaderConfig};

pub trait ReaderConfiguration<D>
where
    D: Data,
{
    type Reader: Chunkable<D> + Sized;
    fn make_reader(self) -> Self::Reader;
}
