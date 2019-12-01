use std::net::Ipv4Addr;
use std::sync::Arc;

mod file_reader;
use crate::*;
pub use file_reader::LocalFsReaderConfig;

pub trait ReaderConfiguration {
    type Reader: Rdd<Item = Vec<u8>>;
    fn make_reader(self, context: Arc<Context>) -> Self::Reader;
}

/// A particular kind of distributed dataset where a range of partitions will be pinned
/// to a particular host. This allows for controlled computation of remote functions in parallel
/// across different machines.
///
/// This may allow for particular implementations and optimizations at the cost of giving up
/// some guarantees like fault tolerance, in addition certain schedulers (e.g. YARN)
/// may not be able to perform any actions on PinnedDD.
///
/// Implementors must give a way to convert to a regular RDD with the same containing data type.
pub trait PinnedDD: RddBase + 'static {
    type Item: Data;
    /// Contrary to preferred location, this guarantees that a given partition will execute
    /// in the specified host.
    fn get_pinned_host_for_split(&self) -> (Box<dyn Split>, Ipv4Addr);
}
