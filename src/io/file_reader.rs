use super::*;

use std::fs;
use std::io::prelude::*;
use std::io::{BufReader, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use log::debug;
use rand::prelude::*;

pub struct LocalFsReaderConfig {
    filter_ext: Option<std::ffi::OsString>,
    expect_dir: bool,
    dir_path: PathBuf,
    executor_partitions: Option<u64>,
}

impl LocalFsReaderConfig {
    /// Read all the files from a directory or a path.
    pub fn new<T: Into<PathBuf>>(path: T) -> LocalFsReaderConfig {
        LocalFsReaderConfig {
            filter_ext: None,
            expect_dir: true,
            dir_path: path.into(),
            executor_partitions: None,
        }
    }
    /// Only will read files with a given extension.
    pub fn filter_extension<T: Into<String>>(&mut self, extension: T) {
        self.filter_ext = Some(extension.into().into());
    }
    /// Default behaviour is to expect the directory to exist in every node,
    /// if it doesn't the executor will panic.
    pub fn expect_directory(&mut self, should_exist: bool) {
        self.expect_dir = should_exist;
    }
    /// Number of partitions to use per executor, to perform the load tasks,
    /// ideally one executor is used per host with as many partitions as CPUs available.
    // TODO: profile this for actual sensible defaults
    pub fn num_partitions_per_executor(&mut self, num: u64) {
        self.executor_partitions = Some(num);
    }
}

impl<D> ReaderConfiguration<D> for LocalFsReaderConfig
where
    D: Data,
    LocalFsReader: Chunkable<D> + Sized,
{
    type Reader = LocalFsReader;
    // TODO: give the option to load files from several hosts at the same time
    // right now the only option is to load from a single machine in parallel but it would be nice
    // to load from different machines at the same time from a likewise location
    fn make_reader(self) -> Self::Reader {
        LocalFsReader::new(self)
    }
}

/// Reads all files specified in a given directory from the local directory
/// on all executors on every worker node.
pub struct LocalFsReader {
    path: PathBuf,
    is_single_file: bool,
    filter_ext: Option<std::ffi::OsString>,
    expect_dir: bool,
    files: Vec<Vec<PathBuf>>,
    executor_partitions: u64,
}

impl LocalFsReader {
    pub fn new(config: LocalFsReaderConfig) -> LocalFsReader {
        let LocalFsReaderConfig {
            dir_path,
            expect_dir,
            filter_ext,
            executor_partitions,
        } = config;

        let is_single_file = {
            let path: &Path = dir_path.as_ref();
            path.is_file()
        };

        let num_parts = if let Some(num_partitions) = executor_partitions {
            num_partitions
        } else {
            use num_cpus;
            num_cpus::get() as u64
        };

        LocalFsReader {
            path: dir_path,
            is_single_file,
            filter_ext,
            expect_dir,
            files: vec![],
            executor_partitions: num_parts,
        }
    }

    /// Consumed self and load the files in the current host.
    /// This function should be called once per host to come with the paralel workload.
    fn load_local_files(mut self) -> Result<Vec<Vec<PathBuf>>> {
        let mut total_size = 0_u64;
        if self.is_single_file {
            let size = fs::metadata(&self.path)?.len();
            total_size += size;
            self.files.push(vec![self.path.clone()]);
            return Ok(self.files);
        }

        let mut files: Vec<(u64, PathBuf)> = vec![];
        // We compute std deviation incrementally to estimate a good breakpoint
        // of size per partition.
        let mut total_files = 0_u64;
        let mut k = 0;
        let mut ex = 0.0;
        let mut ex2 = 0.0;

        for (i, entry) in fs::read_dir(&self.path)?.enumerate() {
            let path = entry?.path();
            if path.is_file() {
                let is_proper_file = {
                    self.filter_ext.is_none()
                        || path.extension() == self.filter_ext.as_ref().map(|s| s.as_ref())
                };
                if !is_proper_file {
                    continue;
                }
                let size = fs::metadata(&path)?.len();
                if i == 0 {
                    // assign first file size as reference sample
                    k = size;
                }
                // compute the necessary statistics
                ex += (size - k) as f32;
                ex2 += (size - k) as f32 * (size - k) as f32;
                total_size += size;
                total_files += 1;

                files.push((size, path));
            }
        }

        let file_size_mean = (total_size / total_files) as u64;
        let std_dev = ((ex2 - (ex * ex) / total_files as f32) / total_files as f32).sqrt();

        if total_files < self.executor_partitions as u64 {
            // Coerce the number of partitions to the number of files
            self.executor_partitions = total_files;
        }

        let avg_partition_size = (total_size / self.executor_partitions as u64) as u64;

        let partitions =
            self.assign_files_to_partitions(files, file_size_mean, avg_partition_size, std_dev);

        Ok(partitions)
    }

    /// Assign files according to total avg partition size and file size.
    /// This should return a fairly balanced partition size.
    fn assign_files_to_partitions(
        &self,
        files: Vec<(u64, PathBuf)>,
        file_size_mean: u64,
        avg_partition_size: u64,
        std_dev: f32,
    ) -> Vec<Vec<PathBuf>> {
        let num_partitions = self.executor_partitions as u64;
        // Accept ~ 0.25 std deviations top from the average partition size
        // when assigning a file to a partition.
        let high_part_size_bound = (avg_partition_size + (std_dev * 0.25) as u64) as u64;

        debug!(
            "the average part size is {} with a high bound of {}",
            avg_partition_size, high_part_size_bound
        );
        debug!(
            "assigning files from local fs to partitions, file size mean: {}; std_dev: {}",
            file_size_mean, std_dev
        );

        let mut partitions = Vec::with_capacity(num_partitions as usize);
        let mut partition = Vec::with_capacity(0);
        let mut curr_part_size = 0_u64;
        let mut rng = rand::thread_rng();

        for (size, file) in files.into_iter() {
            if partitions.len() as u64 == num_partitions - 1 {
                partition.push(file);
                continue;
            }

            let new_part_size = curr_part_size + size;
            let larger_than_mean = rng.gen::<bool>();
            if (larger_than_mean && new_part_size < high_part_size_bound)
                || (!larger_than_mean && new_part_size <= avg_partition_size)
            {
                partition.push(file);
                curr_part_size = new_part_size;
            } else if size > avg_partition_size as u64 {
                partitions.push(partition);
                partitions.push(vec![file]);
                partition = vec![];
                curr_part_size = 0;
            } else {
                partitions.push(partition);
                partition = vec![file];
                curr_part_size = size;
            }
        }
        partitions.push(partition);
        let mut current_pos = partitions.len() - 1;
        while (partitions.len() as u64) < self.executor_partitions {
            // If the number of specified partitions is relativelly equal to the number of files
            // or the file size of the last files is low skew can happen and there can be fewer
            // partitions than specified. This the number of partitions is actually the specified.
            if partitions.get(current_pos).unwrap().len() > 1 {
                // Only get elements from part as long as it has more than one element
                let last_part = partitions.get_mut(current_pos).unwrap().pop().unwrap();
                partitions.push(vec![last_part])
            } else if current_pos > 0 {
                current_pos -= 1;
            } else {
                break;
            }
        }
        partitions
    }
}

impl Chunkable<DistributedLocalReader> for LocalFsReader {
    fn slice_with_set_parts(self, parts: usize) -> Vec<Arc<Vec<DistributedLocalReader>>> {
        let mut files_by_part = self.load_local_files().expect("failed to load local files");
        // for each chunk we create a new loader to be run in parallel
        let mut loaders = Vec::with_capacity(files_by_part.len());
        for chunk in files_by_part {
            // a bit hacky, but the only necessary attribute in the next phase is `files`
            let partial_loader = DistributedLocalReader { files: chunk };
            loaders.push(Arc::new(vec![partial_loader]));
        }
        loaders
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DistributedLocalReader {
    files: Vec<PathBuf>,
}

impl IntoIterator for DistributedLocalReader {
    type Item = Vec<u8>;
    type IntoIter = LocalExecutorFsReader;
    fn into_iter(self) -> Self::IntoIter {
        LocalExecutorFsReader { files: self.files }
    }
}

pub struct LocalExecutorFsReader {
    files: Vec<PathBuf>,
}

impl Iterator for LocalExecutorFsReader {
    type Item = Vec<u8>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(path) = self.files.pop() {
            let mut file = fs::File::open(path).unwrap();
            let mut content = vec![];
            let mut reader = BufReader::new(file);
            reader.read_to_end(&mut content);
            Some(content)
        } else {
            None
        }
    }
}

#[cfg(test)]
#[test]
fn load_files() {
    let mut loader = LocalFsReader {
        path: "A".into(),
        is_single_file: false,
        filter_ext: None,
        expect_dir: true,
        files: vec![],
        executor_partitions: 4,
    };

    let file_size_mean = 1628;
    let avg_partition_size = 2850;
    let high_part_size_bound = 3945f32;

    // Skewed file sizes
    let files = vec![
        (500u64, "A".into()),
        (2000, "B".into()),
        (3900, "C".into()),
        (2000, "D".into()),
        (1000, "E".into()),
        (1500, "F".into()),
        (500, "G".into()),
    ];

    let files = loader.assign_files_to_partitions(
        files,
        file_size_mean,
        avg_partition_size,
        high_part_size_bound,
    );
    assert_eq!(files.len(), 4);

    // Even size and less files than parts
    loader.executor_partitions = 8;
    let files = vec![
        (500u64, "A".into()),
        (500, "B".into()),
        (500, "C".into()),
        (500, "D".into()),
        (500, "E".into()),
        (500, "F".into()),
    ];

    let files = loader.assign_files_to_partitions(
        files,
        file_size_mean,
        avg_partition_size,
        high_part_size_bound,
    );
    assert_eq!(files.len(), 6);
}
