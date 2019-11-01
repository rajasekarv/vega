pub use file_reader::{LocalFsReaderConfig, ReaderConfiguration};

mod file_reader {
    use crate::*;
    use std::fs;
    use std::io::prelude::*;
    use std::io::{BufReader, Result};
    use std::path::{Path, PathBuf};

    pub trait ReaderConfiguration {}

    pub struct LocalFsReaderConfig {
        filter_ext: Option<std::ffi::OsString>,
        expect_dir: bool,
        dir_path: PathBuf,
        executor_partitions: Option<usize>,
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
        /// Number of partitions to use per executor
        pub fn num_partitions_per_executor(&mut self, num: usize) {
            self.executor_partitions = Some(num);
        }
        fn build(self) -> LocalFsReader {
            LocalFsReader::new(self)
        }
    }

    impl ReaderConfiguration for LocalFsReaderConfig {}

    /// Reads all files specified in a given directory from the local directory
    /// on all executors on every worker node.
    struct LocalFsReader {
        path: PathBuf,
        is_single_file: bool,
        filter_ext: Option<std::ffi::OsString>,
        expect_dir: bool,
        files: Vec<Vec<PathBuf>>,
        open_file: Option<BufReader<fs::File>>,
        executor_partitions: usize,
    }

    // TODO: optimally read should be async so the executors can do other work meanwhile,
    // and shouldn't run too many parallel reads per worker node as it's wasteful.
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
                0
            };

            LocalFsReader {
                path: dir_path,
                is_single_file,
                filter_ext,
                expect_dir,
                files: vec![],
                open_file: None,
                executor_partitions: num_parts,
            }
        }

        fn load_files(&mut self) -> Result<()> {
            let mut total_size = 0_u64;
            if self.is_single_file {
                let size = fs::metadata(&self.path)?.len();
                total_size += size;
                self.files.push(vec![self.path.clone()]);
                return Ok(());
            }

            let num_partitions = self.executor_partitions as u64;

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
                    if !self.is_proper_file(&path) {
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
            let std_dev =
                ((ex2 - (ex * ex) / total_files as f32) / total_files as f32).sqrt() as u64;
            let avg_partition_size = (total_size / num_partitions) as u64;
            let high_part_size_bound = (avg_partition_size + std_dev) as u64;
            info!(
                "assigning files from local fs to partitions, file size mean: {}; std_dev: {}",
                file_size_mean, std_dev
            );
            let partitions = self.assign_files_to_partitions(
                files,
                file_size_mean,
                avg_partition_size,
                high_part_size_bound,
            );

            Ok(())
        }

        fn is_proper_file(&self, path: &Path) -> bool {
            if let Some(ext_filter) = &self.filter_ext {
                if let Some(extension) = path.extension() {
                    extension == ext_filter
                } else {
                    false
                }
            } else {
                true
            }
        }

        /// Assign files according to total avg partition size and file size.
        /// This should return a fairly balanced partition size.
        fn assign_files_to_partitions(
            &self,
            files: Vec<(u64, PathBuf)>,
            file_size_mean: u64,
            avg_partition_size: u64,
            high_part_size_bound: u64,
        ) -> Vec<Vec<PathBuf>> {
            let num_partitions = self.executor_partitions as u64;
            info!(
                "the average part size is {} with a high bound of {}",
                avg_partition_size, high_part_size_bound
            );

            let mut partitions = Vec::with_capacity(num_partitions as usize);
            let mut partition = Vec::with_capacity(0);
            let mut curr_part_size = 0_u64;
            for (size, file) in files.into_iter() {
                if partitions.len() as u64 == num_partitions - 1 {
                    partition.push(file);
                    continue;
                }
                let new_part_size = curr_part_size + size;
                if new_part_size < high_part_size_bound {
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
            partitions
        }
    }

    impl Read for LocalFsReader {
        fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
            if let Some(open_reader) = self.open_file.as_mut() {
                open_reader.read_exact(buf)?;
                Ok(buf.len())
            } else if let Some(mut file) = self.files.pop() {
                // FIXME: must be flattened per reader
                let file = file.pop().unwrap();
                let file = fs::File::open(file)?;
                let mut new_reader = BufReader::new(file);
                new_reader.read_exact(buf)?;
                self.open_file = Some(new_reader);
                Ok(buf.len())
            } else {
                Ok(0)
            }
        }
    }

    #[cfg(test)]
    #[test]
    fn load_files() {
        let loader = LocalFsReader {
            path: "A".into(),
            is_single_file: false,
            filter_ext: None,
            expect_dir: true,
            files: vec![],
            open_file: None,
            executor_partitions: 4,
        };

        let file_size_mean = 1628;
        let avg_partition_size = 2850;
        let high_part_size_bound = 3945;

        let files = vec![
            (500_u64, "A".into()),
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
    }
}
