pub use file_reader::{LocalFsReaderConfig, ReaderConfiguration};

mod file_reader {
    use std::fs;
    use std::io::prelude::*;
    use std::io::{BufReader, Result};
    use std::path::{Path, PathBuf};

    pub trait ReaderConfiguration {}

    pub struct LocalFsReaderConfig {
        filter_ext: Option<std::ffi::OsString>,
        expect_dir: bool,
        dir_path: PathBuf,
    }

    impl LocalFsReaderConfig {
        /// Read all the files from a directory or a path.
        pub fn new<T: Into<PathBuf>>(path: T) -> LocalFsReaderConfig {
            LocalFsReaderConfig {
                filter_ext: None,
                expect_dir: true,
                dir_path: path.into(),
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
    }

    // TODO: optimally read should be async so the executors can do other work meanwhile,
    // and shouldn't run too many parallel reads per worker node as it's wasteful.
    impl LocalFsReader {
        pub fn new(config: LocalFsReaderConfig) -> LocalFsReader {
            let LocalFsReaderConfig {
                dir_path,
                expect_dir,
                filter_ext,
            } = config;

            let is_single_file = {
                let path: &Path = dir_path.as_ref();
                path.is_file()
            };

            LocalFsReader {
                path: dir_path,
                is_single_file,
                filter_ext,
                expect_dir,
                files: vec![],
                open_file: None,
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

            let num_partitions = 0;

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

            // Partition files according to total avg partition size and std dev of file size
            let std_dev = ((ex2 - (ex * ex) / total_files as f32) / total_files as f32).sqrt();
            let avg_partition_size = (total_size / num_partitions) as f32;

            let file_size_mean = (total_size / total_files) as f32;
            let lower_bound = (file_size_mean - std_dev) as u64;
            let higher_bound = (file_size_mean + std_dev) as u64;

            let mut partitions = vec![];
            let mut partition = vec![];
            let mut curr_part_size = 0;
            for (i, (size, file)) in files.into_iter().enumerate() {
                if i == 0 {
                    partition.push(file);
                    continue;
                }
                let new_part_size = curr_part_size + size;
                if new_part_size < higher_bound && new_part_size > lower_bound {
                    partition.push(file);
                    curr_part_size = new_part_size;
                } else if size > higher_bound {
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
}

#[cfg(test)]
mod test {
    #[test]
    fn load_files() {
        let files = vec![
            (500_u64, 'A'),
            (2000, 'B'),
            (3000, 'C'),
            (2000, 'D'),
            (1000, 'E'),
            (1500, 'F'),
            (500, 'G'),
        ];
    }
}
