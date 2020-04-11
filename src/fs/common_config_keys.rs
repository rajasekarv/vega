//! We will try to keep the defaults as close to HDFS as possible. Therefore, the same configuration
//! values as `org.apache.hadoop.fs.CommonConfigurationKeys` are used here too.

/// Default location for user home directories
const FS_HOME_DIR_KEY: &str = "fs.homeDir";
