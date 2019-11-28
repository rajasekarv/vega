use std::ffi::OsString;
use std::path::PathBuf;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to run {command}")]
    CommandOutput {
        source: std::io::Error,
        command: String,
    },

    #[error("configuration failure: {0}")]
    GetOrCreateConfig(&'static str),

    #[error("failed to create the log file")]
    CreateLogFile(#[source] std::io::Error),

    #[error("failed to create the terminal logger")]
    CreateTerminalLogger,

    #[error("couldn't determine the current binary's name")]
    CurrentBinaryName,

    #[error("couldn't determine the path to the current binary")]
    CurrentBinaryPath,

    #[error("failed to parse the executor port")]
    ExecutorPort(#[source] std::num::ParseIntError),

    #[error("partitioner not set")]
    LackingPartitioner,

    #[error("failed to load hosts file from {}", path.display())]
    LoadHosts {
        source: std::io::Error,
        path: PathBuf,
    },

    #[error("failed to determine the home directory")]
    NoHome,

    #[error("failed to convert {:?} to a String", .0)]
    OsStringToString(OsString),

    #[error("failed to parse hosts file at {}", path.display())]
    ParseHosts {
        source: toml::de::Error,
        path: PathBuf,
    },

    #[error("failed to convetr {} to a String", .0.display())]
    PathToString(PathBuf),

    #[error("failed to parse slave address {0}")]
    ParseSlaveAddress(String),

    #[error("Got split object from different concrete type other than {0}")]
    SplitDowncast(&'static str),

    #[error("operation not supported: {0}")]
    UnsupportedOperation(&'static str),
}
