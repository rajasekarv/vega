use std::ffi::OsString;
use std::path::PathBuf;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to create the log file")]
    CreateLogFile(#[source] std::io::Error),

    #[error("couldn't determine the path to the current binary")]
    CurrentBinaryPath,

    #[error("failed to create the terminal logger")]
    CreateTerminalLogger,

    #[error("failed to parse the executor port")]
    ExecutorPort(#[source] std::num::ParseIntError),

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
}
