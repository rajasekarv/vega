use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to create the log file")]
    CreateLogFile(#[source] std::io::Error),

    #[error("couldn't determine the path to the current binary")]
    CurrentBinaryPath,
}
