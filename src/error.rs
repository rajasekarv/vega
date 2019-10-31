use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("couldn't determine the path to the current binary")]
    CurrentBinaryPath,
}
