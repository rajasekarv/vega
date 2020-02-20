use std::result::Result as StdResult;

use http::uri::InvalidUri;
use hyper::{
    client::Client, server::conn::AddrIncoming, service::Service, Body, Request, Response, Server,
    StatusCode, Uri,
};
use rand::Rng;
use thiserror::Error;

pub(self) mod shuffle_fetcher;
pub(self) mod shuffle_manager;
pub(self) mod shuffle_map_task;
// re-exports:
pub(crate) use shuffle_fetcher::ShuffleFetcher;
pub(crate) use shuffle_manager::ShuffleManager;
pub(crate) use shuffle_map_task::ShuffleMapTask;

pub(self) type Result<T> = StdResult<T, ShuffleError>;

#[derive(Debug, Error)]
pub enum ShuffleError {
    #[error("failure while initializing/running the async runtime")]
    AsyncRuntimeError,

    #[error("failed to create local shuffle dir after 10 attempts")]
    CouldNotCreateShuffleDir,

    #[error("deserialization error")]
    DeserializationError(#[from] bincode::Error),

    #[error("incorrect URI sent in the request")]
    IncorrectUri(#[from] http::uri::InvalidUri),

    #[error("internal server error")]
    InternalError,

    #[error("shuffle fetcher failed while fetching chunk")]
    FailedFetchOp,

    #[error("failed to start shuffle server")]
    FailedToStart,

    #[error("failed to find free port: {0}")]
    FreePortNotFound(u16),

    #[error("not valid request")]
    NotValidRequest,

    #[error("cached data not found")]
    RequestedCacheNotFound,

    #[error("unexpected shuffle server problem")]
    UnexpectedServerError(#[from] hyper::error::Error),

    #[error("unexpected URI sent in the request: {0}")]
    UnexpectedUri(String),
}

impl Into<Response<Body>> for ShuffleError {
    fn into(self) -> Response<Body> {
        match self {
            ShuffleError::UnexpectedUri(uri) => Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(format!("Failed to parse: {}", uri)))
                .unwrap(),
            ShuffleError::RequestedCacheNotFound => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(&[] as &[u8]))
                .unwrap(),
            _ => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(&[] as &[u8]))
                .unwrap(),
        }
    }
}

impl ShuffleError {
    fn no_port(&self) -> bool {
        match self {
            ShuffleError::FreePortNotFound(_) => true,
            _ => false,
        }
    }

    fn deserialization_err(&self) -> bool {
        match self {
            ShuffleError::DeserializationError(_) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
fn get_free_port() -> u16 {
    use std::net::TcpListener;

    let mut port = 0;
    for _ in 0..100 {
        port = crate::utils::get_dynamic_port();
        if TcpListener::bind(format!("127.0.0.1:{}", port)).is_ok() {
            return port;
        }
    }
    panic!("failed to find free port while testing");
}
