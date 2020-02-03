use std::convert::Infallible;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::result::Result as StdResult;
use std::task::{Context, Poll};
use std::thread;

use crate::env;
use actix_web::{
    get,
    web::{Bytes, Path},
    App, HttpServer,
};
use futures::future;
use hyper::{
    server::conn::AddrIncoming, service::Service, Body, Request, Response, Server, StatusCode,
};
use log::info;
use rand::Rng;
use serde_derive::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

type Result<T> = StdResult<T, ShuffleManagerError>;

/// Creates directories and files required for storing shuffle data.
/// It also creates the file server required for serving files via http request.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub(crate) struct ShuffleManager {
    local_dir: PathBuf,
    shuffle_dir: PathBuf,
    server_uri: String,
}

//TODO replace all hardcoded values with environment variables
impl ShuffleManager {
    pub fn new() -> Result<Self> {
        let local_dir = ShuffleManager::get_local_work_dir()?;
        let shuffle_dir = local_dir.join("shuffle");
        fs::create_dir_all(&shuffle_dir);
        let server_uri = ShuffleManager::start_server()?;
        Ok(ShuffleManager {
            local_dir,
            shuffle_dir,
            server_uri,
        })
    }

    pub fn get_server_uri(&self) -> String {
        self.server_uri.clone()
    }

    pub fn get_output_file(&self, shuffle_id: usize, input_id: usize, output_id: usize) -> String {
        let path = self
            .shuffle_dir
            .join(format!("/{}/{}", shuffle_id, input_id));
        fs::create_dir_all(&path);
        let file_path = path.join(format!("{}", output_id));
        fs::File::create(&file_path);
        file_path.to_str().unwrap().to_owned()
    }

    /// Returns the shuffle server URI as a string.
    fn start_server() -> Result<String> {
        // let server_address = format!("{}:{}", env::Configuration::get().local_ip.clone(), port);
        // log::debug!("server_address {:?}", server_address)
        // let server_address_clone = server_address;
        // thread::spawn(move || {
        //     #[get("/shuffle/{shuffleid}/{inputid}/{reduceid}")]
        //     fn get_shuffle_data(info: Path<(usize, usize, usize)>) -> Bytes {
        //         Bytes::from(
        //             &env::shuffle_cache
        //                 .read()
        //                 .get(&(info.0, info.1, info.2))
        //                 .unwrap()[..],
        //         )
        //     }
        //     log::debug!("starting server for shuffle task");
        //     #[get("/")]
        //     fn no_params() -> &'static str {
        //         "Hello world!\r"
        //     }
        //     match HttpServer::new(move || App::new().service(get_shuffle_data).service(no_params))
        //         .workers(8)
        //         .bind(server_address_clone)
        //     {
        //         Ok(s) => {
        //             log::debug!("server for shufflemap task binded");
        //             match s.run() {
        //                 Ok(_) => {
        //                     log::debug!("server for shufflemap task started");
        //                 }
        //                 Err(e) => {
        //                     log::debug!("cannot start server for shufflemap task started {}", e);
        //                 }
        //             }
        //         }
        //         Err(e) => {
        //             log::debug!("cannot bind server for shuffle map task {}", e);
        //             std::process::exit(0)
        //         }
        //     }
        // });
        let port = if let Some(port) = &env::Configuration::get().shuffle_svc_port {
            *port
        } else {
            // for experimenting this should not lead to any clashes
            5000 + rand::thread_rng().gen_range(0, 1000)
        };
        let server_uri = format!(
            "http://{}:{}",
            env::Configuration::get().local_ip.clone(),
            port,
        );
        log::debug!("server_uri {:?}", server_uri);
        Ok(server_uri)
    }

    fn get_local_work_dir() -> Result<PathBuf> {
        let local_dir_root = &env::Configuration::get().local_dir;
        let mut local_dir = PathBuf::new();
        for _ in 0..10 {
            let uuid = Uuid::new_v4();
            let local_dir_uuid = uuid.to_string();
            local_dir = local_dir_root.join(format!("/spark-local-{}", local_dir_uuid));
            if !local_dir.exists() {
                log::debug!("creating directory at path: {:?}", &local_dir);
                fs::create_dir_all(&local_dir);
                log::debug!("local_dir path: {:?}", local_dir);
                return Ok(local_dir);
            }
        }
        Err(ShuffleManagerError::CouldNotCreateShuffleDir)
    }
}

//TODO implement drop for deleting files created when the shuffle manager stops

type ShuffleServer = Server<AddrIncoming, ShuffleSvcMaker>;

fn start_server(port: Option<u16>) -> ShuffleServer {
    let bind_ip = env::Configuration::get().local_ip.clone();
    let bind_port = if let Some(port) = port {
        port
    } else {
        5000 + rand::thread_rng().gen_range(0, 1000)
    };
    let bind_addr = SocketAddr::from((bind_ip, bind_port));
    Server::bind(&bind_addr).serve(ShuffleSvcMaker)
}

struct ShuffleService;

impl ShuffleService {
    fn parse_path_part(part: &str) -> Result<usize> {
        Ok(u64::from_str_radix(part, 10)
            .map_err(|_| ShuffleManagerError::FailedToParseUri("".to_owned()))? as usize)
    }

    fn get_cached_data(&self, uri: &hyper::Uri) -> Result<Vec<u8>> {
        // the path is: .../{shuffleid}/{inputid}/{reduceid}
        let parts: Vec<_> = uri.path().split('/').collect();
        if parts.len() != 5 {
            return Err(ShuffleManagerError::FailedToParseUri(format!("{}", uri)));
        }

        let parts: Vec<_> = match (&parts[2..])
            .iter()
            .map(|part| ShuffleService::parse_path_part(part))
            .collect::<Result<_>>()
        {
            Err(err) => {
                return Err(ShuffleManagerError::FailedToParseUri(format!("{}", uri)));
            }
            Ok(parts) => parts,
        };
        let cache = env::shuffle_cache.read();
        if let Some(cached_data) = cache.get(&(parts[0], parts[1], parts[2])) {
            Ok(Vec::from(&cached_data[..]))
        } else {
            Err(ShuffleManagerError::RequestedCacheNotFound)
        }
    }
}

impl Service<Request<Body>> for ShuffleService {
    type Response = Response<Body>;
    type Error = ShuffleManagerError;
    type Future = future::Ready<StdResult<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context) -> Poll<StdResult<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        match self.get_cached_data(req.uri()) {
            Ok(cached_data) => {
                let rsp = Response::builder();
                let body = Body::from(Vec::from(&cached_data[..]));
                let rsp = rsp.status(200).body(body).unwrap();
                future::ok(rsp)
            }
            Err(err) => future::ok(err.into()),
        }
    }
}

struct ShuffleSvcMaker;

impl<T> Service<T> for ShuffleSvcMaker {
    type Response = ShuffleService;
    type Error = ShuffleManagerError;
    type Future = future::Ready<StdResult<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context) -> Poll<StdResult<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, _: T) -> Self::Future {
        future::ok(ShuffleService)
    }
}

#[derive(Debug, Error)]
pub enum ShuffleManagerError {
    #[error("failed to start shuffle server")]
    FailedToStart,

    #[error("incorrect URI sent in the request: {0}")]
    FailedToParseUri(String),

    #[error("cached data not found")]
    RequestedCacheNotFound,

    #[error("failed to create local shuffle dir after 10 attempts")]
    CouldNotCreateShuffleDir,
}

impl Into<Response<Body>> for ShuffleManagerError {
    fn into(self) -> Response<Body> {
        match self {
            ShuffleManagerError::FailedToStart => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(&[] as &[u8]))
                .unwrap(),
            ShuffleManagerError::FailedToParseUri(uri) => Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(format!("Failed to parse: {}", uri)))
                .unwrap(),
            ShuffleManagerError::RequestedCacheNotFound => Response::builder()
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::time::Duration;
    use tokio::prelude::*;

    fn blocking_runtime(port: u16) {
        let mut rt = tokio::runtime::Builder::new()
            .enable_all()
            .basic_scheduler()
            .build()
            .expect("build runtime");

        thread::spawn(move || {
            rt.block_on(async {
                let server = start_server(Some(port));
                server.await.unwrap();
            })
        });
    }

    #[test]
    fn cached_data_not_found() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
        blocking_runtime(5001);

        let url = format!(
            "http://{}:5001/shuffle/0/1/2",
            env::Configuration::get().local_ip
        );
        let mut retries = 0;
        loop {
            let res = reqwest::get(&url);
            if let Ok(res) = res {
                assert_eq!(res.status(), reqwest::StatusCode::NOT_FOUND);
                return Ok(());
            }
            retries += 1;
            if retries > 10 {
                return Err(Box::new(ShuffleManagerError::FailedToStart));
            }
            thread::sleep(Duration::from_millis(25));
        }
        Ok(())
    }

    #[test]
    fn get_cached_data() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
        blocking_runtime(5002);

        let data = b"some random bytes".iter().copied().collect::<Vec<u8>>();
        {
            let mut cache = env::shuffle_cache.write();
            cache.insert((2, 1, 0), data.clone());
        }
        let url = format!(
            "http://{}:5002/shuffle/2/1/0",
            env::Configuration::get().local_ip
        );
        let mut retries = 0;
        let res = reqwest::get(&url)?;
        assert_eq!(res.status(), reqwest::StatusCode::OK);
        assert_eq!(
            res.bytes()
                .into_iter()
                .map(|c| c.unwrap())
                .collect::<Vec<u8>>(),
            data
        );
        Ok(())
    }
}
