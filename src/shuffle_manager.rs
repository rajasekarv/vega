use std::convert::TryFrom;
use std::fs;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::result::Result as StdResult;
use std::task::{Context, Poll};
use std::thread;
use std::time::Duration;

use crate::env;
use crossbeam::channel as cb_channel;
use futures::future;
use hyper::{
    client::Client, server::conn::AddrIncoming, service::Service, Body, Request, Response, Server,
    StatusCode, Uri,
};
use log::info;
use rand::Rng;
use serde_derive::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

type Result<T> = StdResult<T, ShuffleManagerError>;

/// Creates directories and files required for storing shuffle data.
/// It also creates the file server required for serving files via http request.
#[derive(Debug)]
pub(crate) struct ShuffleManager {
    local_dir: PathBuf,
    shuffle_dir: PathBuf,
    server_uri: String,
    ask_status: cb_channel::Sender<()>,
    rcv_status: cb_channel::Receiver<StatusCode>,
}

impl ShuffleManager {
    pub fn new() -> Result<Self> {
        let local_dir = ShuffleManager::get_local_work_dir()?;
        let shuffle_dir = local_dir.join("shuffle");
        fs::create_dir_all(&shuffle_dir);
        let shuffle_port = env::Configuration::get().shuffle_svc_port;
        let server_uri = ShuffleManager::start_server(shuffle_port)?;
        let (send_main, rcv_main) = ShuffleManager::init_status_checker(&server_uri);
        let manager = ShuffleManager {
            local_dir,
            shuffle_dir,
            server_uri,
            ask_status: send_main,
            rcv_status: rcv_main,
        };
        if let StatusCode::OK = manager.check_status() {
            Ok(manager)
        } else {
            Err(ShuffleManagerError::FailedToStart)
        }
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

    pub fn check_status(&self) -> StatusCode {
        self.ask_status.send(());
        self.rcv_status.recv().unwrap()
    }

    /// Returns the shuffle server URI as a string.
    fn start_server(port: Option<u16>) -> Result<String> {
        let bind_ip = env::Configuration::get().local_ip.clone();
        let port = if let Some(bind_port) = port {
            ShuffleManager::launch_async_server(bind_ip, bind_port)?;
            bind_port
        } else {
            let mut port = 0;
            for retry in 0..10 {
                let bind_port = get_dynamic_port();
                if let Ok(server) = ShuffleManager::launch_async_server(bind_ip, bind_port) {
                    port = bind_port;
                    break;
                } else if retry == 9 {
                    return Err(ShuffleManagerError::FreePortNotFound(bind_port));
                }
            }
            port
        };
        let server_uri = format!(
            "http://{}:{}",
            env::Configuration::get().local_ip.clone(),
            port,
        );
        log::debug!("server_uri {:?}", server_uri);
        Ok(server_uri)
    }

    fn launch_async_server(bind_ip: Ipv4Addr, bind_port: u16) -> Result<()> {
        let (s, r) = cb_channel::bounded::<Result<()>>(1);
        thread::spawn(move || {
            match tokio::runtime::Builder::new()
                .enable_all()
                .threaded_scheduler()
                .build()
                .map_err(|_| ShuffleManagerError::FailedToStart)
            {
                Err(err) => {
                    s.send(Err(err));
                }
                Ok(mut rt) => {
                    if let Err(err) = rt.block_on(async move {
                        let bind_addr = SocketAddr::from((bind_ip, bind_port));
                        Server::try_bind(&bind_addr.clone())
                            .map_err(|_| ShuffleManagerError::FreePortNotFound(bind_port))?
                            .serve(ShuffleSvcMaker)
                            .await
                            .map_err(|_| ShuffleManagerError::FailedToStart)
                    }) {
                        s.send(Err(err));
                    };
                }
            }
        });
        cb_channel::select! {
            recv(r) -> msg => { msg.map_err(|_| ShuffleManagerError::FailedToStart)??; }
            // wait a prudential time to check that initialization is ok and the move on
            default(Duration::from_millis(100)) => log::debug!("started shuffle server @ {}", bind_port),
        };
        Ok(())
    }

    fn init_status_checker(
        server_uri: &str,
    ) -> (cb_channel::Sender<()>, cb_channel::Receiver<StatusCode>) {
        // Build a two way com lane between the main thread and the background running executor
        let (send_child, rcv_main) = cb_channel::unbounded::<StatusCode>();
        let (send_main, rcv_child) = cb_channel::unbounded::<()>();
        let status_uri = Uri::try_from(&format!("{}/status", server_uri)).unwrap();
        thread::spawn(|| {
            let mut rt = tokio::runtime::Builder::new()
                .enable_all()
                .basic_scheduler()
                .core_threads(1)
                .thread_stack_size(1024)
                .build()
                .map_err(|_| crate::error::Error::AsyncRuntimeError)
                .unwrap();
            rt.block_on(async move {
                let client = Client::builder().http2_only(true).build_http::<Body>();
                let req = client.get(status_uri.clone());
                // loop forever waiting for requests to send
                loop {
                    // dispatch all queued requests
                    while let Ok(()) = rcv_child.recv() {
                        let req = client.get(status_uri.clone());
                        send_child.send(req.await.unwrap().status());
                    }
                    // sleep for a while before checking again if there are status requests
                    std::thread::sleep(Duration::from_millis(50));
                }
            })
        });
        (send_main, rcv_main)
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

impl Default for ShuffleManager {
    fn default() -> Self {
        ShuffleManager::new().unwrap()
    }
}

//TODO implement drop for deleting files created when the shuffle manager stops

fn get_dynamic_port() -> u16 {
    const FIRST_DYNAMIC_PORT: u16 = 49152;
    const LAST_DYNAMIC_PORT: u16 = 65535;
    FIRST_DYNAMIC_PORT + rand::thread_rng().gen_range(0, LAST_DYNAMIC_PORT - FIRST_DYNAMIC_PORT)
}

type ShuffleServer = Server<AddrIncoming, ShuffleSvcMaker>;

struct ShuffleService;

enum ShuffleResponse {
    Status(StatusCode),
    CachedData(Vec<u8>),
}

impl ShuffleService {
    fn response_type(&self, uri: &Uri) -> Result<ShuffleResponse> {
        let parts: Vec<_> = uri.path().split('/').collect();
        match parts.as_slice() {
            [_, endpoint] if *endpoint == "status" => Ok(ShuffleResponse::Status(StatusCode::OK)),
            [_, endpoint, shuffle_id, input_id, reduce_id] if *endpoint == "shuffle" => Ok(
                ShuffleResponse::CachedData(
                    self.get_cached_data(uri, &[*shuffle_id, *input_id, *reduce_id])?,
                ),
            ),
            _ => Err(ShuffleManagerError::FailedToParseUri(format!("{}", uri))),
        }
    }

    fn get_cached_data(&self, uri: &Uri, parts: &[&str]) -> Result<Vec<u8>> {
        // the path is: .../{shuffleid}/{inputid}/{reduceid}
        let parts: Vec<_> = match parts
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

    #[inline]
    fn parse_path_part(part: &str) -> Result<usize> {
        Ok(u64::from_str_radix(part, 10)
            .map_err(|_| ShuffleManagerError::FailedToParseUri("".to_owned()))? as usize)
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
        match self.response_type(req.uri()) {
            Ok(response) => match response {
                ShuffleResponse::Status(code) => {
                    let rsp = Response::builder();
                    let body = Body::from(&[] as &[u8]);
                    let rsp = rsp.status(code).body(body).unwrap();
                    future::ok(rsp)
                }
                ShuffleResponse::CachedData(cached_data) => {
                    let rsp = Response::builder();
                    let body = Body::from(Vec::from(&cached_data[..]));
                    let rsp = rsp.status(200).body(body).unwrap();
                    future::ok(rsp)
                }
            },
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

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ShuffleManagerError {
    #[error("failed to create local shuffle dir after 10 attempts")]
    CouldNotCreateShuffleDir,

    #[error("incorrect URI sent in the request: {0}")]
    FailedToParseUri(String),

    #[error("failed to start shuffle server")]
    FailedToStart,

    #[error("failed to find free port: {0}")]
    FreePortNotFound(u16),

    #[error("cached data not found")]
    RequestedCacheNotFound,

    #[error("not valid endpoint")]
    NotValidEndpoint,
}

impl Into<Response<Body>> for ShuffleManagerError {
    fn into(self) -> Response<Body> {
        match self {
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
    use std::net::TcpListener;
    use std::time::Duration;
    use tokio::prelude::*;

    fn get_free_port() -> u16 {
        let mut port = 0;
        for _ in 0..100 {
            port = get_dynamic_port();
            if !TcpListener::bind(format!("127.0.0.1:{}", port)).is_err() {
                return port;
            }
        }
        panic!("failed to find free port while testing");
    }

    #[test]
    fn start_ok() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
        let port = get_free_port();
        ShuffleManager::start_server(Some(port))?;

        let url = format!(
            "http://{}:{}/status",
            env::Configuration::get().local_ip,
            port
        );
        let res = reqwest::get(&url)?;
        assert_eq!(res.status(), reqwest::StatusCode::OK);
        Ok(())
    }

    #[test]
    fn start_failure() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
        let port = get_free_port();
        // bind first so it fails while trying to start
        let bind = TcpListener::bind(format!("127.0.0.1:{}", port))?;
        assert_eq!(
            ShuffleManager::start_server(Some(port)).unwrap_err(),
            ShuffleManagerError::FreePortNotFound(port)
        );
        Ok(())
    }

    #[test]
    fn status_cheking_ok() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
        let port = get_free_port();
        let manager = ShuffleManager::new()?;
        for _ in 0..100 {
            assert_eq!(manager.check_status(), StatusCode::OK);
        }
        Ok(())
    }

    #[test]
    fn cached_data_found() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
        let port = get_free_port();
        ShuffleManager::start_server(Some(port))?;
        let data = b"some random bytes".iter().copied().collect::<Vec<u8>>();
        {
            let mut cache = env::shuffle_cache.write();
            cache.insert((2, 1, 0), data.clone());
        }
        let url = format!(
            "http://{}:{}/shuffle/2/1/0",
            env::Configuration::get().local_ip,
            port
        );
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

    #[test]
    fn cached_data_not_found() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
        let port = get_free_port();
        ShuffleManager::start_server(Some(port))?;

        let url = format!(
            "http://{}:{}/shuffle/0/1/2",
            env::Configuration::get().local_ip,
            port
        );
        let res = reqwest::get(&url)?;
        assert_eq!(res.status(), reqwest::StatusCode::NOT_FOUND);
        Ok(())
    }

    #[test]
    fn not_valid_endpoint() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
        let port = get_free_port();
        ShuffleManager::start_server(Some(port))?;

        let url = format!(
            "http://{}:{}/not_valid",
            env::Configuration::get().local_ip,
            port
        );
        let mut res = reqwest::get(&url)?;
        assert_eq!(res.status(), reqwest::StatusCode::BAD_REQUEST);
        assert_eq!(res.text()?, format!("Failed to parse: /not_valid"));
        Ok(())
    }
}
