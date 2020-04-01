use std::convert::TryFrom;
use std::fs;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::task::{Context, Poll};
use std::thread;
use std::time::Duration;

use crate::env;
use crate::error::StdResult;
use crate::shuffle::*;
use crossbeam::channel as cb_channel;
use futures::future;
use hyper::{
    client::Client, server::conn::AddrIncoming, service::Service, Body, Request, Response, Server,
    StatusCode, Uri,
};
use tokio::time::delay_for;
use uuid::Uuid;

pub(crate) type Result<T> = StdResult<T, ShuffleError>;

/// Creates directories and files required for storing shuffle data.
/// It also creates the file server required for serving files via HTTP request.
#[derive(Debug)]
pub(crate) struct ShuffleManager {
    local_dir: PathBuf,
    shuffle_dir: PathBuf,
    server_uri: String,
    ask_status: cb_channel::Sender<()>,
    rcv_status: cb_channel::Receiver<Result<StatusCode>>,
}

impl ShuffleManager {
    pub fn new() -> Result<Self> {
        let local_dir = ShuffleManager::get_local_work_dir()?;
        let shuffle_dir = local_dir.join("shuffle");
        fs::create_dir_all(&shuffle_dir).map_err(|_| ShuffleError::CouldNotCreateShuffleDir)?;
        let shuffle_port = env::Configuration::get().shuffle_svc_port;
        let server_uri = ShuffleManager::start_server(shuffle_port)?;
        let (send_main, rcv_main) = ShuffleManager::init_status_checker(&server_uri)?;
        let manager = ShuffleManager {
            local_dir,
            shuffle_dir,
            server_uri,
            ask_status: send_main,
            rcv_status: rcv_main,
        };
        if let Ok(StatusCode::OK) = manager.check_status() {
            Ok(manager)
        } else {
            Err(ShuffleError::FailedToStart)
        }
    }

    pub fn get_server_uri(&self) -> String {
        self.server_uri.clone()
    }

    pub fn get_output_file(
        &self,
        shuffle_id: usize,
        input_id: usize,
        output_id: usize,
    ) -> StdResult<String, Box<dyn std::error::Error>> {
        let path = self
            .shuffle_dir
            .join(format!("{}/{}", shuffle_id, input_id));
        fs::create_dir_all(&path)?;
        let file_path = path.join(format!("{}", output_id));
        fs::File::create(&file_path)?;
        Ok(file_path
            .to_str()
            .ok_or_else(|| ShuffleError::CouldNotCreateShuffleDir)?
            .to_owned())
    }

    pub fn check_status(&self) -> Result<StatusCode> {
        self.ask_status.send(()).unwrap();
        self.rcv_status
            .recv()
            .map_err(|_| ShuffleError::AsyncRuntimeError)?
    }

    /// Returns the shuffle server URI as a string.
    pub(super) fn start_server(port: Option<u16>) -> Result<String> {
        let bind_ip = env::Configuration::get().local_ip;
        let port = if let Some(bind_port) = port {
            ShuffleManager::launch_async_server(bind_ip, bind_port)?;
            bind_port
        } else {
            let bind_port = crate::utils::get_free_port()?;
            ShuffleManager::launch_async_server(bind_ip, bind_port)?;
            bind_port
        };
        let server_uri = format!("http://{}:{}", env::Configuration::get().local_ip, port,);
        log::debug!("server_uri {:?}", server_uri);
        Ok(server_uri)
    }

    fn launch_async_server(bind_ip: Ipv4Addr, bind_port: u16) -> Result<()> {
        let (s, r) = cb_channel::bounded::<Result<()>>(1);
        thread::spawn(move || {
            // TODO: use the main async global runtime
            match tokio::runtime::Builder::new()
                .enable_all()
                .threaded_scheduler()
                .build()
                .map_err(|_| ShuffleError::AsyncRuntimeError)
            {
                Err(err) => {
                    s.send(Err(err)).unwrap();
                }
                Ok(mut rt) => {
                    if let Err(err) = rt.block_on(async move {
                        let bind_addr = SocketAddr::from((bind_ip, bind_port));
                        Server::try_bind(&bind_addr.clone())
                            .map_err(|_| crate::NetworkError::FreePortNotFound(bind_port, 0))?
                            .serve(ShuffleSvcMaker)
                            .await
                            .map_err(|_| ShuffleError::FailedToStart)
                    }) {
                        s.send(Err(err)).unwrap();
                    };
                }
            }
        });
        cb_channel::select! {
            recv(r) -> msg => { msg.map_err(|_| ShuffleError::FailedToStart)??; }
            // wait a prudential time to check that initialization is ok and the move on
            default(Duration::from_millis(100)) => log::debug!("started shuffle server @ {}", bind_port),
        };
        Ok(())
    }

    fn init_status_checker(
        server_uri: &str,
    ) -> Result<(
        cb_channel::Sender<()>,
        cb_channel::Receiver<Result<StatusCode>>,
    )> {
        // Build a two way com lane between the main thread and the background running executor
        let (send_child, rcv_main) = cb_channel::unbounded::<Result<StatusCode>>();
        let (send_main, rcv_child) = cb_channel::unbounded::<()>();
        let uri_str = format!("{}/status", server_uri);
        let status_uri = Uri::try_from(&uri_str)?;
        thread::Builder::new()
            .name(format!("{}_shuffle_server_hc", env::THREAD_PREFIX))
            .spawn(|| -> Result<()> {
                // TODO: use the main async global runtime
                let mut rt = tokio::runtime::Builder::new()
                    .enable_all()
                    .basic_scheduler()
                    .core_threads(1)
                    .thread_stack_size(1024)
                    .build()
                    .map_err(|_| ShuffleError::AsyncRuntimeError)?;
                rt.block_on(
                    #[allow(unreachable_code)]
                    async move {
                        let client = Client::builder().http2_only(true).build_http::<Body>();
                        // loop forever waiting for requests to send
                        loop {
                            let res = client.get(status_uri.clone()).await?;
                            // dispatch all queued requests responses
                            while let Ok(()) = rcv_child.try_recv() {
                                send_child.send(Ok(res.status())).unwrap();
                            }
                            // sleep for a while before checking again if there are status requests
                            delay_for(Duration::from_millis(25)).await
                        }
                        Ok::<(), ShuffleError>(())
                    },
                )?;
                Err(ShuffleError::AsyncRuntimeError)
            })
            .map_err(|_| ShuffleError::FailedToStart)?;
        Ok((send_main, rcv_main))
    }

    fn get_local_work_dir() -> Result<PathBuf> {
        let local_dir_root = &env::Configuration::get().local_dir;
        for _ in 0..10 {
            let local_dir = local_dir_root.join(format!("ns-local-{}", Uuid::new_v4().to_string()));
            if !local_dir.exists() {
                log::debug!("creating directory at path: {:?}", &local_dir);
                fs::create_dir_all(&local_dir)
                    .map_err(|_| ShuffleError::CouldNotCreateShuffleDir)?;
                return Ok(local_dir);
            }
        }
        Err(ShuffleError::CouldNotCreateShuffleDir)
    }
}

//TODO implement drop for deleting files created when the shuffle manager stops

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
            _ => Err(ShuffleError::UnexpectedUri(uri.path().to_string())),
        }
    }

    fn get_cached_data(&self, uri: &Uri, parts: &[&str]) -> Result<Vec<u8>> {
        // the path is: .../{shuffleid}/{inputid}/{reduceid}
        let parts: Vec<_> = match parts
            .iter()
            .map(|part| ShuffleService::parse_path_part(part))
            .collect::<Result<_>>()
        {
            Err(_err) => {
                return Err(ShuffleError::UnexpectedUri(format!("{}", uri)));
            }
            Ok(parts) => parts,
        };
        let params = &(parts[0], parts[1], parts[2]);
        if let Some(cached_data) = env::SHUFFLE_CACHE.get(params) {
            log::debug!(
                "got a request @ `{}`, params: {:?}, returning data",
                uri,
                params
            );
            Ok(Vec::from(&cached_data[..]))
        } else {
            Err(ShuffleError::RequestedCacheNotFound)
        }
    }

    #[inline]
    fn parse_path_part(part: &str) -> Result<usize> {
        Ok(u64::from_str_radix(part, 10).map_err(|_| ShuffleError::NotValidRequest)? as usize)
    }
}

impl Service<Request<Body>> for ShuffleService {
    type Response = Response<Body>;
    type Error = ShuffleError;
    type Future = future::Ready<StdResult<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context) -> Poll<StdResult<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        match self.response_type(req.uri()) {
            Ok(response) => match response {
                ShuffleResponse::Status(code) => {
                    let body = Body::from(&[] as &[u8]);
                    match Response::builder().status(code).body(body) {
                        Ok(rsp) => future::ok(rsp),
                        Err(_) => future::err(ShuffleError::InternalError),
                    }
                }
                ShuffleResponse::CachedData(cached_data) => {
                    let body = Body::from(Vec::from(&cached_data[..]));
                    match Response::builder().status(200).body(body) {
                        Ok(rsp) => future::ok(rsp),
                        Err(_) => future::err(ShuffleError::InternalError),
                    }
                }
            },
            Err(err) => future::ok(err.into()),
        }
    }
}

struct ShuffleSvcMaker;

impl<T> Service<T> for ShuffleSvcMaker {
    type Response = ShuffleService;
    type Error = ShuffleError;
    type Future = future::Ready<StdResult<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context) -> Poll<StdResult<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, _: T) -> Self::Future {
        future::ok(ShuffleService)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::sync::Arc;

    fn client() -> Client<hyper::client::HttpConnector, Body> {
        Client::builder().http2_only(true).build_http::<Body>()
    }

    #[tokio::test]
    async fn start_ok() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
        let port = get_free_port();
        ShuffleManager::start_server(Some(port))?;

        let url = format!(
            "http://{}:{}/status",
            env::Configuration::get().local_ip,
            port
        );
        let res = client().get(Uri::try_from(&url)?).await?;
        assert_eq!(res.status(), StatusCode::OK);
        Ok(())
    }

    #[test]
    fn start_failure() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
        let port = get_free_port();
        // bind first so it fails while trying to start
        let _bind = TcpListener::bind(format!("127.0.0.1:{}", port))?;
        assert!(ShuffleManager::start_server(Some(port))
            .unwrap_err()
            .no_port());
        Ok(())
    }

    #[test]
    fn status_checking_ok() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
        let parallelism = num_cpus::get();
        let manager = Arc::new(ShuffleManager::new()?);
        let mut threads = Vec::with_capacity(parallelism);
        for _ in 0..parallelism {
            let manager = manager.clone();
            threads.push(thread::spawn(move || -> Result<()> {
                for _ in 0..10 {
                    match manager.check_status() {
                        Ok(StatusCode::OK) => {}
                        _ => return Err(ShuffleError::AsyncRuntimeError),
                    }
                }
                Ok(())
            }));
        }
        let results = threads
            .into_iter()
            .filter_map(|res| res.join().ok())
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(results.len(), parallelism);
        Ok(())
    }

    #[tokio::test]
    async fn cached_data_found() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
        let port = get_free_port();
        ShuffleManager::start_server(Some(port))?;
        let data = b"some random bytes".iter().copied().collect::<Vec<u8>>();
        {
            env::SHUFFLE_CACHE.insert((2, 1, 0), data.clone());
        }
        let url = format!(
            "http://{}:{}/shuffle/2/1/0",
            env::Configuration::get().local_ip,
            port
        );
        let res = client().get(Uri::try_from(&url)?).await?;
        assert_eq!(res.status(), StatusCode::OK);
        let body = hyper::body::to_bytes(res.into_body()).await?;
        assert_eq!(body.to_vec(), data);
        Ok(())
    }

    #[tokio::test]
    async fn cached_data_not_found() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
        let port = get_free_port();
        ShuffleManager::start_server(Some(port))?;

        let url = format!(
            "http://{}:{}/shuffle/0/1/2",
            env::Configuration::get().local_ip,
            port
        );
        let res = client().get(Uri::try_from(&url)?).await?;
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        Ok(())
    }

    #[tokio::test]
    async fn not_valid_endpoint() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
        use std::iter::FromIterator;
        let port = get_free_port();
        ShuffleManager::start_server(Some(port))?;

        let url = format!(
            "http://{}:{}/not_valid",
            env::Configuration::get().local_ip,
            port
        );
        let res = client().get(Uri::try_from(&url)?).await?;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let body = hyper::body::to_bytes(res.into_body()).await?;
        assert_eq!(
            String::from_iter(body.into_iter().map(|b| b as char)),
            "Failed to parse: /not_valid".to_string()
        );
        Ok(())
    }
}
