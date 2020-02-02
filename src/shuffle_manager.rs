use std::convert::Infallible;
use std::fs;
use std::net::SocketAddr;
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

/// Creates directories and files required for storing shuffle data.
/// It also creates the file server required for serving files via http request.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub(crate) struct ShuffleManager {
    local_dir: String,
    shuffle_dir: String,
    server_uri: String,
}

impl ShuffleManager {
    pub fn new() -> Self {
        //TODO replace all hardcoded values with environment variables
        let local_dir_root = "/tmp";
        let mut tries = 0;
        let mut found_local_dir = false;
        let mut local_dir = String::new();
        let mut local_dir_uuid = String::new();
        //TODO error logging
        while (!found_local_dir) && (tries < 10) {
            tries += 1;
            let uuid = Uuid::new_v4();
            local_dir_uuid = uuid.to_string();
            local_dir = format!("{}/spark-local-{}", local_dir_root, local_dir_uuid);
            let path = std::path::Path::new(&local_dir);
            if !path.exists() {
                log::debug!("creating directory at path {:?} loc {:?}", path, local_dir);
                fs::create_dir_all(path);
                found_local_dir = true;
            }
        }
        if !found_local_dir {
            panic!(
                "failed 10 attempts to create local dir in {}",
                local_dir_root
            );
        }
        let shuffle_dir = format!("{}/shuffle", local_dir);
        fs::create_dir_all(shuffle_dir.clone());

        // for experimenting this should not lead to any clashes
        let port = 5000 + rand::thread_rng().gen_range(0, 1000);
        let server_uri = format!(
            "http://{}:{}",
            env::Configuration::get().local_ip.clone(),
            port,
        );
        log::debug!("server_uri {:?}", server_uri);
        let server_address = format!("{}:{}", env::Configuration::get().local_ip.clone(), port);
        log::debug!("server_address {:?}", server_address);
        let relative_path = format!("/spark-local-{}", local_dir_uuid);
        let local_dir_clone = local_dir.clone();
        let server_address_clone = server_address;
        log::debug!("relative path {}", relative_path);
        log::debug!("local_dir path {}", local_dir);
        log::debug!("shuffle dir path {}", shuffle_dir);
        thread::spawn(move || {
            #[get("/shuffle/{shuffleid}/{inputid}/{reduceid}")]
            fn get_shuffle_data(info: Path<(usize, usize, usize)>) -> Bytes {
                Bytes::from(
                    &env::shuffle_cache
                        .read()
                        .get(&(info.0, info.1, info.2))
                        .unwrap()[..],
                )
            }
            log::debug!("starting server for shuffle task");
            #[get("/")]
            fn no_params() -> &'static str {
                "Hello world!\r"
            }
            match HttpServer::new(move || App::new().service(get_shuffle_data).service(no_params))
                .workers(8)
                .bind(server_address_clone)
            {
                Ok(s) => {
                    log::debug!("server for shufflemap task binded");
                    match s.run() {
                        Ok(_) => {
                            log::debug!("server for shufflemap task started");
                        }
                        Err(e) => {
                            log::debug!("cannot start server for shufflemap task started {}", e);
                        }
                    }
                }
                Err(e) => {
                    log::debug!("cannot bind server for shuffle map task {}", e);
                    std::process::exit(0)
                }
            }
        });
        let s = ShuffleManager {
            local_dir,
            shuffle_dir,
            server_uri,
        };
        log::debug!("shuffle manager inside new {:?}", s);
        s
    }

    pub fn get_server_uri(&self) -> String {
        self.server_uri.clone()
    }

    pub fn get_output_file(&self, shuffle_id: usize, input_id: usize, output_id: usize) -> String {
        let path = format!("{}/{}/{}", self.shuffle_dir, shuffle_id, input_id);
        fs::create_dir_all(&path);
        let file_path = format!("{}/{}", path, output_id);
        fs::File::create(file_path.clone());
        file_path
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

struct ShuffleManager2;

impl ShuffleManager2 {
    fn parse_path_part(part: &str) -> Result<usize, ShuffleManagerError> {
        Ok(u64::from_str_radix(part, 10)
            .map_err(|_| ShuffleManagerError::FailedToParseUri("".to_owned()))? as usize)
    }

    fn get_cached_data(&self, uri: &hyper::Uri) -> Result<Vec<u8>, ShuffleManagerError> {
        // the path is: .../{shuffleid}/{inputid}/{reduceid}
        let parts: Vec<_> = uri.path().split('/').collect();
        if parts.len() != 5 {
            return Err(ShuffleManagerError::FailedToParseUri(format!("{}", uri)));
        }

        let parts: Vec<_> = match (&parts[2..])
            .iter()
            .map(|part| ShuffleManager2::parse_path_part(part))
            .collect::<Result<_, _>>()
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

impl Service<Request<Body>> for ShuffleManager2 {
    type Response = Response<Body>;
    type Error = ShuffleManagerError;
    type Future = future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
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
    type Response = ShuffleManager2;
    type Error = ShuffleManagerError;
    type Future = future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, _: T) -> Self::Future {
        future::ok(ShuffleManager2)
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
                run(port).await.unwrap();
            })
        });
    }

    async fn run(port: u16) -> Result<(), Box<dyn std::error::Error + 'static>> {
        let server = start_server(Some(port));
        server.await?;
        Ok(())
    }

    #[test]
    fn cached_data_not_found() -> Result<(), Box<dyn std::error::Error + 'static>> {
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
    fn get_cached_data() -> Result<(), Box<dyn std::error::Error + 'static>> {
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
