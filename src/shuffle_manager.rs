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
use hyper::{server::conn::AddrIncoming, service::Service, Body, Request, Response, Server};
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

async fn get_shuffle(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let response = "hello";
    todo!()
}

struct ShuffleManager2 {}

impl Service<Request<Body>> for ShuffleManager2 {
    type Response = Response<Body>;
    type Error = std::io::Error;
    type Future = future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let rsp = Response::builder();
        let body = Body::from(Vec::from(&b"hey"[..]));
        let rsp = rsp.status(200).body(body).unwrap();
        future::ok(rsp)
    }
}

struct ShuffleSvcMaker;

impl<T> Service<T> for ShuffleSvcMaker {
    type Response = ShuffleManager2;
    type Error = std::io::Error;
    type Future = future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, _: T) -> Self::Future {
        future::ok(ShuffleManager2 {})
    }
}

#[derive(Debug, Error)]
pub enum ShuffleManagerError {
    #[error("failed to start shuffle server")]
    FailedToStart,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::prelude::*;

    fn blocking_runtime() {
        let mut rt = tokio::runtime::Builder::new()
            .enable_all()
            .basic_scheduler()
            .build()
            .expect("build runtime");

        thread::spawn(move || {
            rt.block_on(async {
                run().await.unwrap();
            })
        });
    }

    async fn run() -> Result<(), Box<dyn std::error::Error + 'static>> {
        let server = start_server(Some(5001));
        server.await?;
        Ok(())
    }

    #[test]
    fn shuffle_server_up() -> Result<(), Box<dyn std::error::Error + 'static>> {
        blocking_runtime();

        let url = format!(
            "http://{}:5001/shuffle/0/1/2",
            env::Configuration::get().local_ip
        );
        let mut retries = 0;
        while let Err(err) = reqwest::get(&url) {
            retries += 1;
            thread::sleep(Duration::from_millis(25));
            if retries > 10 {
                return Err(Box::new(ShuffleManagerError::FailedToStart));
            }
        }
        Ok(())
    }
}
