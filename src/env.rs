use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;

use crate::cache::BoundedMemoryCache;
use crate::cache_tracker::CacheTracker;
use crate::hosts::Hosts;
use crate::map_output_tracker::MapOutputTracker;
use crate::shuffle::{ShuffleFetcher, ShuffleManager};
// use clap::{App, Arg, SubCommand};
use dashmap::DashMap;
use log::LevelFilter;
use once_cell::sync::{Lazy, OnceCell};
use serde::Deserialize;
use thiserror::Error;
use tokio::runtime::{Handle, Runtime};

type ShuffleCache = Arc<DashMap<(usize, usize, usize), Vec<u8>>>;

const ENV_VAR_PREFIX: &str = "NS";
pub(crate) const THREAD_PREFIX: &str = "_NS";
static CONF: OnceCell<Configuration> = OnceCell::new();
static ENV: OnceCell<Env> = OnceCell::new();
static ASYNC_HANDLE: Lazy<Handle> = Lazy::new(Handle::current);

pub(crate) static SHUFFLE_CACHE: Lazy<ShuffleCache> = Lazy::new(|| Arc::new(DashMap::new()));
pub(crate) static BOUNDED_MEM_CACHE: Lazy<BoundedMemoryCache> = Lazy::new(BoundedMemoryCache::new);

pub(crate) struct Env {
    pub map_output_tracker: MapOutputTracker,
    pub shuffle_manager: ShuffleManager,
    pub shuffle_fetcher: ShuffleFetcher,
    pub cache_tracker: CacheTracker,
    async_rt: Option<Runtime>,
}

/// Builds an async executor for executing DAG tasks according to env,
/// machine properties and schedulling mode.
fn build_async_executor() -> Option<Runtime> {
    if Handle::try_current().is_ok() {
        None
    } else {
        Some(
            tokio::runtime::Builder::new()
                .enable_all()
                .threaded_scheduler()
                .build()
                .unwrap(),
        )
    }
}

impl Env {
    pub fn get() -> &'static Env {
        ENV.get_or_init(Self::new)
    }

    /// Get a handle to the current running async executor to spawn tasks.
    pub fn get_async_handle() -> &'static Handle {
        if let Some(executor) = &ENV.get_or_init(Self::new).async_rt {
            executor.handle()
        } else {
            &ASYNC_HANDLE
        }
    }

    fn new() -> Self {
        let conf = Configuration::get();
        let master_addr = Hosts::get().unwrap().master;
        Env {
            map_output_tracker: MapOutputTracker::new(conf.is_master, master_addr),
            shuffle_manager: ShuffleManager::new().unwrap(),
            shuffle_fetcher: ShuffleFetcher,
            cache_tracker: CacheTracker::new(
                conf.is_master,
                master_addr,
                conf.local_ip,
                &BOUNDED_MEM_CACHE,
            ),
            async_rt: build_async_executor(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum DeploymentMode {
    Distributed,
    Local,
}

pub(crate) const SLAVE_DEPLOY_CMD: &str = "deploy_slave";

pub(crate) struct Configuration {
    pub is_master: bool,
    pub local_ip: Ipv4Addr,
    pub local_dir: PathBuf,
    pub port: Option<u16>,
    pub deployment_mode: DeploymentMode,
    pub log_level: LevelFilter,
    pub shuffle_svc_port: Option<u16>,
}

/// Struct used for parsing environment vars
#[derive(Deserialize, Debug)]
struct EnvConfig {
    deployment_mode: Option<DeploymentMode>,
    local_ip: Option<String>,
    local_dir: Option<String>,
    log_level: Option<LogLevel>,
    shuffle_service_port: Option<u16>,
    slave_deployment: Option<bool>,
    slave_port: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum LogLevel {
    Error,
    Warn,
    Debug,
    Trace,
    Info,
}

impl Into<LevelFilter> for LogLevel {
    fn into(self) -> LevelFilter {
        match self {
            LogLevel::Error => LevelFilter::Error,
            LogLevel::Warn => LevelFilter::Warn,
            LogLevel::Debug => LevelFilter::Debug,
            LogLevel::Trace => LevelFilter::Trace,
            _ => LevelFilter::Info,
        }
    }
}

impl Configuration {
    pub fn get() -> &'static Configuration {
        CONF.get_or_init(Self::new)
    }

    fn new() -> Self {
        use DeploymentMode::*;

        let config = envy::prefixed(ENV_VAR_PREFIX)
            .from_env::<EnvConfig>()
            .unwrap();

        let deployment_mode = match config.deployment_mode {
            Some(Distributed) => Distributed,
            _ => Local,
        };

        let local_dir = if let Some(dir) = config.local_dir {
            PathBuf::from(dir)
        } else {
            std::env::temp_dir()
        };

        let log_level: LevelFilter = match config.log_level {
            Some(val) => val.into(),
            _ => LogLevel::Info.into(),
        };
        log::set_max_level(log_level);

        let local_ip: Ipv4Addr = {
            if let Some(ip) = config.local_ip {
                ip.parse().unwrap()
            } else if deployment_mode == Distributed {
                panic!("Local IP required while deploying in distributed mode.")
            } else {
                Ipv4Addr::LOCALHOST
            }
        };

        let port: Option<u16>;
        let is_master;
        match config.slave_deployment {
            Some(true) => {
                port = {
                    if let Some(port) = config.slave_port {
                        Some(
                            port.parse()
                                .map_err(ConfigurationError::PortParsing)
                                .unwrap(),
                        )
                    } else {
                        panic!("Port required while deploying a worker.")
                    }
                };
                is_master = false;
            }
            _ => {
                port = None;
                is_master = true;
            }
        }

        Configuration {
            is_master,
            local_ip,
            local_dir,
            port,
            deployment_mode,
            log_level,
            shuffle_svc_port: config.shuffle_service_port,
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigurationError {
    #[error("failed to parse port")]
    PortParsing(#[source] std::num::ParseIntError),
}
