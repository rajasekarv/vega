use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use self::config_vars::*;
use crate::cache::BoundedMemoryCache;
use crate::cache_tracker::CacheTracker;
use crate::error::Error;
use crate::hosts::Hosts;
use crate::map_output_tracker::MapOutputTracker;
use crate::shuffle::{ShuffleFetcher, ShuffleManager};
use clap::{App, Arg, SubCommand};
use log::LevelFilter as LogLevel;
use once_cell::sync::{Lazy, OnceCell};
use parking_lot::{Mutex, RwLock};
use thiserror::Error;
use tokio::runtime::{Handle, Runtime};

type ShuffleCache = Arc<RwLock<HashMap<(usize, usize, usize), Vec<u8>>>>;

pub(crate) mod config_vars {
    pub const DEPLOYMENT_MODE: &str = "NS_DEPLOYMENT_MODE";
    pub const LOCAL_DIR: &str = "NS_LOCAL_DIR";
    pub const LOCAL_IP: &str = "NS_LOCAL_IP";
    pub const LOG_LEVEL: &str = "NS_LOG_LEVEL";
    pub const PORT: &str = "NS_PORT";
    pub const SHUFFLE_SERVICE_PORT: &str = "NS_SHUFFLE_SERVICE_PORT";
}

pub(crate) const THREAD_PREFIX: &str = "_NS";
static CONF: OnceCell<Configuration> = OnceCell::new();
static ENV: OnceCell<Env> = OnceCell::new();
static ASYNC_HANDLE: Lazy<Handle> = Lazy::new(Handle::current);

pub(crate) static shuffle_cache: Lazy<ShuffleCache> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));
pub(crate) static the_cache: Lazy<BoundedMemoryCache> = Lazy::new(BoundedMemoryCache::new);

pub(crate) struct Env {
    pub map_output_tracker: MapOutputTracker,
    pub shuffle_manager: ShuffleManager,
    pub shuffle_fetcher: ShuffleFetcher,
    pub cache_tracker: CacheTracker,
    async_rt: Option<Runtime>,
}

/// Builds an async executor for executing DAG tasks according to env,
/// machine properties and schedulling mode.
fn build_async_executor(is_master: bool) -> Option<Runtime> {
    if Handle::try_current().is_ok() || !is_master {
        // don't initialize a runtime if this is not the master
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
                &the_cache,
            ),
            async_rt: build_async_executor(conf.is_master),
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
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
    pub log_level: LogLevel,
    pub shuffle_svc_port: Option<u16>,
}

impl Configuration {
    pub fn get() -> &'static Configuration {
        CONF.get_or_init(Self::new)
    }

    fn new() -> Self {
        let arguments = App::new("NativeSpark")
            .arg(
                Arg::with_name(DEPLOYMENT_MODE)
                    .long("deployment_mode")
                    .short("d")
                    .takes_value(true)
                    .env(DEPLOYMENT_MODE)
                    .default_value("local"),
            )
            .arg(
                Arg::with_name(LOCAL_IP)
                    .long("local_ip")
                    .require_equals(true)
                    .env(LOCAL_IP)
                    .takes_value(true)
                    .required_if(DEPLOYMENT_MODE, "distributed")
                    .default_value_if(
                        DEPLOYMENT_MODE,
                        Some("local"),
                        &Ipv4Addr::LOCALHOST.to_string(),
                    ),
            )
            .arg(Arg::with_name(LOCAL_DIR).long("local_dir").env(LOCAL_DIR))
            .arg(
                Arg::with_name(LOG_LEVEL)
                    .long("log_level")
                    .env(LOG_LEVEL)
                    .takes_value(true)
                    .require_equals(true),
            )
            .arg(
                Arg::with_name(SHUFFLE_SERVICE_PORT)
                    .long("shuffle.port")
                    .env(SHUFFLE_SERVICE_PORT),
            )
            .subcommand(
                SubCommand::with_name(SLAVE_DEPLOY_CMD)
                    .about("deploys an slave at the executing machine")
                    .arg(
                        Arg::with_name(PORT)
                            .long("port")
                            .short("p")
                            .env(PORT)
                            .takes_value(true)
                            .require_equals(true)
                            .required(true),
                    ),
            )
            .get_matches();

        let deployment_mode = match arguments.value_of(DEPLOYMENT_MODE) {
            Some("distributed") => DeploymentMode::Distributed,
            _ => DeploymentMode::Local,
        };

        let local_dir = if let Some(dir) = arguments.value_of(LOCAL_DIR) {
            PathBuf::from(dir.to_owned())
        } else {
            std::env::temp_dir()
        };

        let log_level = match arguments
            .value_of(LOG_LEVEL)
            .map(|s| s.to_lowercase())
            .as_deref()
        {
            Some("error") => LogLevel::Error,
            Some("warn") => LogLevel::Warn,
            Some("debug") => LogLevel::Debug,
            Some("trace") => LogLevel::Trace,
            _ => LogLevel::Info,
        };
        log::set_max_level(log_level);

        let local_ip = arguments.value_of(LOCAL_IP).unwrap().parse().unwrap();
        let port: Option<u16>;
        let is_master;
        if let Some(slave_deployment) = arguments.subcommand_matches(SLAVE_DEPLOY_CMD) {
            port = Some(
                slave_deployment
                    .value_of(PORT)
                    .unwrap()
                    .parse()
                    .map_err(ConfigurationError::PortParsing)
                    .unwrap(),
            );
            is_master = false;
        } else {
            port = None;
            is_master = true;
        }
        Configuration {
            is_master,
            local_ip,
            local_dir,
            port,
            deployment_mode,
            log_level,
            shuffle_svc_port: arguments.value_of(SHUFFLE_SERVICE_PORT).map(|port| {
                port.parse()
                    .map_err(ConfigurationError::PortParsing)
                    .unwrap()
            }),
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigurationError {
    #[error("failed to parse port")]
    PortParsing(#[source] std::num::ParseIntError),
}
