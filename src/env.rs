use super::*;

use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use clap::{App, Arg, SubCommand};
use log;
use log::LevelFilter as LogLevel;
use once_cell::sync::{Lazy, OnceCell};
use parking_lot::{Mutex, RwLock};

type ShuffleCache = Arc<RwLock<HashMap<(usize, usize, usize), Vec<u8>>>>;

static CONF: OnceCell<Configuration> = OnceCell::new();
static ENV: OnceCell<Env> = OnceCell::new();
pub(crate) static shuffle_cache: Lazy<ShuffleCache> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));
pub(crate) static the_cache: Lazy<BoundedMemoryCache> = Lazy::new(BoundedMemoryCache::new);

pub(crate) struct Env {
    pub map_output_tracker: MapOutputTracker,
    pub shuffle_manager: ShuffleManager,
    pub shuffle_fetcher: ShuffleFetcher,
    pub cache_tracker: CacheTracker,
}

impl Env {
    pub fn get() -> &'static Env {
        ENV.get_or_init(Self::new)
    }

    fn new() -> Self {
        let conf = Configuration::get();
        let master_addr = Hosts::get().unwrap().master;
        Env {
            map_output_tracker: MapOutputTracker::new(conf.is_master, master_addr),
            shuffle_manager: ShuffleManager::new(),
            shuffle_fetcher: ShuffleFetcher,
            cache_tracker: CacheTracker::new(
                conf.is_master,
                master_addr,
                conf.local_ip,
                &the_cache,
            ),
        }
    }
}

mod config_vars {
    pub(super) const LOCAL_IP: &str = "NS_LOCAL_IP";
    pub(super) const PORT: &str = "NS_PORT";
    pub(super) const DEPLOYMENT_MODE: &str = "NS_DEPLOYMENT_MODE";
    pub(super) const LOG_LEVEL: &str = "NS_LOG_LEVEL";
}

use config_vars::*;

#[derive(Clone, Copy)]
pub enum DeploymentMode {
    Distributed,
    Local,
}

pub(crate) struct Configuration {
    pub is_master: bool,
    pub local_ip: Ipv4Addr,
    pub port: Option<u16>,
    pub deployment_mode: DeploymentMode,
    pub log_level: LogLevel,
}

impl Configuration {
    pub fn get() -> &'static Configuration {
        CONF.get_or_init(Self::new)
    }

    fn new() -> Self {
        const SLAVE_DEPLOY_CMD: &str = "deploy_slave";

        let arguments = App::new("NativeSpark")
            .arg(
                Arg::with_name(LOCAL_IP)
                    .long("local_ip")
                    .require_equals(true)
                    .env(LOCAL_IP)
                    .takes_value(true)
                    .required(true),
            )
            .arg(
                Arg::with_name(DEPLOYMENT_MODE)
                    .long("deployment_mode")
                    .short("d")
                    .takes_value(true)
                    .env(DEPLOYMENT_MODE)
                    .default_value("local"),
            )
            .arg(
                Arg::with_name(LOG_LEVEL)
                    .long("log_level")
                    .takes_value(true)
                    .require_equals(true),
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

        let local_ip = arguments.value_of(LOCAL_IP).unwrap().parse().unwrap();

        let log_level = match arguments.value_of(LOG_LEVEL) {
            Some("error") => LogLevel::Error,
            Some("warn") => LogLevel::Warn,
            Some("debug") => LogLevel::Debug,
            Some("trace") => LogLevel::Trace,
            Some("info") | _ => LogLevel::Info,
        };
        log::set_max_level(log_level);

        let deployment_mode = match arguments.value_of(DEPLOYMENT_MODE) {
            Some("distributed") => DeploymentMode::Distributed,
            _ => DeploymentMode::Local,
        };

        let port: Option<u16>;
        let is_master;
        if let Some(slave_deployment) = arguments.subcommand_matches(SLAVE_DEPLOY_CMD) {
            port = Some(
                slave_deployment
                    .value_of(PORT)
                    .unwrap()
                    .parse()
                    .map_err(Error::ExecutorPort)
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
            port,
            deployment_mode,
            log_level,
        }
    }
}
