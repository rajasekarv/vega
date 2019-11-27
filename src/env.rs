use super::*;

use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use clap::{App, Arg, SubCommand};
use log::Level as LogLevel;
use parking_lot::{Mutex, RwLock};

pub struct Env {
    pub map_output_tracker: MapOutputTracker,
    pub shuffle_manager: ShuffleManager,
    pub shuffle_fetcher: ShuffleFetcher,
    pub cache_tracker: CacheTracker,
}

impl Env {
    pub fn new(master_addr: SocketAddr) -> Self {
        Env {
            map_output_tracker: MapOutputTracker::new(config.is_master, master_addr),
            shuffle_manager: ShuffleManager::new(),
            shuffle_fetcher: ShuffleFetcher,
            cache_tracker: CacheTracker::new(config.is_master, master_addr, &the_cache),
        }
    }
}

mod config_vars {
    pub(super) const LOCAL_IP: &str = "LOCAL_IP";
    pub(super) const PORT: &str = "PORT";
    pub(super) const DEPLOYMENT_MODE: &str = "DEPLOYMENT_MODE";
    pub(super) const LOG_LEVEL: &str = "LOG_LEVEL";
}

use config_vars::*;

pub(crate) struct Configuration {
    pub is_master: bool,
    pub local_ip: Ipv4Addr,
    pub port: Option<u16>,
    pub deployment_mode: DeploymentMode,
    pub log_level: LogLevel,
}

#[derive(Clone, Copy)]
pub enum DeploymentMode {
    Distributed,
    Local,
}

impl Configuration {
    fn new() -> Configuration {
        const SLAVE_DEPLOY_CMD: &str = "deployment_mode";

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
                            .env("NS_PORT")
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

        let deployment_mode = match arguments.value_of("deployment_mode") {
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

lazy_static! {
    // Too lazy to choose a proper logger. Currently using a static log file to log the whole process. Just a crude version of logger.
    pub(crate) static ref config: Configuration = Configuration::new();
    pub static ref shuffle_cache: Arc<RwLock<HashMap<(usize, usize, usize), Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
    pub static ref the_cache: BoundedMemoryCache = BoundedMemoryCache::new();
    pub static ref hosts: Hosts = Hosts::load().unwrap();
    pub static ref env: Env = Env::new(hosts.master);
}
