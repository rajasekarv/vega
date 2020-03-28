use std::fs;
use std::io::Write;
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use crate::distributed_scheduler::DistributedScheduler;
use crate::error::{Error, Result};
use crate::executor::{Executor, Signal};
use crate::io::ReaderConfiguration;
use crate::local_scheduler::LocalScheduler;
use crate::parallel_collection_rdd::ParallelCollection;
use crate::rdd::union_rdd::UnionRdd;
use crate::rdd::{Rdd, RddBase};
use crate::scheduler::NativeScheduler;
use crate::serializable_traits::{Data, SerFunc};
use crate::serialized_data_capnp::serialized_data;
use crate::task::TaskContext;
use crate::{env, hosts};
use log::error;
use once_cell::sync::OnceCell;
use simplelog::*;
use uuid::Uuid;

// There is a problem with this approach since T needs to satisfy PartialEq, Eq for Range.
// No such restrictions are needed for Vec.
pub enum Sequence<T> {
    Range(Range<T>),
    Vec(Vec<T>),
}

#[derive(Clone)]
enum Schedulers {
    Local(Arc<LocalScheduler>),
    Distributed(Arc<DistributedScheduler>),
}

impl Default for Schedulers {
    fn default() -> Schedulers {
        Schedulers::Local(Arc::new(LocalScheduler::new(20, true)))
    }
}

impl Schedulers {
    pub fn run_job<T: Data, U: Data, F>(
        &self,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        partitions: Vec<usize>,
        allow_local: bool,
    ) -> Result<Vec<U>>
    where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        use Schedulers::*;
        match self {
            Distributed(distributed) => {
                distributed
                    .clone()
                    .run_job(func, final_rdd, partitions, allow_local)
            }
            Local(local) => local
                .clone()
                .run_job(func, final_rdd, partitions, allow_local),
        }
    }
}

#[derive(Default)]
pub struct Context {
    next_rdd_id: Arc<AtomicUsize>,
    next_shuffle_id: Arc<AtomicUsize>,
    scheduler: Schedulers,
    pub(crate) address_map: Vec<SocketAddrV4>,
    distributed_driver: bool,
    /// the executing job tmp work_dir
    work_dir: PathBuf,
}

impl Drop for Context {
    fn drop(&mut self) {
        //TODO clean up temp files
        #[cfg(debug_assertions)]
        {
            let deployment_mode = env::Configuration::get().deployment_mode;
            if self.distributed_driver && deployment_mode == env::DeploymentMode::Distributed {
                log::info!("inside context drop in master");
            } else if deployment_mode == env::DeploymentMode::Distributed {
                log::info!("inside context drop in executor");
            }
        }
        self.drop_executors();
        Context::clean_up_work_dir(&self.work_dir);
    }
}

impl Context {
    pub fn new() -> Result<Arc<Self>> {
        Context::with_mode(env::Configuration::get().deployment_mode)
    }

    pub fn with_mode(mode: env::DeploymentMode) -> Result<Arc<Self>> {
        match mode {
            env::DeploymentMode::Distributed => {
                if env::Configuration::get().is_driver {
                    Context::init_distributed_driver()
                } else {
                    Context::init_distributed_worker()?
                }
            }
            env::DeploymentMode::Local => Context::init_local_scheduler(),
        }
    }

    fn init_local_scheduler() -> Result<Arc<Self>> {
        let job_id = Uuid::new_v4().to_string();
        let job_work_dir = env::Configuration::get()
            .local_dir
            .join(format!("ns-job-{}", job_id));
        fs::create_dir_all(&job_work_dir).unwrap();

        initialize_loggers(job_work_dir.join("ns-driver.log"));
        let scheduler = Schedulers::Local(Arc::new(LocalScheduler::new(20, true)));

        Ok(Arc::new(Context {
            next_rdd_id: Arc::new(AtomicUsize::new(0)),
            next_shuffle_id: Arc::new(AtomicUsize::new(0)),
            scheduler,
            address_map: vec![SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)],
            distributed_driver: false,
            work_dir: job_work_dir,
        }))
    }

    /// Initialization function for the application driver.
    /// * Distributes the configuration setup to the workers.
    /// * Distributes a copy of the application binary to all the active worker host nodes.
    /// * Launches the workers in the remote machine using the same binary (required).
    /// * Creates and returns a working Context.
    fn init_distributed_driver() -> Result<Arc<Self>> {
        let mut port: u16 = 10000;
        let mut address_map = Vec::new();
        let job_id = Uuid::new_v4().to_string();
        let job_work_dir = env::Configuration::get()
            .local_dir
            .join(format!("ns-job-{}", job_id));
        let job_work_dir_str = job_work_dir
            .to_str()
            .ok_or_else(|| Error::PathToString(job_work_dir.clone()))?;

        let binary_path = std::env::current_exe().map_err(|_| Error::CurrentBinaryPath)?;
        let binary_path_str = binary_path
            .to_str()
            .ok_or_else(|| Error::PathToString(binary_path.clone()))?
            .into();
        let binary_name = binary_path
            .file_name()
            .ok_or(Error::CurrentBinaryName)?
            .to_os_string()
            .into_string()
            .map_err(Error::OsStringToString)?;

        fs::create_dir_all(&job_work_dir).unwrap();
        let conf_path = job_work_dir.join("config.toml");
        let conf_path = conf_path.to_str().unwrap();
        initialize_loggers(job_work_dir.join("ns-driver.log"));

        for address in &hosts::Hosts::get()?.slaves {
            log::debug!("deploying executor at address {:?}", address);
            let address_ip: Ipv4Addr = address
                .split('@')
                .nth(1)
                .ok_or_else(|| Error::ParseHostAddress(address.into()))?
                .parse()
                .map_err(|x| Error::ParseHostAddress(format!("{}", x)))?;
            address_map.push(SocketAddrV4::new(address_ip, port));

            // Create work dir:
            Command::new("ssh")
                .args(&[address, "mkdir", &job_work_dir_str])
                .output()
                .map_err(|e| Error::CommandOutput {
                    source: e,
                    command: "ssh mkdir".into(),
                })?;

            // Copy conf file to remote:
            Context::create_workers_config_file(address_ip, port, conf_path)?;
            let remote_path = format!("{}:{}/config.toml", address, job_work_dir_str);
            Command::new("scp")
                .args(&[conf_path, &remote_path])
                .output()
                .map_err(|e| Error::CommandOutput {
                    source: e,
                    command: "scp config".into(),
                })?;

            // Copy binary:
            let remote_path = format!("{}:{}/{}", address, job_work_dir_str, binary_name);
            Command::new("scp")
                .args(&[&binary_path_str, &remote_path])
                .output()
                .map_err(|e| Error::CommandOutput {
                    source: e,
                    command: "scp executor".into(),
                })?;

            // Deploy a remote slave:
            let path = format!("{}/{}", job_work_dir_str, binary_name);
            log::debug!("remote path {}", path);
            Command::new("ssh")
                .args(&[address, &path])
                .spawn()
                .map_err(|e| Error::CommandOutput {
                    source: e,
                    command: "ssh run".into(),
                })?;
            port += 5000;
        }

        Ok(Arc::new(Context {
            next_rdd_id: Arc::new(AtomicUsize::new(0)),
            next_shuffle_id: Arc::new(AtomicUsize::new(0)),
            scheduler: Schedulers::Distributed(Arc::new(DistributedScheduler::new(
                20,
                true,
                Some(address_map.clone()),
                10000,
            ))),
            address_map,
            distributed_driver: true,
            work_dir: job_work_dir,
        }))
    }

    fn init_distributed_worker() -> Result<!> {
        let mut work_dir = PathBuf::from("");
        match std::env::current_exe().map_err(|_| Error::CurrentBinaryPath) {
            Ok(binary_path) => {
                match binary_path.parent().ok_or_else(|| Error::CurrentBinaryPath) {
                    Ok(dir) => work_dir = dir.into(),
                    Err(err) => Context::worker_clean_up(Err(err), work_dir)?,
                };
                initialize_loggers(work_dir.join("ns-executor.log"));
            }
            Err(err) => Context::worker_clean_up(Err(err), work_dir)?,
        }

        log::debug!("starting worker");
        let port = match env::Configuration::get()
            .slave
            .as_ref()
            .map(|c| c.port)
            .ok_or(Error::GetOrCreateConfig("executor port not set"))
        {
            Ok(port) => port,
            Err(err) => Context::worker_clean_up(Err(err), work_dir)?,
        };
        let executor = Arc::new(Executor::new(port));
        Context::worker_clean_up(executor.worker(), work_dir)
    }

    fn worker_clean_up(run_result: Result<Signal>, work_dir: PathBuf) -> Result<!> {
        match run_result {
            Err(err) => {
                log::error!("executor failed with error: {}", err);
                Context::clean_up_work_dir(&work_dir);
                std::process::exit(1);
            }
            Ok(value) => {
                log::info!("executor closed gracefully with signal: {:?}", value);
                Context::clean_up_work_dir(&work_dir);
                std::process::exit(0);
            }
        }
    }

    #[allow(unused_must_use)]
    fn clean_up_work_dir(work_dir: &Path) {
        if env::Configuration::get().loggin.log_cleanup {
            // Remove created files.
            if fs::remove_dir_all(&work_dir).is_err() {
                log::error!("failed removing tmp work dir: {}", work_dir.display());
            }
        } else if let Ok(dir) = fs::read_dir(work_dir) {
            for e in dir {
                if let Ok(p) = e {
                    if let Ok(m) = p.metadata() {
                        if m.is_dir() {
                            fs::remove_dir_all(p.path());
                        } else {
                            let file = p.path();
                            if let Some(ext) = file.extension() {
                                if ext.to_str() != "log".into() {
                                    fs::remove_file(file);
                                }
                            } else {
                                fs::remove_file(file);
                            }
                        }
                    }
                }
            }
        }
    }

    fn create_workers_config_file(local_ip: Ipv4Addr, port: u16, config_path: &str) -> Result<()> {
        let mut current_config = env::Configuration::get().clone();
        current_config.local_ip = local_ip;
        current_config.slave = Some(std::convert::From::<(bool, u16)>::from((true, port)));
        current_config.is_driver = false;

        let config_string = toml::to_string_pretty(&current_config).unwrap();
        let mut config_file = fs::File::create(config_path).unwrap();
        config_file.write_all(config_string.as_bytes()).unwrap();
        Ok(())
    }

    fn drop_executors(&self) {
        if env::Configuration::get().deployment_mode.is_local() {
            return;
        }

        for socket_addr in self.address_map.clone() {
            log::debug!(
                "dropping executor in {:?}:{:?}",
                socket_addr.ip(),
                socket_addr.port()
            );
            if let Ok(mut stream) =
                TcpStream::connect(format!("{}:{}", socket_addr.ip(), socket_addr.port() + 10))
            {
                let signal = bincode::serialize(&Signal::ShutDownGracefully).unwrap();
                let mut message = capnp::message::Builder::new_default();
                let mut task_data = message.init_root::<serialized_data::Builder>();
                task_data.set_msg(&signal);
                capnp::serialize::write_message(&mut stream, &message)
                    .map_err(Error::InputRead)
                    .unwrap();
            } else {
                error!(
                    "Failed to connect to {}:{} in order to stop its executor",
                    socket_addr.ip(),
                    socket_addr.port()
                );
            }
        }
    }

    pub fn new_rdd_id(self: &Arc<Self>) -> usize {
        self.next_rdd_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn new_shuffle_id(self: &Arc<Self>) -> usize {
        self.next_shuffle_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn make_rdd<T: Data, I>(
        self: &Arc<Self>,
        seq: I,
        num_slices: usize,
    ) -> serde_traitobject::Arc<dyn Rdd<Item = T>>
    where
        I: IntoIterator<Item = T>,
    {
        self.parallelize(seq, num_slices)
    }

    pub fn parallelize<T: Data, I>(
        self: &Arc<Self>,
        seq: I,
        num_slices: usize,
    ) -> serde_traitobject::Arc<dyn Rdd<Item = T>>
    where
        I: IntoIterator<Item = T>,
    {
        serde_traitobject::Arc::new(ParallelCollection::new(self.clone(), seq, num_slices))
    }

    /// Load from a distributed source and turns it into a parallel collection.
    pub fn read_source<F, C, I: Data, O: Data>(
        self: &Arc<Self>,
        config: C,
        func: F,
    ) -> impl Rdd<Item = O>
    where
        F: SerFunc(I) -> O,
        C: ReaderConfiguration<I>,
    {
        config.make_reader(self.clone(), func)
    }

    pub fn run_job<T: Data, U: Data, F>(
        self: &Arc<Self>,
        rdd: Arc<dyn Rdd<Item = T>>,
        func: F,
    ) -> Result<Vec<U>>
    where
        F: SerFunc(Box<dyn Iterator<Item = T>>) -> U,
    {
        let cl = Fn!(move |(task_context, iter)| (func)(iter));
        let func = Arc::new(cl);
        self.scheduler.run_job(
            func,
            rdd.clone(),
            (0..rdd.number_of_splits()).collect(),
            false,
        )
    }

    pub fn run_job_with_partitions<T: Data, U: Data, F, P>(
        self: &Arc<Self>,
        rdd: Arc<dyn Rdd<Item = T>>,
        func: F,
        partitions: P,
    ) -> Result<Vec<U>>
    where
        F: SerFunc(Box<dyn Iterator<Item = T>>) -> U,
        P: IntoIterator<Item = usize>,
    {
        let cl = Fn!(move |(task_context, iter)| (func)(iter));
        self.scheduler
            .run_job(Arc::new(cl), rdd, partitions.into_iter().collect(), false)
    }

    pub fn run_job_with_context<T: Data, U: Data, F>(
        self: &Arc<Self>,
        rdd: Arc<dyn Rdd<Item = T>>,
        func: F,
    ) -> Result<Vec<U>>
    where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        log::debug!("inside run job in context");
        let func = Arc::new(func);
        self.scheduler.run_job(
            func,
            rdd.clone(),
            (0..rdd.number_of_splits()).collect(),
            false,
        )
    }

    pub(crate) fn get_preferred_locs(
        &self,
        rdd: Arc<dyn RddBase>,
        partition: usize,
    ) -> Vec<std::net::Ipv4Addr> {
        match &self.scheduler {
            Schedulers::Distributed(scheduler) => scheduler.get_preferred_locs(rdd, partition),
            Schedulers::Local(scheduler) => scheduler.get_preferred_locs(rdd, partition),
        }
    }

    pub fn union<T: Data>(rdds: &[Arc<dyn Rdd<Item = T>>]) -> Result<UnionRdd<T>> {
        UnionRdd::new(rdds)
    }
}

static LOGGER: OnceCell<()> = OnceCell::new();

fn initialize_loggers<P: Into<PathBuf>>(file_path: P) {
    fn _initializer(file_path: PathBuf) {
        let log_level = env::Configuration::get().loggin.log_level.into();
        log::info!("path for file logger: {}", file_path.display());
        let file_logger: Box<dyn SharedLogger> = WriteLogger::new(
            log_level,
            Config::default(),
            fs::File::create(file_path).expect("not able to create log file"),
        );
        let mut combined = vec![file_logger];
        if let Some(term_logger) =
            TermLogger::new(log_level, Config::default(), TerminalMode::Mixed)
        {
            let logger: Box<dyn SharedLogger> = term_logger;
            combined.push(logger);
        }
        CombinedLogger::init(combined).unwrap();
    }

    LOGGER.get_or_init(move || _initializer(file_path.into()));
}
