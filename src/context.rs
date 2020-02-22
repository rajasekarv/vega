use std::convert::TryFrom;
use std::fs::File;
use std::io::Write;
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
use std::ops::Range;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::distributed_scheduler::DistributedScheduler;
use crate::error::{Error, Result};
use crate::executor::Executor;
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
use capnp::serialize_packed;
use log::error;
use simplelog::*;
use uuid::Uuid;

// there is a problem with this approach since T needs to satisfy PartialEq, Eq for Range
// No such restrictions are needed for Vec
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
        //        let map_output_tracker = MapOutputTracker::new(true, "".to_string(), 0);
        Schedulers::Local(Arc::new(LocalScheduler::new(num_cpus::get(), 20, true)))
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
    distributed_master: bool,
}

impl Drop for Context {
    fn drop(&mut self) {
        //TODO clean up temp files
        log::debug!("inside context drop in master {}", self.distributed_master);
        self.drop_executors();
    }
}

impl Context {
    pub fn new() -> Result<Arc<Self>> {
        Context::with_mode(env::Configuration::get().deployment_mode)
    }

    // Sends the binary to all nodes present in hosts.conf and starts them
    pub fn with_mode(mode: env::DeploymentMode) -> Result<Arc<Self>> {
        let next_rdd_id = Arc::new(AtomicUsize::new(0));
        let next_shuffle_id = Arc::new(AtomicUsize::new(0));
        use Schedulers::*;
        match mode {
            env::DeploymentMode::Distributed => {
                let mut port: u16 = 10000;
                let mut address_map = Vec::new();
                if env::Configuration::get().is_master {
                    let uuid = Uuid::new_v4().to_string();
                    initialize_loggers(format!("/tmp/master-{}", uuid));
                    let binary_path =
                        std::env::current_exe().map_err(|_| Error::CurrentBinaryPath)?;
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
                    for address in &hosts::Hosts::get()?.slaves {
                        log::debug!("deploying executor at address {:?}", address);
                        let address_cli: Ipv4Addr = address
                            .split('@')
                            .nth(1)
                            .ok_or_else(|| Error::ParseHostAddress(address.into()))?
                            .parse()
                            .map_err(|x| Error::ParseHostAddress(format!("{}", x)))?;
                        address_map.push(SocketAddrV4::new(address_cli, port));
                        let local_dir_root = "/tmp";
                        let uuid = Uuid::new_v4();
                        let local_dir_uuid = uuid.to_string();
                        let local_dir =
                            format!("{}/spark-binary-{}", local_dir_root, local_dir_uuid);
                        let mkdir_output = Command::new("ssh")
                            .args(&[address, "mkdir", &local_dir.clone()])
                            .output()
                            .map_err(|e| Error::CommandOutput {
                                source: e,
                                command: "ssh mkdir".into(),
                            })?;
                        let remote_path = format!("{}:{}/{}", address, local_dir, binary_name);
                        let scp_output = Command::new("scp")
                            .args(&[&binary_path_str, &remote_path])
                            .output()
                            .map_err(|e| Error::CommandOutput {
                                source: e,
                                command: "scp executor".into(),
                            })?;
                        let path = format!("{}/{}", local_dir, binary_name);
                        log::debug!("remote path {}", path);
                        // Deploy a remote slave
                        Command::new("ssh")
                            .args(&[
                                address,
                                &path,
                                &env::SLAVE_DEPLOY_CMD.to_string(),
                                &format!("--port={}", port),
                            ])
                            .spawn()
                            .map_err(|e| Error::CommandOutput {
                                source: e,
                                command: "ssh run".into(),
                            })?;
                        port += 5000;
                    }
                    Ok(Arc::new(Context {
                        next_rdd_id,
                        next_shuffle_id,
                        scheduler: Distributed(Arc::new(DistributedScheduler::new(
                            20,
                            true,
                            Some(address_map.clone()),
                            10000,
                        ))),
                        address_map,
                        distributed_master: true,
                    }))
                } else {
                    let uuid = Uuid::new_v4().to_string();
                    initialize_loggers(format!("/tmp/executor-{}", uuid));
                    log::debug!("started client");
                    let executor = Arc::new(Executor::new(
                        env::Configuration::get()
                            .port
                            .ok_or(Error::GetOrCreateConfig("executor port not set"))?,
                    ));
                    executor.worker();
                    log::debug!("got executor end signal");
                    std::process::exit(0);
                }
            }
            env::DeploymentMode::Local => {
                let uuid = Uuid::new_v4().to_string();
                initialize_loggers(format!("/tmp/master-{}", uuid));
                let scheduler = Local(Arc::new(LocalScheduler::new(num_cpus::get(), 20, true)));
                Ok(Arc::new(Context {
                    next_rdd_id,
                    next_shuffle_id,
                    scheduler,
                    address_map: vec![SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)],
                    distributed_master: false,
                }))
            }
        }
    }

    fn drop_executors(&self) {
        use crate::executor::Signal;
        for socket_addr in self.address_map.clone() {
            log::debug!(
                "dropping executor in {:?}:{:?}",
                socket_addr.ip(),
                socket_addr.port()
            );
            if let Ok(mut stream) =
                TcpStream::connect(format!("{}:{}", socket_addr.ip(), socket_addr.port() + 10))
            {
                let signal = bincode::serialize(&Signal::ShutDown).unwrap();
                let mut message = ::capnp::message::Builder::new_default();
                let mut task_data = message.init_root::<serialized_data::Builder>();
                task_data.set_msg(&signal);
                serialize_packed::write_message(&mut stream, &message);
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

    // currently it accepts only vector.
    // TODO change this to accept any iterator
    // &Arc<Self> is an unstable feature. used here just to keep the user end context usage same as before.
    // Can be removed if sc.clone() API seems ok.
    pub fn make_rdd<T: Data, I>(
        self: &Arc<Self>,
        seq: I,
        num_slices: usize,
    ) -> serde_traitobject::Arc<dyn Rdd<Item = T>>
    where
        I: IntoIterator<Item = T>,
    {
        //let num_slices = seq.len() / num_slices;
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

fn initialize_loggers(file_path: String) {
    let term_logger = TermLogger::new(
        env::Configuration::get().log_level,
        Config::default(),
        TerminalMode::Mixed,
    );
    let file_logger: Box<dyn SharedLogger> = WriteLogger::new(
        env::Configuration::get().log_level,
        Config::default(),
        File::create(file_path).expect("not able to create log file"),
    );
    let mut combined = vec![file_logger];
    if let Some(logger) = term_logger {
        let logger: Box<dyn SharedLogger> = logger;
        combined.push(logger);
    }
    CombinedLogger::init(combined);
}
