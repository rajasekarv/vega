use super::*;
use capnp::serialize_packed;
use simplelog::*;
//use parking_lot::Mutex;
//use serde_derive;
//use std::collections::HashMap;
use std::fs::File;
//use std::io::prelude::*;
//use std::net::TcpListener;
use std::net::TcpStream;
use std::ops::Range;
//use std::option::Iter;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
//use std::sync::Mutex;
//use std::thread;
//use std::time;
//use std::time::Duration;
//use std::time::{SystemTime, UNIX_EPOCH};
use toml;
//use uuid::parser::Expected::Exact;
use uuid::Uuid;

// there is a problem with this approach since T needs to satisfy PartialEq, Eq for Range
// No such restrictions are needed for Vec
pub enum Sequence<T> {
    Range(Range<T>),
    Vec(Vec<T>),
}

#[derive(Clone)]
enum Schedulers {
    Local(LocalScheduler),
    Distributed(DistributedScheduler),
}

impl Default for Schedulers {
    fn default() -> Schedulers {
        //        let map_output_tracker = MapOutputTracker::new(true, "".to_string(), 0);
        Schedulers::Local(LocalScheduler::new(num_cpus::get(), 20, true))
    }
}

impl Schedulers {
    pub fn run_job<T: Data, U: Data, F, RT>(
        &mut self,
        func: Arc<F>,
        final_rdd: Arc<RT>,
        partitions: Vec<usize>,
        allow_local: bool,
    ) -> Vec<U>
    where
        F: SerFunc((TasKContext, Box<dyn Iterator<Item = T>>)) -> U,
        RT: Rdd<T> + 'static,
    {
        use Schedulers::*;
        match self {
            Distributed(local) => local.run_job(func, final_rdd, partitions, allow_local),
            Local(local) => local.run_job(func, final_rdd, partitions, allow_local),
        }
    }
}

#[derive(Clone, Default)]
pub struct Context {
    next_rdd_id: Arc<AtomicUsize>,
    next_shuffle_id: Arc<AtomicUsize>,
    scheduler: Schedulers,
    address_map: Vec<(String, usize)>,
    distributed_master: bool,
}

#[derive(Deserialize)]
struct Hosts {
    master: String,
    slaves: Vec<String>,
}

impl Context {
    // Sends the binary to all nodes present in hosts.conf and starts them
    pub fn new(mode: &str) -> Result<Self> {
        let next_rdd_id = Arc::new(AtomicUsize::new(0));
        let next_shuffle_id = Arc::new(AtomicUsize::new(0));
        use Schedulers::*;
        //        let slave_string = "slave".to_string();
        match mode {
            "distributed" => {
                //TODO proper command line argument parsing
                let mut port = 10000;
                let args = std::env::args().skip(1).collect::<Vec<_>>();
                //                println!("args {:?}", args);
                let mut address_map: Vec<(String, usize)> = Vec::new();
                match args.get(0).as_ref().map(|arg| &arg[..]) {
                    Some("slave") => {
                        let uuid = Uuid::new_v4().to_string();
                        let _ = CombinedLogger::init(vec![
                            //TermLogger::new(LevelFilter::Info, Config::default(), TerminalMode::Mixed).expect("not able to create term logger"),
                            WriteLogger::new(
                                LevelFilter::Info,
                                Config::default(),
                                File::create(format!("/tmp/executor-{}", uuid))
                                    .expect("not able to create log file"),
                            ),
                        ]);
                        info!("started client");
                        let mut host_file =
                            File::open("hosts.conf").expect("Unable to open the file");
                        let mut hosts = String::new();
                        host_file
                            .read_to_string(&mut hosts)
                            .expect("Unable to read the file");
                        //                        let hosts: Hosts =
                        //                            toml::from_str(&hosts).expect("unable to process the hosts.conf file");
                        let executor = Executor::new(
                            args[1].parse().expect("problem with executor arguments"),
                        );
                        executor.worker();
                        info!("initiated executor worker exit");
                        executor.exit_signal();
                        info!("got executor end signal");
                        std::process::exit(0);
                    }
                    _ => {
                        let uuid = Uuid::new_v4().to_string();
                        let _ = CombinedLogger::init(vec![
                            TermLogger::new(
                                LevelFilter::Info,
                                Config::default(),
                                TerminalMode::Mixed,
                            )
                            .expect("not able to create term logger"),
                            WriteLogger::new(
                                LevelFilter::Info,
                                Config::default(),
                                File::create(format!("/tmp/master-{}", uuid))
                                    .expect("not able to create log file"),
                            ),
                        ]);
                        let mut host_file =
                            File::open("hosts.conf").expect("Unable to open the file");
                        let mut hosts = String::new();
                        host_file
                            .read_to_string(&mut hosts)
                            .expect("Unable to read the file");
                        //                        println!("{:?}", hosts);
                        let hosts: Hosts = toml::from_str(&hosts)
                            .expect("unable to process the hosts.conf file in master");
                        for address in &hosts.slaves {
                            info!("deploying executor at address {:?}", address);
                            let path = std::env::current_exe()
                                .map_err(|_| Error::CurrentBinaryPath)?
                                .into_os_string()
                                .into_string()
                                .expect("couldn't convert os_string to string");
                            //                            let path = path.split(" ").collect::<Vec<_>>();
                            //                            let path = path.join("\\ ");
                            //                            println!("{} {:?} slave", address, path);
                            let address_cli = address
                                .split('@')
                                .nth(1)
                                .expect("format of address is wrong")
                                .to_string();
                            address_map.push((address_cli, port));
                            let local_dir_root = "/tmp";
                            let uuid = Uuid::new_v4();
                            let local_dir_uuid = uuid.to_string();
                            let local_dir =
                                format!("{}/spark-binary-{}", local_dir_root, local_dir_uuid);
                            //                            println!("local binary dir {:?}", local_dir);
                            let mkdir_output = Command::new("ssh")
                                .args(&[address, "mkdir", &local_dir.clone()])
                                .output()
                                .expect("ls command failed to start");
                            //                            println!("mkdir output {:?}", mkdir_output);

                            let binary_name: Vec<_> = path.split('/').collect();
                            let binary_name = binary_name
                                .last()
                                .expect("some problem with executable path");

                            let remote_path = format!("{}:{}/{}", address, local_dir, binary_name);
                            //                            println!("remote dir {}", remote_path);
                            //                            println!("local binary path {}", path);
                            let scp_output = Command::new("scp")
                                .args(&[&path, &remote_path])
                                .output()
                                .expect("ls command failed to start");
                            let path = format!("{}/{}", local_dir, binary_name);
                            info!("remote path {}", path);
                            Command::new("ssh")
                                .args(&[address, &path, &"slave".to_string(), &port.to_string()])
                                .spawn()
                                .expect("ls command failed to start");
                            port += 5000;
                        }
                        Ok(Context {
                            next_rdd_id,
                            next_shuffle_id,
                            scheduler: Distributed(DistributedScheduler::new(
                                4,
                                20,
                                true,
                                Some(address_map.clone()),
                                10000,
                            )),
                            address_map,
                            distributed_master: true,
                        })
                        //TODO handle if master is in another node than from where the program is executed
                        //                        ::std::process::exit(0);
                    }
                }
            }
            "local" => {
                let uuid = Uuid::new_v4().to_string();
                let _ = CombinedLogger::init(vec![
                    TermLogger::new(LevelFilter::Info, Config::default(), TerminalMode::Mixed)
                        .expect("not able to create term logger"),
                    WriteLogger::new(
                        LevelFilter::Info,
                        Config::default(),
                        File::create(format!("/tmp/master-{}", uuid))
                            .expect("not able to create log file"),
                    ),
                ]);
                let scheduler = Local(LocalScheduler::new(num_cpus::get(), 20, true));
                Ok(Context {
                    next_rdd_id,
                    next_shuffle_id,
                    scheduler,
                    address_map: Vec::new(),
                    distributed_master: false,
                })
            }
            _ => {
                let scheduler = Local(LocalScheduler::new(num_cpus::get(), 20, true));
                Ok(Context {
                    next_rdd_id,
                    next_shuffle_id,
                    scheduler,
                    address_map: Vec::new(),
                    distributed_master: false,
                })
            }
        }
    }
    pub fn drop_executors(self) {
        info!("inside context drop in master {}", self.distributed_master);

        for (address, port) in self.address_map.clone() {
            //            while let Err(_) = TcpStream::connect(format!("{}:{}", address, port + 10)) {
            //                continue;
            //            }
            if let Ok(mut stream) = TcpStream::connect(format!("{}:{}", address, port + 10)) {
                let signal = true;
                let signal = bincode::serialize(&signal).unwrap();
                let mut message = ::capnp::message::Builder::new_default();
                let mut task_data = message.init_root::<serialized_data::Builder>();
                task_data.set_msg(&signal);
                serialize_packed::write_message(&mut stream, &message);
            } else {
                error!(
                    "Failed to connect to {}:{} in order to stop its executor",
                    address, port
                );
            }
        }
    }
    pub fn new_rdd_id(&self) -> usize {
        self.next_rdd_id.fetch_add(1, Ordering::SeqCst)
    }
    pub fn new_shuffle_id(&self) -> usize {
        self.next_shuffle_id.fetch_add(1, Ordering::SeqCst)
    }

    // currently it accepts only vector.
    // TODO change this to accept any iterator
    pub fn make_rdd<T: Data>(&self, seq: Vec<T>, num_slices: usize) -> ParallelCollection<T> {
        //let num_slices = seq.len() / num_slices;
        self.parallelize(seq, num_slices)
    }

    pub fn parallelize<T: Data>(&self, seq: Vec<T>, num_slices: usize) -> ParallelCollection<T> {
        ParallelCollection::new(self.clone(), seq, num_slices)
    }

    pub fn run_job<T: Data, U: Data, RT, F>(&mut self, rdd: Arc<RT>, func: F) -> Vec<U>
    where
        F: SerFunc(Box<dyn Iterator<Item = T>>) -> U,
        RT: Rdd<T> + 'static,
    {
        let cl = Fn!([func] move | (task_context, iter) | (*func)(iter));
        let func = Arc::new(cl);
        self.scheduler.run_job(
            func,
            rdd.clone(),
            (0..rdd.number_of_splits()).collect(),
            false,
        )
    }

    pub fn run_job_on_partitions<T: Data, U: Data, RT, F, P>(
        &mut self,
        rdd: Arc<RT>,
        func: F,
        partitions: P,
    ) -> Vec<U>
    where
        F: Fn(Box<dyn Iterator<Item = T>>) -> U
            + Send
            + Sync
            + Clone
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + 'static,
        RT: Rdd<T> + 'static,
        P: IntoIterator<Item = usize>,
    {
        let cl = Fn!([func] move | (task_context, iter) | (*func)(iter));
        self.scheduler
            .run_job(Arc::new(cl), rdd, partitions.into_iter().collect(), false)
    }

    pub fn run_job_with_context<T: Data, U: Data, RT, F>(&mut self, rdd: Arc<RT>, func: F) -> Vec<U>
    where
        F: SerFunc((TasKContext, Box<dyn Iterator<Item = T>>)) -> U,
        RT: Rdd<T> + 'static,
    {
        info!("inside run job in context");
        let func = Arc::new(func);
        self.scheduler.run_job(
            func,
            rdd.clone(),
            (0..rdd.number_of_splits()).collect(),
            false,
        )
    }
}
