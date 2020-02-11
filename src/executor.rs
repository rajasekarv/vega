use crate::serialized_data_capnp::serialized_data;
use crate::task::TaskOption;
use capnp::serialize_packed;
use log::info;
use std::future::Future;
use std::net::TcpListener;
use std::pin::Pin;
use std::thread;
use std::time::Instant;
use threadpool::ThreadPool;

pub(crate) struct Executor {
    port: u16,
}

impl Executor {
    pub fn new(port: u16) -> Self {
        Executor { port }
    }

    /// Worker which spawns threads for received tasks, deserializes it and executes the task and send the result back to the master.
    ///
    /// This will spawn it's own Tokio runtime to run tasks on.
    #[allow(clippy::drop_copy)]
    pub fn worker(&self) -> Result<(), ()> {
        let listener = match TcpListener::bind(format!("0.0.0.0:{}", self.port,)) {
            Ok(s) => {
                log::debug!("created server in executor for task {} ", self.port,);
                s
            }
            _ => {
                log::debug!("unable to create server in executor for task {}", self.port);
                return Err(());
            }
        };

        let server_port = self.port;
        let parallelism = num_cpus::get();
        let mut rt = tokio::runtime::Builder::new()
            .enable_all()
            .threaded_scheduler()
            .core_threads(parallelism)
            .build()
            .unwrap();
        let process_stream: Pin<Box<dyn Future<Output = Result<(), ()>>>> = Box::pin(async move {
            for stream in listener.incoming() {
                match stream {
                    Err(_) => continue,
                    Ok(mut stream) => {
                        async move {
                            // TODO reduce the redundant copies
                            let r = ::capnp::message::ReaderOptions {
                                traversal_limit_in_words: std::u64::MAX,
                                nesting_limit: 64,
                            };
                            let mut stream_r = std::io::BufReader::new(&mut stream);
                            let message_reader =
                                match serialize_packed::read_message(&mut stream_r, r) {
                                    Ok(s) => s,
                                    Err(_) => return,
                                };
                            let task_data = message_reader
                                .get_root::<serialized_data::Reader>()
                                .unwrap();
                            log::debug!(
                                "task in executor {} {} slave task len",
                                server_port,
                                task_data.get_msg().unwrap().len()
                            );
                            let start = Instant::now();
                            // let local_dir_root = "/tmp";
                            // let uuid = Uuid::new_v4();
                            // let local_dir_uuid = uuid.to_string();
                            // let local_dir_path =
                            //     format!("{}/spark-task-{}", local_dir_root, local_dir_uuid);
                            // let local_dir = fs::create_dir_all(local_dir_path.clone()).unwrap();
                            // let task_dir_path =
                            //     format!("{}/spark-task-{}/task", local_dir_root, local_dir_uuid);
                            // let mut f = fs::File::create(task_dir_path.clone()).unwrap();
                            let msg = match task_data.get_msg() {
                                Ok(s) => {
                                    log::debug!("got the task in executor",);
                                    s
                                }
                                Err(e) => {
                                    log::debug!("problem in getting the task in executor {:?}", e);
                                    std::process::exit(0);
                                }
                            };
                            // f.write(msg);
                            // let f = fs::File::open(task_dir_path.clone()).unwrap();
                            // let mut f = fs::File::open(task_dir_path).unwrap();
                            // let mut buffer = vec![0; msg.len()];
                            // f.read(&mut buffer).unwrap();
                            let des_task: TaskOption = match bincode::deserialize(&msg) {
                                Ok(s) => {
                                    log::debug!("serialized the task in executor",);
                                    s
                                }
                                Err(e) => {
                                    log::debug!(
                                        "problem in serializing the task in executor {:?}",
                                        e
                                    );
                                    std::process::exit(0);
                                }
                            };
                            log::debug!(
                                "task in executor {:?} {} slave task id",
                                server_port,
                                des_task.get_task_id(),
                            );
                            std::mem::drop(task_data);
                            log::debug!(
                                "time taken in server for deserializing:{} {}",
                                server_port,
                                start.elapsed().as_millis(),
                            );
                            let start = Instant::now();
                            log::debug!("executing the trait from server port {}", server_port);
                            //TODO change attempt id from 0 to proper value
                            let result = des_task.run(0).await;

                            log::debug!(
                                "time taken in server for running:{} {}",
                                server_port,
                                start.elapsed().as_millis(),
                            );
                            let start = Instant::now();
                            let result = bincode::serialize(&result).unwrap();
                            log::debug!(
                                "task in executor {} {} slave result len",
                                server_port,
                                result.len()
                            );
                            log::debug!(
                                "time taken in server for serializing:{} {}",
                                server_port,
                                start.elapsed().as_millis(),
                            );
                            let mut message = ::capnp::message::Builder::new_default();
                            let mut task_data = message.init_root::<serialized_data::Builder>();
                            log::debug!("sending data to master");
                            task_data.set_msg(&result);
                            serialize_packed::write_message(&mut stream, &message);
                        }
                        .await;
                    }
                }
            }
            // This should be unreachable as long as the executor keeps running.
            Err(())
        });
        rt.block_on(process_stream)
    }

    // A thread listening for exit signal from master to end the whole slave process
    pub fn exit_signal(&self) {
        let listener = match TcpListener::bind(format!("0.0.0.0:{}", self.port + 10,)) {
            Ok(s) => {
                log::debug!(
                    "created end signal server in executor for task {} ",
                    self.port + 10,
                );
                s
            }
            _ => {
                log::debug!(
                    "unable to create end signal server in executor for task {}",
                    self.port + 10
                );
                return;
            }
        };
        for stream in listener.incoming() {
            match stream {
                Err(_) => continue,
                Ok(mut stream) => {
                    log::debug!("inside end signal stream");
                    let r = ::capnp::message::ReaderOptions {
                        traversal_limit_in_words: std::u64::MAX,
                        nesting_limit: 64,
                    };
                    let mut stream_r = std::io::BufReader::new(&mut stream);
                    let message_reader = match serialize_packed::read_message(&mut stream_r, r) {
                        Ok(s) => s,
                        Err(_) => return,
                    };
                    let signal_data = message_reader
                        .get_root::<serialized_data::Reader>()
                        .unwrap();
                    log::debug!("got end signal inside server");
                    let signal: bool =
                        bincode::deserialize(signal_data.get_msg().unwrap()).unwrap();
                    if signal {
                        return;
                    }
                }
            }
        }
    }
}
