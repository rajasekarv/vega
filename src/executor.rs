use super::*;
use capnp::serialize_packed;
use std::net::TcpListener;
use std::thread;
use std::time::Instant;
use threadpool::ThreadPool;

pub struct Executor {
    port: u16,
}

impl Executor {
    pub fn new(port: u16) -> Self {
        Executor { port }
    }

    // Worker which spawns threads for received tasks, deserializes it and executes the task and send the result back to the master.
    // Try to pin the threads to particular core of the machine to avoid unnecessary cache misses.
    #[allow(clippy::drop_copy)]
    pub fn worker(&self) {
        let listener = match TcpListener::bind(format!("0.0.0.0:{}", self.port,)) {
            Ok(s) => {
                info!("created server in executor for task {} ", self.port,);
                s
            }
            _ => {
                info!("unable to create server in executor for task {}", self.port);
                return;
            }
        };
        let server_port = self.port;

        thread::spawn(move || {
            //TODO change hardcoded value to number of cores in the system
            let threads = num_cpus::get();
            let thread_pool = ThreadPool::new(threads);
            for stream in listener.incoming() {
                match stream {
                    Err(_) => continue,
                    Ok(mut stream) => {
                        thread_pool.execute(move || {
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
                            info!(
                                "task in executor {} {} slave task len",
                                server_port,
                                task_data.get_msg().unwrap().len()
                            );
                            let start = Instant::now();
                            //                            let local_dir_root = "/tmp";
                            //                            let uuid = Uuid::new_v4();
                            //                            let local_dir_uuid = uuid.to_string();
                            //                            let local_dir_path =
                            //                                format!("{}/spark-task-{}", local_dir_root, local_dir_uuid);
                            //                            let local_dir = fs::create_dir_all(local_dir_path.clone()).unwrap();
                            //                            let task_dir_path =
                            //                                format!("{}/spark-task-{}/task", local_dir_root, local_dir_uuid);
                            //                            let mut f = fs::File::create(task_dir_path.clone()).unwrap();
                            let msg = match task_data.get_msg() {
                                Ok(s) => {
                                    info!("got the task in executor",);
                                    s
                                }
                                Err(e) => {
                                    info!("problem in getting the task in executor {:?}", e);
                                    std::process::exit(0);
                                }
                            };
                            //                            f.write(msg);
                            //                            let f = fs::File::open(task_dir_path.clone()).unwrap();
                            //                            let mut f = fs::File::open(task_dir_path).unwrap();
                            //                            let mut buffer = vec![0; msg.len()];

                            //                            f.read(&mut buffer).unwrap();
                            let des_task: TaskOption = match bincode::deserialize(&msg) {
                                Ok(s) => {
                                    info!("serialized the task in executor",);
                                    s
                                }
                                Err(e) => {
                                    info!("problem in serializing the task in executor {:?}", e);
                                    std::process::exit(0);
                                }
                            };
                            info!(
                                "task in executor {:?} {} slave task id",
                                server_port,
                                des_task.get_task_id(),
                            );
                            std::mem::drop(task_data);
                            info!(
                                "time taken in server for deserializing:{} {}",
                                server_port,
                                start.elapsed().as_millis(),
                            );
                            let start = Instant::now();
                            info!("executing the trait from server port {}", server_port);
                            //TODO change attempt id from 0 to proper value
                            let result = des_task.run(0);

                            info!(
                                "time taken in server for running:{} {}",
                                server_port,
                                start.elapsed().as_millis(),
                            );
                            let start = Instant::now();
                            let result = bincode::serialize(&result).unwrap();
                            info!(
                                "task in executor {} {} slave result len",
                                server_port,
                                result.len()
                            );
                            info!(
                                "time taken in server for serializing:{} {}",
                                server_port,
                                start.elapsed().as_millis(),
                            );
                            let mut message = ::capnp::message::Builder::new_default();
                            let mut task_data = message.init_root::<serialized_data::Builder>();
                            info!("sending data to master");
                            task_data.set_msg(&result);
                            serialize_packed::write_message(&mut stream, &message);
                        });
                    }
                }
            }
        });
    }

    // A thread listening for exit signal from master to end the whole slave process
    pub fn exit_signal(&self) {
        let listener = match TcpListener::bind(format!("0.0.0.0:{}", self.port + 10,)) {
            Ok(s) => {
                info!(
                    "created end signal server in executor for task {} ",
                    self.port + 10,
                );
                s
            }
            _ => {
                info!(
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
                    info!("inside end signal stream");
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
                    info!("got end signal inside server");
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
