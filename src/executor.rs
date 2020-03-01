use std::future::Future;
use std::net::{SocketAddr, TcpListener};
use std::pin::Pin;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::env;
use crate::error::{Error, NetworkError, Result, StdResult};
use crate::serialized_data_capnp::serialized_data;
use crate::task::TaskOption;
// use async_std::net::TcpListener;
use capnp::{
    message::{Builder as MsgBuilder, HeapAllocator, Reader as CpnpReader, ReaderOptions},
    serialize::OwnedSegments,
    serialize_packed,
};
//use capnp_futures::serialize;
use futures::{io::BufReader, AsyncReadExt, AsyncWriteExt, FutureExt, StreamExt};
use serde::{Deserialize, Serialize};
use threadpool::ThreadPool;
use tokio::sync::oneshot::{channel, Receiver, Sender};

const CAPNP_BUF_READ_OPTS: ReaderOptions = ReaderOptions {
    traversal_limit_in_words: std::u64::MAX,
    nesting_limit: 64,
};

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
                log::debug!("created server in executor for task {} ", self.port,);
                s
            }
            _ => {
                log::debug!("unable to create server in executor for task {}", self.port);
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
                            log::debug!(
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
                                    log::debug!("got the task in executor",);
                                    s
                                }
                                Err(e) => {
                                    log::debug!("problem in getting the task in executor {:?}", e);
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
                            let result = des_task.run(0);

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

#[derive(Serialize, Deserialize)]
pub(crate) enum Signal {
    ShutDown,
    Continue,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::TaskContext;
    use crate::utils::test_utils::{create_test_task, get_free_port};
    use std::net::TcpStream;

    /// Wait until everything is running
    async fn initialize_exec(signal: bool) -> (std::thread::JoinHandle<Result<()>>, TcpStream) {
        let mut port = get_free_port();
        let err = thread::spawn(move || {
            let executor = Arc::new(Executor::new(port));
            executor.worker()
        });

        let mut i: usize = 0;
        if signal {
            // connect to signal handling port
            port += 10;
        }
        let stream = loop {
            if let Ok(stream) = TcpStream::connect(format!("{}:{}", "0.0.0.0", port)) {
                break stream;
            }
            tokio::time::delay_for(Duration::from_millis(10)).await;
            i += 1;
            if i > 10 {
                panic!("failed openning tcp conn");
            }
        };
        (err, stream)
    }

    #[tokio::test]
    #[ignore]
    async fn send_shutdown_signal() -> Result<()> {
        let (err, mut stream) = initialize_exec(true).await;
        let mut buf = BufReader::new(stream);

        let signal = bincode::serialize(&Signal::ShutDown)?;
        let mut message = capnp::message::Builder::new_default();
        let mut msg_data = message.init_root::<serialized_data::Builder>();
        msg_data.set_msg(&signal);
        serialize::write_message(&mut buf, &message);
        buf.flush().await;

        assert!(err.join().unwrap().unwrap_err().executor_shutdown());
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn send_task() -> Result<()> {
        let (_, mut stream) = initialize_exec(false).await;
        let mut buf = BufReader::new(stream);

        // Mock data:
        let func = Fn!(
            move |(task_context, iter): (TaskContext, Box<dyn Iterator<Item = u8>>)| -> u8 {
                // let iter = iter.collect::<Vec<u8>>();
                // eprintln!("{:?}", iter);
                iter.into_iter().next().unwrap()
            }
        );
        let mock_task: TaskOption = create_test_task(func).into();
        let ser_task = bincode::serialize(&mock_task)?;

        // Send task to executor:
        let mut message = capnp::message::Builder::new_default();
        let mut msg_data = message.init_root::<serialized_data::Builder>();
        msg_data.set_msg(&ser_task);
        serialize::write_message(&mut buf, &message);
        buf.flush().await;

        // Get the results back:
        if let Some(msg) = serialize::read_message(&mut buf, CAPNP_BUF_READ_OPTS).await? {
            assert!(true);
        }
        Ok(())
    }
}
