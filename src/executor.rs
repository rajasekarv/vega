use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use crate::env;
use crate::error::{Error, NetworkError, Result, StdResult};
use crate::serialized_data_capnp::serialized_data;
use crate::task::TaskOption;
use capnp::{
    message::{Builder as MsgBuilder, HeapAllocator, Reader as CpnpReader},
    serialize::OwnedSegments,
    serialize_packed,
};
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server,
};
use log::info;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, BufReader},
    net::TcpListener,
    stream::StreamExt,
    sync::mpsc::{channel, Receiver, Sender},
    task,
};

const CONN_ERROR_GUARD: usize = 10;

pub(crate) struct Executor {
    port: u16,
}

impl Executor {
    pub fn new(port: u16) -> Self {
        // let (send_child, rcv_main) = cb_channel::bounded::<Signal>(1);
        Executor { port }
    }

    /// Worker which spawns threads for received tasks, deserializes it and executes the task and send the result back to the master.
    ///
    /// This will spawn it's own Tokio runtime to run tasks on.
    pub fn worker(self: Arc<Self>) -> Result<()> {
        let parallelism = num_cpus::get();
        let (send_child, rcv_main) = channel(1);
        let mut rt = tokio::runtime::Builder::new()
            .enable_all()
            .threaded_scheduler()
            .core_threads(parallelism)
            .build()
            .unwrap();
        Arc::clone(&self).start_exit_signal(send_child)?;
        rt.block_on(self.process_stream(rcv_main))?;
        Err(Error::ExecutorShutdown)
    }

    #[allow(clippy::drop_copy)]
    async fn process_stream(self: Arc<Self>, mut rcv_main: Receiver<Signal>) -> Result<()> {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.port));
        let mut listener = TcpListener::bind(addr)
            .await
            .map_err(NetworkError::TcpListener)?;
        let mut conn_errors = 0usize;
        // TODO profile which way is the most effitient of doing this
        // 1) reuse the same buffer and only await on run task;
        // 2) create a new buffer per stream and deserialize them in parallel;
        //    unlikely to gain much here as the socket is physicallly occupied
        let mut buf: Vec<u8> = Vec::new();
        while let Some(stream) = listener.incoming().next().await {
            match stream {
                Ok(stream) => {
                    if let Ok(Signal::ShutDown) = rcv_main.try_recv() {
                        return Err(Error::ExecutorShutdown);
                    }
                    buf.clear();
                    let mut reader = BufReader::new(stream);
                    let signal = reader
                        .read_to_end(&mut buf)
                        .await
                        .map_err(Error::InputRead)?;
                    let message_reader =
                        task::block_in_place(|| -> Result<CpnpReader<OwnedSegments>> {
                            let r = capnp::message::ReaderOptions {
                                traversal_limit_in_words: std::u64::MAX,
                                nesting_limit: 64,
                            };
                            let mut stream_r = std::io::BufReader::new(&*buf);
                            serialize_packed::read_message(&mut stream_r, r)
                                .map_err(Error::CapnpDeserialization)
                        })?;
                    let message = self.run_task(message_reader).await.unwrap();
                    serialize_packed::write_message(&mut buf, &message);
                }
                Err(_) => {
                    conn_errors += 1;
                    if conn_errors > CONN_ERROR_GUARD {
                        return Err(NetworkError::ConnectionFailure.into());
                    }
                }
            }
        }
        Err(Error::ExecutorShutdown)
    }

    #[allow(clippy::drop_copy)]
    async fn run_task(
        self: &Arc<Self>,
        message_reader: CpnpReader<OwnedSegments>,
    ) -> Result<MsgBuilder<HeapAllocator>> {
        let task_data = message_reader
            .get_root::<serialized_data::Reader>()
            .unwrap();
        log::debug!(
            "task in executor {} {} slave task len",
            self.port,
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
        let des_task: TaskOption = bincode::deserialize(&msg)?;
        log::debug!(
            "task in executor {:?} {} slave task id",
            self.port,
            des_task.get_task_id(),
        );
        std::mem::drop(task_data);
        log::debug!(
            "time taken in server for deserializing:{} {}",
            self.port,
            start.elapsed().as_millis(),
        );
        let start = Instant::now();
        log::debug!("executing the trait from server port {}", self.port);
        //TODO change attempt id from 0 to proper value
        let result = des_task.run(0).await;

        log::debug!(
            "time taken in server for running:{} {}",
            self.port,
            start.elapsed().as_millis(),
        );
        let start = Instant::now();
        let result = bincode::serialize(&result).unwrap();
        log::debug!(
            "task in executor {} {} slave result len",
            self.port,
            result.len()
        );
        log::debug!(
            "time taken in server for serializing:{} {}",
            self.port,
            start.elapsed().as_millis(),
        );
        let mut message = capnp::message::Builder::new_default();
        let mut task_data = message.init_root::<serialized_data::Builder>();
        log::debug!("sending data to master");
        task_data.set_msg(&result);
        Ok(message)
    }

    /// A listener for exit signal from master to end the whole slave process.
    fn start_exit_signal(self: Arc<Self>, send_child: Sender<Signal>) -> Result<()> {
        let thread =
            thread::Builder::new().name(format!("{}_exec_signal_handler", env::THREAD_PREFIX));
        thread
            .spawn(move || -> Result<()> {
                let mut rt = tokio::runtime::Builder::new()
                    .enable_all()
                    .basic_scheduler()
                    .core_threads(1)
                    .thread_stack_size(1024)
                    .build()
                    .map_err(|_| Error::AsyncRuntimeError)?;
                rt.block_on(self.signal_handler(send_child))?;
                Err(Error::AsyncRuntimeError)
            })
            .unwrap()
            .join()
            .unwrap()
    }

    async fn signal_handler(self: Arc<Self>, mut send_child: Sender<Signal>) -> Result<()> {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.port + 10));
        let mut listener = TcpListener::bind(addr)
            .await
            .map_err(NetworkError::TcpListener)?;
        let mut buf: Vec<u8> = Vec::new();
        let mut conn_errors = 0usize;
        while let Some(stream) = listener.incoming().next().await {
            log::debug!("inside end signal stream");
            match stream {
                Ok(stream) => {
                    buf.clear();
                    let mut reader = BufReader::new(stream);
                    let signal = reader
                        .read_to_end(&mut buf)
                        .await
                        .map_err(Error::InputRead)?;
                    if let Signal::ShutDown = bincode::deserialize::<Signal>(&buf)? {
                        // signal shut down to the main executor task receiving thread
                        send_child.send(Signal::ShutDown);
                        return Err(Error::ExecutorShutdown);
                    }
                }
                Err(_) => {
                    conn_errors += 1;
                    if conn_errors > CONN_ERROR_GUARD {
                        return Err(NetworkError::ConnectionFailure.into());
                    }
                }
            }
        }
        Err(Error::ExecutorShutdown)
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) enum Signal {
    ShutDown,
    Continue,
}
