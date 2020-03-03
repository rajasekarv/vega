use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::env;
use crate::error::{Error, NetworkError, Result, StdResult};
use crate::serialized_data_capnp::serialized_data;
use crate::task::TaskOption;
use capnp::{
    message::{Builder as MsgBuilder, HeapAllocator, Reader as CpnpReader, ReaderOptions},
    serialize::OwnedSegments,
    serialize_packed,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    stream::StreamExt,
    sync::oneshot::{channel, Receiver, Sender},
};

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

    /// Worker which spawns threads for received tasks, deserializes them,
    /// executes the task and sends the result back to the master.
    ///
    /// This will spawn it's own Tokio runtime to run the tasks on.
    #[allow(clippy::drop_copy)]
    pub fn worker(self: Arc<Self>) -> Result<()> {
        let parallelism = num_cpus::get();
        let executor = env::Env::get_async_handle();
        executor.enter(move || {
            futures::executor::block_on(async move {
                let (send_child, rcv_main) = channel::<Signal>();
                let process_err = Arc::clone(&self).process_stream(rcv_main);
                let handler_err = tokio::spawn(Arc::clone(&self).signal_handler(send_child));
                tokio::select! {
                    err = process_err => err,
                    err = handler_err => err?,
                }
            })
        });
        Err(Error::ExecutorShutdown)
    }

    #[allow(clippy::drop_copy)]
    async fn process_stream(self: Arc<Self>, mut rcv_main: Receiver<Signal>) -> Result<()> {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.port));
        let mut listener = TcpListener::bind(addr)
            .await
            .map_err(NetworkError::TcpListener)?;
        let mut buf: Vec<u8> = Vec::new();
        while let Some(stream) = listener.incoming().next().await {
            if let Ok(stream) = stream {
                if let Ok(Signal::ShutDown) = rcv_main.try_recv() {
                    return Err(Error::ExecutorShutdown);
                }
                buf.clear();
                let mut reader = BufReader::new(stream);
                let signal = reader
                    .read_to_end(&mut buf)
                    .await
                    .map_err(Error::InputRead)?;
                let message_reader = {
                    let mut stream_r = std::io::BufReader::new(&*buf);
                    serialize_packed::read_message(&mut stream_r, CAPNP_BUF_READ_OPTS)
                        .map_err(Error::CapnpDeserialization)
                }?;
                let self_clone = Arc::clone(&self);
                let message = tokio::task::spawn_blocking(move || {
                    let des_task = self_clone.deserialize_task(message_reader)?;
                    self_clone.run_task(des_task)
                })
                .await??;
                serialize_packed::write_message(&mut buf, &message);
                reader.write_all(&*buf).await.map_err(Error::OutputWrite)?;
            }
        }
        Err(Error::ExecutorShutdown)
    }

    #[allow(clippy::drop_copy)]
    fn deserialize_task(
        self: &Arc<Self>,
        message_reader: CpnpReader<OwnedSegments>,
    ) -> Result<TaskOption> {
        let task_data = message_reader
            .get_root::<serialized_data::Reader>()
            .unwrap();
        log::debug!(
            "task in executor {} {} slave task len",
            self.port,
            task_data.get_msg().unwrap().len()
        );
        let port = self.port;
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
        std::mem::drop(task_data);
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
        log::debug!(
            "time taken in server for deserializing:{} {}",
            self.port,
            start.elapsed().as_millis(),
        );
        Ok(des_task)
    }

    fn run_task(self: &Arc<Self>, des_task: TaskOption) -> Result<MsgBuilder<HeapAllocator>> {
        // Run execution + serialization in parallel in the executor threadpool
        let result: Result<Vec<u8>> = {
            let start = Instant::now();
            log::debug!("executing the task from server port {}", self.port);
            //TODO change attempt id from 0 to proper value
            let result = des_task.run(0);
            log::debug!(
                "time taken in server for running:{} {}",
                self.port,
                start.elapsed().as_millis(),
            );
            let start = Instant::now();
            let result = bincode::serialize(&result)?;
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
            Ok(result)
        };

        let mut message = capnp::message::Builder::new_default();
        let mut task_data = message.init_root::<serialized_data::Builder>();
        log::debug!("sending data to master");
        task_data.set_msg(&(result?));
        Ok(message)
    }

    /// A listener for exit signal from master to end the whole slave process.
    async fn signal_handler(self: Arc<Self>, mut send_child: Sender<Signal>) -> Result<()> {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.port + 10));
        let mut listener = TcpListener::bind(addr)
            .await
            .map_err(NetworkError::TcpListener)?;
        let mut buf = Vec::new();
        while let Some(stream) = listener.incoming().next().await {
            log::debug!("inside end signal stream");
            let mut stream = stream.unwrap();
            buf.clear();
            let mut reader = BufReader::new(stream);
            let signal = reader
                .read_to_end(&mut buf)
                .await
                .map_err(Error::InputRead)?;
            let mut stream_r = std::io::BufReader::new(&*buf);
            let signal_data = serialize_packed::read_message(&mut stream_r, CAPNP_BUF_READ_OPTS)?;
            let data = bincode::deserialize::<Signal>(
                signal_data
                    .get_root::<serialized_data::Reader>()?
                    .get_msg()?,
            )?;
            if let Signal::ShutDown = data {
                // signal shut down to the main executor task receiving thread
                log::debug!("received shutdown signal @ {}", self.port);
                send_child.send(Signal::ShutDown);
                break;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::TaskContext;
    use crate::utils::{get_free_port, test_utils::create_test_task};
    use std::net::SocketAddr;

    /// Wait until everything is running
    async fn initialize_exec(signal: bool) -> (std::thread::JoinHandle<Result<()>>, TcpStream) {
        let mut port = get_free_port().unwrap();
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
            let addr: SocketAddr = format!("{}:{}", "0.0.0.0", port).parse().unwrap();
            if let Ok(stream) = TcpStream::connect(addr).await {
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

    fn set_shutdown_signal_msg(buf: &mut Vec<u8>) -> Result<()> {
        let signal = bincode::serialize(&Signal::ShutDown)?;
        let mut message = capnp::message::Builder::new_default();
        let mut msg_data = message.init_root::<serialized_data::Builder>();
        msg_data.set_msg(&signal);
        serialize_packed::write_message(buf, &message);
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn send_shutdown_signal() -> Result<()> {
        let (err, mut stream) = initialize_exec(true).await;
        let mut reader = BufReader::new(stream);

        let mut buf = Vec::new();
        set_shutdown_signal_msg(&mut buf);
        reader.write_all(&*buf).await.map_err(Error::OutputWrite)?;

        assert!(err.join().unwrap().unwrap_err().executor_shutdown());
        Ok(())
    }

    #[tokio::test]
    async fn send_task() -> Result<()> {
        let (_, mut stream) = initialize_exec(false).await;
        let mut reader = BufReader::new(stream);

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
        let mut buf = Vec::new();
        serialize_packed::write_message(&mut buf, &message);
        reader.write_all(&*buf).await.map_err(Error::OutputWrite)?;

        // Get the results back:
        buf.clear();
        let mut stream_r = std::io::BufReader::new(&*buf);
        serialize_packed::read_message(&mut stream_r, CAPNP_BUF_READ_OPTS);

        // Shutdown
        buf.clear();
        set_shutdown_signal_msg(&mut buf);
        reader.write_all(&*buf).await.map_err(Error::OutputWrite)?;
        Ok(())
    }
}
