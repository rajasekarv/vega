use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use crate::env;
use crate::error::{Error, NetworkError, Result};
use crate::serialized_data_capnp::serialized_data;
use crate::task::TaskOption;
use crate::utils::get_message_size;
use capnp::{
    message::{Builder as MsgBuilder, HeapAllocator, Reader as CpnpReader, ReaderOptions},
    serialize::OwnedSegments,
    serialize_packed,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    stream::StreamExt,
    sync::oneshot::{channel, Receiver, Sender},
};

const CAPNP_BUF_READ_OPTS: ReaderOptions = ReaderOptions {
    traversal_limit_in_words: std::u64::MAX,
    nesting_limit: 64,
};

pub(crate) struct Executor {
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
        let executor = env::Env::get_async_handle();
        executor.enter(move || -> Result<()> {
            futures::executor::block_on(async move {
                let (send_child, rcv_main) = channel::<Signal>();
                let process_err = Arc::clone(&self).process_stream(rcv_main);
                let handler_err = tokio::spawn(Arc::clone(&self).signal_handler(send_child));
                tokio::select! {
                    err = process_err => err,
                    err = handler_err => err?,
                }
            })
        })?;
        Err(Error::ExecutorShutdown)
    }

    #[allow(clippy::drop_copy)]
    async fn process_stream(self: Arc<Self>, mut rcv_main: Receiver<Signal>) -> Result<()> {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.port));
        let mut listener = TcpListener::bind(addr)
            .await
            .map_err(NetworkError::TcpListener)?;
        while let Some(stream) = listener.incoming().next().await {
            if let Ok(mut stream) = stream {
                if let Ok(Signal::ShutDown) = rcv_main.try_recv() {
                    return Err(Error::ExecutorShutdown);
                }
                let self_clone = Arc::clone(&self);
                log::debug!("received new task @{} executor", self.port);
                tokio::spawn(async move {
                    log::debug!("inside executor tp running task");
                    let (mut receiver, mut writter) = stream.split();

                    let msg_size = get_message_size(&mut receiver).await?;
                    log::debug!("receiving task of {} bytes", msg_size);
                    let mut buf: Vec<u8> = vec![0; msg_size];
                    let message = {
                        receiver
                            .read_exact(&mut buf)
                            .await
                            .map_err(Error::InputRead)?;
                        log::debug!(
                            "read {} bytes from stream @{} exec",
                            buf.len(),
                            self_clone.port
                        );
                        let message_reader = {
                            let mut stream_r = std::io::BufReader::new(&*buf);
                            serialize_packed::read_message(&mut stream_r, CAPNP_BUF_READ_OPTS)
                                .map_err(Error::CapnpDeserialization)
                        }?;

                        let des_task = self_clone.deserialize_task(message_reader)?;
                        self_clone.run_task(des_task)
                    }?;
                    buf.clear();
                    serialize_packed::write_message(&mut buf, &message)
                        .map_err(Error::OutputWrite)?;

                    // send outgoing message size to executor
                    let msg_len = u64::to_le_bytes(buf.len() as u64);
                    writter
                        .write_all(&msg_len)
                        .await
                        .map_err(Error::OutputWrite)
                        .unwrap();
                    log::debug!(
                        "sending response bytes ({}) to driver from @{}",
                        buf.len(),
                        self_clone.port
                    );

                    // send the message
                    writter.write_all(&*buf).await.map_err(Error::OutputWrite)?;
                    log::debug!("sent data to driver");

                    Ok::<(), Error>(())
                });
            }
            tokio::task::yield_now().await;
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
            "deserialized task @{} executor with {} bytes",
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
                log::debug!("got the task message in executor {}", self.port);
                s
            }
            Err(e) => {
                log::debug!("problem while getting the task in executor: {:?}", e);
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
            "deserialized task at executor @{} with id {}, deserialization took {}ms",
            self.port,
            des_task.get_task_id(),
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
                "time taken @{} executor running task #{}: {}ms",
                self.port,
                des_task.get_task_id(),
                start.elapsed().as_millis(),
            );
            let start = Instant::now();
            let result = bincode::serialize(&result)?;
            log::debug!(
                "time taken @{} executor serializing task #{} result of size {}: {}ms",
                self.port,
                des_task.get_task_id(),
                result.len(),
                start.elapsed().as_millis(),
            );
            Ok(result)
        };

        let mut message = capnp::message::Builder::new_default();
        let mut task_data = message.init_root::<serialized_data::Builder>();
        task_data.set_msg(&(result?));
        Ok(message)
    }

    /// A listener for exit signal from master to end the whole slave process.
    async fn signal_handler(self: Arc<Self>, send_child: Sender<Signal>) -> Result<()> {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.port + 10));
        log::debug!("signal handler port open @ {}", addr.port());
        let mut listener = TcpListener::bind(addr)
            .await
            .map_err(NetworkError::TcpListener)?;
        let mut buf = Vec::new();
        while let Some(stream) = listener.incoming().next().await {
            let stream = stream.unwrap();
            buf.clear();
            let mut reader = BufReader::new(stream);
            reader
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
                send_child
                    .send(Signal::ShutDown)
                    .map_err(|_| Error::AsyncRuntimeError)?;
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
    use std::io::Write;
    use std::thread;
    use tokio::sync::mpsc::{
        unbounded_channel, UnboundedReceiver as Receiver, UnboundedSender as Sender,
    };

    type Port = u16;
    type ComputeResult = std::result::Result<(), ()>;

    fn initialize_exec() -> (Arc<Executor>, Port) {
        let port = get_free_port().unwrap();
        (Arc::new(Executor::new(port)), port)
    }

    fn connect_to_executor(mut port: u16, signal_handler: bool) -> Result<std::net::TcpStream> {
        use std::net::TcpStream;

        let mut i: usize = 0;
        if signal_handler {
            // connect to signal handling port
            port += 10;
        }

        loop {
            let addr: SocketAddr = format!("{}:{}", "0.0.0.0", port).parse().unwrap();
            if let Ok(stream) = TcpStream::connect(addr) {
                return Ok(stream);
            }
            thread::sleep_ms(10);
            i += 1;
            if i > 10 {
                break;
            }
        }
        Err(Error::AsyncRuntimeError)
    }

    fn _start_test<TF, CF, R>(test_func: TF, checker_func: CF) -> Result<R>
    where
        TF: FnOnce(Receiver<ComputeResult>) -> Result<()> + Send + 'static,
        CF: FnOnce(Sender<ComputeResult>) -> Result<R>,
    {
        let (send_exec, client_rcv) = unbounded_channel::<ComputeResult>();
        thread::spawn(move || test_func(client_rcv));
        checker_func(send_exec)
    }

    fn set_shutdown_signal_msg(buf: &mut Vec<u8>) -> Result<()> {
        let signal = bincode::serialize(&Signal::ShutDown)?;
        let mut message = capnp::message::Builder::new_default();
        let mut msg_data = message.init_root::<serialized_data::Builder>();
        msg_data.set_msg(&signal);
        serialize_packed::write_message(buf, &message).map_err(Error::OutputWrite)?;
        Ok(())
    }

    #[test]
    fn send_shutdown_signal() -> Result<()> {
        let (executor, port) = initialize_exec();

        let test = move |mut client_rcv: Receiver<ComputeResult>| -> Result<()> {
            let mut buf = Vec::new();
            set_shutdown_signal_msg(&mut buf)?;
            loop {
                thread::sleep_ms(5);
                if let Ok(mut stream) = connect_to_executor(port, true) {
                    stream.write_all(&*buf).map_err(Error::OutputWrite)?;
                    stream.flush().map_err(Error::OutputWrite)?;
                    match client_rcv.try_recv() {
                        Ok(Ok(_)) => return Ok(()),
                        Ok(Err(_)) => return Err(Error::AsyncRuntimeError),
                        _ => {}
                    }
                }
            }
        };

        let result_checker = |sender: Sender<ComputeResult>| -> Result<()> {
            let result = match executor.worker() {
                Err(Error::ExecutorShutdown) => Ok(()),
                Err(err) => Err(err),
                _ => Err(Error::AsyncRuntimeError),
            };
            sender.send(Ok(())).map_err(|_| Error::AsyncRuntimeError)?;
            result
        };

        _start_test(test, result_checker)
    }

    #[test]
    #[ignore] // requires fixing
    fn send_task() -> Result<()> {
        let (executor, port) = initialize_exec();

        let test = move |mut client_rcv: Receiver<ComputeResult>| -> Result<()> {
            // Mock data:
            let func =
                Fn!(
                    move |(task_context, iter): (TaskContext, Box<dyn Iterator<Item = u8>>)| -> u8 {
                        // let iter = iter.collect::<Vec<u8>>();
                        // eprintln!("{:?}", iter);
                        iter.into_iter().next().unwrap()
                    }
                );
            let mock_task: TaskOption = create_test_task(func).into();
            let ser_task = bincode::serialize(&mock_task)?;
            let mut message = capnp::message::Builder::new_default();
            let mut msg_data = message.init_root::<serialized_data::Builder>();
            msg_data.set_msg(&ser_task);
            let mut buf = Vec::new();
            serialize_packed::write_message(&mut buf, &message).map_err(Error::OutputWrite)?;

            loop {
                thread::sleep_ms(5);
                if let Ok(mut stream) = connect_to_executor(port, false) {
                    // Send task to executor:
                    buf.clear();
                    stream.write_all(&*buf).map_err(Error::OutputWrite)?;
                    stream.flush().map_err(Error::OutputWrite)?;
                    if let Ok(Err(_)) = client_rcv.try_recv() {
                        return Err(Error::AsyncRuntimeError);
                    }
                    // Get the results back:
                    // buf.clear();
                    // stream.read_to_end(&mut buf);
                    // let mut stream_r = std::io::BufReader::new(&*buf);
                    // if let Ok(res) =
                    //     serialize_packed::read_message(&mut stream_r, CAPNP_BUF_READ_OPTS)
                    // {
                    //     let task_data = res.get_root::<serialized_data::Reader>().unwrap();
                    //     let des_task: TaskResult =
                    //         bincode::deserialize(&*task_data.get_msg().unwrap())?;

                    //     if let Ok(mut stream) = connect_to_executor(port, true) {
                    //         buf.clear();
                    //         set_shutdown_signal_msg(&mut buf);
                    //         stream.write_all(&*buf).map_err(Error::OutputWrite)?;
                    //         stream.flush();
                    //     }
                    // }
                }
            }
        };

        let result_checker = |sender: Sender<ComputeResult>| -> Result<()> {
            let result = match executor.worker() {
                Err(Error::ExecutorShutdown) => Ok(()),
                Err(err) => Err(err),
                _ => Err(Error::AsyncRuntimeError),
            };
            if result.is_err() {
                sender.send(Err(())).map_err(|_| Error::AsyncRuntimeError)?;
            } else {
                sender.send(Ok(())).map_err(|_| Error::AsyncRuntimeError)?;
            }
            result
        };

        _start_test(test, result_checker)
    }
}
