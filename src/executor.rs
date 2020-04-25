use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::env;
use crate::error::{Error, NetworkError, Result};
use crate::scheduler::TaskOption;
use crate::serialized_data_capnp::serialized_data;
use capnp::{
    message::{Builder as MsgBuilder, HeapAllocator, Reader as CpnpReader, ReaderOptions},
    serialize::OwnedSegments,
};
use capnp_futures::serialize as capnp_serialize;
use crossbeam::{channel::bounded, Receiver, Sender};
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpListener,
    stream::StreamExt,
    task::{spawn, spawn_blocking},
    time::delay_for,
};
use tokio_util::compat::{Tokio02AsyncReadCompatExt, Tokio02AsyncWriteCompatExt};

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
    pub fn worker(self: Arc<Self>) -> Result<Signal> {
        env::Env::run_in_async_rt(move || -> Result<Signal> {
            futures::executor::block_on(async move {
                let (send_child, rcv_main) = bounded::<Signal>(100);
                let process_err = Arc::clone(&self).process_stream(rcv_main);
                let handler_err = spawn(Arc::clone(&self).signal_handler(send_child));
                tokio::select! {
                    err = process_err => err,
                    err = handler_err => err?,
                }
            })
        })
    }

    #[allow(clippy::drop_copy)]
    async fn process_stream(self: Arc<Self>, rcv_main: Receiver<Signal>) -> Result<Signal> {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.port));
        let mut listener = TcpListener::bind(addr)
            .await
            .map_err(NetworkError::TcpListener)?;
        while let Some(Ok(mut stream)) = listener.incoming().next().await {
            let rcv_main = rcv_main.clone();
            let selfc = Arc::clone(&self);
            let res: Result<Signal> = spawn(async move {
                let (reader, writer) = stream.split();
                let reader = reader.compat();
                let mut writer = writer.compat_write();
                match rcv_main.try_recv() {
                    Ok(Signal::ShutDownError) => {
                        log::info!("shutting down executor @{} due to error", selfc.port);
                        return Err(Error::ExecutorShutdown);
                    }
                    Ok(Signal::ShutDownGracefully) => {
                        log::info!("shutting down executor @{} gracefully", selfc.port);
                        return Ok(Signal::ShutDownGracefully);
                    }
                    _ => {}
                }
                log::debug!("received new task @{} executor", selfc.port);
                let message = {
                    let message_reader = capnp_serialize::read_message(reader, CAPNP_BUF_READ_OPTS)
                        .await?
                        .ok_or_else(|| NetworkError::NoMessageReceived)?;
                    spawn_blocking(move || -> Result<_> {
                        let des_task = selfc.deserialize_task(message_reader)?;
                        selfc.run_task(des_task)
                    })
                    .await??
                };
                // TODO: remove blocking call when possible
                futures::executor::block_on(capnp_serialize::write_message(&mut writer, &message))
                    .map_err(Error::CapnpDeserialization)?;
                log::debug!("sent result data to driver");
                Ok(Signal::Continue)
            })
            .await?;
            match res {
                Ok(Signal::Continue) => continue,
                Ok(s) => return Ok(s),
                Err(s) => return Err(s),
            }
        }
        Err(Error::ExecutorShutdown)
    }

    #[allow(clippy::drop_copy)]
    fn deserialize_task(
        self: &Arc<Self>,
        message_reader: CpnpReader<OwnedSegments>,
    ) -> Result<TaskOption> {
        let start = Instant::now();
        let task_data = message_reader.get_root::<serialized_data::Reader>()?;
        let msg = match task_data.get_msg() {
            Ok(s) => {
                log::debug!("got the task message in executor {}", self.port);
                s
            }
            Err(e) => {
                log::debug!("problem while getting the task in executor: {:?}", e);
                return Err(Error::CapnpDeserialization(e));
            }
        };
        log::debug!(
            "deserialized data task @{} executor with {} bytes, took {}ms",
            self.port,
            msg.len(),
            start.elapsed().as_millis()
        );
        std::mem::drop(task_data);
        let start = Instant::now();
        let des_task: TaskOption = bincode::deserialize(&msg)?;
        log::debug!(
            "deserialized task at executor @{} with id #{}, deserialization, took {}ms",
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
            // TODO: change attempt id from 0 to proper value
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
                "time taken @{} executor serializing task #{} result of size {} bytes: {}ms",
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
    async fn signal_handler(self: Arc<Self>, send_child: Sender<Signal>) -> Result<Signal> {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.port + 10));
        log::debug!("signal handler port open @ {}", addr.port());
        let mut listener = TcpListener::bind(addr)
            .await
            .map_err(NetworkError::TcpListener)?;
        let mut signal: Result<Signal> = Err(Error::ExecutorShutdown);
        while let Some(Ok(stream)) = listener.incoming().next().await {
            let stream = stream.compat();
            let signal_data = capnp_serialize::read_message(stream, CAPNP_BUF_READ_OPTS)
                .await?
                .ok_or_else(|| NetworkError::NoMessageReceived)?;
            let data = bincode::deserialize::<Signal>(
                signal_data
                    .get_root::<serialized_data::Reader>()?
                    .get_msg()?,
            )?;
            match data {
                Signal::ShutDownError => {
                    log::info!("received error shutdown signal @ {}", self.port);
                    send_child
                        .send(Signal::ShutDownError)
                        .map_err(|_| Error::Other)?;
                    signal = Err(Error::ExecutorShutdown);
                    break;
                }
                Signal::ShutDownGracefully => {
                    log::info!("received graceful shutdown signal @ {}", self.port);
                    send_child
                        .send(Signal::ShutDownGracefully)
                        .map_err(|_| Error::Other)?;
                    signal = Ok(Signal::ShutDownGracefully);
                    break;
                }
                _ => {}
            }
        }
        // give some time to the executor threads to shut down hopefully
        delay_for(Duration::from_millis(1_000)).await;
        signal
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum Signal {
    ShutDownError,
    ShutDownGracefully,
    Continue,
}

#[cfg(test)]
mod tests {
    #![allow(unused_must_use)]

    use super::*;
    use crate::scheduler::{TaskContext, TaskResult};
    use crate::utils::{get_dynamic_port, test_utils::create_test_task};
    use crate::Fn;
    use crossbeam::channel::{unbounded, Receiver, Sender};
    use std::io::Write;
    use std::thread;
    use std::time::Duration;

    type Port = u16;
    type ComputeResult = std::result::Result<(), ()>;

    fn initialize_exec() -> Arc<Executor> {
        let port = get_dynamic_port();
        Arc::new(Executor::new(port))
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
        Err(Error::Other)
    }

    fn send_shutdown_signal_msg(stream: &mut std::net::TcpStream) -> Result<()> {
        let signal = bincode::serialize(&Signal::ShutDownGracefully)?;
        let mut message = capnp::message::Builder::new_default();
        let mut msg_data = message.init_root::<serialized_data::Builder>();
        msg_data.set_msg(&signal);
        capnp::serialize::write_message(stream, &message).map_err(Error::OutputWrite)?;
        Ok(())
    }

    async fn _start_test<TF, CF>(test_func: TF, checker_func: CF) -> Result<()>
    where
        TF: FnOnce(Receiver<ComputeResult>, Port) -> Result<()> + Send + 'static,
        CF: FnOnce(Sender<ComputeResult>, Result<Signal>) -> Result<()>,
    {
        let executor = initialize_exec();
        let port = executor.port;
        let (send_exec, client_rcv) = unbounded::<ComputeResult>();

        let test_fut = spawn_blocking(move || test_func(client_rcv, port));
        let worker_fut = spawn_blocking(move || executor.worker());
        let (test_res, worker_res) = tokio::join!(test_fut, worker_fut);
        checker_func(send_exec, worker_res?)?;
        test_res?
    }

    #[tokio::test]
    async fn send_shutdown_signal() -> Result<()> {
        fn test(client_rcv: Receiver<ComputeResult>, port: Port) -> Result<()> {
            let end = Instant::now() + Duration::from_millis(150);
            while Instant::now() < end {
                match client_rcv.try_recv() {
                    Ok(Ok(_)) => return Ok(()),
                    Ok(Err(_)) => return Err(Error::Other),
                    _ => {}
                }
                if let Ok(mut stream) = connect_to_executor(port, true) {
                    send_shutdown_signal_msg(&mut stream)?;
                    return Ok(());
                }
                thread::sleep_ms(5);
            }
            Err(Error::Other)
        }

        fn result_checker(sender: Sender<ComputeResult>, result: Result<Signal>) -> Result<()> {
            match result {
                Ok(Signal::ShutDownGracefully) => {
                    sender.send(Ok(()));
                    Ok(())
                }
                Ok(_) | Err(_) => {
                    sender.send(Err(()));
                    Err(Error::Other)
                }
            }
        }

        _start_test(test, result_checker).await
    }

    #[tokio::test]
    async fn send_task() -> Result<()> {
        fn test(client_rcv: Receiver<ComputeResult>, port: Port) -> Result<()> {
            // Mock data:
            let func = Fn!(move |(_task_context, iter): (
                TaskContext,
                Box<dyn Iterator<Item = u8>>
            )|
                  -> u8 {
                // let iter = iter.collect::<Vec<u8>>();
                // eprintln!("{:?}", iter);
                iter.into_iter().next().unwrap()
            });
            let mock_task: TaskOption = create_test_task(func).into();
            let ser_task = bincode::serialize(&mock_task)?;
            let mut message = capnp::message::Builder::new_default();
            let mut msg_data = message.init_root::<serialized_data::Builder>();
            msg_data.set_msg(&ser_task);
            let mut buf = Vec::new();
            capnp::serialize::write_message(&mut buf, &message).map_err(Error::OutputWrite)?;

            let end = Instant::now() + Duration::from_millis(150);
            while Instant::now() < end {
                match client_rcv.try_recv() {
                    Ok(Ok(_)) => return Ok(()),
                    Ok(Err(_)) => return Err(Error::Other),
                    _ => {}
                }
                if let Ok(mut stream) = connect_to_executor(port, false) {
                    // Send task to executor:
                    stream.write_all(&*buf).map_err(Error::OutputWrite)?;
                    if let Ok(Err(_)) = client_rcv.try_recv() {
                        return Err(Error::Other);
                    }

                    // Get the results back:
                    if let Ok(res) =
                        capnp::serialize::read_message(&mut stream, CAPNP_BUF_READ_OPTS)
                    {
                        let task_data = res.get_root::<serialized_data::Reader>().unwrap();

                        match bincode::deserialize::<TaskResult>(&*task_data.get_msg().unwrap())? {
                            TaskResult::ResultTask(_) => {}
                            _ => return Err(Error::DowncastFailure("incorrect task result")),
                        }

                        let mut signal_handler = connect_to_executor(port, true)?;
                        send_shutdown_signal_msg(&mut signal_handler)?;
                        return Ok(());
                    } else {
                        return Err(Error::Other);
                    }
                }
                thread::sleep_ms(5);
            }
            Err(Error::Other)
        };

        fn result_checker(sender: Sender<ComputeResult>, result: Result<Signal>) -> Result<()> {
            match result {
                Ok(Signal::ShutDownGracefully) => Ok(()),
                Ok(_) => {
                    sender.send(Ok(()));
                    Err(Error::Other)
                }
                Err(err) => {
                    sender.send(Err(()));
                    Err(err)
                }
            }
        }

        _start_test(test, result_checker).await
    }
}
