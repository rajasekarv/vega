use std::collections::HashMap;
use std::convert::TryFrom;
use std::future::Future;
use std::io::Read;
use std::pin::Pin;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;
use std::sync::{atomic, atomic::AtomicBool, Arc};

use crate::context::Context;
use crate::env;
use crate::error::StdResult;
use crate::serializable_traits::Data;
use crate::shuffle::*;
use futures::future;
use hyper::{
    client::Client, server::conn::AddrIncoming, service::Service, Body, Request, Response, Server,
    StatusCode, Uri,
};
use log::info;
use parking_lot::Mutex;
use threadpool::ThreadPool;

/// Parallel shuffle fetcher.
pub(crate) struct ShuffleFetcher;

impl ShuffleFetcher {
    pub async fn fetch_2<K: Data, V: Data>(
        sc: Arc<Context>,
        shuffle_id: usize,
        reduce_id: usize,
        mut func: impl FnMut((K, V)) -> (),
    ) -> Result<()> {
        use tokio::sync::mpsc;
        use tokio::sync::Mutex;

        log::debug!("inside fetch function");
        let mut inputs_by_uri = HashMap::new();
        let server_uris = env::Env::get()
            .map_output_tracker
            .get_server_uris(shuffle_id);
        log::debug!(
            "server uris for shuffle id {:?} - {:?}",
            shuffle_id,
            server_uris
        );
        for (index, server_uri) in server_uris.into_iter().enumerate() {
            inputs_by_uri
                .entry(server_uri)
                .or_insert_with(Vec::new)
                .push(index);
        }
        let mut server_queue = Vec::new();
        let mut total_results = 0;
        for (key, value) in inputs_by_uri {
            total_results += value.len();
            server_queue.push((key, value));
        }
        log::debug!(
            "servers for shuffle id {:?}, reduce id {:?} - {:?}",
            shuffle_id,
            reduce_id,
            server_queue
        );
        let num_tasks = server_queue.len();
        let server_queue = Arc::new(Mutex::new(server_queue));
        let failure = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::with_capacity(num_tasks);
        for _ in 0..num_tasks {
            let server_queue = server_queue.clone();
            let failure = failure.clone();
            // spawn a future for each expected result set
            let task = async move {
                let client = Client::builder().http2_only(true).build_http::<Body>();
                let mut lock = server_queue.lock().await;
                if let Some((server_uri, input_ids)) = lock.pop() {
                    let mut server_uri = format!("{}/shuffle/{}", server_uri, shuffle_id);
                    let mut chunk_uri_str = String::with_capacity(server_uri.len() + 12);
                    chunk_uri_str.push_str(&server_uri);
                    let mut shuffle_chunks = Vec::with_capacity(input_ids.len());
                    for input_id in input_ids {
                        if failure.load(atomic::Ordering::Acquire) {
                            // Abort early since the work failed in an other future
                            return Err(ShuffleError::AsyncRuntimeError);
                        }
                        log::debug!("inside parallel fetch {}", input_id);
                        let chunk_uri = ShuffleFetcher::make_chunk_uri(
                            &server_uri,
                            &mut chunk_uri_str,
                            input_id,
                            reduce_id,
                        )?;
                        let data = {
                            let res = client.get(chunk_uri).await?;
                            hyper::body::to_bytes(res.into_body()).await
                        };
                        if let Ok(data) = data {
                            shuffle_chunks.push(data.to_vec());
                        } else {
                            failure.store(true, atomic::Ordering::Release);
                            return Err(ShuffleError::FailedFetchOp);
                        }
                    }
                    Ok(shuffle_chunks)
                } else {
                    Ok(Vec::new())
                }
            };
            // spawning is required so tasks are run in parallel in the tokio tp
            tasks.push(tokio::spawn(task));
        }
        let task_results = future::join_all(tasks.into_iter()).await;
        log::debug!("total_results {}", total_results);
        // TODO: make this pure; instead of modifying the compute results inside the passing closure
        // return the deserialized values iterator to the caller
        task_results
            .into_iter()
            .map(|join_res| {
                if let Ok(results) = join_res {
                    for res_set in &results {
                        res_set
                            .iter()
                            .map(|bytes| {
                                let set = bincode::deserialize::<Vec<(K, V)>>(&bytes)?;
                                set.into_iter().for_each(|kv| func(kv));
                                Ok(())
                            })
                            .fold(
                                Ok(()),
                                |curr: Result<()>, res| if res.is_err() { res } else { curr },
                            )?
                    }
                    Ok(())
                } else {
                    Err(ShuffleError::AsyncRuntimeError)
                }
            })
            .fold(Ok(()), |curr, res| if res.is_err() { res } else { curr })
    }

    fn make_chunk_uri(
        base: &str,
        chunk: &mut String,
        input_id: usize,
        reduce_id: usize,
    ) -> Result<Uri> {
        let input_id = input_id.to_string();
        let reduce_id = reduce_id.to_string();
        let path_tail = ["/".to_string(), input_id, "/".to_string(), reduce_id].concat();
        if chunk.len() == base.len() {
            chunk.push_str(&path_tail);
        } else {
            //chunk.drain((chunk.len() - base.len())..);
            chunk.replace_range(base.len().., &path_tail);
        }
        Ok(Uri::try_from(chunk.as_str())?)
    }

    pub fn fetch<K: Data, V: Data>(
        &self,
        sc: Arc<Context>,
        shuffle_id: usize,
        reduce_id: usize,
        mut func: impl FnMut((K, V)) -> (),
    ) {
        log::debug!("inside fetch function");
        let parallel_fetches = 10; //TODO  make this as env variable
        let thread_pool = ThreadPool::new(parallel_fetches);
        let mut inputs_by_uri = HashMap::new();
        let server_uris = env::Env::get()
            .map_output_tracker
            .get_server_uris(shuffle_id);
        log::debug!(
            "server uris for shuffle id {:?} - {:?}",
            shuffle_id,
            server_uris
        );
        for (index, server_uri) in server_uris.into_iter().enumerate() {
            inputs_by_uri
                .entry(server_uri)
                .or_insert_with(Vec::new)
                .push(index);
        }
        let server_queue = Arc::new(Mutex::new(Vec::new()));
        let total_results: usize = inputs_by_uri.iter().map(|(_, v)| v.len()).sum();
        for (key, value) in inputs_by_uri {
            server_queue.lock().push((key, value));
        }
        log::debug!(
            "servers for shuffle id {:?}, reduce id {:?} - {:?}",
            shuffle_id,
            reduce_id,
            server_queue
        );
        let (producer, consumer) = channel();
        let failure = Arc::new(Mutex::new(None));
        let sent_count = Arc::new(Mutex::new(0));
        for i in 0..parallel_fetches {
            log::debug!("inside parallel fetch {}", i);
            let server_queue = server_queue.clone();
            let producer = producer.clone();
            let failure = failure.clone();
            let sent_count_clone = sent_count.clone();

            thread_pool.execute(move || {
                while let Some((server_uri, input_ids)) = server_queue.lock().pop() {
                    for i in input_ids {
                        if !failure.lock().is_none() {
                            return;
                        }
                        let url =
                            format!("{}/shuffle/{}/{}/{}", server_uri, shuffle_id, i, reduce_id);
                        //TODO logging
                        fn f(
                            producer: Sender<(Vec<u8>, String)>,
                            url: String,
                        ) -> std::result::Result<(), Box<dyn std::error::Error>>
                        {
                            let mut res = reqwest::get(&url)?;
                            let len = &res.content_length();
                            let mut body = vec![0; len.unwrap() as usize];
                            res.read_exact(&mut body)?;
                            producer.send((body, url))?;
                            Ok(())
                        }
                        let producer_clone = producer.clone();
                        f(producer_clone, url).map_err(|e|
                                //TODO change e to FailedFetchException
                                *failure.lock() = Some(e.to_string()));
                        *sent_count_clone.lock() += 1;
                        log::debug!(
                            "total results {} results sent {:?}",
                            total_results,
                            sent_count_clone
                        );
                        log::debug!(
                            "total results {} results sent {:?}",
                            total_results,
                            sent_count_clone
                        );
                    }
                }
            })
        }
        log::debug!("total_results {}", total_results);

        let mut results_done = 0;
        while failure.lock().is_none() && (results_done < total_results) {
            let (result, url) = consumer.recv().unwrap();
            log::debug!(
                "total results {} results done {}",
                total_results,
                results_done
            );
            log::debug!("received from consumer");
            let input: Vec<(K, V)> =
                bincode::deserialize(&result).expect("not able to serialize fetched data");
            for (k, v) in input {
                func((k, v));
            }
            results_done += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fetch() {
        let data = ShuffleFetcher;
    }

    #[test]
    fn build_shuffle_id_uri() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
        let base = "http://127.0.0.1/shuffle";
        let mut chunk = base.to_owned();

        let uri0 = ShuffleFetcher::make_chunk_uri(base, &mut chunk, 0, 1)?;
        let expected = format!("{}/0/1", base);
        assert_eq!(expected.as_str(), uri0);

        let uri1 = ShuffleFetcher::make_chunk_uri(base, &mut chunk, 123, 123)?;
        let expected = format!("{}/123/123", base);
        assert_eq!(expected.as_str(), uri1);

        Ok(())
    }
}
