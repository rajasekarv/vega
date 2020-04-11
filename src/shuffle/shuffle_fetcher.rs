use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::{atomic, atomic::AtomicBool, Arc};

use crate::env;
use crate::serializable_traits::Data;
use crate::shuffle::*;
use futures::future;
use hyper::{client::Client, Uri};
use tokio::sync::Mutex;

/// Parallel shuffle fetcher.
pub(crate) struct ShuffleFetcher;

impl ShuffleFetcher {
    pub async fn fetch<K: Data, V: Data>(
        shuffle_id: usize,
        reduce_id: usize,
        mut func: impl FnMut((K, V)) -> (),
    ) -> Result<()> {
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
                    let server_uri = format!("{}/shuffle/{}", server_uri, shuffle_id);
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
            chunk.replace_range(base.len().., &path_tail);
        }
        Ok(Uri::try_from(chunk.as_str())?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shuffle::get_free_port;

    #[tokio::test]
    async fn fetch_ok() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
        let port = get_free_port();
        ShuffleManager::start_server(Some(port))?;
        {
            let addr = format!("http://127.0.0.1:{}", port);
            let servers = &env::Env::get().map_output_tracker.server_uris;
            servers.insert(0, vec![Some(addr)]);

            let data = vec![(0i32, "example data".to_string())];
            let serialized_data = bincode::serialize(&data).unwrap();
            env::SHUFFLE_CACHE.insert((0, 0, 0), serialized_data);
        }

        let test_func = |(k, v): (i32, String)| {
            assert_eq!(k, 0);
            assert_eq!(v, "example data");
        };
        ShuffleFetcher::fetch(0, 0, test_func).await?;

        Ok(())
    }

    #[tokio::test]
    async fn fetch_failure() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
        let port = get_free_port();
        ShuffleManager::start_server(Some(port))?;
        {
            let addr = format!("http://127.0.0.1:{}", port);
            let servers = &env::Env::get().map_output_tracker.server_uris;
            servers.insert(1, vec![Some(addr)]);

            let data = "corrupted data";
            let serialized_data = bincode::serialize(&data).unwrap();
            env::SHUFFLE_CACHE.insert((1, 0, 0), serialized_data);
        }

        let test_func = |(_k, _v): (i32, String)| {};
        assert!(ShuffleFetcher::fetch(1, 0, test_func)
            .await
            .unwrap_err()
            .deserialization_err());

        Ok(())
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
