use super::*;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::error::Error;
use std::io::Read;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;
use std::sync::Arc;
//use std::sync::Mutex;
use threadpool::ThreadPool;

pub struct ShuffleFetcher;

// Parallel shuffle fetcher. Instead of Thread, convert everything to Tokio based async methods
impl ShuffleFetcher {
    pub fn fetch<K: Data, V: Data>(
        &self,
        sc: Arc<Context>,
        shuffle_id: usize,
        reduce_id: usize,
        mut func: impl FnMut((K, V)) -> (),
    ) {
        info!("inside fetch function");
        let parallel_fetches = 10; //TODO  make this as env variable
        let thread_pool = ThreadPool::new(parallel_fetches);
        let mut inputs_by_uri = HashMap::new();
        let server_uris = env::Env::get()
            .map_output_tracker
            .get_server_uris(shuffle_id);
        info!(
            "server uris for shuffle id {:?} - {:?}",
            shuffle_id, server_uris
        );
        for (index, server_uri) in server_uris.clone().into_iter().enumerate() {
            inputs_by_uri
                .entry(server_uri)
                .or_insert_with(Vec::new)
                .push(index);
        }
        let server_queue = Arc::new(Mutex::new(Vec::new()));
        let total_results: usize = inputs_by_uri.iter().map(|(_, v)| v.len()).sum();
        //TODO see whether randomize is necessary or not
        for (key, value) in inputs_by_uri {
            server_queue.lock().push((key, value));
        }
        info!(
            "servers for shuffle id {:?}, reduce id {:?} - {:?}",
            shuffle_id, reduce_id, server_queue
        );
        //        let log_output = format!("servers {:?}", server_queue);
        let (producer, consumer) = channel();
        let failure = Arc::new(Mutex::new(None));
        let sent_count = Arc::new(Mutex::new(0));
        for i in 0..parallel_fetches {
            info!("inisde parallel fetch {}", i);
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
                        info!(
                            "total results {} results sent {:?}",
                            total_results, sent_count_clone
                        );
                        info!(
                            "total results {} results sent {:?}",
                            total_results, sent_count_clone
                        );
                    }
                }
            })
        }
        info!("total_results {}", total_results);

        let mut results_done = 0;
        while failure.lock().is_none() && (results_done < total_results) {
            let (result, url) = consumer.recv().unwrap();
            info!(
                "total results {} results done {}",
                total_results, results_done
            );
            info!("received from consumer");
            let input: Vec<(K, V)> =
                bincode::deserialize(&result).expect("not able to serialize fetched data");
            for (k, v) in input {
                func((k, v));
            }
            results_done += 1;
        }
    }
}
