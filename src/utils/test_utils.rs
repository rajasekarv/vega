use std::net::TcpListener;
use std::sync::Arc;

use crate::result_task::ResultTask;
use crate::serializable_traits::SerFunc;
use crate::task::TaskContext;
use crate::*;
use serde_traitobject::{Deserialize, Serialize};

pub(crate) fn create_test_task<F>(func: F) -> ResultTask<u8, u8, F>
where
    F: SerFunc((TaskContext, Box<dyn Iterator<Item = u8>>)) -> u8,
{
    let task_ctxt = TaskContext::new(0, 0, 0);
    let ctxt = Context::new().unwrap();
    let rdd_f = Fn!(move |data: u8| -> u8 { data });
    let rdd = ctxt.parallelize(vec![0, 1, 2], 1).map(rdd_f);
    ResultTask::new(2, 0, 0, rdd.into(), Arc::new(func), 0, vec![], 0)
}
