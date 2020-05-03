//! Test whether the library can be used with different running async executors.
use std::sync::Arc;

use once_cell::sync::Lazy;
use vega::*;

static CONTEXT: Lazy<Arc<Context>> = Lazy::new(|| Context::new().unwrap());

#[tokio::test(core_threads = 4)]
async fn existing_tokio_rt() -> Result<()> {
    let initially = async { "initially" }.await;
    assert_eq!(initially, "initially");

    let sc = CONTEXT.clone();
    let col = sc.make_rdd((0..10).collect::<Vec<_>>(), 32);
    let vec_iter = col.map(Fn!(|i| (0..i).collect::<Vec<_>>()));
    let res = vec_iter.collect()?;
    let expected = (0..10)
        .map(|i| (0..i).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(expected, res);

    let finally = async { "finally" }.await;
    assert_eq!(finally, "finally");
    Ok(())
}

#[async_std::test]
async fn existing_async_std_rt() -> Result<()> {
    let initially = async { "initially" }.await;
    assert_eq!(initially, "initially");

    let sc = CONTEXT.clone();
    let col = sc.make_rdd((0..10).collect::<Vec<_>>(), 32);
    let vec_iter = col.map(Fn!(|i| (0..i).collect::<Vec<_>>()));
    let res = vec_iter.collect()?;
    let expected = (0..10)
        .map(|i| (0..i).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(expected, res);

    let finally = async { "finally" }.await;
    assert_eq!(finally, "finally");
    Ok(())
}
