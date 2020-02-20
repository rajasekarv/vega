//! Test whether the library can be used with different running async executors.

use native_spark::*;
#[macro_use]
extern crate serde_closure;

// Because some odd interaction between runtimes this test does not run succesfully
// when executed together with other --lib tests; it runs fine if ran in isolation!
#[ignore]
#[tokio::test]
async fn existing_tokio_rt() -> Result<()> {
    let sc = Context::new()?;
    let col = sc.make_rdd((0..10).collect::<Vec<_>>(), 32);
    let vec_iter = col.map(Fn!(|i| (0..i).collect::<Vec<_>>()));
    let res = vec_iter.collect()?;
    let expected = (0..10)
        .map(|i| (0..i).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(expected, res);
    Ok(())
}

// #[ignore]
#[async_std::test]
async fn existing_async_std_rt() -> Result<()> {
    let sc = Context::new()?;
    let col = sc.make_rdd((0..10).collect::<Vec<_>>(), 32);
    let vec_iter = col.map(Fn!(|i| (0..i).collect::<Vec<_>>()));
    let res = vec_iter.collect()?;
    let expected = (0..10)
        .map(|i| (0..i).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(expected, res);
    Ok(())
}
