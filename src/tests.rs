mod collector;
mod thread_local;

fn run(testcase: impl Fn() + Send + Sync + 'static) {
    #[cfg(not(feature = "loom"))]
    testcase();
    #[cfg(feature = "loom")]
    loom::model(testcase);
}
