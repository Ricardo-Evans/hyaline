use crate::loom_support::{Arc, spawn};
use crate::tests::run;
use crate::thread_local::{ThreadLocal, new_thread};

#[test]
fn current_thread() {
    run(|| {
        let thread_local = Arc::new(ThreadLocal::<u64>::new());
        thread_local.get_or_init(|| 1);
        assert_eq!(thread_local.get().map(|v| *v), Some(1));

        let thread_local_clone = thread_local.clone();
        let thread = spawn(move || {
            thread_local_clone.get_or_init(|| 2);
            assert_eq!(thread_local_clone.get().map(|v| *v), Some(2));
        });

        assert_eq!(thread_local.get().map(|v| *v), Some(1));
        thread.join().unwrap();

        assert_eq!(thread_local.get().map(|v| *v), Some(1));
    })
}

#[test]
fn owned_thread() {
    run(|| {
        let thread_local = Arc::new(ThreadLocal::<u64>::new());
        let thread = new_thread();
        thread_local.get_or_init_by_thread(&thread, || 1);
        assert_eq!(thread_local.get_by_thread(&thread).map(|v| *v), Some(1));

        let thread_local_clone = thread_local.clone();
        let handle = spawn(move || {
            thread_local_clone.get_or_init_by_thread(&thread, || 2);
            assert_eq!(
                thread_local_clone.get_by_thread(&thread).map(|v| *v),
                Some(1)
            );
            thread
        });
        let thread = handle.join().unwrap();

        assert_eq!(thread_local.get_by_thread(&thread).map(|v| *v), Some(1));
    })
}
