pub mod normal {
    pub use parking_lot::Mutex;
    pub use std::cell::{Cell, UnsafeCell};

    pub use std::sync::atomic::{
        AtomicBool, AtomicI8, AtomicI16, AtomicI32, AtomicI64, AtomicIsize, AtomicPtr, AtomicU8,
        AtomicU16, AtomicU32, AtomicU64, AtomicUsize, fence,
    };
    pub use std::sync::{Arc, LazyLock, mpsc};
    pub use std::thread::{JoinHandle, spawn};
    pub use std::thread_local;

    #[derive(Debug, Clone)]
    pub struct ExploreController;

    impl ExploreController {
        pub fn new(_: bool) -> Self {
            Self
        }

        pub fn enable(&self) {}

        pub fn disable(&self) {}
    }
}

#[cfg(feature = "_loom")]
pub mod loom {
    pub use loom::cell::{Cell, ConstPtr, UnsafeCell};
    pub use loom::sync::atomic::{
        AtomicBool, AtomicI8, AtomicI16, AtomicI32, AtomicI64, AtomicIsize, AtomicPtr, AtomicU8,
        AtomicU16, AtomicU32, AtomicU64, AtomicUsize, fence,
    };
    pub use loom::sync::{Arc, Mutex, mpsc};
    pub use loom::thread::{JoinHandle, spawn};
    pub use loom::{lazy_static, thread_local};
    use std::sync::atomic::Ordering;

    #[derive(Debug, Clone)]
    pub struct ExploreController {
        enabled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl ExploreController {
        pub fn new(initial: bool) -> Self {
            Self {
                enabled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(initial)),
            }
        }

        pub fn enable(&self) {
            if !self.enabled.swap(true, Ordering::Relaxed) {
                loom::explore()
            }
        }

        pub fn disable(&self) {
            if self.enabled.swap(false, Ordering::Relaxed) {
                loom::stop_exploring()
            }
        }
    }
}

#[cfg(not(feature = "_loom"))]
pub use normal::*;

#[cfg(feature = "_loom")]
pub use loom::*;
