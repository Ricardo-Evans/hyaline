pub mod normal {
    pub use parking_lot::Mutex;
    pub use std::cell::{Cell, UnsafeCell};
    pub use std::sync::atomic::{
        AtomicBool, AtomicI8, AtomicI16, AtomicI32, AtomicI64, AtomicIsize, AtomicPtr, AtomicU8,
        AtomicU16, AtomicU32, AtomicU64, AtomicUsize, fence,
    };
    pub use std::sync::{Arc, LazyLock};
    pub use std::thread_local;
}

#[cfg(feature = "_loom")]
pub mod loom {
    pub use loom::cell::{Cell, ConstPtr, UnsafeCell};
    pub use loom::sync::atomic::{
        AtomicBool, AtomicI8, AtomicI16, AtomicI32, AtomicI64, AtomicIsize, AtomicPtr, AtomicU8,
        AtomicU16, AtomicU32, AtomicU64, AtomicUsize, fence,
    };
    pub use loom::sync::{Arc, Mutex};
    pub use loom::{lazy_static, thread_local};
}

#[cfg(not(feature = "_loom"))]
pub use normal::*;

#[cfg(feature = "_loom")]
pub use loom::*;
