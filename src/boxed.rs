/// A wrapper around a raw heap pointer to be reclaimed, intended for deferred memory reclaim through [`crate::Collector`].
///
/// # Example Usage
/// ```rust
/// use std::sync::atomic::{AtomicPtr, Ordering};
///
/// use hyaline::boxed::Boxed;
/// use hyaline::Handle;
///
/// fn example() {
///     let collector = hyaline::Collector::<Boxed<usize>>::new(32, 0);
///     let handle = collector.make_local_handle();
///     let pointer = Box::new(0usize);
///     let atomic = AtomicPtr::new(Box::into_raw(pointer));
///     // Some read on the atomic.
///     let guard = handle.pin();
///     let value = atomic.load(Ordering::Acquire);
///     // Some usage on the value, omitted here.
///     drop(value);
///     drop(guard);
///     // Swap out the old value.
///     let swapped = atomic.swap(std::ptr::null_mut(), Ordering::AcqRel);
///     // SAFETY: the pointer is obtained by Box::into_raw, and from now no new readers can access it.
///     // Note we cannot use `Box::from_raw` here, otherwise it will break the Rust alias rule.
///     let swapped = unsafe { Boxed::from_raw(swapped) };
///     // The reclaim will be called later, when hyaline can ensure all potential readers to the old pointer finished.
///     handle.retire(swapped, Boxed::reclaim);
/// }
/// ```
#[repr(transparent)]
#[derive(Debug)]
pub struct Boxed<T>(*mut T);

impl<T> Boxed<T> {
    /// Creates a new boxed value from a raw pointer.
    ///
    /// # Safety
    /// - The pointer must be a non-null valid pointer obtained from [`Box::into_raw`].
    /// - The pointer must not be accessed after calling this function.
    pub unsafe fn from_raw(raw: *mut T) -> Self {
        Self(raw)
    }

    /// Reclaims the raw pointer and deallocates the memory.
    pub fn reclaim(self) {
        // SAFETY: this is why `Self::from_raw` is unsafe.
        let _ = unsafe { Box::from_raw(self.0) };
    }
}

unsafe impl<T> Send for Boxed<T> where T: Send {}
