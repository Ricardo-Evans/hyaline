use crate::Retire;

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
///     let collector = hyaline::Collector::<Boxed>::new(32, 0);
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
///     // The reclaim will be called later, when hyaline can ensure all potential readers to the old pointer finished.
///
///     // SAFETY: the pointer is obtained by `Box::into_raw`, and from now no new readers can access it.
///     // Note we cannot use `Box::from_raw` here, otherwise it will break the Rust alias rule.
///     unsafe { handle.retire_boxed(swapped) };
/// }
/// ```
#[repr(transparent)]
#[derive(Debug)]
pub struct Boxed(*mut !);

impl Boxed {
    fn reclaim_as<T>(self) {
        // Safety: guaranteed by `BoxedHandle::retire_boxed`
        let _ = unsafe { Box::from_raw(self.0 as *mut T) };
    }
}

unsafe impl Send for Boxed {}

/// A [`Handle`] that can defer dropping a [`Box`].
pub trait BoxedHandle: Retire<Boxed> {
    /// Defers dropping the [`Box`] behind `pointer`.
    ///
    /// The collector drops it once every [`crate::Guard`] that might still observe it has ended.
    /// The drop may run on another thread.
    ///
    /// # Safety
    /// - `pointer` must be non-null, obtained from `Box::<T>::into_raw`, and this call takes ownership of it.
    /// - Every dereference of `pointer` must happen while a [`crate::Guard`] is held.
    /// - No [`crate::Guard`] created after this call can obtain `pointer`.
    unsafe fn retire_boxed<T>(&self, pointer: *mut T)
    where
        T: Send,
    {
        self.retire(Boxed(pointer as _), Boxed::reclaim_as::<T>);
    }
}

impl<T> BoxedHandle for T where T: Retire<Boxed> {}
