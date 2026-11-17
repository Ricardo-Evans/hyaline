use crate::Handle;
use std::mem::MaybeUninit;

/// A layout placeholder with the size and alignment of `N` pointers.
///
/// Use it as the type parameter of [`Closure`] to reserve storage for a deferred closure.
/// [`PointerSized<1>`] fits any closure: one that fits is stored in place, and a larger one is boxed.
pub struct PointerSized<const N: usize = 1>([*const !; N]);

// SAFETY: the raw pointers are a layout placeholder and are never dereferenced.
unsafe impl<const N: usize> Send for PointerSized<N> {}

/// A wrapper around a closure to be invoked later, intended for deferred execution through [`crate::Collector`].
///
/// The type parameter is a placeholder used to control the size and alignment of the closure storage.
/// The placeholder will never be constructed, but it must be [`Send`] to match the requirements of [`Handle`].
///
/// A closure that fits is stored in place, or boxed when a [`Box`] fits in the same slot, otherwise a compile-time error is thrown.
/// Thus, a commonly used placeholder is [`PointerSized`], as a closure contains at least a function pointer, while a [`Box`] always fits too.
///
/// You may use [`Closure::can_store_inplace`] and [`Closure::can_store_boxed`] to determine whether a closure can fit in place or needs to be boxed.
///
/// # Example Usage
/// ```rust
/// use hyaline::closure::{Closure, ClosureHandle, PointerSized};
///
/// fn example() {
///     // One pointer of storage. Small closures stay inline, and larger ones are boxed.
///     let collector = hyaline::Collector::<Closure<PointerSized>>::new(32, 0);
///     let handle = collector.make_local_handle();
///     // The closure runs later, when hyaline can ensure concurrent readers have finished.
///     handle.defer(|| {
///         unimplemented!("some deferred operation")
///     });
/// }
/// ```
#[repr(transparent)]
#[derive(Debug)]
pub struct Closure<T>(MaybeUninit<T>);

impl<T> Closure<T> {
    /// Returns whether `F` fits in this slot by size and alignment.
    pub const fn can_store_inplace<F>() -> bool {
        size_of::<F>() <= size_of::<Self>() && align_of::<F>() <= align_of::<Self>()
    }

    /// Returns whether `Box<F>` fits in this slot by size and alignment.
    pub const fn can_store_boxed<F>() -> bool {
        size_of::<Box<F>>() <= size_of::<Self>() && align_of::<Box<F>>() <= align_of::<Self>()
    }

    /// Writes `f` into the inline storage.
    ///
    /// # Safety
    /// - [`Self::can_store_inplace`] must hold for `F`.
    /// - The value must be taken back with [`Self::call_inplace`] exactly once.
    unsafe fn new_inplace<F>(f: F) -> Self
    where
        F: FnOnce(),
    {
        const {
            assert!(
                Self::can_store_inplace::<F>(),
                "the given function cannot be stored inplace"
            );
        }
        let mut data = MaybeUninit::uninit();
        // SAFETY: the assert above guarantees `F` fits in this slot.
        unsafe { std::ptr::write(data.as_mut_ptr() as *mut _, f) };
        Self(data)
    }

    /// Writes a boxed `f` into the inline storage.
    ///
    /// # Safety
    /// - [`Self::can_store_boxed`] must hold for `F`.
    /// - The value must be taken back with [`Self::call_boxed`] exactly once.
    unsafe fn new_boxed<F>(f: Box<F>) -> Self
    where
        F: FnOnce(),
    {
        let mut data = MaybeUninit::uninit();
        // SAFETY: the caller guarantees `Box<F>` fits in this slot.
        unsafe { std::ptr::write(data.as_mut_ptr() as *mut _, f) };
        Self(data)
    }

    /// Calls a closure previously stored with [`Self::new_inplace`].
    fn call_inplace<F>(self)
    where
        F: FnOnce(),
    {
        let Self(data) = self;
        // SAFETY: `new_inplace::<F>` wrote this `F`, and it is read only here.
        let f: F = unsafe { std::ptr::read(data.as_ptr() as *const _) };
        f();
    }

    /// Calls a closure previously stored with [`Self::new_boxed`].
    fn call_boxed<F>(self)
    where
        F: FnOnce(),
    {
        let Self(data) = self;
        // SAFETY: `new_boxed::<F>` wrote this `Box<F>`, and it is read only here.
        let f: Box<F> = unsafe { std::ptr::read(data.as_ptr() as *const _) };
        f();
    }

    /// Stores `f` and returns the function that calls it.
    ///
    /// # Safety
    /// The returned function must be applied to the returned closure exactly once.
    unsafe fn wrap<F>(f: F) -> (Self, fn(Self))
    where
        F: FnOnce(),
    {
        if const { Self::can_store_inplace::<F>() } {
            // SAFETY: `F` fits in the slot, and `call_inplace::<F>` is the matching call.
            let closure = unsafe { Self::new_inplace(f) };
            (closure, Self::call_inplace::<F>)
        } else {
            const {
                assert!(
                    Self::can_store_boxed::<F>(),
                    "the given closure cannot be stored either inplace or boxed"
                )
            };
            // SAFETY: the assert above guarantees `Box<F>` fits, and `call_boxed::<F>` is the matching call.
            let closure = unsafe { Self::new_boxed(Box::new(f)) };
            (closure, Self::call_boxed::<F>)
        }
    }
}

// SAFETY: storage is `MaybeUninit<T>` with `T: Send`. Safe code only writes a `Send` closure;
// `defer_unchecked` leaves that obligation with the caller.
unsafe impl<T> Send for Closure<T> where T: Send {}

/// A [`Handle`] that can defer a closure stored in [`Closure`].
pub trait ClosureHandle<T>: Handle<Closure<T>>
where
    T: Send,
{
    /// Defers `f` until the collector runs it.
    ///
    /// The closure may run on another thread after this call returns.
    fn defer<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        // SAFETY: `F` is `Send` and `static`, so it can run later on another thread.
        unsafe { self.defer_unchecked(f) };
    }

    /// Defers `f` without the [`Send`] and `static` lifetime bounds of [`Self::defer`].
    ///
    /// # Safety
    /// - `f` must be safe to send to another thread and to call there.
    /// - `f` must ensure all used data is still valid as it may be called later.
    unsafe fn defer_unchecked<F>(&self, f: F)
    where
        F: FnOnce(),
    {
        // SAFETY: `wrap` pairs this closure with the function that calls it, and `retire` invokes that function once.
        let (closure, function) = unsafe { Closure::wrap(f) };
        self.retire(closure, function);
    }
}

impl<X, T> ClosureHandle<T> for X
where
    T: Send,
    X: Handle<Closure<T>>,
{
}
