use crate::loom_support::{AtomicPtr, AtomicU8, AtomicUsize, Cell, Mutex, UnsafeCell};
use std::array::from_fn;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::iter::repeat_with;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering;

#[cfg(not(feature = "loom"))]
std::thread_local! {
    static THREAD: Cell<Option<Thread>> = const { Cell::new(None) };
    static THREAD_GUARD: Cell<ThreadGuard> = const { Cell::new(ThreadGuard { id: INVALID_THREAD_ID }) };
}

#[cfg(feature = "loom")]
loom::thread_local! {
    static THREAD: Cell<Option<Thread>> = Cell::new(None);
    static THREAD_GUARD: Cell<ThreadGuard> = Cell::new(ThreadGuard { id: INVALID_THREAD_ID });
}

type ThreadID = usize;
type AtomicThreadID = AtomicUsize;

const INVALID_THREAD_ID: ThreadID = 0;
const BUCKET_COUNT: usize = ThreadID::BITS as usize;

const fn bucket_size(bucket: usize) -> usize {
    1 << bucket
}

const fn bucket_at(thread_id: ThreadID) -> usize {
    BUCKET_COUNT - 1 - (thread_id.leading_zeros() as usize)
}

const fn entry_at(thread_id: ThreadID) -> usize {
    let bucket = bucket_at(thread_id);
    thread_id - bucket_size(bucket)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Thread {
    bucket: usize,
    entry: usize,
}

impl Thread {
    fn new(id: ThreadID) -> Self {
        Self {
            bucket: bucket_at(id),
            entry: entry_at(id),
        }
    }
}

#[derive(Debug)]
#[cfg_attr(feature = "loom", allow(dead_code))]
struct ThreadGuard {
    id: ThreadID,
}

impl Drop for ThreadGuard {
    fn drop(&mut self) {
        // we cannot access a lazy or thread local object during destruction in loom
        #[cfg(not(feature = "loom"))]
        if self.id != INVALID_THREAD_ID {
            THREAD.with(|thread| thread.take());
            thread_manager().free(self.id);
        }
    }
}

#[derive(Debug)]
pub struct OwnedThread {
    id: ThreadID,
    thread: Thread,
    /// Marker to make the struct ![`Sync`]
    _phantom: PhantomData<Cell<()>>,
}

impl OwnedThread {
    fn new(id: ThreadID) -> Self {
        Self {
            id,
            thread: Thread::new(id),
            _phantom: PhantomData,
        }
    }
}

impl Drop for OwnedThread {
    fn drop(&mut self) {
        thread_manager().free(self.id);
    }
}

struct ThreadManager {
    free_from: AtomicThreadID,
    free_list: Mutex<BinaryHeap<Reverse<ThreadID>>>,
}

impl ThreadManager {
    #[cfg(not(feature = "loom"))]
    const fn new() -> Self {
        Self {
            free_from: AtomicThreadID::new(1),
            free_list: Mutex::new(BinaryHeap::new()),
        }
    }

    #[cfg(feature = "loom")]
    fn new() -> Self {
        Self {
            free_from: AtomicThreadID::new(1),
            free_list: Mutex::new(BinaryHeap::new()),
        }
    }

    #[cfg(not(feature = "loom"))]
    fn free_list(&self) -> impl DerefMut<Target = BinaryHeap<Reverse<ThreadID>>> {
        self.free_list.lock()
    }

    #[cfg(feature = "loom")]
    fn free_list(&self) -> impl DerefMut<Target = BinaryHeap<Reverse<ThreadID>>> {
        self.free_list.lock().unwrap()
    }

    fn allocate(&self) -> ThreadID {
        let mut free_list = self.free_list();
        if let Some(Reverse(id)) = free_list.pop() {
            id
        } else {
            // the free list mutex protects the free-from counter
            let id = self.free_from.load(Ordering::Relaxed);
            if id == ThreadID::MAX {
                std::hint::cold_path();
                panic!("too many threads");
            }
            // [`Ordering::SeqCst`] here to synchronize with potential fence outside [`Self::thread_id_limit_hint`]
            self.free_from.store(id + 1, Ordering::SeqCst);
            // loom cannot handle [`Ordering::SeqCst`] on atomic variables properly
            #[cfg(feature = "loom")]
            crate::loom_support::fence(Ordering::SeqCst);
            id
        }
    }

    fn free(&self, id: ThreadID) {
        self.free_list().push(Reverse(id));
    }

    /// Returns an exclusive upper-bound hint for issued thread IDs.
    ///
    /// Without external synchronization this is only a weak snapshot and may omit IDs issued concurrently.
    ///
    /// If a `SeqCst` fence is sequenced before this call, the returned bound includes every ID whose `SeqCst` issuance
    /// store precedes that fence in the global `SeqCst` order. IDs issued after the fence may still be omitted.
    fn thread_id_limit_hint(&self) -> ThreadID {
        self.free_from.load(Ordering::Relaxed)
    }
}

pub fn new_thread() -> OwnedThread {
    let id = thread_manager().allocate();
    OwnedThread::new(id)
}

fn current_thread() -> Thread {
    THREAD.with(|cell| match cell.get() {
        Some(thread) => thread,
        None => {
            std::hint::cold_path();
            let id = thread_manager().allocate();
            let thread = Thread::new(id);
            cell.set(Some(thread));
            #[cfg(not(feature = "loom"))]
            THREAD_GUARD.set(ThreadGuard { id });
            #[cfg(feature = "loom")]
            THREAD_GUARD.with(|guard| guard.set(ThreadGuard { id }));
            thread
        }
    })
}

fn thread_manager() -> &'static ThreadManager {
    #[cfg(not(feature = "loom"))]
    static MANAGER: crate::loom_support::LazyLock<ThreadManager> =
        crate::loom_support::LazyLock::new(|| ThreadManager::new());
    #[cfg(feature = "loom")]
    crate::loom_support::lazy_static! {
        static ref MANAGER: ThreadManager = ThreadManager::new();
    }
    &*MANAGER
}

struct Entry<T> {
    state: AtomicU8,
    value: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Entry<T> {
    const EMPTY: u8 = 0;
    const INITIALIZING: u8 = 1;
    const READY: u8 = 2;

    /// # Safety
    ///
    /// The value must be initialized, immutable, and alive for the returned lifetime.
    unsafe fn read_value(&self) -> DefaultEntryValue<'_, T> {
        // SAFETY: the caller guarantees the value is initialized and immutable.
        unsafe {
            #[cfg(not(feature = "loom"))]
            let value = (*self.value.get()).assume_init_ref();
            #[cfg(feature = "loom")]
            let guard = self.value.get();
            #[cfg(feature = "loom")]
            let value = guard.deref().as_ptr();
            EntryValue {
                #[cfg(feature = "loom")]
                guard: guard.into(),
                value,
                _phantom: PhantomData,
            }
        }
    }

    /// # Safety
    ///
    /// The entry must be exclusively owned, uninitialized, and have no value references.
    #[cfg(not(feature = "loom"))]
    unsafe fn write_value(&self, value: T) -> DefaultEntryValue<'_, T> {
        // SAFETY: the caller guarantees exclusive access to uninitialized storage.
        let value_mut = unsafe { &mut *self.value.get() };
        EntryValue {
            value: value_mut.write(value),
            _phantom: PhantomData,
        }
    }

    /// # Safety
    ///
    /// The entry must be exclusively owned, uninitialized, and have no value references.
    #[cfg(feature = "loom")]
    unsafe fn write_value(&self, value: T) -> DefaultEntryValue<'_, T> {
        // SAFETY: the caller guarantees exclusive access to uninitialized storage.
        unsafe {
            let value_mut = self.value.get_mut();
            let value_mut = value_mut.deref();
            value_mut.write(value);
        }
        // SAFETY: the value was initialized above and the mutable guard was dropped.
        unsafe { self.read_value() }
    }

    fn get(&self) -> Option<DefaultEntryValue<'_, T>> {
        let state = self.state.load(Ordering::Acquire);
        if state == Self::READY {
            // SAFETY: acquiring `READY` observes the initialized value.
            Some(unsafe { self.read_value() })
        } else {
            None
        }
    }

    /// # Safety
    ///
    /// The synchronization of the [`Ready`] state and value read must be guaranteed by the caller.
    unsafe fn get_owned(&self) -> Option<DefaultEntryValue<'_, T>> {
        // `Relaxed` here is enough for the owned thread
        let state = self.state.load(Ordering::Relaxed);
        if state == Self::READY {
            // SAFETY: acquiring `READY` observes the initialized value.
            Some(unsafe { self.read_value() })
        } else {
            None
        }
    }

    /// # Safety
    ///
    /// The synchronization of the [`Ready`] state and value read must be guaranteed by the caller.
    unsafe fn get_or_init(&self, provider: impl FnOnce() -> T) -> DefaultEntryValue<'_, T> {
        // `Relaxed` here is enough since only owned thread can initialize
        if self.state.load(Ordering::Relaxed) == Self::READY {
            // SAFETY: acquiring `READY` observes the initialized value.
            unsafe { self.read_value() }
        } else {
            let value = provider();
            // `Relaxed` here is enough since only owned thread can initialize
            self.state
                .compare_exchange(
                    Self::EMPTY,
                    Self::INITIALIZING,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .expect("recursive initialization happened");
            // SAFETY: this thread exclusively changed `EMPTY` to `INITIALIZING`,
            // so no initialized value or competing writer exists.
            let result = unsafe { self.write_value(value) };
            self.state
                .compare_exchange(
                    Self::INITIALIZING,
                    Self::READY,
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .expect("this should never fail as we already set the state to initializing");
            result
        }
    }
}

impl<T> Default for Entry<T> {
    fn default() -> Self {
        Self {
            state: AtomicU8::new(Self::EMPTY),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

impl<T> Drop for Entry<T> {
    #[cfg(not(feature = "loom"))]
    fn drop(&mut self) {
        if *self.state.get_mut() == Self::READY {
            // SAFETY: `READY` means initialized and `&mut self` excludes users.
            unsafe {
                let value_ref = &mut *self.value.get();
                value_ref.assume_init_drop();
            }
        }
    }

    #[cfg(feature = "loom")]
    fn drop(&mut self) {
        if self.state.with_mut(|v| *v) == Self::READY {
            // SAFETY: `READY` means initialized and `&mut self` excludes users.
            unsafe {
                let value_ref = self.value.get_mut();
                value_ref.deref().assume_init_drop();
            }
        }
    }
}

#[derive(Debug)]
#[cfg_attr(not(feature = "loom"), derive(Copy))]
#[cfg_attr(not(feature = "loom"), repr(transparent))]
pub struct EntryValue<'a, T, G> {
    #[cfg(feature = "loom")]
    guard: crate::loom_support::Arc<crate::loom_support::ConstPtr<G>>,
    #[cfg(not(feature = "loom"))]
    value: &'a T,
    #[cfg(feature = "loom")]
    value: *const T,
    _phantom: PhantomData<&'a G>,
}

impl<'a, T, G> EntryValue<'a, T, G> {
    #[inline]
    pub(crate) fn map<X>(self, f: impl FnOnce(&T) -> &X) -> EntryValue<'a, X, G> {
        #[cfg(not(feature = "loom"))]
        let value = EntryValue {
            value: f(self.value),
            _phantom: PhantomData,
        };
        #[cfg(feature = "loom")]
        let value = EntryValue {
            guard: self.guard,
            // SAFETY: the guard is transferred to the result and keeps the
            // projected pointer alive.
            value: f(unsafe { &*self.value }),
            _phantom: PhantomData,
        };
        value
    }
}

// SAFETY: `EntryValue` exposes only shared access to `T`, and `T: Sync`
// permits moving that access between threads.
unsafe impl<T, G> Send for EntryValue<'_, T, G> where T: Sync {}

// SAFETY: all accessible data is shared `T`, and `T: Sync` permits concurrent shared access.
unsafe impl<T, G> Sync for EntryValue<'_, T, G> where T: Sync {}

impl<T, G> Clone for EntryValue<'_, T, G> {
    fn clone(&self) -> Self {
        Self {
            #[cfg(feature = "loom")]
            guard: self.guard.clone(),
            value: self.value,
            _phantom: PhantomData,
        }
    }
}

impl<T, G> Deref for EntryValue<'_, T, G> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        #[cfg(not(feature = "loom"))]
        let value = self.value;
        #[cfg(feature = "loom")]
        // SAFETY: the retained guard keeps the initialized pointee alive.
        let value = unsafe { &*self.value };
        value
    }
}

pub type DefaultEntryValue<'a, T> = EntryValue<'a, T, MaybeUninit<T>>;

/// A thread local storage.
///
/// All items stored in it are destructed when this struct is dropped, instead of when the corresponding thread exits.
/// The items stored must be `Send`, as they may be destructed by other threads.
///
/// New threads may observe old items stored by another already exited thread; however, it's still safe as no race happens.
pub struct ThreadLocal<T>
where
    T: Send,
{
    buckets: [AtomicPtr<Entry<T>>; BUCKET_COUNT],
}

#[allow(dead_code)]
impl<T> ThreadLocal<T>
where
    T: Send,
{
    fn allocate_bucket(bucket: usize) -> *mut [Entry<T>] {
        let entries = repeat_with(Entry::default)
            .take(bucket_size(bucket))
            .collect::<Box<[_]>>();
        Box::into_raw(entries)
    }

    /// # Safety
    ///
    /// `bucket` must be an exact, uniquely owned pointer returned by `allocate_bucket`.
    unsafe fn free_bucket(bucket: *mut [Entry<T>]) {
        // SAFETY: guaranteed by the caller.
        unsafe {
            let _ = Box::from_raw(bucket);
        }
    }

    #[cfg(not(feature = "loom"))]
    pub const fn new() -> Self {
        Self {
            buckets: [const { AtomicPtr::new(std::ptr::null_mut()) }; BUCKET_COUNT],
        }
    }

    #[cfg(feature = "loom")]
    pub fn new() -> Self {
        Self {
            buckets: from_fn(|_| AtomicPtr::new(std::ptr::null_mut())),
        }
    }

    pub fn with_capacity(size: usize) -> Self {
        let allocated = BUCKET_COUNT - (size.leading_zeros() as usize);
        let buckets = from_fn(|bucket| {
            if bucket < allocated {
                AtomicPtr::new(Self::allocate_bucket(bucket).cast())
            } else {
                AtomicPtr::new(std::ptr::null_mut())
            }
        });
        Self { buckets }
    }

    fn get_or_init_bucket(&self, bucket: usize) -> *mut Entry<T> {
        let atomic_bucket = &self.buckets[bucket];
        let bucket_ptr = atomic_bucket.load(Ordering::Acquire);
        if bucket_ptr.is_null() {
            // bucket not initialized, should be rare
            std::hint::cold_path();
            let bucket = Self::allocate_bucket(bucket);
            match atomic_bucket.compare_exchange(
                std::ptr::null_mut(),
                bucket.cast(),
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => bucket.cast(),
                Err(new_bucket) => {
                    // concurrent initializations should be rare
                    debug_assert_ne!(new_bucket, std::ptr::null_mut());
                    std::hint::cold_path();
                    // SAFETY: this allocation lost the CAS, was never published, and remains uniquely owned here.
                    unsafe { Self::free_bucket(bucket) };
                    new_bucket
                }
            }
        } else {
            bucket_ptr
        }
    }

    fn get_inner(&self, thread: &Thread) -> Option<DefaultEntryValue<'_, T>> {
        let bucket_ptr = self.buckets[thread.bucket].load(Ordering::Acquire);
        if bucket_ptr.is_null() {
            return None;
        }
        // SAFETY: the thread ID maps to an entry within this allocated bucket.
        let entry = unsafe { &*bucket_ptr.add(thread.entry) };
        // SAFETY: only the owning thread can initialize the entry.
        unsafe { entry.get_owned() }
    }

    pub fn get(&self) -> Option<DefaultEntryValue<'_, T>> {
        self.get_inner(&current_thread())
    }

    /// Get a reference to the value in the thread local storage for the given thread, if it exists.
    pub fn get_by_thread<'a>(
        &'a self,
        thread: &'a OwnedThread,
    ) -> Option<DefaultEntryValue<'a, T>> {
        self.get_inner(&thread.thread)
    }

    /// Get a reference to the value in the thread local storage for the given thread, if it exists.
    /// Similar to [`Self::get_by_thread`], but without lifetime checks.
    /// This may be very useful when the owner thread wants to cache the TLS value and forms a self-referenced struct.
    ///
    /// # Safety
    /// The caller must ensure the thread lives longer than the returned value.
    pub unsafe fn get_by_thread_unsafe(
        &'_ self,
        thread: &OwnedThread,
    ) -> Option<DefaultEntryValue<'_, T>> {
        self.get_inner(&thread.thread)
    }

    fn get_or_init_inner(
        &self,
        thread: &Thread,
        provider: impl FnOnce() -> T,
    ) -> DefaultEntryValue<'_, T> {
        let bucket_ptr = self.get_or_init_bucket(thread.bucket);
        // SAFETY: the thread ID maps to an entry within this allocated bucket.
        let entry = unsafe { &*bucket_ptr.add(thread.entry) };
        // SAFETY: only the owning thread can initialize the entry.
        unsafe { entry.get_or_init(provider) }
    }

    /// Get a reference to the value in the thread local storage for the current thread or creates a new one if it doesn't exist.
    ///
    /// The given `provider` function is only called if the initialization is required, and it must guarantee no recursive initialization.
    pub fn get_or_init(&self, provider: impl FnOnce() -> T) -> DefaultEntryValue<'_, T> {
        self.get_or_init_inner(&current_thread(), provider)
    }

    /// Get a reference to the value in the thread local storage for the given thread or creates a new one if it doesn't exist.
    ///
    /// The given `provider` function is only called if the initialization is required, and it must guarantee no recursive initialization.
    pub fn get_or_init_by_thread<'a>(
        &'a self,
        thread: &'a OwnedThread,
        provider: impl FnOnce() -> T,
    ) -> DefaultEntryValue<'a, T> {
        self.get_or_init_inner(&thread.thread, provider)
    }

    /// Get a reference to the value in the thread local storage for the given thread or creates a new one if it doesn't exist.
    /// Similar to [`Self::get_or_init_by_thread`], but without lifetime checks.
    /// This may be very useful when the owner thread wants to cache the TLS value and forms a self-referenced struct.
    ///
    /// # Safety
    /// The caller must ensure the thread lives longer than the returned value.
    pub unsafe fn get_or_init_by_thread_unsafe(
        &'_ self,
        thread: &OwnedThread,
        provider: impl FnOnce() -> T,
    ) -> DefaultEntryValue<'_, T> {
        self.get_or_init_inner(&thread.thread, provider)
    }

    /// Returns an iterator over all thread local entries.
    ///
    /// Weak snapshot:
    /// - The iterator may not observe an entry initialized concurrently by another thread.
    /// - The iterator may and usually observe entries initialized by already exited threads, since entries are not removed when threads exit.
    ///
    /// However, there is a notable special exception in this project:
    /// If an external [`Ordering::SeqCst`] fence sequenced before the iteration forms a sequentially consistent order with other [`Ordering::SeqCst`]
    /// operations on the values of this thread local, the iteration is guaranteed to not violate the visibility required by that order.
    pub fn iter(&'_ self) -> impl Iterator<Item = DefaultEntryValue<'_, T>>
    where
        T: Sync,
    {
        let thread_limit = thread_manager().thread_id_limit_hint();
        let bucket_limit = bucket_at(thread_limit);
        let entry_limit = entry_at(thread_limit);
        self.buckets
            .iter()
            .enumerate()
            // only these buckets may contain initialized entries
            .take(bucket_limit + 1)
            .flat_map(move |(bucket_id, bucket)| {
                let bucket = bucket.load(Ordering::Acquire).cast_const();
                if bucket.is_null() {
                    &[]
                } else {
                    let len = if bucket_id < bucket_limit {
                        bucket_size(bucket_id)
                    } else {
                        entry_limit
                    };
                    // Safety: the bucket is allocated as a boxed slice
                    unsafe { std::slice::from_raw_parts(bucket, len) }
                }
            })
            .flat_map(Entry::get)
    }
}

impl<T> Drop for ThreadLocal<T>
where
    T: Send,
{
    fn drop(&mut self) {
        for (bucket, bucket_ptr) in self.buckets.iter_mut().enumerate() {
            #[cfg(not(feature = "loom"))]
            let bucket_ptr = *bucket_ptr.get_mut();
            #[cfg(feature = "loom")]
            let bucket_ptr = bucket_ptr.with_mut(|v| *v);
            if !bucket_ptr.is_null() {
                // SAFETY: `&mut self` excludes users;
                // this uniquely owned allocation has exactly `bucket_size(bucket)` entries.
                unsafe {
                    Self::free_bucket(std::ptr::slice_from_raw_parts_mut(
                        bucket_ptr,
                        bucket_size(bucket),
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::thread_local::{BUCKET_COUNT, ThreadID};

    #[test]
    fn bucket_count() {
        assert_eq!(BUCKET_COUNT, 64);
    }

    #[test]
    fn bucket_size() {
        assert_eq!(super::bucket_size(0), 1);
        assert_eq!(super::bucket_size(1), 2);
        assert_eq!(super::bucket_size(2), 4);
        assert_eq!(super::bucket_size(3), 8);
        assert_eq!(
            super::bucket_size(BUCKET_COUNT - 1),
            1 << (BUCKET_COUNT - 1)
        );
    }

    #[test]
    fn bucket_at() {
        assert_eq!(super::bucket_at(1), 0);
        assert_eq!(super::bucket_at(2), 1);
        assert_eq!(super::bucket_at(3), 1);
        assert_eq!(super::bucket_at(4), 2);
        assert_eq!(super::bucket_at(5), 2);
        assert_eq!(super::bucket_at(6), 2);
        assert_eq!(super::bucket_at(7), 2);
        assert_eq!(super::bucket_at(8), 3);
        assert_eq!(super::bucket_at(ThreadID::MAX), BUCKET_COUNT - 1);
    }

    #[test]
    fn entry_at() {
        assert_eq!(super::entry_at(1), 0);
        assert_eq!(super::entry_at(2), 0);
        assert_eq!(super::entry_at(3), 1);
        assert_eq!(super::entry_at(4), 0);
        assert_eq!(super::entry_at(5), 1);
        assert_eq!(super::entry_at(6), 2);
        assert_eq!(super::entry_at(7), 3);
        assert_eq!(super::entry_at(8), 0);
        assert_eq!(super::entry_at(ThreadID::MAX), ThreadID::MAX / 2);
    }
}
