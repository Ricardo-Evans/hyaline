use crate::loom_support;
use crate::loom_support::{AtomicPtr, AtomicUsize};
use crate::thread_local::{EntryValue, OwnedThread, ThreadLocal};
use crossbeam_utils::CachePadded;
use std::cell::Cell;
use std::marker::PhantomPinned;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;

pub mod handle;

pub use handle::*;

#[cfg(not(feature = "_loom"))]
mod normal {
    use crate::collector::{Entry, TLSRetiredList};
    use crate::loom_support::AtomicPtr;
    use std::marker::PhantomData;
    use std::ops::Deref;

    pub(super) union EntryState<T> {
        head: *const AtomicPtr<Entry<T>>,
        next: *const Entry<T>,
    }

    pub(super) struct EntryStates<'a, T>(PhantomData<&'a T>);

    impl<T> EntryState<T> {
        #[inline]
        pub(super) fn mark<'a>(&mut self, head: TLSRetiredList<'a, T>, _: &mut EntryStates<'a, T>) {
            self.head = &*head;
        }

        /// Unmark the given entry state and retrieve the previously marked value.
        ///
        /// # Safety
        ///
        /// The state must have been marked at lease once from the last unmark.
        #[inline]
        pub(super) unsafe fn unmark<'a>(
            &mut self,
            _: &mut EntryStates<'a, T>,
        ) -> impl Deref<Target = AtomicPtr<Entry<T>>> + 'a {
            // SAFETY: `mark` initialized `head`, whose TLS allocation lives for `'a`.
            unsafe { &*self.head }
        }

        /// Turn the current entry into a shared one and link it to another entry.
        ///
        /// # Safety
        ///
        /// The entry must be unpublished and exclusively accessed.
        #[inline]
        pub(super) unsafe fn link(&mut self, next: *const Entry<T>) {
            self.next = next;
        }

        /// Retrieve the next entry in the list.
        ///
        /// # Safety
        ///
        /// The entry must have been linked and remain alive.
        #[inline]
        pub(super) unsafe fn next(&self) -> *const Entry<T> {
            // SAFETY: `link` initialized the active `next` union field.
            unsafe { self.next }
        }
    }

    impl<T> Default for EntryState<T> {
        #[inline]
        fn default() -> Self {
            Self {
                next: std::ptr::null(),
            }
        }
    }

    impl<T> Default for EntryStates<'_, T> {
        #[inline]
        fn default() -> Self {
            Self(PhantomData)
        }
    }
}

#[cfg(feature = "_loom")]
mod loom {
    use crate::collector::{Entry, TLSRetiredList};
    use crate::loom_support::AtomicPtr;
    use derive_more::{Deref, DerefMut, From};
    use std::collections::HashMap;
    use std::ops::Deref;

    #[derive(From, Deref, DerefMut)]
    pub(super) struct EntryStates<'a, T>(HashMap<*const EntryState<T>, TLSRetiredList<'a, T>>);

    #[derive(Default)]
    pub(super) enum EntryState<T> {
        #[default]
        Empty,
        Owned,
        Shared(*const Entry<T>),
    }

    impl<T> EntryState<T> {
        #[inline]
        pub(super) fn mark<'a>(
            &mut self,
            head: TLSRetiredList<'a, T>,
            states: &mut EntryStates<'a, T>,
        ) {
            match self {
                EntryState::Empty => *self = EntryState::Owned,
                EntryState::Owned => {}
                EntryState::Shared(_) => panic!("shared entries cannot be marked"),
            }
            states.insert(self, head);
        }

        /// Unmark the given entry state and retrieve the previously marked value.
        ///
        /// # Safety
        ///
        /// The state must have been marked at lease once from the last unmark.
        #[inline]
        pub(super) unsafe fn unmark<'a>(
            &mut self,
            states: &mut EntryStates<'a, T>,
        ) -> impl Deref<Target = AtomicPtr<Entry<T>>> + 'a {
            assert!(
                !matches!(self, EntryState::Shared(_)),
                "shared entries cannot be unmarked"
            );
            states
                .remove(&(self as *const _))
                .expect("state must be marked first")
        }

        /// Turn the current entry into a shared one and link it to another entry.
        ///
        /// # Safety
        ///
        /// The entry must be unpublished and exclusively accessed.
        #[inline]
        pub(super) unsafe fn link(&mut self, next: *const Entry<T>) {
            *self = EntryState::Shared(next);
        }

        /// Retrieve the next entry in the list.
        ///
        /// # Safety
        ///
        /// The entry must have been linked and remain alive.
        #[inline]
        pub(super) unsafe fn next(&self) -> *const Entry<T> {
            match self {
                EntryState::Empty => std::ptr::null(),
                EntryState::Owned => panic!("next cannot be called on owned entries"),
                EntryState::Shared(next) => *next,
            }
        }
    }

    impl<T> Default for EntryStates<'_, T> {
        #[inline]
        fn default() -> Self {
            Self(HashMap::default())
        }
    }
}

#[cfg(not(feature = "_loom"))]
use normal::*;

#[cfg(feature = "_loom")]
use loom::*;

struct Entry<T> {
    data: T,
    operation: fn(T),
    batch: NonNull<Batch<T>>,
    state: EntryState<T>,
}

impl<T> Entry<T> {
    /// The sentinel value for inactive entries.
    /// Since the [`Entry`] is not a zero-sized type, this value can never be a valid pointer.
    const INACTIVE: *mut Entry<T> = std::ptr::without_provenance_mut(usize::MAX);

    #[inline]
    fn new(data: T, operation: fn(T), batch: NonNull<Batch<T>>) -> Self {
        Self {
            data,
            operation,
            batch,
            state: EntryState::default(),
        }
    }
}

// SAFETY: `T` is `Send`; published state is immutable, and the batch is protected by its atomic reference count.
unsafe impl<T> Send for Entry<T> where T: Send {}

struct Batch<T> {
    references: AtomicUsize,
    entries: Vec<Entry<T>>,
    _phantom: PhantomPinned,
}

impl<T> Batch<T> {
    #[inline]
    fn with_capacity(capacity: usize) -> Box<Self> {
        Box::new(Self {
            references: AtomicUsize::new(0),
            entries: Vec::with_capacity(capacity),
            _phantom: PhantomPinned,
        })
    }

    #[inline]
    fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    fn free(self: Box<Self>) {
        self.entries
            .into_iter()
            .for_each(|entry| (entry.operation)(entry.data));
    }
}

struct LastEpoch<T> {
    begin: Cell<*const Entry<T>>,
    guards: Cell<usize>,
}

impl<T> LastEpoch<T> {
    #[inline]
    fn is_active(&self) -> bool {
        self.guards.get() > 0
    }
}

struct CurrentEpoch {
    guards: Cell<usize>,
    /// the times of all pins
    pins: Cell<usize>,
}

impl CurrentEpoch {
    #[inline]
    fn is_active(&self) -> bool {
        self.guards.get() > 0
    }
}

type EpochID = usize;

/// Thread local state of the collector, exclusively owned by the thread
struct ThreadState<T> {
    /// the thread local batch
    batch: Cell<NonNull<Batch<T>>>,
    /// the flush threshold
    flush_threshold: Cell<usize>,
    /// the current epoch id
    epoch_id: Cell<EpochID>,
    /// the last epoch
    last_epoch: LastEpoch<T>,
    /// the current epoch
    current_epoch: CurrentEpoch,
}

enum EnterAction {
    None(EpochID),
    Advance(EpochID),
    Activate(EpochID),
}

enum LeaveAction<'a, T> {
    /// No action needed
    None,
    /// The current epoch is drained, but the last epoch is still active, defer the release to the last epoch
    Defer,
    /// The current epoch is drained, and the last epoch is inactive, deactivate the current epoch
    Deactivate,
    /// The last epoch is drained, but the current epoch is still active, only need to release the last epoch
    Release(&'a LastEpoch<T>),
    /// The last epoch is drained, and the current epoch is inactive, deactivate and release the last epoch
    DeactivateAndRelease(&'a LastEpoch<T>),
}

impl<T> ThreadState<T> {
    /// Advance the epoch after detaching the retired list
    #[inline]
    fn advance_epoch(&self, detached: *const Entry<T>) {
        let last_epoch = &self.last_epoch;
        let current_epoch = &self.current_epoch;
        last_epoch.guards.set(current_epoch.guards.replace(0));
        last_epoch.begin.set(detached);
        current_epoch.pins.set(0);
        self.epoch_id.update(|epoch| epoch.wrapping_add(1));
    }

    #[inline]
    fn enter(&self, epoch_size: usize) -> EnterAction {
        let current_epoch = &self.current_epoch;
        let existing_guards = current_epoch.guards.get();
        let epoch_id = self.epoch_id.get();
        if epoch_size == 0 {
            current_epoch.guards.set(
                existing_guards
                    .checked_add(1)
                    .expect("epoch guards overflow"),
            );
            return if existing_guards == 0 {
                EnterAction::Activate(epoch_id)
            } else {
                EnterAction::None(epoch_id)
            };
        }

        let pins = current_epoch.pins.get().saturating_add(1);
        current_epoch.guards.set(
            existing_guards
                .checked_add(1)
                .expect("epoch guards overflow"),
        );
        current_epoch.pins.set(pins);

        if self.last_epoch.is_active() {
            EnterAction::None(epoch_id)
        } else if existing_guards == 0 {
            EnterAction::Activate(epoch_id)
        } else if pins >= epoch_size {
            EnterAction::Advance(epoch_id)
        } else {
            EnterAction::None(epoch_id)
        }
    }

    #[inline]
    fn leave(&'_ self, epoch_id: EpochID) -> LeaveAction<'_, T> {
        let current_epoch_id = self.epoch_id.get();
        if epoch_id == current_epoch_id {
            let current_epoch = &self.current_epoch;
            let guards = current_epoch.guards.get() - 1;
            current_epoch.guards.set(guards);
            if guards > 0 {
                LeaveAction::None
            } else if self.last_epoch.is_active() {
                LeaveAction::Defer
            } else {
                LeaveAction::Deactivate
            }
        } else {
            debug_assert_eq!(epoch_id, current_epoch_id.wrapping_sub(1));
            let last_epoch = &self.last_epoch;
            let guards = last_epoch.guards.get() - 1;
            last_epoch.guards.set(guards);
            if guards > 0 {
                LeaveAction::None
            } else if self.current_epoch.is_active() {
                LeaveAction::Release(last_epoch)
            } else {
                LeaveAction::DeactivateAndRelease(last_epoch)
            }
        }
    }
}

// SAFETY: handles provide exclusive logical ownership of the cells,
// and `T` may be transferred to the thread that eventually frees the batch.
unsafe impl<T> Send for ThreadState<T> where T: Send {}

impl<T> Drop for ThreadState<T> {
    fn drop(&mut self) {
        let batch = self.batch.replace(NonNull::dangling());
        // SAFETY: this is the live local batch uniquely owned by the state,
        // and its leaked `Box` is reconstructed exactly once.
        let batch = unsafe { Box::from_non_null(batch) };
        batch.free();
    }
}

type TLSThreadState<'a, T> =
    EntryValue<'a, ThreadState<T>, MaybeUninit<CachePadded<ThreadState<T>>>>;
type TLSRetiredList<'a, T> =
    EntryValue<'a, AtomicPtr<Entry<T>>, MaybeUninit<CachePadded<AtomicPtr<Entry<T>>>>>;

pub struct Collector<T>
where
    T: Send,
{
    /// once a batch reaches this size, try to retire it
    batch_size: usize,
    /// once the total guards created in an epoch reach this size, try to advance the epoch
    epoch_size: usize,
    /// the thread local state
    state: ThreadLocal<CachePadded<ThreadState<T>>>,
    retired: ThreadLocal<CachePadded<AtomicPtr<Entry<T>>>>,
}

impl<T> Collector<T>
where
    T: Send,
{
    #[cfg(not(feature = "_loom"))]
    pub const fn new(batch_size: usize, epoch_size: usize) -> Self {
        assert!(batch_size > 0, "batch size must be greater than 0");
        Self {
            batch_size,
            epoch_size,
            state: ThreadLocal::new(),
            retired: ThreadLocal::new(),
        }
    }

    #[cfg(feature = "_loom")]
    pub fn new(batch_size: usize, epoch_size: usize) -> Self {
        assert!(batch_size > 0, "batch size must be greater than 0");
        Self {
            batch_size,
            epoch_size,
            state: ThreadLocal::new(),
            retired: ThreadLocal::new(),
        }
    }

    pub fn with_threads(batch_size: usize, epoch_size: usize, threads: usize) -> Self {
        assert!(batch_size > 0, "batch size must be greater than 0");
        Self {
            batch_size,
            epoch_size,
            state: ThreadLocal::with_capacity(threads),
            retired: ThreadLocal::with_capacity(threads),
        }
    }

    pub fn make_local_handle(&self) -> LocalHandle<'_, T> {
        LocalHandle::new(self)
    }

    pub fn make_owned_handle(&'_ self) -> OwnedHandle<'_, T> {
        OwnedHandle::new(self)
    }

    fn current_thread_state(&'_ self) -> TLSThreadState<'_, T> {
        if let Some(state) = self.state.get() {
            state.map(Deref::deref)
        } else {
            // only need to initialize the thread state at most once for every single thread
            std::hint::cold_path();
            self.state
                .get_or_init(|| self.new_thread_state())
                .map(Deref::deref)
        }
    }

    /// Get the thread state for the given thread.
    ///
    /// # Safety
    /// The caller must ensure the thread lives longer than the returned value.
    unsafe fn owned_thread_state(&'_ self, thread: &OwnedThread) -> TLSThreadState<'_, T> {
        // Safety: this is why this function is unsafe
        unsafe {
            if let Some(state) = self.state.get_by_thread_unsafe(thread) {
                state.map(Deref::deref)
            } else {
                // only need to initialize the thread state at most once for every single thread
                std::hint::cold_path();
                self.state
                    .get_or_init_by_thread_unsafe(thread, || self.new_thread_state())
                    .map(Deref::deref)
            }
        }
    }

    fn current_retired_list(&'_ self) -> TLSRetiredList<'_, T> {
        if let Some(retired_list) = self.retired.get() {
            retired_list.map(Deref::deref)
        } else {
            // only need to initialize the retired list at most once for every single thread
            std::hint::cold_path();
            self.retired
                .get_or_init(|| self.new_retired_list())
                .map(Deref::deref)
        }
    }

    /// Get the retired list for the given thread.
    ///
    /// # Safety
    /// The caller must ensure the thread lives longer than the returned value.
    unsafe fn owned_retired_list(&'_ self, thread: &OwnedThread) -> TLSRetiredList<'_, T> {
        // Safety: this is why this function is unsafe
        unsafe {
            if let Some(retired_list) = self.retired.get_by_thread_unsafe(thread) {
                retired_list.map(Deref::deref)
            } else {
                // only need to initialize the retired list at most once for every single thread
                std::hint::cold_path();
                self.retired
                    .get_or_init_by_thread_unsafe(thread, || self.new_retired_list())
                    .map(Deref::deref)
            }
        }
    }

    #[inline]
    fn new_batch(&self) -> NonNull<Batch<T>> {
        Box::leak(Batch::with_capacity(self.batch_size)).into()
    }

    #[inline]
    fn new_thread_state(&self) -> CachePadded<ThreadState<T>> {
        CachePadded::new(ThreadState {
            batch: Cell::new(self.new_batch()),
            flush_threshold: Cell::new(self.batch_size),
            epoch_id: Cell::new(0),
            last_epoch: LastEpoch {
                begin: Cell::new(std::ptr::null()),
                guards: Cell::new(0),
            },
            current_epoch: CurrentEpoch {
                guards: Cell::new(0),
                pins: Cell::new(0),
            },
        })
    }

    #[inline]
    fn new_retired_list(&self) -> CachePadded<AtomicPtr<Entry<T>>> {
        CachePadded::new(AtomicPtr::new(Entry::<T>::INACTIVE))
    }

    fn retire(&self, state: &ThreadState<T>, data: T, operation: fn(T)) {
        let mut batch_ptr = state.batch.get();
        // SAFETY: the calling handle exclusively owns this unpublished batch.
        let batch = unsafe { batch_ptr.as_mut() };
        batch.entries.push(Entry::new(data, operation, batch_ptr));
        let flush_threshold = state.flush_threshold.get();
        if batch.len() >= flush_threshold {
            if !self.flush_batch(state, batch) {
                state.flush_threshold.set(flush_threshold.saturating_mul(2));
            } else {
                state.flush_threshold.set(self.batch_size);
            }
        }
    }

    fn mark_batch<'a>(
        &'a self,
        batch: &mut Batch<T>,
        states: &mut EntryStates<'a, T>,
    ) -> Option<usize> {
        let mut marked = 0usize;
        let mut entries_iterator = batch.entries.iter_mut();
        // synchronize with the [`Ordering::SeqCst`] store in [`Self::enter`], also complies with the special exception of [`ThreadLocal::iter`]
        loom_support::fence(Ordering::SeqCst);
        for head in self
            .retired
            .iter()
            // [`Ordering::Relaxed`] here for better performance, with a [`Ordering::Acquire`] fence later for correctness
            .filter(|head| head.load(Ordering::Relaxed) != Entry::INACTIVE)
        {
            let Some(entry) = entries_iterator.next() else {
                // the batch does not have enough entries, retry next time
                return None;
            };
            // store the head of the retired list to link later
            entry.state.mark(head.clone().map(Deref::deref), states);
            marked += 1;
        }
        Some(marked)
    }

    fn link_batch<'a>(
        &'a self,
        batch: NonNull<Batch<T>>,
        marked: usize,
        states: &mut EntryStates<'a, T>,
    ) -> usize {
        // SAFETY: the detached batch is live,
        // and its vector will not move or reallocate after entries are published.
        let entries = unsafe { (*batch.as_ptr()).entries.as_ptr() };

        let mut active = 0usize;
        // SAFETY: `marked` does not exceed the number of entries.
        for entry in (0..marked).map(|i| unsafe { entries.add(i).cast_mut() }) {
            // SAFETY: this entry is unpublished and its state is exclusively accessed.
            let state = unsafe { &mut (*entry).state };
            // SAFETY: `mark_batch` marked this state once,
            // and the referenced TLS retired list remains alive for the collector lifetime.
            let head = unsafe { state.unmark(states) };
            let mut next = head.load(Ordering::Relaxed);
            loop {
                if next == Entry::INACTIVE {
                    std::hint::cold_path();
                    // the thread becomes inactive, skip it.
                    break;
                }
                // SAFETY: the entry is still unpublished and exclusively accessed.
                unsafe { state.link(next) };
                // try to link the entry to the retired list
                match head.compare_exchange_weak(next, entry, Ordering::Release, Ordering::Relaxed)
                {
                    Ok(_) => {
                        active += 1;
                        break;
                    }
                    Err(new) => next = new,
                }
            }
        }
        active
    }

    fn flush_batch(&self, state: &ThreadState<T>, batch: &mut Batch<T>) -> bool {
        let mut states = EntryStates::default();
        let Some(marked) = self.mark_batch(batch, &mut states) else {
            return false;
        };

        // swap out the local batch, the batch will be dropped once the reference count drops to zero.
        // we have to use pointers instead of references to avoid breaking aliasing rules
        let batch = state.batch.replace(self.new_batch());

        if marked > 0 {
            // SAFETY: the detached batch is live,
            // and `references` is accessed only through atomic operations.
            let references = unsafe { &(*batch.as_ptr()).references };
            // before link the batch to other threads, adjusting the reference count by `marked + 1` so it will not be dropped during this period
            references.fetch_add(marked.wrapping_add(1), Ordering::Relaxed);

            let active = self.link_batch(batch, marked, &mut states);
            // some previously marked threads may become inactive, adjusting the reference count by delta
            let delta = (marked - active).wrapping_add(1);
            if references.fetch_sub(delta, Ordering::Release) != delta {
                // there are still some references to the batch
                return true;
            }
        }
        // either `mark_batch` founds no active threads, or the `link_batch` founds all active threads become inactive now
        // barrier to synchronize with the release on the retired list and the other release on the reference count
        loom_support::fence(Ordering::Acquire);
        // SAFETY: the count is zero after the [`Ordering::Acquire`] fence, so the batch is exclusively owned now;
        // reconstruct the leaked `Box` exactly once.
        let batch = unsafe { Box::from_raw(batch.as_ptr()) };
        batch.free();

        true
    }
}
