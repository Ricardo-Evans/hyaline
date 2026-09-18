use crate::collector::{Collector, Entry, TLSRetiredList, TLSThreadState, ThreadState};
use crate::loom_support;
use crate::loom_support::{Arc, AtomicPtr};
use crate::thread_local::{OwnedThread, new_thread};
use std::cell::Cell;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::Ordering;

trait CollectorHandle<T>
where
    T: Send,
{
    type ThreadState<'a>: Deref<Target = ThreadState<T>> + 'a
    where
        Self: 'a;
    type RetiredList<'a>: Deref<Target = AtomicPtr<Entry<T>>> + 'a
    where
        Self: 'a;

    fn collector(&self) -> &Collector<T>;

    fn thread_state(&'_ self) -> Self::ThreadState<'_>;

    fn retired_list(&'_ self) -> Self::RetiredList<'_>;

    fn enter(&'_ self) -> Self::ThreadState<'_> {
        let state = self.thread_state();
        let state_ref: &ThreadState<T> = &*state;
        let guards = &state_ref.guards;
        let existing_guards = guards.get();
        if existing_guards == 0 {
            // synchronize with the [`Ordering::SeqCst`] barrier in [`Self::flush_batch`]
            self.retired_list()
                .store(std::ptr::null_mut(), Ordering::SeqCst);
            // loom cannot handle [`Ordering::SeqCst`] on atomic variables properly
            #[cfg(feature = "_loom")]
            loom_support::fence(Ordering::SeqCst);
        }
        guards.set(
            existing_guards
                .checked_add(1)
                .expect("the guards counter overflowed"),
        );
        state
    }

    fn leave(&self, state: &ThreadState<T>) {
        let guards = &state.guards;
        let remaining_guards = guards.get();
        guards.set(remaining_guards - 1);
        if remaining_guards == 1 {
            // the last guard just leaved, mark the thread as inactive
            let begin = self.retired_list().swap(Entry::INACTIVE, Ordering::AcqRel);

            // adjusting reference counts of retired batches
            let mut current = begin.cast_const();
            while !current.is_null() {
                // SAFETY: `current` is a published entry protected by its outstanding batch reference,
                // which should be at least 1 since this thread is leaving
                let batch = unsafe { (*current).batch };
                // SAFETY: published entries have an initialized immutable `next`,
                // read before releasing the current batch reference.
                current = unsafe { (*current).state.next() };
                // Safety: the batch is not dropped yet since this thread is leaving, which means the reference count does not drop to zero yet,
                // and only const references to the reference count are made in this project.
                let references = unsafe { &batch.as_ref().references };
                if references.fetch_sub(1, Ordering::Release) == 1 {
                    // barrier to synchronize with the other release on the reference count
                    loom_support::fence(Ordering::Acquire);
                    // SAFETY: this was the final reference after the [`Ordering::Acquire`] fence;
                    // reconstruct the leaked `Box` exactly once.
                    let batch = unsafe { Box::from_raw(batch.as_ptr()) };
                    batch.free();
                }
            }
        }
    }
}

#[allow(private_bounds)]
pub struct Guard<'g, T, H>
where
    T: Send,
    H: CollectorHandle<T>,
{
    handle: &'g H,
    state: H::ThreadState<'g>,
}

#[allow(private_bounds)]
impl<'g, T, H> Guard<'g, T, H>
where
    T: Send,
    H: CollectorHandle<T>,
{
    fn new(handle: &'g H) -> Self {
        Guard {
            handle,
            state: handle.enter(),
        }
    }

    pub fn retire(&self, data: T, operation: fn(T)) {
        self.handle
            .collector()
            .retire(&*self.state, data, operation);
    }
}

impl<T, H> Drop for Guard<'_, T, H>
where
    T: Send,
    H: CollectorHandle<T>,
{
    fn drop(&mut self) {
        self.handle.leave(&*self.state)
    }
}

pub struct LocalHandle<'a, T>
where
    T: Send,
{
    collector: &'a Collector<T>,
    /// Marker to make the struct neither [`Send`] nor [`Sync`]
    _phantom: PhantomData<*const ()>,
}

impl<'a, T> LocalHandle<'a, T>
where
    T: Send,
{
    pub(super) fn new(collector: &'a Collector<T>) -> Self {
        Self {
            collector,
            _phantom: PhantomData,
        }
    }

    pub fn pin(&'_ self) -> Guard<'_, T, Self> {
        Guard::new(self)
    }
}

impl<T> CollectorHandle<T> for LocalHandle<'_, T>
where
    T: Send,
{
    type ThreadState<'a>
        = TLSThreadState<'a, T>
    where
        Self: 'a;
    type RetiredList<'a>
        = TLSRetiredList<'a, T>
    where
        Self: 'a;

    fn collector(&self) -> &Collector<T> {
        self.collector
    }

    fn thread_state(&self) -> Self::ThreadState<'_> {
        self.collector.current_thread_state()
    }

    fn retired_list(&self) -> Self::RetiredList<'_> {
        self.collector.current_retired_list()
    }
}

pub struct OwnedHandle<'a, T>
where
    T: Send,
{
    collector: &'a Collector<T>,
    thread: OwnedThread,
}

impl<'a, T> OwnedHandle<'a, T>
where
    T: Send,
{
    pub(super) fn new(collector: &'a Collector<T>) -> Self {
        Self {
            collector,
            thread: new_thread(),
        }
    }

    pub fn pin(&'_ self) -> Guard<'_, T, Self> {
        Guard::new(self)
    }
}

impl<T> CollectorHandle<T> for OwnedHandle<'_, T>
where
    T: Send,
{
    type ThreadState<'a>
        = TLSThreadState<'a, T>
    where
        Self: 'a;
    type RetiredList<'a>
        = TLSRetiredList<'a, T>
    where
        Self: 'a;

    fn collector(&self) -> &Collector<T> {
        self.collector
    }

    fn thread_state(&'_ self) -> Self::ThreadState<'_> {
        self.collector.owned_thread_state(&self.thread)
    }

    fn retired_list(&'_ self) -> Self::RetiredList<'_> {
        self.collector.owned_retired_list(&self.thread)
    }
}

pub struct SharedHandle<T>
where
    T: Send,
{
    collector: Arc<Collector<T>>,
    /// Marker to make the struct not [`Sync`]
    _phantom: PhantomData<Cell<()>>,
}

impl<T> SharedHandle<T>
where
    T: Send,
{
    pub(super) fn new(collector: Arc<Collector<T>>) -> Self {
        Self {
            collector,
            _phantom: PhantomData,
        }
    }

    pub fn pin(&'_ self) -> Guard<'_, T, Self> {
        Guard::new(self)
    }
}

impl<T> Clone for SharedHandle<T>
where
    T: Send,
{
    fn clone(&self) -> Self {
        Self {
            collector: self.collector.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T> CollectorHandle<T> for SharedHandle<T>
where
    T: Send,
{
    type ThreadState<'a>
        = TLSThreadState<'a, T>
    where
        Self: 'a;
    type RetiredList<'a>
        = TLSRetiredList<'a, T>
    where
        Self: 'a;

    fn collector(&self) -> &Collector<T> {
        &*self.collector
    }

    fn thread_state(&self) -> Self::ThreadState<'_> {
        self.collector.current_thread_state()
    }

    fn retired_list(&self) -> Self::RetiredList<'_> {
        self.collector.current_retired_list()
    }
}
