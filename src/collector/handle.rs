use crate::collector::{
    Collector, EnterAction, Entry, LeaveAction, TLSRetiredList, TLSThreadState, ThreadState,
};
use crate::loom_support;
use crate::loom_support::Arc;
use crate::thread_local::{OwnedThread, new_thread};
use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;

trait CollectorHandle<T>: Sized
where
    T: Send,
{
    fn collector(&self) -> &Collector<T>;

    fn thread_state(&'_ self) -> TLSThreadState<'_, T>;

    fn retired_list(&'_ self) -> TLSRetiredList<'_, T>;

    fn enter(&'_ self) -> Guard<'_, T, Self> {
        let state = self.thread_state();
        let collector = self.collector();

        let active_epoch = &state.current_epoch;

        let epoch_id = match state.enter() {
            EnterAction::None(epoch_id) => {
                // try to advance the epoch
                if state.can_advance_epoch(collector.epoch_size) {
                    let retired_list = self.retired_list();
                    let detached = retired_list
                        .swap(std::ptr::null_mut(), Ordering::Acquire)
                        .cast_const();
                    state.advance_epoch(detached);
                }
                epoch_id
            }
            EnterAction::Activate(epoch_id) => {
                let retired_list = self.retired_list();
                // synchronize with the [`Ordering::SeqCst`] barrier in [`Collector::flush_batch`]
                retired_list.store(std::ptr::null_mut(), Ordering::SeqCst);
                // loom cannot handle [`Ordering::SeqCst`] on atomic variables properly
                #[cfg(feature = "_loom")]
                loom_support::fence(Ordering::SeqCst);
                // reset the pins to 1
                active_epoch.pins.set(1);
                epoch_id
            }
        };
        Guard {
            handle: self,
            epoch_id,
            state,
        }
    }

    fn release(begin: *const Entry<T>) {
        // adjusting reference counts of retired batches
        let mut current = begin;
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

    fn leave(&self, epoch_id: usize, state: &ThreadState<T>) {
        match state.leave(epoch_id) {
            LeaveAction::None => {}
            LeaveAction::Release(last_epoch) => {
                Self::release(last_epoch.begin.get());
            }
            LeaveAction::Defer => {}
            LeaveAction::Deactivate => {
                // mark the thread as inactive
                let begin = self
                    .retired_list()
                    .swap(Entry::INACTIVE, Ordering::AcqRel)
                    .cast_const();
                Self::release(begin);
            }
            LeaveAction::DeactivateAndRelease(last_epoch) => {
                // mark the thread as inactive
                let current_head = self
                    .retired_list()
                    .swap(Entry::INACTIVE, Ordering::AcqRel)
                    .cast_const();
                // read the last head before releasing, to avoid recursive operations during retiring try to advance the epoch
                let last_head = last_epoch.begin.get();
                // reverse ordered release
                Self::release(current_head);
                Self::release(last_head);
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
    epoch_id: usize,
    state: TLSThreadState<'g, T>,
}

#[allow(private_bounds)]
impl<'g, T, H> Guard<'g, T, H>
where
    T: Send,
    H: CollectorHandle<T>,
{
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
        self.handle.leave(self.epoch_id, &*self.state)
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
        self.enter()
    }
}

impl<T> CollectorHandle<T> for LocalHandle<'_, T>
where
    T: Send,
{
    fn collector(&self) -> &Collector<T> {
        self.collector
    }

    fn thread_state(&self) -> TLSThreadState<'_, T> {
        self.collector.current_thread_state()
    }

    fn retired_list(&self) -> TLSRetiredList<'_, T> {
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
        self.enter()
    }
}

impl<T> CollectorHandle<T> for OwnedHandle<'_, T>
where
    T: Send,
{
    fn collector(&self) -> &Collector<T> {
        self.collector
    }

    fn thread_state(&'_ self) -> TLSThreadState<'_, T> {
        self.collector.owned_thread_state(&self.thread)
    }

    fn retired_list(&'_ self) -> TLSRetiredList<'_, T> {
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
        self.enter()
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
    fn collector(&self) -> &Collector<T> {
        &*self.collector
    }

    fn thread_state(&'_ self) -> TLSThreadState<'_, T> {
        self.collector.current_thread_state()
    }

    fn retired_list(&self) -> TLSRetiredList<'_, T> {
        self.collector.current_retired_list()
    }
}
