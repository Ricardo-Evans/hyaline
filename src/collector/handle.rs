use crate::collector::{Collector, TLSRetiredList, TLSThreadState};
use crate::handle::sealed::Handle as SealedHandle;
use crate::thread_local::{OwnedThread, new_thread};
use std::marker::PhantomData;

mod sealed {
    use crate::collector::{EnterAction, Entry, LeaveAction, TLSRetiredList, TLSThreadState};
    use crate::{Collector, Guard};
    use std::marker::PhantomData;
    use std::sync::atomic::Ordering;

    pub(super) trait Handle<T>: Sized
    where
        T: Send,
    {
        fn collector(&self) -> &Collector<T>;

        fn thread_state(&'_ self) -> TLSThreadState<'_, T>;

        fn retired_list(&'_ self) -> TLSRetiredList<'_, T>;

        #[inline]
        fn enter(&'_ self) -> Guard<'_, T, Self> {
            let state = self.thread_state();
            let collector = self.collector();

            let epoch_id = match state.enter(collector.epoch_size) {
                EnterAction::None(epoch_id) => epoch_id,
                EnterAction::Advance(epoch_id) => {
                    let retired_list = self.retired_list();
                    let detached = retired_list
                        .swap(std::ptr::null_mut(), Ordering::Acquire)
                        .cast_const();
                    state.advance_epoch(detached);
                    epoch_id
                }
                EnterAction::Activate(epoch_id) => {
                    let retired_list = self.retired_list();
                    // synchronize with the [`Ordering::SeqCst`] barrier in [`Collector::flush_batch`]
                    retired_list.store(std::ptr::null_mut(), Ordering::SeqCst);
                    // loom cannot handle [`Ordering::SeqCst`] on atomic variables properly
                    #[cfg(feature = "_loom")]
                    crate::loom_support::fence(Ordering::SeqCst);
                    epoch_id
                }
            };
            Guard {
                handle: self,
                epoch_id,
                _phantom: PhantomData,
            }
        }

        fn release(begin: *const Entry<T>) {
            // adjusting reference counts of retired batches
            let mut current = begin;
            while !current.is_null() {
                // SAFETY: `current` is a published entry protected by its outstanding batch reference,
                // which should be at least 1 since this thread is leaving
                let batch = unsafe { (*current).batch };
                // SAFETY: published entries have an initialized immutable `next`, read before releasing the current batch reference.
                current = unsafe { (*current).state.next() };
                // Safety: the batch is not dropped yet since this thread is leaving, which means the reference count does not drop to zero yet,
                // and only const references to the reference count are made in this project.
                let references = unsafe { &batch.as_ref().references };
                // a `Release` sub with an `Acquire` fence only in the zero case would also be correct but not worthwhile here,
                // as this counter usually stays small, unlike a general-purpose reference counter.
                if references.fetch_sub(1, Ordering::AcqRel) == 1 {
                    // SAFETY: this was the final reference; reconstruct the leaked `Box` exactly once.
                    let batch = unsafe { Box::from_raw(batch.as_ptr()) };
                    batch.free();
                }
            }
        }

        #[inline]
        fn leave(&self, epoch_id: usize) {
            let state = self.thread_state();
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
}

#[allow(private_bounds)]
pub trait Handle<T>: SealedHandle<T>
where
    T: Send,
{
    #[inline]
    fn pin(&'_ self) -> Guard<'_, T, Self> {
        self.enter()
    }

    #[inline]
    fn retire(&self, data: T, operation: fn(T)) {
        let collector = self.collector();
        let state = self.thread_state();
        collector.retire(&*state, data, operation);
    }
}

impl<T, H> Handle<T> for H
where
    T: Send,
    H: SealedHandle<T>,
{
}

pub struct Guard<'g, T, H>
where
    T: Send,
    H: Handle<T>,
{
    handle: &'g H,
    epoch_id: usize,
    _phantom: PhantomData<T>,
}

impl<'g, T, H> Guard<'g, T, H>
where
    T: Send,
    H: Handle<T>,
{
    pub fn retire(&self, data: T, operation: fn(T)) {
        self.handle.retire(data, operation);
    }
}

impl<T, H> Drop for Guard<'_, T, H>
where
    T: Send,
    H: Handle<T>,
{
    fn drop(&mut self) {
        self.handle.leave(self.epoch_id)
    }
}

pub struct LocalHandle<'a, T>
where
    T: Send,
{
    collector: &'a Collector<T>,
    thread_state: TLSThreadState<'a, T>,
    retired_list: TLSRetiredList<'a, T>,
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
            thread_state: collector.current_thread_state(),
            retired_list: collector.current_retired_list(),
            _phantom: PhantomData,
        }
    }
}

impl<T> SealedHandle<T> for LocalHandle<'_, T>
where
    T: Send,
{
    #[inline]
    fn collector(&self) -> &Collector<T> {
        self.collector
    }

    #[inline]
    fn thread_state(&self) -> TLSThreadState<'_, T> {
        self.thread_state.clone()
    }

    #[inline]
    fn retired_list(&self) -> TLSRetiredList<'_, T> {
        self.retired_list.clone()
    }
}

pub struct OwnedHandle<'a, T>
where
    T: Send,
{
    collector: &'a Collector<T>,
    thread_state: TLSThreadState<'a, T>,
    retired_list: TLSRetiredList<'a, T>,
    _thread: OwnedThread,
}

impl<'a, T> OwnedHandle<'a, T>
where
    T: Send,
{
    pub(super) fn new(collector: &'a Collector<T>) -> Self {
        let thread = new_thread();
        // Safety: the thread is owned by the handle
        let thread_state = unsafe { collector.owned_thread_state(&thread) };
        let retired_list = unsafe { collector.owned_retired_list(&thread) };
        Self {
            collector,
            thread_state,
            retired_list,
            _thread: thread,
        }
    }
}

unsafe impl<T> Send for OwnedHandle<'_, T> where T: Send {}

impl<T> SealedHandle<T> for OwnedHandle<'_, T>
where
    T: Send,
{
    #[inline]
    fn collector(&self) -> &Collector<T> {
        self.collector
    }

    #[inline]
    fn thread_state(&'_ self) -> TLSThreadState<'_, T> {
        self.thread_state.clone()
    }

    #[inline]
    fn retired_list(&'_ self) -> TLSRetiredList<'_, T> {
        self.retired_list.clone()
    }
}
