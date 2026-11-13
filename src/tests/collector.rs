use crate::loom_support::{mpsc, spawn};
use crate::tests::run;
use crate::{Collector, Handle};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

#[derive(Debug, Clone, Copy)]
struct Resource(&'static AtomicUsize);

impl Resource {
    fn reset(inner: &'static AtomicUsize) -> Self {
        inner.store(0, Ordering::Relaxed);
        Self(inner)
    }

    fn reclaim(self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    fn reclaimed_times(self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

#[test]
fn pin_blocks_reclaim() {
    static RESOURCE: AtomicUsize = AtomicUsize::new(0);

    run(|| {
        let resource = Resource(&RESOURCE);
        let collector = Collector::new(1, 0);
        let handle = collector.make_local_handle();
        let guard = handle.pin();
        guard.retire(resource, Resource::reclaim);
        assert_eq!(resource.reclaimed_times(), 0);
        drop(guard);
        assert_eq!(resource.reclaimed_times(), 1);
    });
}

#[test]
fn overlap_blocks_reclaim() {
    static RESOURCE: AtomicUsize = AtomicUsize::new(0);

    run(|| {
        let resource = Resource::reset(&RESOURCE);
        let collector = Collector::new(1, 0);
        let handle = collector.make_local_handle();
        let outer = handle.pin();
        let inner = handle.pin();
        outer.retire(resource, Resource::reclaim);
        assert_eq!(resource.reclaimed_times(), 0);
        drop(outer);
        assert_eq!(resource.reclaimed_times(), 0);
        drop(inner);
        assert_eq!(resource.reclaimed_times(), 1);
    });
}

#[test]
fn local_peer_blocks() {
    static RESOURCE: AtomicUsize = AtomicUsize::new(0);

    run(|| {
        let resource = Resource::reset(&RESOURCE);
        let collector = Arc::new(Collector::new(2, 0));
        let local = collector.make_local_handle();
        let local_guard = local.pin();
        let collector_for_worker = collector.clone();

        let worker = spawn(move || {
            let handle = collector_for_worker.make_local_handle();
            let guard = handle.pin();
            guard.retire(resource, Resource::reclaim);
            guard.retire(resource, Resource::reclaim);
            drop(guard);
        });

        worker.join().unwrap();
        assert_eq!(resource.reclaimed_times(), 0);
        drop(local_guard);
        assert_eq!(resource.reclaimed_times(), 2);
    });
}

#[test]
fn owned_blocks_reclaim() {
    static RESOURCE: AtomicUsize = AtomicUsize::new(0);

    run(|| {
        let resource = Resource::reset(&RESOURCE);
        let collector = Collector::new(2, 0);
        let local = collector.make_local_handle();
        let handle = collector.make_owned_handle();
        let owned_guard = handle.pin();
        let local_guard = local.pin();
        owned_guard.retire(resource, Resource::reclaim);
        owned_guard.retire(resource, Resource::reclaim);
        drop(local_guard);
        assert_eq!(resource.reclaimed_times(), 0);
        drop(owned_guard);
        assert_eq!(resource.reclaimed_times(), 2);
    });
}

#[test]
fn owned_slot_reclaims() {
    static RESOURCE: AtomicUsize = AtomicUsize::new(0);

    run(|| {
        let resource = Resource::reset(&RESOURCE);
        let collector = Collector::new(1, 0);
        let local = collector.make_local_handle();
        let handle = collector.make_owned_handle();
        let owned_guard = handle.pin();
        owned_guard.retire(resource, Resource::reclaim);
        let local_guard = local.pin();
        drop(owned_guard);
        assert_eq!(resource.reclaimed_times(), 1);
        drop(local_guard);
    });
}

#[test]
fn overlap_makes_progress() {
    static RESOURCE: AtomicUsize = AtomicUsize::new(0);

    run(|| {
        let resource = Resource::reset(&RESOURCE);
        let collector = Collector::new(1, 1);
        let handle = collector.make_local_handle();
        let mut guard = handle.pin();
        let mut retired = 0;
        let mut seen = 0;
        let mut steps = 0;

        for _ in 0..8 {
            guard.retire(resource, Resource::reclaim);
            retired += 1;
            let next = handle.pin();
            drop(guard);
            guard = next;
            let now = resource.reclaimed_times();
            assert!(now < retired);
            if now > seen {
                steps += 1;
                seen = now;
            }
            if steps >= 2 {
                break;
            }
        }

        assert!(steps >= 2);
        drop(guard);
        assert_eq!(resource.reclaimed_times(), retired);
    });
}

#[test]
fn concurrent_reclaims_once() {
    static RESOURCE: AtomicUsize = AtomicUsize::new(0);

    run(|| {
        let resource = Resource::reset(&RESOURCE);
        let collector = Arc::new(Collector::new(2, 0));
        let handle = collector.make_local_handle();
        let guard = handle.pin();
        let (ready_tx, ready_rx) = mpsc::channel();
        let collector_for_worker = collector.clone();

        let worker = spawn(move || {
            let handle = collector_for_worker.make_local_handle();
            let guard = handle.pin();
            ready_tx.send(()).unwrap();
            guard.retire(resource, Resource::reclaim);
            guard.retire(resource, Resource::reclaim);
        });
        ready_rx.recv().unwrap();
        guard.retire(resource, Resource::reclaim);
        guard.retire(resource, Resource::reclaim);
        drop(guard);
        worker.join().unwrap();
        assert_eq!(resource.reclaimed_times(), 4);
    });
}
