use criterion::measurement::WallTime;
use criterion::{
    BatchSize, BenchmarkGroup, Criterion, Throughput, criterion_group, criterion_main,
};
use crossbeam_utils::sync::WaitGroup;
use seize::Guard;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::thread::Scope;

const THREADS: usize = 16;
const BATCH_SIZE: usize = 32;
const OPERATION_COUNT: usize = 128;
const GUARD_COUNT: usize = 64;

fn pin_crossbeam(
    group: &mut BenchmarkGroup<WallTime>,
    collector: &crossbeam_epoch::Collector,
    resources: &[AtomicPtr<()>; OPERATION_COUNT],
) {
    group.bench_function("crossbeam", |b| {
        b.iter_batched(
            || collector.register(),
            |handle| {
                for _ in 0..GUARD_COUNT {
                    let guard = handle.pin();
                    for resource in resources {
                        black_box(resource.load(Ordering::Relaxed));
                    }
                    drop(guard)
                }
            },
            BatchSize::PerIteration,
        );
    });
}

fn recursive_pin_crossbeam(
    group: &mut BenchmarkGroup<WallTime>,
    collector: &crossbeam_epoch::Collector,
    resources: &[AtomicPtr<()>; OPERATION_COUNT],
) {
    group.bench_function("crossbeam", |b| {
        b.iter_batched(
            || collector.register(),
            |handle| {
                let guards: [_; GUARD_COUNT] = std::array::from_fn(|_| {
                    let guard = handle.pin();
                    for resource in resources {
                        black_box(resource.load(Ordering::Relaxed));
                    }
                    guard
                });
                drop(guards);
            },
            BatchSize::PerIteration,
        );
    });
}

fn pin_seize(
    group: &mut BenchmarkGroup<WallTime>,
    collector: &seize::Collector,
    resources: &[AtomicPtr<()>; OPERATION_COUNT],
) {
    group.bench_function("seize", |b| {
        b.iter(|| {
            for _ in 0..GUARD_COUNT {
                let guard = collector.enter();
                for resource in resources {
                    black_box(guard.protect(&resource, Ordering::Relaxed));
                }
                drop(guard)
            }
        })
    });
}

fn recursive_pin_seize(
    group: &mut BenchmarkGroup<WallTime>,
    collector: &seize::Collector,
    resources: &[AtomicPtr<()>; OPERATION_COUNT],
) {
    group.bench_function("seize", |b| {
        b.iter(|| {
            let guards: [_; GUARD_COUNT] = std::array::from_fn(|_| {
                let guard = collector.enter();
                for resource in resources {
                    black_box(guard.protect(&resource, Ordering::Relaxed));
                }
                guard
            });
            drop(guards);
        })
    });
}

fn pin_hyaline<T>(
    group: &mut BenchmarkGroup<WallTime>,
    collector: &hyaline::Collector<T>,
    resources: &[AtomicPtr<()>; OPERATION_COUNT],
) where
    T: Send,
{
    group.bench_function("hyaline", |b| {
        b.iter_batched(
            || collector.make_local_handle(),
            |handle| {
                for _ in 0..GUARD_COUNT {
                    let guard = handle.pin();
                    for resource in resources {
                        black_box(resource.load(Ordering::Relaxed));
                    }
                    drop(guard)
                }
            },
            BatchSize::PerIteration,
        );
    });
}

fn recursive_pin_hyaline<T>(
    group: &mut BenchmarkGroup<WallTime>,
    collector: &hyaline::Collector<T>,
    resources: &[AtomicPtr<()>; OPERATION_COUNT],
) where
    T: Send,
{
    group.bench_function("hyaline", |b| {
        b.iter_batched(
            || collector.make_local_handle(),
            |handle| {
                let guards: [_; GUARD_COUNT] = std::array::from_fn(|_| {
                    let guard = handle.pin();
                    for resource in resources {
                        black_box(resource.load(Ordering::Relaxed));
                    }
                    guard
                });
                drop(guards);
            },
            BatchSize::PerIteration,
        );
    });
}

fn make_threads<'scope, 'env, T>(scope: &'scope Scope<'scope, 'env>, task: T) -> Arc<AtomicBool>
where
    T: FnOnce(WaitGroup, &AtomicBool) + Send + Copy + 'scope,
    'env: 'scope,
{
    let ready = WaitGroup::new();
    let exit = Arc::new(AtomicBool::new(false));
    for _ in 0..THREADS {
        let ready = ready.clone();
        let exit = exit.clone();
        scope.spawn(move || {
            task(ready, &exit);
        });
    }
    ready.wait();
    exit
}

fn reclaim(_: ()) {}

unsafe fn ignore(_: *mut (), _: &seize::Collector) {}

fn pin(c: &mut Criterion) {
    let mut group = c.benchmark_group("pin");
    group.throughput(Throughput::Elements((GUARD_COUNT * OPERATION_COUNT) as u64));
    let resources = [const { AtomicPtr::<()>::new(std::ptr::null_mut()) }; OPERATION_COUNT];

    let crossbeam_collector = crossbeam_epoch::Collector::new();
    pin_crossbeam(&mut group, &crossbeam_collector, &resources);
    drop(crossbeam_collector);

    let seize_collector = seize::Collector::new().batch_size(BATCH_SIZE);
    pin_seize(&mut group, &seize_collector, &resources);
    drop(seize_collector);

    let hyaline_collector = hyaline::collector::Collector::<()>::new(BATCH_SIZE, 0);
    pin_hyaline(&mut group, &hyaline_collector, &resources);
    drop(hyaline_collector);
}

fn recursive_pin(c: &mut Criterion) {
    let mut group = c.benchmark_group("recursive_pin");
    group.throughput(Throughput::Elements((GUARD_COUNT * OPERATION_COUNT) as u64));
    let resources = [const { AtomicPtr::<()>::new(std::ptr::null_mut()) }; OPERATION_COUNT];

    let crossbeam_collector = crossbeam_epoch::Collector::new();
    recursive_pin_crossbeam(&mut group, &crossbeam_collector, &resources);
    drop(crossbeam_collector);

    let seize_collector = seize::Collector::new().batch_size(BATCH_SIZE);
    recursive_pin_seize(&mut group, &seize_collector, &resources);
    drop(seize_collector);
    let hyaline_collector = hyaline::collector::Collector::<()>::new(BATCH_SIZE, 0);
    recursive_pin_hyaline(&mut group, &hyaline_collector, &resources);
    drop(hyaline_collector);
}

fn concurrent_pin(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_pin");
    group.throughput(Throughput::Elements((GUARD_COUNT * OPERATION_COUNT) as u64));
    let resources = [const { AtomicPtr::<()>::new(std::ptr::null_mut()) }; OPERATION_COUNT];

    let crossbeam_collector = crossbeam_epoch::Collector::new();
    std::thread::scope(|s| {
        let exit = make_threads(s, |ready, exit| {
            let crossbeam_handle = crossbeam_collector.register();
            drop(ready);
            while !exit.load(Ordering::Relaxed) {
                let crossbeam_guard = crossbeam_handle.pin();
                drop(crossbeam_guard);
            }
        });
        pin_crossbeam(&mut group, &crossbeam_collector, &resources);
        exit.store(true, Ordering::Relaxed);
    });
    drop(crossbeam_collector);

    let seize_collector = seize::Collector::new().batch_size(BATCH_SIZE);
    std::thread::scope(|s| {
        let exit = make_threads(s, |ready, exit| {
            drop(ready);
            while !exit.load(Ordering::Relaxed) {
                let seize_guard = seize_collector.enter();
                drop(seize_guard);
            }
        });
        pin_seize(&mut group, &seize_collector, &resources);
        exit.store(true, Ordering::Relaxed);
    });
    drop(seize_collector);

    let hyaline_collector = hyaline::collector::Collector::<()>::new(BATCH_SIZE, 0);
    std::thread::scope(|s| {
        let exit = make_threads(s, |ready, exit| {
            let hyaline_handle = hyaline_collector.make_local_handle();
            drop(ready);
            while !exit.load(Ordering::Relaxed) {
                let hyaline_guard = hyaline_handle.pin();
                drop(hyaline_guard);
            }
        });
        pin_hyaline(&mut group, &hyaline_collector, &resources);
        exit.store(true, Ordering::Relaxed);
    });
    drop(hyaline_collector);
}

fn concurrent_recursive_pin(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_recursive_pin");
    group.throughput(Throughput::Elements((GUARD_COUNT * OPERATION_COUNT) as u64));
    let resources = [const { AtomicPtr::<()>::new(std::ptr::null_mut()) }; OPERATION_COUNT];

    let crossbeam_collector = crossbeam_epoch::Collector::new();
    std::thread::scope(|s| {
        let exit = make_threads(s, |ready, exit| {
            let crossbeam_handle = crossbeam_collector.register();
            drop(ready);
            while !exit.load(Ordering::Relaxed) {
                let crossbeam_guard = crossbeam_handle.pin();
                drop(crossbeam_guard);
            }
        });
        recursive_pin_crossbeam(&mut group, &crossbeam_collector, &resources);
        exit.store(true, Ordering::Relaxed);
    });
    drop(crossbeam_collector);

    let seize_collector = seize::Collector::new().batch_size(BATCH_SIZE);
    std::thread::scope(|s| {
        let exit = make_threads(s, |ready, exit| {
            drop(ready);
            while !exit.load(Ordering::Relaxed) {
                let seize_guard = seize_collector.enter();
                drop(seize_guard);
            }
        });
        recursive_pin_seize(&mut group, &seize_collector, &resources);
        exit.store(true, Ordering::Relaxed);
    });
    drop(seize_collector);

    let hyaline_collector = hyaline::collector::Collector::<()>::new(BATCH_SIZE, 0);
    std::thread::scope(|s| {
        let exit = make_threads(s, |ready, exit| {
            let hyaline_handle = hyaline_collector.make_local_handle();
            drop(ready);
            while !exit.load(Ordering::Relaxed) {
                let hyaline_guard = hyaline_handle.pin();
                drop(hyaline_guard);
            }
        });
        recursive_pin_hyaline(&mut group, &hyaline_collector, &resources);
        exit.store(true, Ordering::Relaxed);
    });
    drop(hyaline_collector);
}

fn pin_and_retire(c: &mut Criterion) {
    let mut group = c.benchmark_group("pin_and_retire");
    group.throughput(Throughput::Elements((GUARD_COUNT * OPERATION_COUNT) as u64));
    let resources = [const { AtomicPtr::<()>::new(std::ptr::null_mut()) }; OPERATION_COUNT];

    let crossbeam_collector = crossbeam_epoch::Collector::new();
    std::thread::scope(|s| {
        let exit = make_threads(s, |ready, exit| {
            let crossbeam_handle = crossbeam_collector.register();
            drop(ready);
            while !exit.load(Ordering::Relaxed) {
                let guard = crossbeam_handle.pin();
                for _ in 0..BATCH_SIZE {
                    guard.defer(|| {});
                }
            }
        });
        pin_crossbeam(&mut group, &crossbeam_collector, &resources);
        exit.store(true, Ordering::Relaxed);
    });
    drop(crossbeam_collector);

    let seize_collector = seize::Collector::new().batch_size(BATCH_SIZE);
    std::thread::scope(|s| {
        let exit = make_threads(s, |ready, exit| {
            drop(ready);
            while !exit.load(Ordering::Relaxed) {
                let guard = seize_collector.enter();
                for _ in 0..BATCH_SIZE {
                    unsafe { guard.defer_retire(std::ptr::null_mut(), ignore) };
                }
            }
        });
        pin_seize(&mut group, &seize_collector, &resources);
        exit.store(true, Ordering::Relaxed);
    });
    drop(seize_collector);

    let hyaline_collector = hyaline::collector::Collector::<()>::new(BATCH_SIZE, 0);
    std::thread::scope(|s| {
        let exit = make_threads(s, |ready, exit| {
            let hyaline_handle = hyaline_collector.make_local_handle();
            drop(ready);
            while !exit.load(Ordering::Relaxed) {
                let guard = hyaline_handle.pin();
                for _ in 0..BATCH_SIZE {
                    guard.retire((), reclaim);
                }
            }
        });
        pin_hyaline(&mut group, &hyaline_collector, &resources);
        exit.store(true, Ordering::Relaxed);
    });
    drop(hyaline_collector);
}

fn retire(c: &mut Criterion) {
    let mut group = c.benchmark_group("retire");
    group.throughput(Throughput::Elements((GUARD_COUNT * OPERATION_COUNT) as u64));

    let crossbeam_collector = crossbeam_epoch::Collector::new();
    std::thread::scope(|s| {
        let exit = make_threads(s, |ready, exit| {
            let handle = crossbeam_collector.register();
            drop(ready);
            while !exit.load(Ordering::Relaxed) {
                let guard = handle.pin();
                drop(guard);
            }
        });
        group.bench_function("crossbeam", |b| {
            b.iter_batched(
                || crossbeam_collector.register(),
                |handle| {
                    for _ in 0..GUARD_COUNT {
                        let guard = handle.pin();
                        for _ in 0..OPERATION_COUNT {
                            guard.defer(|| {});
                        }
                    }
                },
                BatchSize::PerIteration,
            );
        });
        exit.store(true, Ordering::Relaxed);
    });
    drop(crossbeam_collector);

    let seize_collector = seize::Collector::new().batch_size(BATCH_SIZE);
    std::thread::scope(|s| {
        let exit = make_threads(s, |ready, exit| {
            drop(ready);
            while !exit.load(Ordering::Relaxed) {
                let guard = seize_collector.enter();
                drop(guard);
            }
        });
        group.bench_function("seize", |b| {
            b.iter(|| {
                for _ in 0..GUARD_COUNT {
                    let guard = seize_collector.enter();
                    for _ in 0..OPERATION_COUNT {
                        unsafe { guard.defer_retire(std::ptr::null_mut(), ignore) };
                    }
                }
            });
        });
        exit.store(true, Ordering::Relaxed);
    });
    drop(seize_collector);

    let hyaline_collector = hyaline::collector::Collector::<()>::new(BATCH_SIZE, 0);
    std::thread::scope(|s| {
        let exit = make_threads(s, |ready, exit| {
            let handle = hyaline_collector.make_local_handle();
            drop(ready);
            while !exit.load(Ordering::Relaxed) {
                let guard = handle.pin();
                drop(guard);
            }
        });
        group.bench_function("hyaline", |b| {
            b.iter_batched(
                || hyaline_collector.make_local_handle(),
                |handle| {
                    for _ in 0..GUARD_COUNT {
                        let guard = handle.pin();
                        for _ in 0..OPERATION_COUNT {
                            guard.retire((), reclaim);
                        }
                    }
                },
                BatchSize::PerIteration,
            );
        });
        exit.store(true, Ordering::Relaxed);
    });
    drop(hyaline_collector);
}

criterion_group!(
    benches,
    pin,
    recursive_pin,
    concurrent_pin,
    concurrent_recursive_pin,
    pin_and_retire,
    retire,
);
criterion_main!(benches);
