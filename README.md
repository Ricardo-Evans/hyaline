# `hyaline`

A safe and efficient deferred resource reclamation library for concurrent scenarios, powered by the **Hyaline-1** algorithm.

## Overview

In high-performance concurrent programming—especially in lock-free data structures—it is challenging to determine when a removed resource can be safely reclaimed. While a writer may atomically update a shared pointer, concurrent readers might still hold references to the old resource; freeing it immediately leads to use-after-free bugs.

A common approach in Rust is atomic reference counting (`Arc`). However, `Arc` requires atomic increments and decrements on every read access, causing severe cache-line bouncing across CPU cores under read-heavy workloads.

**Safe Memory Reclamation (SMR)** solves this by deferring reclamation until all concurrent readers that might have observed the resource have exited their critical sections. Various SMR schemes exist to track active readers, such as Quiescent State-Based Reclamation (QSBR), Epoch-Based Reclamation (EBR), and Hazard Pointers.

`hyaline` is an SMR implementation based on **Hyaline-1**, a fast reference-counted reclamation scheme that avoids frequent counter contention. Instead of blindly counting references on every resource access, it only tracks references for resources that have been removed and are awaiting reclamation. In read-heavy workloads, only a small fraction of resources undergoes this process, thus significantly improving overall performance.

## Usage

Here is a typical reader-writer example:

```rust
// you may need some wrappers around the raw pointers, as `Collector` requires the resource type to be `Send` 
fn create() -> *mut ResourceType {
    todo!("some creation logic")
}

fn reclaim(resource: *mut ResourceType) {
    todo!("some reclamation logic")
}

// the core logic
fn main() {
    let collector = Collector::new(batch_size, epoch_size);
    let resource = AtomicPtr::new(create());

    // reader
    todo!("maybe some thread spawning")
    let handle = collector.make_local_handle();
    loop {
        // All resources accessed after pinning are protected by the guard
        let guard = handle.pin();
        // Load the current pointer
        let pointer = resource.load(Ordering::Acquire);
        // SAFETY: The pointed-to resource will not be freed while protected by the guard
        some_operation(unsafe { &*pointer });
        // The guard is dropped here
    }

    // writer
    todo!("maybe some thread spawning")
    let handle = collector.make_local_handle();
    let guard = handle.pin();
    // From this point on, new readers will no longer observe the old resource
    let to_be_reclaimed = resource.swap(create(), Ordering::AcqRel);
    // reclaim will be called once hyaline ensures all potential readers holding the old resource have finished
    guard.retire(to_be_reclaimed, reclaim)
}
```

Beyond basic usage, `hyaline` also supports more advanced features, including `Send` handles (useful for coroutines), epochs (useful in asynchronous programming), and more. For more details, please refer to the [documentation](https://docs.rs/hyaline/latest).

## Comparison

### Functionality

- **Generic resources**: Unlike [`seize`](https://crates.io/crates/seize), whose retirement API accepts raw pointers only, `hyaline` can defer an operation on any `Send` value. This is useful when the resource to reclaim is not represented by a raw pointer.
- **Movable handles**: `OwnedHandle` is `Send`, so one logical participant can move between threads, including across `.await` points. `seize` also supports this use case with `OwnedGuard`; each owned guard has its own participant slot, while a `hyaline` owned handle can create multiple guards. In a typical asynchronous server loop, keeping one `OwnedHandle` for the lifetime of the loop is useful because creating either a `hyaline` `OwnedHandle` or a `seize` `OwnedGuard` requires locking to allocate a participant slot.
- **Overlapping guards**: Asynchronous operations may keep older guards alive while the same handle continues to create newer ones. Without a way to separate them, continuously overlapping guards can keep the participant active and delay reclamation indefinitely. `hyaline` uses configurable local epochs to group guards, allowing an older group to release its references once all of its guards have been dropped, even if newer guards remain active.
- **Reclamation timing**: `hyaline` reclaims eligible batches when a retired batch is published or when dropping a guard releases its final reference. EBR depends on global epoch advancement; implementations such as `crossbeam-epoch` periodically perform collection during `pin` or an explicit `flush`. This makes the exact reclamation time less predictable and can put reclamation work on the read-side hot path. Hazard pointers can reclaim with finer granularity, but each protected load generally requires publishing and validating a hazard pointer, and reclamation requires scanning those pointers, adding overhead to both access and reclamation.

### Performance

In the [benchmarks](#benchmarks) below, `hyaline` achieves the highest throughput in many scenarios, particularly under concurrent pinning, and remains competitive in the others. No implementation wins every workload, but `hyaline` provides consistently strong performance across both read-side and retirement-heavy scenarios.

## Correctness

`hyaline` includes tests built with [Loom](https://github.com/tokio-rs/loom), which explores possible thread interleaving under its concurrency model to help detect synchronization and memory-ordering bugs. Enabling the `loom` feature replaces the synchronization primitives used by the tests with their Loom equivalents and runs each concurrent test inside `loom::model`.

Run the Loom test suite with:

```console
cargo test --features loom
```

## Benchmarks

The tables below list [Criterion](https://bheisler.github.io/criterion.rs/book/) throughput from `benches/pin.rs`, comparing `hyaline` with [`crossbeam-epoch`](https://docs.rs/crossbeam-epoch) and [`seize`](https://docs.rs/seize). The results depend on the CPU architecture, the operating system version, and the rest of the machine configuration, and random OS thread scheduling can shift them as well. Treat them as a reference only.

Each sample runs 64 guards with 128 operations per guard. `recursive_*` holds every guard at once instead of dropping it before the next pin. `concurrent_*` runs the measured thread alongside 16 background threads that pin and drop; `pin_and_retire` measures that read path while the background threads retire, and `retire` measures retirement while the background threads pin. Cells are point estimates in million-operations per second. Higher is faster.

Raw benchmark results are available [here](https://github.com/Ricardo-Evans/hyaline/releases/tag/benchmark).

#### Benchmark on AMD EPYC 7K62 (x86-64) with Linux 6.6.119

| Benchmark                  | crossbeam-epoch |      seize |    hyaline |
|----------------------------|----------------:|-----------:|-----------:|
| `pin`                      |          2289.9 |     2126.0 | **2309.8** |
| `recursive_pin`            |          2278.5 |     2372.1 | **2476.3** |
| `concurrent_pin`           |          593.02 |     1214.2 | **1332.7** |
| `concurrent_recursive_pin` |          888.20 |     1098.5 | **1348.0** |
| `pin_and_retire`           |          19.223 | **1110.5** |     859.45 |
| `retire`                   |      **29.107** |     1.7861 |     9.7960 |

#### Benchmark on Intel i7-12700H (x86-64) with Windows 11 26100.9457

| Benchmark                  | crossbeam-epoch |      seize |    hyaline |
|----------------------------|----------------:|-----------:|-----------:|
| `pin`                      |          3442.2 | **3685.0** |     3488.8 |
| `recursive_pin`            |          3478.6 | **3744.6** |     3686.7 |
| `concurrent_pin`           |          970.55 |     1643.5 | **1736.2** |
| `concurrent_recursive_pin` |          1592.4 |     1882.9 | **2140.4** |
| `pin_and_retire`           |          10.334 |     525.80 | **1191.5** |
| `retire`                   |      **77.817** |     6.4946 |     19.933 |

#### Benchmark on AMD 7800X3D (x86-64) with Linux 6.18.58.1 (WSL2)

| Benchmark                  | crossbeam-epoch |      seize |    hyaline |
|----------------------------|----------------:|-----------:|-----------:|
| `pin`                      |          2244.9 |     2247.8 | **2332.3** |
| `recursive_pin`            |          2248.0 |     4045.1 | **4177.8** |
| `concurrent_pin`           |          859.28 | **1106.3** |     1032.7 |
| `concurrent_recursive_pin` |          1023.7 |     1906.5 | **2118.8** |
| `pin_and_retire`           |          20.666 | **982.83** |     815.74 |
| `retire`                   |      **67.529** |     2.5039 |     29.810 |

#### Benchmark on Kunpeng 920 (aarch64) with Linux 6.6.98

| Benchmark                  | crossbeam-epoch |      seize |    hyaline |
|----------------------------|----------------:|-----------:|-----------:|
| `pin`                      |      **1626.0** |     1129.7 |     1335.1 |
| `recursive_pin`            |      **1639.1** |     1092.7 |     1191.5 |
| `concurrent_pin`           |          575.63 |     1131.0 | **1325.9** |
| `concurrent_recursive_pin` |          1155.5 |     1119.2 | **1194.3** |
| `pin_and_retire`           |          55.495 | **965.08** |     788.75 |
| `retire`                   |      **39.476** |     2.2073 |     19.623 |
