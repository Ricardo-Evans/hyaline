/// Infrastructure for deferred reclamation of memory allocated through [`Box`].
pub mod boxed;
/// Infrastructure for defer execution of arbitrary closures.
pub mod closure;
/// The implementations of the core algorithm, including the collector, handles, and guards.
pub mod collector;
mod loom_support;
mod thread_local;

#[cfg(test)]
mod tests;

pub use collector::*;
