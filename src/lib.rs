//! This crate is intentionally dependency-free (std-only), Phase 1 of a Raft-modelled
//! crash-resilient, replicated KV store built in ordered phases.
//! No external crates, no consensus library: the WAL, RCU snapshot and engine are written
//! from scratch in this phase.
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::doc_lazy_continuation)] // long module-doc prose uses continuation lines
#![warn(clippy::all)]
pub mod rcu;
pub mod types;
pub mod wal;
pub mod snapshot;

mod crc;
pub mod valuestore;
pub mod engine;
pub mod raft;
pub mod checker;

pub mod rt;

/// Re-export the public entry point at the crate root.
pub use engine::Store;
/// Re-export the core value/op types for ergonomic `use keystore::...`.
pub use types::{Entry, Op, Snapshot};
