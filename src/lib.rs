//! # keystory
//!
//! A crash-resilient key/value store engine, built from scratch in approval-gated
//! phases. `ROADMAP.md` holds the state at handoff, the open backlog, the design
//! decisions in force, and a condensed history of each phase.
//!
//! ## What is here
//!
//! | Module | Role |
//! |--------|------|
//! | [`engine`] | [`Store`], the public entry point: group commit (one WAL append + `fsync` per group), atomic batches, RCU publish; `open` = latest usable checkpoint + WAL-tail replay. |
//! | [`wal`] | Append-only, segmented, CRC-guarded write-ahead log: torn-tail repair at open, batch records, segment boundaries for checkpoints. |
//! | [`snapshot`] | Durable checkpoint file: streamed body, tmp + `fsync` + atomic rename, previous generation retained. |
//! | [`rcu`] | `RcuSwap<T>`: the snapshot cell readers load without touching the commit lock. |
//! | [`types`] | `Op`, `Entry`, `Snapshot`: the deterministic, index-ordered state model. |
//! | [`checker`] | Offline MVCC sequential-consistency oracle used by the Jepsen-lite tests. |
//! | [`raft`] | Pure Raft FSM, an in-process synchronous cluster driver (sticky leader, leader reads), and an `async fn` facade. |
//! | [`btree_store`] | Ordered B+ tree with a CRC-guarded document format; standalone, not used by `Store`. |
//! | [`valuestore`] | Off-heap blob log for large values with ids stable across compaction: a tested primitive, not yet wired into `Store`. |
//! | [`epoch_rcu`] | Thread-safe epoch-based reclamation model; not the hot path (see its docs). |
//! | [`rt`] | Single-threaded cooperative async runtime with hand-built wakers, slot reuse and in-task spawning. |
//! | `asyncio` | `mio` reactor: real kernel readiness on a Unix-stream pair, with deadlines (Unix only). |
//!
//! ## Policies
//!
//! * **Dependencies:** exactly one external crate, `mio`, added in Phase 5 for
//!   non-blocking I/O. Everything else (CRC-32, binary formats, consensus, runtime) is
//!   hand-written; Phases 1 to 4 were std-only.
//! * **`unsafe`:** confined to the raw-waker vtable in [`rt`], each block annotated.
//! * **Ordering:** only the logical commit index orders events. Nothing depends on
//!   wall-clock time, which is what makes recovery deterministic.
//!
//! ## Honest status
//!
//! The durable single-node engine and the Raft layer are **not yet integrated**: the
//! cluster driver keeps in-memory logs and state and never touches [`Store`]. The
//! cooperative runtime and the `mio` reactor are likewise not yet connected to each
//! other. Both head the Phase 7 backlog in `ROADMAP.md`.
#![allow(clippy::module_name_repetitions)]
#![warn(clippy::all)]

pub mod epoch_rcu;
pub mod rcu;
pub mod snapshot;
pub mod types;
pub mod wal;

pub mod btree_store;
pub mod checker;
mod crc;
pub mod engine;
pub mod raft;
pub mod valuestore;

#[cfg(unix)]
pub mod asyncio;
pub mod rt;

/// Re-export the public entry point (and its counters) at the crate root.
pub use engine::{Stats, Store};
/// Re-export the core value/op types for ergonomic `use keystory::...`.
pub use types::{Entry, Op, Shared, Snapshot};
