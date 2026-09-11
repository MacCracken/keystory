//! # keystory
//!
//! A crash-resilient key/value store engine, built from scratch in approval-gated
//! phases. `ROADMAP.md` is the design log: the threat/failure audit of each phase, the
//! decisions taken, and the open backlog.
//!
//! ## What is here
//!
//! | Module | Role |
//! |--------|------|
//! | [`engine`] | [`Store`], the public entry point: commit lock, WAL append + `fsync`, then RCU publish; `open` = last checkpoint + WAL-tail replay. |
//! | [`wal`] | Append-only, segmented, CRC-guarded write-ahead log with torn-tail detection. |
//! | [`snapshot`] | Durable checkpoint file: streamed body, tmp + `fsync` + atomic rename. |
//! | [`rcu`] | `RcuSwap<T>`: the snapshot cell readers load without touching the commit lock. |
//! | [`types`] | `Op`, `Entry`, `Snapshot`: the deterministic, index-ordered state model. |
//! | [`checker`] | Offline MVCC sequential-consistency oracle used by the Jepsen-lite tests. |
//! | [`raft`] | Pure Raft FSM, an in-process synchronous cluster driver, and an `async fn` facade. |
//! | [`btree_store`] | Ordered B+ tree with a CRC-guarded document format; standalone, not used by `Store`. |
//! | [`valuestore`] | Off-heap blob log for large values: a tested primitive, not yet wired into `Store`. |
//! | [`epoch_rcu`] | Epoch-based reclamation model; not the hot path (see its docs). |
//! | [`rt`] | Single-threaded cooperative async runtime with hand-built wakers. |
//! | `asyncio` | `mio` reactor: real kernel readiness on a Unix-stream pair (Unix only). |
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
//! other. `ROADMAP.md` tracks both under the consolidation backlog.
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

/// Re-export the public entry point at the crate root.
pub use engine::Store;
/// Re-export the core value/op types for ergonomic `use keystory::...`.
pub use types::{Entry, Op, Snapshot};
