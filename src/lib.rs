//! # keystory
//!
//! A crash-resilient, embedded key/value store. Every write is appended to a checksummed,
//! segmented write-ahead log and `fsync`'d before any reader can see it; readers work on
//! immutable point-in-time snapshots and never wait for writers; concurrent writers share
//! log appends and `fsync`s; and checkpoints fold the log into an atomically published
//! snapshot file while writers keep running.
//!
//! ```
//! use keystory::{Op, Store};
//!
//! # fn main() -> std::io::Result<()> {
//! # let dir = std::env::temp_dir().join(format!("keystory-doc-crate-{}", std::process::id()));
//! # let _ = std::fs::remove_dir_all(&dir);
//! let store = Store::open(&dir)?;
//! let index = store.put("user:42", "alice")?; // durable once this returns
//! assert_eq!(store.get("user:42"), Some(b"alice".to_vec()));
//!
//! // Several ops as one commit: one index, all or nothing, even across a crash.
//! store.apply_batch([Op::put("user:43", "bob"), Op::delete("user:42")])?;
//!
//! // A consistent view for many reads, unaffected by later writes.
//! let view = store.snapshot();
//! store.put("user:44", "carol")?;
//! assert_eq!(view.index(), index + 1);
//! assert_eq!(view.scan("user:").count(), 1);
//!
//! store.checkpoint()?; // fold the log into a checkpoint; writers are not blocked
//! drop(store);
//!
//! let store = Store::open(&dir)?; // recovery: the checkpoint plus the log since
//! assert_eq!(store.len(), 2);
//! # drop(store);
//! # std::fs::remove_dir_all(&dir)?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Guarantees
//!
//! * **Durable before visible.** A write returns only after its log record is `fsync`'d,
//!   and no reader sees it before then. After a crash, [`Store::open`] recovers every
//!   acknowledged write: the only records a crash can tear are ones no caller was told
//!   had been written. Damage no crash explains is refused (`InvalidData`) rather than
//!   skipped -- except at the end of the log (see *Limits*).
//! * **Atomic batches.** [`Store::apply_batch`] commits several ops under one index: after
//!   a crash, all of them or none.
//! * **Snapshot reads.** The commit index totally orders every write, and every read sees
//!   one prefix of that order; a [`Snapshot`] keeps its prefix for as long as it is held.
//! * **One owner.** A directory is locked while a [`Store`] has it open, against other
//!   processes as well as this one (where the filesystem supports file locks; on one that
//!   does not, `open` goes ahead without the lock).
//!
//! [`Store`] documents the error contract; `ROADMAP.md` in the repository lists exactly
//! what the tests prove and what was measured.
//!
//! ## Limits
//!
//! * The whole live dataset is held in memory.
//! * A commit clones the snapshot's map, at a cost linear in the number of keys: past
//!   roughly 100k keys that clone, not the disk, bounds the commit rate.
//! * Nothing checkpoints automatically: call [`Store::checkpoint`] to bound the log, and
//!   with it the time recovery takes. Each checkpoint reclaims the log that the previous
//!   one covers.
//! * Recovery is point-in-time at the end of the log. A damaged record in the last segment
//!   ends the log there, whether a crash tore it or the medium corrupted it afterwards,
//!   and the loss of the newest segment files would look the same: nothing records how
//!   long the log should be.
//! * Linux and macOS are tested in CI; other platforms are not.
//!
//! ## Files and format versions
//!
//! | File | Contents | Format |
//! |------|----------|--------|
//! | `wal-NNNNNNNNNN.log` | Write-ahead log segments, numbered in append order | `KSWL` version 1 |
//! | `snap.dat` | The latest checkpoint | `KSN1` version 1 |
//! | `snap.prev` | The previous checkpoint: the fall-back if `snap.dat` is damaged | `KSN1` version 1 |
//! | `snap.tmp` | A checkpoint being written | |
//! | `LOCK` | The single-owner lock | |
//!
//! Every file a [`Store`] reads names its format version, and a build refuses a version
//! it does not know with [`std::io::ErrorKind::Unsupported`] rather than misreading it.
//! Before 1.0 a minor release may change a format (its release notes will say so); a
//! store written in an incompatible format is refused, never misread. From 1.0, every 1.x
//! release reads every format an earlier 1.x wrote.
//!
//! ## Feature flags
//!
//! The default build has no dependencies. The `experimental` feature adds the building
//! blocks toward replication that [`Store`] does not use yet -- a Raft state machine with
//! an in-process cluster driver, a cooperative async runtime, a `mio` reactor, a B+ tree,
//! an off-heap value store and an epoch-based reclamation model -- and, with them, the
//! `mio` dependency. They are tested, but carry no stability promise.
//!
//! [`checker`] is an offline sequential-consistency oracle: record a concurrent
//! workload's writes and reads, then check every read against the commit order.
#![warn(missing_docs)]
#![deny(unsafe_code)]

mod checkpoint;
mod crc;
mod engine;
mod rcu;
mod types;
mod wal;

pub mod checker;

#[cfg(all(feature = "experimental", unix))]
pub mod asyncio;
#[cfg(feature = "experimental")]
pub mod btree_store;
#[cfg(feature = "experimental")]
pub mod epoch_rcu;
#[cfg(feature = "experimental")]
pub mod raft;
#[cfg(feature = "experimental")]
#[allow(unsafe_code)] // the raw-waker vtable: the crate's only `unsafe`, each block annotated
pub mod rt;
#[cfg(feature = "experimental")]
pub mod valuestore;

pub use engine::{Options, Stats, Store};
pub use types::{Entries, Op, Snapshot};

/// The README's examples, compiled by `cargo test --doc` so they cannot drift from the API.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeDoctests;
