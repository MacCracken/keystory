//! # Leader-based replicated KV (Raft-inspired): Phases 2 and 4
//!
//! A dependency-free replication layer: a pure Raft FSM ([`node`]), an in-process,
//! synchronous cluster driver ([`cluster`]), and an `async fn` facade over that driver
//! ([`async_driver`]) running on the cooperative runtime in [`crate::rt`].
//!
//! **Honest scope.** The protocol logic (quorum election, log matching, majority commit)
//! is real; the transport is not: peers are called on one thread's stack, there are no
//! sockets, no election timers, and no persistence. Nodes keep in-memory logs and state
//! and do **not** use the durable [`crate::Store`] yet. `ROADMAP.md` records what is
//! proven and what is not; read it before using this for anything real.

pub mod async_driver;
pub mod cluster;
pub mod node;

pub use async_driver::{get, put, run, scan};
pub use cluster::{ClusterError, RaftCluster};
pub use node::{Cmd, Log, LogEntry, Map, Node, NodeId, Role};
