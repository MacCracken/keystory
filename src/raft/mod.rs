//! # Phase 2 -- leader-based replicated KV (Raft-inspired)
//!
//! A small, dependency-free replication layer built on the Phase-1 durable engine.
//! Its design and **limitations are stated in `ROADMAP.md`; read that before using this
//! for anything real.**
//!
//! This module starts with the pure FSM core; the in-process cluster driver is added as
//! it lands.
//!
//! *Safety:* quorum commit, leader-authoritative application, and the log-matching
//! property are the parts that make replication *correct*; the in-process, synchronous
//! transport is the part that is *not yet* production-grade.

pub mod node;
pub mod cluster;
pub mod async_driver;

pub use node::{Cmd, Log, LogEntry, Map, Node, NodeId, Role};
pub use cluster::{RaftCluster, ClusterError};
pub use async_driver::{get, put, run, scan};
