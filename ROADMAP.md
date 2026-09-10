# keystory — roadmap & design log

A crash-resilient, replicated key/value store in Rust, modelled on Raft and built in
four ordered phases. Each phase must compile, pass its tests, and pause for approval
before the next begins. The guiding principle established in Phase 1: be **honest**
about the threat model — say what is proven and what is not yet, and do not pre-empt
the design questions a later phase exists to answer.

## North star

A correct, durable, *linearizable* replicated KV store that survives process crashes,
node failures and network partitions, and that can run over async I/O. Built from
scratch, dependency-free, with a written audit of the threat/failure model at every
phase boundary.

## Phases

| # | Phase | Scope | Status |
|---|-------|-------|--------|
| 1 | Crash-resilient single-node KV engine | RCU snapshots, segmented WAL, durable checkpoint + recovery, Jepsen-lite checker | **DONE** ✅ |
| 2 | Fault-tolerant replicated store (Raft) | leader election, replicated log, quorum/commit, failover, partition tolerance | planned |
| 3 | Production-grade I/O | log-structured storage, zero-copy/mmap, efficient snapshots, large-value handling | planned |
| 4 | Async end-to-end + async I/O | async runtime + async-safe API; revisit RCU reclamation | planned |

Legend: **DONE** = compiles, tests green, audit written, approval given. **planned** =
design intent only, not yet implemented.

---

## Phase 1 — Crash-resilient, single-node KV engine — **DONE** ✅

**Goal.** An in-memory, thread-safe KV engine whose authoritative state is a
version-stamped snapshot (RCU-style, MVCC-readable), backed by a segmented WAL with
`fsync` durability and recovery by *snapshot + WAL-tail replay*, plus a checkpoint
mechanism and a Jepsen-lite linearisability checker.

### Design decisions (recorded so later phases don't re-litigate them)

- **`RcuSwap<Snapshot>` is a `RwLock<Arc<Snapshot>>`.** Snapshot isolation with provable
  correctness and allocation-free reads. A true lock-free, epoch-based RCU (`arc-swap`
  or Linux-RCU style) with snapshot *reclamation* is an **open question** for a later
  phase.
- **Commit path is `WAL.append + fsync` *before* `RCU.publish`.** Durable before
  observable: a crash before the append has no effect; a crash after the append is
  recovered.
- **`Op::apply` is idempotent.** Same-value `Put` and `Delete` of an absent key change
  nothing — correct under exact *or* at-least-once log replay; the version bumps only on
  a real state change (an exactly-once log, e.g. Raft, would *not* bump it).
- **Recovery = load the last snapshot (O(1) in log length) + replay only WAL records
  newer than it + reclaim the consumed WAL.** Torn trailing records are dropped, never
  trusted.
- **The logical commit index total-orders events** (not wall-clock time). `term` is a
  single node's Raft term (always `1` here) — the field is reserved for the log format
  Raft will need, with no behaviour yet.
- **Dependency-free.** No `serde`, no `base64`, no consensus crate; CRC-32 and the
  snapshot/WAL binary formats are hand-rolled. Only the crash-runner *shells out to the
  OS `kill(1)` utility* so a test can SIGKILL a child without adding a `libc` dependency.

### Components

| Module | LOC | Role |
|--------|----:|------|
| `src/rcu.rs` | ~160 | `RcuSwap<T>` snapshot cell; parallel snapshot readers |
| `src/types.rs` | ~148 | `Bytes`, `Entry{value,version}`, `Op` (idempotent), `Snapshot{index,data}` |
| `src/wal.rs` | ~425 | segmented WAL (append + fsync, rotate) and `replay()` with torn-tail handling |
| `src/snapshot.rs` | ~290 | compact binary checkpoint; atomic publish (tmp + fsync + rename); `purge()` |
| `src/engine.rs` | ~389 | `Store`: commit-lock → WAL + fsync → RCU publish; `open` = snapshot + WAL tail |
| `src/checker.rs` | ~193 | offline MVCC sequential-consistency checker (`Model`, `CheckModel`) |
| `src/main.rs` | ~121 | `keystore-crash-runner` for the SIGKILL integration test |
| `tests/` | ~379 | crash-recovery (×3) plus a Jepsen-lite linearisability test (×1) |

### Threat & failure audit

**Proven by tests.**

- **Write durability under crash.** Every commit is `fsync`'d before it is observable.
  A *real* `SIGKILL` proves it, via
  `tests/crash_recover.rs::survives_sigkill_then_recovers_full_state`; a WAL-only crash
  recovers full state too. `crc_detects_bit_flip` and `torn_tail_is_dropped_cleanly`
  prove a corrupt or truncated trailing record is dropped, never trusted.
- **Snapshot + WAL-tail recovery.**
  `crash_recovery_from_snapshot_then_wal_tail` and
  `checkpoint_then_crash_recovers_from_snapshot_plus_tail` verify an O(1) checkpoint plus
  a tail-only replay, across a simulated second crash.
- **Snapshot isolation.** `rcu::parallel_readers_observe_no_torn_snapshots`:
  concurrent readers never observe a half-updated map.
- **Sequential consistency / linearisation.**
  `concurrent_writers_and_readers_are_sequentially_consistent` runs 8 writers over 64 keys
  for 100 writes each, interleaved with 6 readers, every commit and read recorded into the
  model and checked: **0 violations**. Plus a focused no-lost-update check.
- **Checkpoint atomicity.** The snapshot is published via tmp + `fsync` + `rename`, so it
  is never half-written.

**Known limitations / scope not yet covered (by design).**

- **No fault tolerance.** Single node, `term == 1`; node loss and partitions are unhandled
  — that is Phase 2. The linearisation proven here is *internal* (a commit-index total
  order): the necessary precondition for replication, not replication itself.
- **Durability assumes `fsync` is honoured** by the storage medium (write-cache policy,
  batteries). That is a real-world caveat, not a code defect, and it is not modelled.
- **`RcuSwap` is safe, not async-RCU.** `RwLock<Arc<_>>` is provably correct with
  allocation-free reads, but it is **not** a lock-free epoch reclaimer. Open question (3).
- **`Snapshot` is a full `BTreeMap` cloned per commit (O(N)).** A log-structured engine
  would amortise writes; a crash-consistent log-structured store is the Phase-3 question
  (2). **Async I/O** is not yet in the picture — that is Phase 4 (question 1).

### Open questions carried forward (do **not** pre-empt them)

1. **Async vs sync.** The engine is currently synchronous (`fsync` in-thread); whether to
   build an async engine plus async I/O is a Phase-4 decision.
2. **Log-structured vs B-tree.** The per-commit full-map clone is O(N); weigh a
   log-structured store, with crash-consistency as a hard requirement.
3. **Reader lifetime and snapshot reclamation.** Revisit `RcuSwap`'s reclamation;
   consider an epoch-based or async-RCU design.

### Evidence (reproduce with `cargo test --all-targets` and `cargo clippy --all-targets`)

```
rustc 1.98.1 · 0 external dependencies · 0 unsafe · clippy --all-targets: 0 warnings (-Dwarnings)
34 tests pass under -Dwarnings (debug and release both build clean):
  lib unit       30   (rcu 5, types 4, wal 4, snapshot 5, engine 7, checker 4, + get_at)
  integration     3   (a real SIGKILL / WAL-only / snapshot+tail — a real process + real kill -9)
  jepsen-lite     1   (8 writers × 64 keys × 100 writes + 6 readers, 0 consistency violations)
```

### Note on reproducibility (open)

There is no `rust-toolchain.toml` pin yet; the build is assumed on the host rustc
(observed 1.98.1). Pinning the toolchain is small hygiene to owe to any later phase.

---

## Phase 2 — Fault-tolerant replicated store (Raft) — planned

Intended scope (design intent, not yet built):

- A multi-node cluster built on the Phase-1 durable store; each node uses the same
  WAL/checkpoint machinery as its *local* log.
- **Leader election + heartbeat** (Raft terms — the `term` field is already reserved).
- **Replicated log + quorum commit.** An entry commits when a majority ack it, and that
  commit advances the published RCU snapshot on every node.
- **Failover + partition tolerance.** Leader-loss elections, split-brain avoidance via
  quorum, and a network-partition test that proves a minority does not commit.
- Extend the `Model` checker to span *multiple* nodes' histories — the *real* Jepsen-lite
  linearisability proof that the Phase-1 checker only prefigures.

**Decision points to raise before building:** wire-protocol framing, log-compaction
strategy, and whether the node transport is TCP or a unix socket (Phase 4's async may
dictate this).

## Phase 3 — Production-grade I/O — planned

Log-structured storage; zero-copy / mmap reads; an `fsync`/AIO (POSIX) and io_uring
(Linux) backend; large-value and large-snapshot handling; an on-disk B-tree or similar.

## Phase 4 — Async end-to-end + async I/O — planned

An async runtime; an async-safe API; revisiting RCU reclamation and I/O concurrency; and
resolving the log-structured-vs-B-tree question (open #2) here or in Phase 3.

---

*Convention: when a phase finishes, add its threat/failure audit and test evidence here,
mark it **DONE**, then pause for approval before starting the next phase.*
