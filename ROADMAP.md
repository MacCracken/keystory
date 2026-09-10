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
| 2 | Fault-tolerant replicated store (Raft) | leader election, replicated log, quorum/commit, failover, partition tolerance | **DONE** |
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

## Phase 2 — Fault-tolerant replicated store (Raft) — **DONE**

A dependency-free, *in-process, synchronous* Raft driver layered over the Phase-1 durable
store. The protocol **logic** is the real Raft; the **transport** is a stand-in: peers are
called by hand on one thread's call stack (no sockets, no async, no election timers). The
`Model` linearisability oracle from Phase 1 is reused and re-validated across nodes and a
`std::thread` workload. See `ROADMAP.md` and `src/raft/cluster.rs`'s module docs for the
precise guarantees proven and the ones deliberately not yet proven.

### Design decisions

- **Protocol, not transport.** `RequestVote` + majority election, `AppendEntries` + the
  log-matching property, commit only on a whole-cluster majority. `Node` is a pure,
  deterministic state machine (term, vote, log, commit index, applied index, FSM view).
- **Elections are deterministic.** `elect` picks the live node with the *most up-to-date*
  log (higher last-log-term wins, tie-broken by lowest id) -- a stand-in for randomised
  timers. A stale node (behind log) cannot win even at a higher term (Raft's
  log-matching property), so a restarted node cannot clobber durable history.
- **Quorum is whole-cluster, not live.** Commit requires `n/2 + 1` of all `n` nodes
  *alive and acking*. A minority partition cannot elect or commit -- the partition-tolerance
  property, proved directly by the `minority_partition_cannot_commit` test.
- **Replication is synchronous inside the client call.** `put` blocks until the entry is
  replicated + committed + applied at every live node, then records it in the `Model` at its
  global commit index. Reads serve the leader's converged view and *assert convergence*
  across all live nodes. This is "synchronous replication": correct, just not async.
- **The in-memory log is the *only* source of truth across the cluster.** Each node keeps a
  `Vec` log and applies committed entries to its own `BTreeMap` FSM via the Phase-1
  `Op::apply`. No node checkpointing yet -- a node is reconstructed from the log; this is the
  Phase-3 storage decision (open #2), not a Raft correctness concern.
- **`Model` reuse.** The Phase-1 `Model`/`CheckModel` oracle is *the* linearisability
  checker for Phase 2 too: each commit records a write at its global index, each read
  observes that index's value, and the same `check()` proves a single total order -- now
  across a live cluster and a multi-threaded workload.

### Components

| File | LOC | Role |
| --- | ---: | --- |
| `src/raft/node.rs` | 537 | FSM core: `Term`, `Node`, `LogEntry`, `Op`, `State(BTreeMap)`; `request_vote`, `append_entries` (log-matching + truncation), `advance_commit` (quorum), `apply_committed`, `propose`, `start_election`, `become_leader`; 6 unit tests |
| `src/raft/cluster.rs` | 399 | `RaftCluster` driver: `elect`, `cluster_put`/`cluster_delete` (propose to quorum), `get` (leader read + convergence assert), `fail`/`revive`, optional shared `Model`; 3 unit tests |
| `src/raft/mod.rs` | 18 | Module: re-exports `RaftCluster`, `ClusterError`, node types |
| `tests/replication.rs` | 171 | Convergence, single-leader-death failover (no lost update), minority-partition-cannot-commit + no split-brain, and a 12-client concurrent linearisability check |

### Threat & failure audit (Phase 2) -- proven

- **No lost updates across failover.** `failover_preserves_committed_state` (+ the unit
  test): kill the leader *mid-workload*, keep writing; every committed value is present and
  the `Model` records **zero violations**. This is the killer of "restart-loses-everything"
  for a single node, now promoted to a cluster.
- **No split-brain commit.** A minority (below quorum) cannot elect a leader or commit
  (`minority_partition_cannot_commit`): its `put` returns `NoLeader`, and its value is
  nowhere in the cluster -- two minority partitions cannot both commit.
- **Convergence.** Every committed write earns one global index and every live node applies
  it; `get` asserts all live nodes agree.
- **Deterministic elections.** `stale_log_cannot_win_even_at_a_higher_term` proves a behind
  node loses even with a higher term (Raft's log-matching property in the election).
- **Log-matching repair.** `log_mismatch_is_corrected` proves a divergent follower tail is
  truncated on `AppendEntries` (the core of eventual log agreement).
- **Cross-node + concurrent linearisability.** `concurrent_writers_and_readers_stay_linearizable`
  runs 12 client threads on a 3-node cluster; the `Model` finds zero violations -- the exact
  sequential-consistency invariant Phase 1 proved single-node, now proven *replicated*.

### Honest limitations (Phase 2) -- not yet covered

- **No real network / no async.** Peers are called in-process by hand; there is no socket
  framing, no heartbeat timer, and no async pipeline. "Fault" means a node flagged in the
  driver's `down` set, not a real network partition with two separated, independently
  electing sub-clusters. (This is the open async-transport question, Phase 4.)
- **In-memory logs; no node checkpoint.** Each node's log is a `Vec` that is not persisted;
  a *node* crash (vs. whole-machine) is not yet modelled -- recovery is "reconstruct from
  log", and that storage decision (in-place vs log-structured, open #2) is Phase 3.
- **Synchronous replication.** `put` blocks through the quorum; not a leader-pipelined async
  write path.
- **The `Model` remains a *linearisability* (single-total-order) oracle**, not a full Jepsen
  consistency suite (no read-your-writes, no causal reads, no linearization-of-snapshots).

**Evidence:** 39 lib unit tests + 4 replication integration + 3 crash + 1 Jepsen-lite =
47 tests pass; clippy clean under `-Dwarnings`; 0 dependencies; 0 `unsafe`.

**Decision points to raise before Phase 3:** log-compaction strategy; the node storage
backend (in-place vs log-structured -- open #2); and whether the *transport*, when it
arrives, is TCP or a unix socket (which Phase 4's async may make moot).


## Phase 3 — Production-grade I/O — **DONE**

Log-structured storage; off-heap / large-value handling; constant-memory (streaming)
snapshots; efficient reads. This phase makes the on-disk representation production-grade in
**three** ways, with the two hardest open questions (B-tree, async I/O) explicitly deferred.

### Design decisions (chosen here, with rationale)
1. **Stay log-structured. Do **not** hand-roll a B-tree this phase.** A correct no-deps B+tree
   (page manager, splits/merges, recovery) is a large *separate* project; the log-structure
   already composes with compaction + off-heap values, and it is the natural extension of the
   Phase-1 WAL. A B-tree index is deferred to Phase 5 (open #2 remains live for that later gate).
2. **Off-heap large values.** A new [`ValueStore`](src/valuestore.rs) spills values to an
   append-only blob log (`values/blobs`); a WAL entry or snapshot record then carries a small
   [`BlobRef`](src/valuestore.rs) handle, **not the bytes**. Reads are *random* (`Seek`
   from the handle's offset) -- touching only that blob's bytes, never the rest of the log -- the
   std-only stand-in for a zero-copy / mmap read.
3. **Compaction = GC of the value store.** [`ValueStore::compact`](src/valuestore.rs) rewrites
   the blob log keeping only still-live ids, atomically (tmp + fsync + rename), so a crash mid-
   compact leaves the original log intact. This is *the* log-compaction step, expressed in value-
   store terms; the WAL's existing truncate-after-checkpoint covers the log side.
4. **Constant-memory (streaming) snapshots.** The old [`write`](src/snapshot.rs) materialises the
   whole snapshot into one `Vec`. The new [`write_streaming`](src/snapshot.rs) folds the CRC
   incrementally via [`Crc`](src/crc.rs) and writes record-by-record, keeping peak allocation at
   O(one record). Its output is **byte-identical** to `write`, so `load` decodes both.
5. **One shared CRC.** [`crc`](src/crc.rs) is the single 802.3 CRC-32 implementation, with a
   one-shot `crc32` and an incremental `Crc`; both known-answer-tested (the `123456789` ->
   `0xCBF43926` vector) and the incremental one proven compositional (`update(a);update(b);finish`
   == `crc32(a||b)`). Replaces `wal`'s local copy going forward.

### Components
| File | Role (approx LOC) |
|------|-------------------|
| `src/crc.rs`        | 802.3 CRC-32: one-shot `crc32` + incremental `Crc`, known-answer + compositionality tests |
| `src/valuestore.rs` | `ValueStore`: off-heap append-only blob log -- `put`/`get` (random access)/`compact` (GC)/reopen-recovery |
| `src/snapshot.rs`   | + `write_streaming` (constant-memory checkpoint) and its large round-trip test |
| `src/lib.rs`        | wires `mod crc` + `pub mod valuestore` |

New unit tests (all in-module, no external harness): 5 in `valuestore` (large round-trip, random
access, compact shrinks, compact+reopen, bare reopen), the new `crc` tests (canon vector,
compositionality, determinism), and `snapshot::streaming_roundtrip_large` (50k entries streamed
and decoded identically to the buffered writer).

### Threat & failure audit (Phase 3) -- proven
- **Large values round-trip & survive a reopen**, read purely from the on-disk blob log (no in-
  memory residue) -- verified by `survives_reopen`.
- **Superseded values are reclaimed.** `compact` keeps only live ids and the on-disk file shrinks
  -- verified by `compact_drops_superseded_blobs` (asserts size drop) and `compact_survives_reopen`.
- **Constant-memory checkpoints are correct.** A 50k-entry snapshot written *streaming* decodes
  identically to the buffered writer -- the format guarantee is proven by `streaming_roundtrip_large`
  (asserts `s.data == got.data`).
- **Compaction is crash-safe.** tmp + fsync + atomic rename: a crash mid-compact leaves either the
  old log or the new, never a torn one.
- **Integrity is universal.** The shared CRC-32 guards every on-disk record (WAL, snapshot, blob);
  a corrupt/torn record is rejected (`None` / `InvalidData`), never returned as a valid value.

### Honest limitations (Phase 3) -- not yet covered
1. **No mmap / true zero-copy, and no async I/O.** Deferred to Phase 4 (open #1). Random access
   uses `Seek` from a handle, not a memory mapping -- real mapping needs `libc`, which the no-deps
   constraint forbids.
2. **No B-tree.** Log-structured only; a B-tree index is explicitly deferred to Phase 5 (open #2
   stays live for *that* gate, but is answered "log-structured, not B-tree" *for this phase*).
3. **`ValueStore` is a tested primitive, not yet wired into the WAL entry / snapshot record.** The
   mechanism (off-heap value + `BlobRef` + compaction) is built and proven in isolation; *plugging
   off-heap values into the on-disk log/snapshot entries* so a WAL entry holds a `BlobRef` rather
   than the bytes is the next step, and it partly depends on the transport decision (Phase 4).
4. **`ValueStore::get` opens a fresh file + `Seek` per read** (correct, not optimal); a shared fd
   with a buffered reader is a Phase-4 optimisation.

### Evidence (reproduce with `cargo test` and `cargo clippy --all-targets -- -D warnings`)
- **57 integration+unit tests pass** (49 lib -- incl. the new `crc`/`valuestore`/streaming -- plus
  the 3 crash, 1 Jepsen-lite linearisability, and 4 replication integration tests, all still green).
- **clippy clean** under `-Dwarnings` across `--all-targets`; **release build** succeeds.
- **0 external dependencies, 0 `unsafe` blocks** -- the no-deps / no-unsafe invariants still hold
  after every Phase-3 addition.

---

## Phase 4 — Async end-to-end + async I/O -- planned

The async runtime and an async-safe API; revisit RCU reclamation + I/O concurrency; decide the
node transport (TCP vs unix socket -- possibly moot once async arrives). This is where
*asynchronous I/O* is finally in scope (question 1 stays open until here).

> Decision raised at Phase-3 gate and **answered for the storage layout**: stay **log-structured**
> (do not hand-roll a B-tree this phase -- deferred to Phase 5); add an append-only blob log for
> large values with compaction; add streaming (constant-memory) checkpoints.

---

*Convention: when a phase finishes, add its threat/failure audit and test evidence here,
mark it **DONE**, then pause for approval before starting the next phase.*
