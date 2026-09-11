# keystory — roadmap & design log

A crash-resilient, replicated key/value store in Rust, modelled on Raft and built in
ordered phases. Each phase must compile, pass its tests, and pause for approval
before the next begins. The guiding principle established in Phase 1: be **honest**
about the threat model — say what is proven and what is not yet, and do not pre-empt
the design questions a later phase exists to answer.

## North star

A correct, durable, *linearizable* replicated KV store that survives process crashes,
node failures and network partitions, and that can run over async I/O. Built from
scratch (std-only through Phase 4; `mio` is the single dependency since Phase 5), with
a written audit of the threat/failure model at every phase boundary.

## Phases

| # | Phase | Scope | Status |
|---|-------|-------|--------|
| 1 | Crash-resilient single-node KV engine | RCU snapshots, segmented WAL, durable checkpoint + recovery, Jepsen-lite checker | **DONE** ✅ |
| 2 | Fault-tolerant replicated store (Raft) | leader election, replicated log, quorum/commit, failover, partition tolerance | **DONE** |
| 3 | Production-grade I/O | log-structured storage, zero-copy/mmap, efficient snapshots, large-value handling | **DONE** ✅ |
| 4 | Async end-to-end + async I/O | async runtime + async-safe API; revisit RCU reclamation | **DONE** |
| 5 | Production-grade async I/O + B-tree + true RCU | B+ page store, epoch-based RCU, real non-blocking I/O (via `mio`) | **DONE** ✅ |
| 6 | Consolidation | act on the 2026-09-11 review: hygiene, confirmed bugs, design gaps, Raft on the durable store | **in progress** |

Legend: **DONE** = compiles, tests green, audit written, approval given. **planned** =
design intent only, not yet implemented. Phase sections are a log: each *Evidence* block
records the state at that gate (test counts, dependency and `unsafe` counts), not today's.

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
  newer than it.** Torn trailing records are dropped, never trusted. (Phase 1 also
  deleted the WAL at open; Phase 6 found that lost data on a second reopen and moved
  reclamation to `checkpoint()` only.)
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
| `src/main.rs` | ~121 | `keystory-crash-runner` for the SIGKILL integration test |
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

### Note on reproducibility (resolved in Phase 5)

Resolved: `rust-toolchain.toml` pins the exact development toolchain
(`channel = "1.98.1"`, with `rustfmt` + `clippy`, `profile = "minimal"`), so a
fresh checkout builds what was tested. It complements the `rust-version = "1.80"`
floor in `Cargo.toml` -- the floor is the minimum a consumer needs; the pin is the
compiler the project's `cargo test` / `cargo clippy --all-targets -- -D warnings`
runs were validated against. (A fresh checkout auto-installs the pinned toolchain
via rustup, or installs it once with `rustup toolchain install 1.98.1`.)

---

## Phase 2 — Fault-tolerant replicated store (Raft) — **DONE**

A dependency-free, *in-process, synchronous* Raft driver that reuses the Phase-1 `Op` and
`Model` types. It does **not** use the durable `Store` (nodes keep in-memory logs and state;
that integration is Phase 6 work). The protocol **logic** is the real Raft; the **transport** is a stand-in: peers are
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

## Phase 4 — Async end-to-end + async I/O — **DONE** (runtime + model; real I/O source deferred)

A std-only, **no-deps cooperative async runtime** and an **async API** that reaches the Phase-2
cluster exactly, honestly scoped to the constraint: the *programming model* is real and tested end-
to-end; the actual I/O *source* is modeled (not a real non-blocking reactor), because that would
need `libc`, which the no-deps invariant forbids.

### Design decisions (chosen here, with rationale)
1. **Stay log-structured -- answer open #2 "yes, log-structured; no B-tree" for this phase** and
   defer a B-tree index to Phase 5. (Same decision as Phase 3; recorded again at the Phase-4 gate
   so it is explicit the question is *answered*, not deferred to Phase 4.)
2. **A hand-rolled cooperative async runtime, no crate.** `scheduler + block_on + spawn` built on
   the language's own `Future`/`Poll`/`Waker`, with **raw wakers built by hand** via
   `RawWakerVTable` (the canonical "build your own runtime" technique). `Arc`-shared ready queue.
3. **The async I/O *source* is modeled, not real.** There is no epoll/kqueue/io_uring. Instead a
   `YieldOnce` future yields once (Pending, then Ready) so the scheduler *genuinely* drives an
   async boundary; an async API over the cluster awaits it between replicated steps. Honest:
   proven the model works; wiring a real non-blocking source is the next, libc-gated step.
4. **Transport decision: unix domain socket, *not* TCP** -- for this host (macOS) it is std-only
   (`std::os::unix::net`), and loopback RPC is the natural fit for an in-process cluster. A real
   RPC layer over it is a further, separate sub-project (flagged, not built this phase).

### Components
| File | Role (approx LOC) |
|------|-------------------|
| `src/rt.rs`             | `Scheduler` + `block_on` + hand-built `Waker`s; `YieldOnce` cooperative yield point |
| `src/raft/async_driver.rs` | async API (`put`/`get`/`scan`/`run`) over `RaftCluster`, driven by the runtime |
| (Phase 3) `src/crc.rs`, `src/valuestore.rs`, `src/snapshot.rs::write_streaming` | carried forward |

New unit tests: `rt::block_on_runs_to_completion`, `rt::await_resumes_cooperatively`, `rt::yields_
once_then_completes`, `rt::cooperative_interleave_counts` (proves await points are really
polled-and-resumed, not optimised away); `async_driver::async_workload_converges`,
`async_driver::async_failover_then_converges` (async workloads converge + stay linearizable).

### Threat & failure audit (Phase 4) -- proven
- **The runtime genuinely cooperates.** `cooperative_interleave_counts` awaits in a loop and a
  shared atomic shows every `.await` was polled and resumed -- not a single-poll optimisation.
- **Async == sync behaviour.** `async_workload_converges` runs 60 replicated puts + 20 reads via
  one async block on the runtime and the shared `Model` reports **zero violations**; the
  `async_failover` test kills a node mid-run and still converges -- *async does not change the
  Raft guarantee*.
- **Wakers are real and correct.** `wake`/`wake_by_ref` re-queue the task; `clone`/`drop`
  manage the raw pointer's lifetime (SAFETY-noted). No memory unsafety: every `unsafe` block is
  a raw-waker pointer with an explicit `// SAFETY` justification.

### Honest limitations (Phase 4) -- not yet covered
1. **No real async I/O source.** No non-blocking socket/reactor; the boundary is modeled by
   `YieldOnce`. Non-blocking I/O needs `libc` (out of the no-deps invariant). This is the single
   biggest honesty gap and the first item Phase 5+ must close if "true async I/O" is required.
2. **No real network transport / RPC.** The async API is over an in-process cluster. A unix-socket
   RPC layer (decided above as the transport of record) is a separate sub-project.
3. **Single-threaded cooperative runtime.** The `Scheduler` runs on one thread; there is no work
   stealing, no thread pool, no `Send`-across-threads. Adequate for the model, not production.
4. **RCU reclamation still Phase 1's** `RcuSwap` (RwLock-backed); not revisited for true
   epoch-based reclamation in this phase (open #3 remains for Phase 5+).

### Evidence (reproduce with `cargo test` and `cargo clippy --all-targets -- -D warnings`)
- **63 tests pass** in total (55 lib -- incl. the new `rt` + `async_driver` -- plus 3 crash, 1
  Jepsen-lite, and 4 replication integration tests, all still green).
- **clippy clean** under `-Dwarnings`; **release build** succeeds.
- **0 external dependencies**, and the **zero-`unsafe`** target is preserved *except for the
  unavoidable raw-`Waker` vtable pointers in `src/rt.rs`, each individually `// SAFETY`-annotated
  (a hand-built runtime cannot avoid the raw-pointer ABI; it is documented per-block).

---

## Phase 5 — Production-grade async I/O + B-tree + true RCU — **DONE** ✅

Closes all three open questions the earlier phases deferred. Split into three
pieces, each committed on its own.

### Design decisions (chosen here, with rationale)

1. **B+ page store.** An ordered B+ tree (`src/btree_store.rs`, ORDER = 5) with
   *separators routing to children* and *values only in leaves* -- the "a
   B-tree page store" of open #2. Internal split lifts `min_leaf_key(right)` as
   the parent separator so `get` routes correctly at any depth; the whole tree
   snapshots to a single CRC-checked document (magic `BTR2`, atomic
   tmp+fsync+rename). In-order `scan` returns leaf rows in key order.
2. **Epoch-based RCU.** `src/epoch_rcu.rs` is a *cooperative* reader-quiescence
   model: readers stamp an entry epoch, `publish` defers the old version into a
   grace-epoch queue, and `reclaim` frees only when every active reader entered
   at or after the retiree's epoch. This is the mechanism a production epoch RCU
   needs; the model documents why it is not yet wired into the hot `RcuSwap`
   (see *Honest limitations* -- the production form's async-safety is a real,
   non-trivial job).
3. **Real non-blocking I/O via `mio` -- the one dependency.** `src/asyncio.rs`
   uses `mio` (kqueue/epoll/IOCP) to drive a *genuine* write -> poll -> read
   round trip on a Unix-stream pair. This **deliberately lifts the no-deps gate**,
   and *only* for I/O: no consensus crate, no async runtime, no RPC. This was the
   long-deferred item #1 -- now honestly resolved with real kernel readiness.

### Components

- **`src/btree_store.rs`** — B+ page store: `BTree`/`Node`, `insert`/`get`/`scan`,
  bottom-up split, full-tree document snapshot with CRC-32, atomic load.
- **`src/epoch_rcu.rs`** — `EpochManager` + `Rcu<T>` + `Reader` guard; epoch-tagged
  deferred reclamation behind a quiescent-state check.
- **`src/asyncio.rs`** — `Reactor` over `mio::Poll`; `register_readable` + `round_trip`
  on a `UnixStream` pair. The single external dependency (`mio` in `Cargo.toml`).
- **`src/lib.rs`** — `pub mod btree_store; pub mod epoch_rcu; pub mod asyncio;`.
- **`Cargo.toml`** / **`Cargo.lock`** — `mio = { version = "0.8", features =
  ["os-ext"] }` is the first (and, by discipline, the last) external dependency.

### Threat & failure audit -- proven

- **B-tree routing at depth.** `get` routes via internal separators down to the leaf
  and linear-pins; the test `in_order_matches_btreemap` inserts 500 ordered keys
  and compares the *exact* scan output to a `std::BTreeMap` walk -- a deep,
  multi-split tree, read back in order. `assert_balanced` checks every internal
  node's children count and key/child invariant on every build.
- **Persistence + corruption.** `persist_and_recover` writes the full document and
  recovers it; `corrupt_document_rejected` flips bytes and confirms the CRC-32
  guard returns `None`.
- **Reclamation correctness.** `deferred_not_freed_while_reader_active` proves a
  reader that began *before* a publish blocks that publish's reclamation for its
  lifetime; `all_readers_must_quiesce` shows two concurrent readers must both exit;
  `later_reader_does_not_block_earlier_retiree` shows a *new* reader cannot block an
  *old* retiree; `clone_released_then_version_drops` proves the old version's
  `Drop` fires only once its last reader is gone.
- **Real I/O.** `real_nonblocking_round_trip` transfers bytes through a live
  `kqueue`/`epoll` readiness event; `readiness_is_observed` asserts a real write
  surfaces as a poller event -- not a fabricated one.

### Honest limitations (Phase 5) -- not yet covered

1. **Epoch RCU is a model, not the hot path.** It is a cooperative, single-producer
   quiescence model; the live `RcuSwap` still uses an `RwLock`. A production epoch
   RCU must (a) bound the stored value `T: Send + Sync` so deferred nodes cross thread
   boundaries, (b) use raw, lock-free pointer storage rather than `Mutex<Arc<T>>`,
   (c) guard a reader parked *inside* its critical section during reclamation, and
   (d) harden against a *preempted* writer. Those are the same async-safety concerns
   a kernel RCU (e.g. Linux `rcutorture`) wrestles with -- real work, deliberately out
   of scope here. The *mechanism* is proven; the *integration* is not.
2. **Real I/O is a substrate, not a network layer.** `asyncio` proves the runtime can
   drive live, non-blocking, per-handle I/O (kqueue/epoll/IOCP). It is *not* a
   TCP/Unix-socket transport, a connection pool, or the RPC layer for
   `TransportedCluster`; those remain a separate sub-project (as Phase 4 scoped).
   The I/O path is Unix-gated this iteration.
3. **B+ tree has no deletion / rebalancing.** `insert` splits (bottom-up, no
   merge/borrow) and `get`/`scan` read; deletion with key redistribution is not
   implemented. The document snapshot is a whole-tree rewrite, not incremental.

### Evidence (reproduce with `cargo test --all-targets` and `cargo clippy --all-targets -- -D warnings`)

```
cargo test --all-targets              # 68 lib tests + integration suites, all green
cargo clippy --all-targets -- -D warnings   # 0 warnings, including the mio module
```

- B-tree: `leaf_splits_and_scan` (300 keys), `in_order_matches_btreemap` (500
  keys vs `BTreeMap`), `persist_and_recover` (150 keys), `corrupt_document_rejected`,
  `empty_round_trips`, `one_key_get`.
- Epoch RCU: `deferred_not_freed_while_reader_active`,
  `later_reader_does_not_block_earlier_retiree`, `reclaims_when_quiescent`,
  `clone_released_then_version_drops`, `all_readers_must_quiesce`.
- Real I/O: `real_nonblocking_round_trip`, `readiness_is_observed`.

### Note on the dependency gate

The project was deliberately std-only through Phase 4. Phase 5 lifts that gate
**once, and only for non-blocking I/O** via `mio`. The discipline is now explicit:
the *only* external dependency is `mio`, used for real I/O; no consensus, no async
runtime, no RPC -- those are deliberately out of scope.

### Phase 5 addendum -- the B-tree wired into the live store (a *maintained* index)

> **Superseded in Phase 6 (2026-09-11).** The maintained index was removed from `Store`:
> its `range` was a full-tree walk (measured about 800× slower than `BTreeMap::range` at
> 200k keys, so the "O(log n + k)" claim below was false), and a delete that emptied a
> leaf could panic inside the commit lock, poisoning it. `Store::range_scan` is now served
> from the snapshot map in O(log n + k). `btree_store` remains a standalone, tested module
> with the panic fixed, a separator-pruned `range`, and an O(1) `len`.

Following the B-tree module, the live `Store` now serves ordered range queries
through a **genuinely maintained** B-tree secondary index, not a per-call build:

     - `Store` gains a `range_index: Mutex<BTree>` (rebuilt from the recovered
        state on `open`, then advanced by every commit under the commit lock).
     - `commit_op` updates it in lockstep with the published RCU snapshot: a
        `Put` inserts, a `Delete` erases, in the same serialised critical section
        that publishes the snapshot -- so the two views **can never drift**.
     - `Store::range_scan(lo, hi)` now reads the tree directly (O(log n + k))
        instead of materialising a B-tree per call.
     - `btree_store` gained `delete` (point erase) and `range` (ordered `[lo,hi)`).

**Why a parallel secondary index, not the primary index:** the authoritative,
version-stamped, crash-recoverable state is the RCU `Snapshot` (a byte-faithful
`BTreeMap<Vec<u8>, Entry>` document). The B-tree holds the same *live keys/ values*
but not the per-key versions, and its deletion does **no leaf split/merge rebalance**
-- it stays *correct* (every present key is reachable) but can become unbalanced after
many deletes. Making the B-tree the *primary* recoverable index instead would require
rewriting the snapshot/recovery format to carry versions; that is a future milestone
(documented as such), not done here. The index is a genuine, tested, incrementally
maintained secondary index that stays provably in step with the snapshot.

**Evidence:** `range_scan_served_by_btree` and
`maintained_range_index_tracks_puts_and_deletes` (100 puts, delete a strided subset,
prove the index-served range equals the snapshot's own scan, 80 live keys, none
deleted, ascending). `btree_store` carries 9 tests incl. `deleted_key_vanishes_...` and
`range_query_is_ordered_and_bounded`.

### Note on the toolchain pin (reproducibility)

`rust-toolchain.toml` pins the development toolchain (`1.98.1`, with `rustfmt` +
`clippy`), so a fresh checkout builds what was tested. It complements the
`rust-version = "1.80"` floor in `Cargo.toml`.

---

## Phase 6 — Consolidation (review of 2026-09-11) — **in progress**

A full read of the codebase plus throwaway probe tests confirmed a set of bugs and
gaps the phase gates missed. This phase works through them in three batches; each
item is ticked as it lands.

### Batch 1 — hygiene and quick wins — **DONE** ✅

- [x] `cargo fmt` applied to the whole tree and enforced in CI.
- [x] `README.md`, `LICENSE-MIT` / `LICENSE-APACHE` and a CI workflow (rustfmt, clippy
      `-D warnings`, rustdoc `-D warnings`, tests on Linux + macOS, MSRV check) added;
      the stray `rustup-init.sh` removed from the repository.
- [x] Package renamed `keystore` → `keystory` to match the repository and this document
      (binary: `keystory-crash-runner`).
- [x] The project targets the **latest stable Rust** by decision: edition 2024, and
      `rust-version` tracks the pinned toolchain (1.98) rather than an older floor. (The
      1.81 floor and its CI job from the first pass were dropped.)
- [x] `mio` bumped 0.8 → 1.x; the Unix-only `asyncio` module gated with `#[cfg(unix)]`
      so the crate builds on Windows.
- [x] Stale docs corrected (crate description, `lib.rs`, `engine`, `rt`, `rcu`, `types`,
      `snapshot`, `valuestore`, `btree_store`, `raft`); rustdoc builds warning-free.
- [x] Duplication removed: one CRC-32 (`crc.rs`; the WAL's private copy is gone), one
      snapshot body encoder (`snapshot::write_body`, streamed by default), one replicated
      commit path (`cluster::cluster_commit` behind `put` and `delete`).
- [x] Dead code and small defects: `checker::observed_keys`, duplicated `#[cfg(test)]`
      attributes, unused temp dirs in two B-tree tests; the crash runner no longer panics
      on a missing `<dir>` and `run` really defaults to 1000 keys; integration tests use a
      `tests/common::TempDir` guard so failed runs do not leak directories.

### Batch 2 — confirmed bugs — **DONE** ✅

- [x] `Store::open` no longer deletes the WAL; only `checkpoint()` reclaims it.
      `Wal::open` resumes the latest segment and truncates a torn tail first, so appends
      after a crash land on a clean boundary (`reopen_without_checkpoint_keeps_wal_data`,
      `open_repairs_torn_tail_then_keeps_appending`).
- [x] The B-tree index is gone from `Store`; `range_scan` reads the snapshot map
      (`Snapshot::range`, O(log n + k), empty for inverted bounds). In `btree_store` the
      internal split now promotes the parent's middle separator instead of reading a leaf
      (`emptied_leaf_then_internal_split_does_not_panic`), `range` prunes by separator
      (`range_matches_btreemap_on_random_keys_after_deletes`), and `len` is O(1).
- [x] Jepsen-lite readers now run, paced, for the whole writer run (joined after the
      writers; the check requires at least 1,000 recorded reads), and the lost-update
      test has readers too, so its check is no longer vacuous.
- [x] `RaftCluster::get`/`scan` are leader reads: they require a live quorum, catch every
      live follower up first, then serve the leader's state -- a revived node is caught up
      on its next read or write (`get_after_revive_catches_the_node_up`).
- [x] `async_failover_then_converges` now fails the actual leader and asserts leadership
      moved.
- [x] `sigkill_mid_write_recovers_a_consistent_prefix`: kill the runner while it is still
      writing, recover a contiguous prefix, then write and reopen again.

Also closed here, pulled forward from batch 3 because the fixes above needed them:

- [x] A leader is sticky: `ensure_leader` elects only when there is no live leader (and a
      leader whose quorum is gone steps down), so the term no longer climbs per write
      (`leader_is_sticky_across_writes`).
- [x] `checker::Model::check` sorts each key's write log by index before validating, so
      writers may record out of order (`out_of_order_recording_is_sorted_before_checking`).
- [x] WAL segment creation and rotation `fsync` the directory (`wal::fsync_dir`, also used
      by the checkpoint rename, whose failure now propagates instead of being ignored).
- [x] A bad record in a non-final WAL segment is reported as `InvalidData` instead of being
      treated like a crash tail (`corruption_before_a_later_segment_is_an_error`).

### Batch 3 — design gaps — planned

- [ ] Raft is not layered on the durable engine (nodes keep in-memory logs and state).
- [ ] Raft FSM deviations: `voted_for` is reset on every `AppendEntries`; no conflict
      check at `prev_index + 1` on append (a redelivered message corrupts the log); the
      mismatch path truncates the matching entry itself.
- [ ] Per-commit deep clone of the whole map, values included (17 ms per put measured at
      100k keys × 1 KiB). Cheapest mitigation: `Arc<[u8]>` values; real fix: a
      structurally shared persistent map.
- [ ] One `fsync` per put and no batch API; group commit is the largest available win.
- [ ] Checkpoint details: `checkpoint()` blocks writers for the whole snapshot write
      (the snapshot is immutable, so the write could happen outside the commit lock if
      the WAL learned to truncate only the segments a checkpoint covers); a corrupt
      snapshot has no older generation to fall back to.
- [ ] `rt`: the `wake` vtable entry leaks its `RawWaker` data (std requires `wake` to
      release it); task slots never compact; `add` from inside a task deadlocks.
- [ ] `asyncio::round_trip` never times out (loops on an empty poll) and never sets the
      sockets non-blocking; `register_readable` panics on failure.
- [ ] `ValueStore`: compaction renumbers ids (invalidating every outstanding `BlobRef`),
      releases its lock mid-rewrite (a concurrent `put` is lost), and trusts the on-disk
      length before allocating.
- [ ] `epoch_rcu::Rcu` is not `Sync` (it stores `Arc<Box<dyn Any>>`), so it cannot be
      shared across threads.

---

*Convention: when a phase finishes, add its threat/failure audit and test evidence here,
mark it **DONE**, then pause for approval before starting the next phase.*
