# keystory — roadmap & design log

A crash-resilient key/value store in Rust, modelled on Raft and built from scratch in
approval-gated phases. This file is the project's memory: the state at handoff, the open
backlog, the design decisions in force, and a condensed history of how each phase got
here. The guiding principle since Phase 1: be **honest** — say what a test proves, what
is merely built, and what is not there.

## Status at handoff (2026-09-12)

**What exists.** A single-node engine (`Store`) with an `fsync`'d segmented WAL, group
commit, atomic multi-op batches, RCU snapshot reads, and streamed checkpoints that keep
one previous generation; an offline linearisability checker; a Raft state machine with
an in-process synchronous cluster driver and an `async fn` facade; a cooperative
single-threaded runtime; a `mio` reactor; a standalone B+ tree; an off-heap value store.
One external dependency (`mio`). Edition 2024 on the latest stable Rust, pinned in
`rust-toolchain.toml`. All of it is documented per module with its honest scope.

**What the tests prove** (116 tests, all green under `-D warnings`):

- Durability under a real `kill -9`, including one delivered mid-write: recovery yields a
  contiguous prefix of the writes and the repaired log keeps accepting and replaying.
- Checkpoint + WAL-tail recovery; a torn tail is truncated at open; corruption in a
  non-final segment is refused rather than skipped; a corrupt latest checkpoint falls
  back to the retained previous one plus the WAL kept since it.
- Sequential consistency under 8 writers and 6 paced readers for the whole run, and
  lost-update-free interleaved writers on one hot key, checked offline against the
  commit order.
- Group commit: concurrent writers share WAL syncs and every commit gets a distinct,
  contiguous index; a batch is one index and applies all-or-nothing under a crash.
- Checkpoints running concurrently with writers and with each other lose nothing.
- Raft: majority election, log matching, idempotent redelivery, conflicting-entry
  replacement, vote persistence across heartbeats, a sticky leader, no minority commit,
  failover without a lost update, and revive-then-read catching the node up.
- Runtime: by-value `wake` releases its data, finished slots are reused without stale
  wakers reaching a new occupant, tasks can spawn tasks. Reactor: deadlines are honoured
  and registered sockets are non-blocking. Epoch RCU is shareable across threads.

**Measured on the development machine** (Apple silicon, macOS, whose `fsync` is a full
flush):

| Workload | Result |
|---|---|
| 1 thread, `put` | 325 commits/s, one sync per commit |
| 8 threads, `put` | 1,101 commits/s, mean group 4.1 |
| 32 threads, `put` | 3,335 commits/s, mean group 16 |
| Clone of a 100k × 1 KiB snapshot map (per commit) | 19 ms with owned bytes → 11 ms with shared bytes |

**What is not there** (see the backlog): Raft does not drive the durable store; there is
no network transport and no election timer; the runtime and the reactor are not
connected; the snapshot map is still cloned whole per commit (O(entries)); the B+ tree
does not rebalance on delete and the value store is not wired into the WAL or snapshot
formats; there are no size limits beyond the `u32` framing, no configuration surface, no
logging or metrics beyond `Store::stats`.

**Verify a checkout with:**

```bash
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
cargo test --all-targets        # about a minute; Unix-only (the crash tests use kill -9)
```

CI (`.github/workflows/ci.yml`) runs the same on Linux and macOS.

**Working conventions:**

- Every module's doc comment states what it is, what it is not, and what is tracked here.
  Keep that true when you change behaviour.
- Every bug fix lands with a regression test whose name says what it guards; every
  claim in this file's status block is backed by a named test or a measurement.
- Dependencies: `mio` only, by decision. Adding one is a roadmap entry, not a footnote.
- `unsafe` is confined to the raw-waker vtable in `src/rt.rs`; each block is annotated.
- Target the latest stable Rust: bump `rust-toolchain.toml` and `rust-version` together.
- When a backlog item lands, move it to the history section; keep the status block
  current at every hand-off.

## Phases

| # | Phase | Status |
|---|-------|--------|
| 1 | Crash-resilient single-node KV engine | **DONE** |
| 2 | Raft state machine + in-process replicated store | **DONE** |
| 3 | Storage I/O: shared CRC, off-heap values, streamed snapshots | **DONE** |
| 4 | Cooperative async runtime + async facade | **DONE** |
| 5 | B+ tree, epoch RCU model, `mio` reactor, toolchain pin | **DONE** |
| 6 | Review and consolidation (2026-09-11/12) | **DONE** |
| 7 | Replication on the durable engine, transport, and the remaining design gaps | **planned** |

**DONE** = compiles, tests green, module docs and this file updated, approval given.

## Backlog — Phase 7 (planned), in order of value

1. **Raft on the durable engine.** Persist `term`, `votedFor` and the log through the
   WAL (records already carry a `term` field), make `Store` the applied state machine,
   and let a checkpoint double as the Raft snapshot. Needs per-entry terms in the record
   format and a `LogEntry` ↔ `Record` mapping, so it bumps the on-disk format versions.
   This is the largest item and the one the north star depends on.
2. **A transport and timers.** Unix-domain-socket RPC (decided in Phase 4), driven by the
   reactor, plus election timeouts and heartbeats. Only then do partitions and node
   crashes become real rather than flags in a driver.
3. **Wire the reactor into the runtime.** `Scheduler` has no I/O source: register wakers
   with the `Reactor` and poll with a timeout whenever the ready queue is empty.
4. **A structurally shared (persistent) map** for the snapshot, so a commit costs
   O(log n) instead of cloning O(entries) of node structure. Shared bytes already made
   the clone independent of value size; this removes the remaining term.
5. **B+ tree rebalancing on delete** (merge/borrow), and a decision on whether it becomes
   the primary index once (4) is designed.
6. **Value-store integration.** Carry a `BlobRef` in WAL and snapshot records for values
   above a threshold; the store's ids are already stable across compaction.
7. **Operational hardening.** Key/value size limits (framing is `u32` today), a
   configuration surface (segment size, checkpoint policy), background checkpointing,
   logging/metrics hooks, and a Windows-capable crash harness.
8. **Testing depth.** Property-based decoders for the WAL, snapshot and B+ tree formats
   (the tests already use hand-rolled xorshift generators), a fault-injecting filesystem
   shim for crash points inside a checkpoint, and a longer soak run in CI.

## Design decisions in force

Recorded so later work does not re-litigate them.

- **Durable before observable.** Ops are appended to the WAL and `fsync`'d before the new
  snapshot is published; a caller is never acknowledged before that `fsync`.
- **Only the logical commit index orders events.** Nothing depends on wall-clock time,
  which is what makes recovery deterministic. `Op::apply` is idempotent.
- **Authoritative state is an immutable snapshot** behind `RcuSwap` (an
  `RwLock<Arc<Snapshot>>`): readers load an `Arc` and never take the commit path. Keys
  and values inside the map are `Arc<[u8]>`; the API boundary uses owned `Vec<u8>`.
- **Group commit.** Writers queue tickets; one flusher commits everything queued with one
  WAL append and one `fsync`, then publishes one snapshot; the rest wait on a condvar.
  Each `put`/`delete` keeps its own index. `put_batch` is one index, one record,
  all-or-nothing.
- **WAL.** Per-record CRC; single-op records keep the Phase-1 layout, batches use tag 3.
  A torn tail is legitimate only in the last segment and is truncated at open;
  corruption elsewhere is an error. Segment creation and rotation `fsync` the directory.
  The log is reclaimed only by checkpoints, never at open.
- **Checkpoints.** Streamed body, tmp + `fsync` + rename + directory `fsync`. The previous
  checkpoint is retained as `snap.prev`, and the segments it covered are deleted only by
  the *next* checkpoint, so a fall-back always has a complete log. Checkpoints exclude
  flushers only for the instant that pins the snapshot and rotates the WAL, and are
  serialised with each other.
- **Reads.** `get`, `scan` and `range_scan` are served from the snapshot map; the B+ tree
  is a standalone module, not an index the store maintains.
- **Raft driver.** Protocol logic is real (majority election, log matching, majority
  commit, whole-cluster quorum); the transport is in-process and synchronous. A leader
  is sticky until it fails or loses its quorum; reads are leader reads that first catch
  every live follower up. The FSM keeps `votedFor` across same-term appends, skips
  redelivered entries and replaces conflicting ones.
- **Dependencies and `unsafe`.** `mio` is the one dependency; `unsafe` lives only in the
  raw-waker vtable.
- **Toolchain.** Latest stable Rust, edition 2024, pinned; no older-MSRV support.

## History (condensed)

Each phase's module docs and tests are the primary record; this is the map.

### Phase 1 — single-node engine

`Store` = commit path (WAL append + `fsync`, then RCU publish), segmented WAL with
per-record CRC and torn-tail detection, checkpoint by tmp + `fsync` + rename, recovery
by checkpoint + WAL tail, the offline MVCC checker, and the `keystory-crash-runner`
harness that proves durability with a real `kill -9`. Three open questions were carried
forward: async vs sync, log-structured vs B-tree, and RCU reclamation. *Revised in
Phase 6:* the WAL was deleted at open, which lost data on a second reopen; reclamation
now belongs to checkpoints only.

### Phase 2 — Raft

A pure Raft FSM (`raft::node`: election, `AppendEntries` with log matching, majority
commit) and an in-process synchronous driver (`raft::cluster`) with deterministic
elections and a whole-cluster quorum, validated with the Phase-1 checker across nodes
and threads: no lost update across a leader failure, no minority commit. The nodes keep
in-memory logs and state and do not use the durable store — that integration is
backlog item 1. *Revised in Phase 6:* elections ran on every write, reads could panic on
a revived node, the FSM reset its vote on heartbeats and appended redelivered entries
twice.

### Phase 3 — storage I/O

One shared CRC-32 (`crc`), the off-heap `ValueStore` (append-only blob log, random
reads, compaction by rewrite), and a streaming checkpoint writer with constant memory.
The value store was, and still is, a tested primitive that the store does not use.
*Revised in Phase 6:* compaction renumbered ids (invalidating handles), released its
lock mid-rewrite and read the whole log into memory; the streaming writer is now the
only snapshot writer.

### Phase 4 — async

A std-only cooperative runtime (`rt`: scheduler, `block_on`, hand-built raw wakers) and
an `async fn` facade over the cluster driver; the async boundary is modelled with a
one-shot yield because there was no I/O source. The transport decision (Unix domain
sockets, not TCP) was recorded but not built. *Revised in Phase 6:* by-value `wake`
leaked its data, task slots never compacted, and spawning from inside a task deadlocked.

### Phase 5 — B+ tree, epoch RCU, `mio`

A standalone B+ tree with a CRC-guarded document format, a model of epoch-based
reclamation, and the `mio` reactor proving real kernel readiness on a Unix-stream pair
— the one dependency, added deliberately. The toolchain was pinned. An addendum wired
the B+ tree into `Store` as a maintained secondary range index. *Revised in Phase 6:*
that index was removed (its `range` was an O(N) walk, about 800× slower than the
snapshot map, and a delete that emptied a leaf could panic inside the commit lock); the
tree itself was fixed, the epoch RCU made thread-safe, and the reactor given deadlines
and non-blocking sockets.

### Phase 6 — review and consolidation

A full review on 2026-09-11 (every module read; suspected bugs confirmed with throwaway
probe tests) followed by three batches of work:

- *Hygiene:* `cargo fmt` across the tree, README, licence files, CI, package renamed to
  `keystory`, stale docs corrected, duplicate CRC and snapshot encoders removed, edition
  2024 with `rust-version` tracking the pinned toolchain, `mio` 1.x.
- *Confirmed bugs:* WAL deleted at open; B-tree index panic and O(N) range; Jepsen-lite
  readers stopping immediately; `get` after `revive` panicking; the async failover test
  failing a node that did not exist; no mid-write crash test. All fixed with named
  regression tests. Pulled forward from the design list: sticky leader, checker sorting
  per-key logs, WAL directory `fsync`, corruption-vs-torn-tail distinction.
- *Design gaps:* wakers, slot reuse and in-task spawning in `rt`; reactor deadlines and
  non-blocking sockets; thread-safe epoch RCU; Raft FSM vote persistence, idempotent
  redelivery and conflict replacement, plus a guarded catch-up loop; value-store id
  stability, single lock, bounded reads, streamed compaction; shared bytes in the
  snapshot map; group commit and atomic batches (multi-op WAL records, one `fsync` per
  group); checkpoints that no longer block writers, with a retained previous generation
  and one-generation-delayed WAL reclamation; `Store::stats`.

---

*Convention: when a phase finishes, update the status block, move its backlog items into
the history, mark it **DONE**, then pause for approval before starting the next.*
