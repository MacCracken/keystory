# keystory — roadmap & design log

A crash-resilient key/value store in Rust, modelled on Raft and built from scratch in
approval-gated phases. This file is the project's memory: the state at handoff, the open
backlog, the design decisions in force, and a condensed history of how each phase got
here. The guiding principle since Phase 1: be **honest** — say what a test proves, what
is merely built, and what is not there.

## Status at handoff (2026-09-26)

**What exists.** A single-node engine (`Store`) with an `fsync`'d segmented WAL, group
commit, atomic multi-op batches, RCU snapshot reads, and streamed checkpoints that keep
one previous generation; an offline linearisability checker; a Raft state machine with
an in-process synchronous cluster driver and an `async fn` facade; a cooperative
single-threaded runtime; a `mio` reactor; a standalone B+ tree; an off-heap value store.
One external dependency (`mio`). Edition 2024 on the latest stable Rust (1.98.1), pinned
in `rust-toolchain.toml`. All of it is documented per module with its honest scope.

**What the tests prove** (135 tests, all green under `-D warnings`):

- Durability under a real `kill -9`, including one delivered mid-write: recovery yields a
  contiguous prefix of the writes and the repaired log keeps accepting and replaying.
- Checkpoint + WAL-tail recovery; a torn tail is truncated at open; corruption in a
  non-final segment is refused rather than skipped; a corrupt latest checkpoint falls
  back to the retained previous one plus the WAL kept since it; a log with missing or
  out-of-order records past the checkpoint is refused rather than opened short.
- Failed writes: a failed WAL append is rolled back, so a retried index is logged once
  and later acknowledged writes survive a reopen; if the rollback fails too, the log is
  poisoned (writes refused, reads served) until a reopen recovers everything
  acknowledged. Records the `u32` framing cannot hold are refused before any write.
- One owner per store directory: a second `open`, in this process or another, is refused
  while the first `Store` lives.
- Sequential consistency under 8 writers and 6 paced readers for the whole run, and
  lost-update-free interleaved writers on one hot key, checked offline against the
  commit order.
- Group commit: concurrent writers share WAL syncs and every commit gets a distinct,
  contiguous index; a batch is one index and applies all-or-nothing under a crash.
- Checkpoints running concurrently with writers and with each other lose nothing.
- Raft: majority election, log matching, idempotent redelivery, conflicting-entry
  replacement, vote persistence across heartbeats, a sticky leader, no minority commit,
  failover without a lost update, and revive-then-read catching the node up. A follower
  commits only the prefix an `AppendEntries` actually checked, and a follower whose log
  is longer than the leader's but stale is repaired rather than counted as an ack.
- Runtime: by-value `wake` releases its data, finished slots are reused without stale
  wakers reaching a new occupant, tasks can spawn tasks. Reactor: deadlines are honoured
  (an unbounded one included) and registered sockets are non-blocking. Epoch RCU is
  shareable across threads.
- Value store: ids survive compaction *and* a reopen without ever being reused; a failed
  put is rolled back (or poisons the store); compaction refuses a log damaged before its
  end instead of dropping the live blobs after the damage.
- Decoders: CRC-valid but malformed snapshot bodies (bogus counts, trailing bytes,
  duplicate keys) and B+ tree documents (lying lengths, childless or mis-shaped nodes,
  runaway nesting) are rejected with `InvalidData`, never a panic; a snapshot's entry
  count is bounded by its size, not a fixed cap.

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
formats; the only size limit is the `u32` record framing (enforced: an oversized commit is
refused with `InvalidInput`); there is no configuration surface and no logging or metrics
beyond `Store::stats`. Failure paths are tested by forcing failures in-process; a real
`ENOSPC` was reproduced only by hand, on a tiny `tmpfs` (backlog item 8).

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
| 6 | Review and consolidation (2026-09-11/12; audit addendum 2026-09-26) | **DONE** |
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
7. **Operational hardening.** Configurable key/value size limits (today only the `u32`
   framing bounds them, now enforced), a configuration surface (segment size, checkpoint
   policy), background checkpointing, logging/metrics hooks, and a Windows-capable crash
   harness.
8. **Testing depth.** Property-based decoders for the WAL, snapshot and B+ tree formats
   (the tests already use hand-rolled xorshift generators), a fault-injecting filesystem
   shim for crash points inside a checkpoint and for `ENOSPC`/`EIO` mid-append (the
   2026-09-26 audit reproduced the WAL and value-store write-failure bugs on a 64 KiB
   `tmpfs` by hand; CI cannot mount one), and a longer soak run in CI.

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
  The log is reclaimed only by checkpoints, never at open. Past the checkpoint, replay
  requires the index to continue without a gap.
- **Failed writes.** A failed append (WAL or blob log) is rolled back to the last durable
  record, so no unacknowledged bytes stay in the log and an index is never logged twice;
  if the rollback fails too, the log is *poisoned* -- writes refused, reads served --
  until a reopen, whose torn-tail repair takes over. A commit that the `u32` framing
  cannot hold is refused before it joins a group, so it fails only its own caller.
- **One owner per directory.** `Store::open` holds an exclusive lock on `LOCK` (std
  `File::try_lock`) for the store's lifetime; it is taken before recovery touches the log.
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
  redelivered entries and replaces conflicting ones, and commits only up to the last
  entry an `AppendEntries` covered (Raft §5.3). Catch-up always reaches the target index,
  and only nodes whose log matches the leader's there apply the committed prefix.
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

### Phase 6 addendum — audit (2026-09-26)

A second full read of every module. The toolchain was already current: 1.98.1 is the
latest stable release, and `mio` and its dependencies were at their latest versions, so
the only bumps were `Cargo.lock` to the v4 format and CI to `actions/checkout@v7`. Every
suspected bug was confirmed with a throwaway probe first -- the write-failure ones against
a real `ENOSPC` on a 64 KiB `tmpfs` -- then fixed with a named regression test:

- *WAL write failures (critical):* a failed append left its partial bytes in the segment
  and the next group reused the same indices. After one `ENOSPC`, later acknowledged and
  `fsync`'d commits were cut off as a "torn tail" at the next open (reproduced: three
  acknowledged writes lost). Appends now roll back or poison the log; a rotation switches
  segments only once the new one is durable
  (`failed_append_is_rolled_back_so_a_retried_index_is_logged_once`,
  `a_failed_rollback_poisons_the_log_until_reopen`,
  `a_failed_rotation_keeps_appending_to_the_current_segment`,
  `a_failed_append_is_never_published_and_loses_nothing_acknowledged`).
- *No directory lock:* a second `Store` on a live directory "repaired" the owner's log and
  reused its indices, silently losing an acknowledged write
  (`a_second_open_of_a_live_store_is_refused`).
- *Recovery accepted gaps:* a log missing records past the checkpoint opened short
  (`open_refuses_a_log_with_missing_or_reordered_records`).
- *Framing overflow:* keys/values past the `u32` framing, or an empty batch, were written
  as records replay reads as a torn tail; checkpoints could write a CRC-valid misparse
  (`records_the_framing_cannot_hold_are_refused_before_writing`). Segment names must now
  be the exact ten-digit form (`stray_files_are_not_mistaken_for_segments`), and old
  segments are deleted oldest first.
- *Snapshot decoder:* a fixed 50-million-entry cap made any larger store unopenable after
  its first checkpoint; trailing bytes and duplicate keys were accepted
  (`entry_count_is_bounded_by_the_body_not_a_fixed_cap`,
  `trailing_bytes_and_duplicate_keys_are_rejected`).
- *Raft:* a follower committed up to its own last index rather than the last entry the
  message covered, so a stale tail could be applied
  (`heartbeat_does_not_commit_a_stale_tail_past_the_checked_prefix`); the driver counted
  a longer-but-stale follower log as caught up and applied it, diverging the cluster
  (`a_longer_stale_follower_log_is_repaired_not_counted_as_an_ack`).
- *Value store:* compaction that dropped the highest ids, then a reopen, reused them, so a
  stale handle read another blob's bytes; a failed put made the next put unreadable and
  lost it at reopen; compaction swallowed write errors and silently dropped live blobs
  after a damaged record; an all-ones length overflowed at open. Fixed with a fence
  record, rollback/poisoning, error propagation and checked arithmetic
  (`ids_are_never_reused_after_compaction_and_reopen`,
  `a_failed_put_is_rolled_back_and_later_blobs_survive`,
  `a_failed_rollback_poisons_the_store_until_reopen`,
  `compaction_refuses_a_damaged_log_instead_of_dropping_live_blobs`,
  `an_all_ones_length_ends_the_log_instead_of_overflowing`). New logs `fsync` their
  directory, and `get` reports real I/O errors instead of "absent".
- *B+ tree:* a CRC-valid but malformed document panicked `open` (or the first `get`), and
  `commit` did not `fsync` the directory after its rename
  (`crc_valid_but_malformed_documents_are_rejected_not_panicked`).
- *Smaller:* `Duration::MAX` panicked the reactor's deadline arithmetic
  (`an_unbounded_timeout_waits_without_a_deadline`); `Model`'s docs were attached to a
  private alias; stale comments in `rust-toolchain.toml` and the async driver.

The `rt` tests (the only `unsafe`) also pass under Miri with strict provenance, as a
one-off check; Miri is not in CI.

---

*Convention: when a phase finishes, update the status block, move its backlog items into
the history, mark it **DONE**, then pause for approval before starting the next.*
