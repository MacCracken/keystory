# keystory — roadmap & design log

A crash-resilient key/value store in Rust, modelled on Raft and built from scratch: first
in approval-gated phases (1 to 6), from 0.1.0 on in versioned releases. This file is the
project's memory: the state at handoff, the road to 1.0, the design decisions in force,
and a condensed history of how each phase got here. The guiding principle since Phase 1:
be **honest** — say what a test proves, what is merely built, and what is not there.

## Status at handoff (2026-09-26, preparing 0.1.0)

**What exists.** A single-node engine (`Store`) with an `fsync`'d segmented WAL whose
segments carry a versioned header, group commit, atomic multi-op batches, RCU snapshot
reads (and `Snapshot` views for many mutually consistent reads), and streamed checkpoints
that keep one previous generation; an offline linearisability checker. Behind the
`experimental` feature: a Raft state machine with an in-process synchronous cluster
driver and an `async fn` facade, a cooperative single-threaded runtime, a `mio` reactor,
a standalone B+ tree, an off-heap value store and an epoch-RCU model. The default build
has no dependencies; `experimental` adds `mio`. Edition 2024 on the latest stable Rust
(1.98.1), pinned in `rust-toolchain.toml`. Every module documents its honest scope, and
every public item is documented (`missing_docs` is enforced).

**What the tests prove** (161 tests with `--all-features` -- 146 unit, 11 integration, 4
doc -- of which the default build runs 95; all green under `-D warnings`):

- Durability under a real `kill -9`, including one delivered mid-write: recovery yields a
  contiguous prefix of the writes and the repaired log keeps accepting and replaying.
- Checkpoint + WAL-tail recovery; a torn tail is truncated at open; corruption in a
  non-final segment is refused rather than skipped; a corrupt latest checkpoint falls
  back to the retained previous one plus the WAL kept since it; a log with records missing
  from its middle, or out of order, past the checkpoint is refused rather than opened short.
- A checkpoint taken after such a fall-back keeps the good generation: even if the new
  checkpoint is then lost, the store reopens with every write.
- Versioned files: every WAL segment starts with a CRC'd header naming its format
  version. A segment or checkpoint of another version is refused with `Unsupported` --
  by replay, by the log's `open` and by `Store::open` -- and left untouched, while a
  version field damaged in place is recognised as damage. A segment whose creation a
  crash cut short is repaired at open. A record whose CRC is valid but which does not
  decode is refused as corruption, never truncated as a torn tail. A store written
  before the header existed is refused (checked against the pre-release code).
- Failed writes: a failed WAL append is rolled back, so a retried index is logged once
  and later acknowledged writes survive a reopen; if the rollback fails too, the log is
  poisoned (writes refused, reads served) until a reopen recovers everything
  acknowledged. Records the `u32` framing cannot hold are refused before any write. A
  failed rotation leaves no newer segment beside the current one, or poisons the log when
  it cannot promise that; each failure point is exercised through a test seam.
- One owner per store directory: a second `open`, in this process or another (the crash
  harness holds it), is refused while the first `Store` lives. A store opened through a
  relative path stays in its directory after the process changes its working directory.
  Opening a store in a new directory syncs every directory it created into its parent,
  outermost first (observed through a test hook, since only a power cut would reveal it).
- Log reclamation: a process that checkpoints once per open keeps its log bounded run
  after run, and a fall-back to the retained generation still finds every record it needs.
- Sequential consistency under 8 writers and 6 paced readers for the whole run, and
  lost-update-free interleaved writers on one hot key, checked offline against the
  commit order. A `Snapshot` is one point in that order: later writes never reach it. Its
  iterators run in both directions, agree with a prefix filter on keys dense in `0x00`
  and `0xFF` bytes, and may outlive the bounds they were made from.
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
- The crate-level walk-through and the README's example compile against the API (doc
  tests), and the regression tests for this release's fixes each fail when their fix is
  reverted (checked once by hand).
- Beyond the suite, run once by hand: a crash loop of 400 `kill -9` rounds against a store
  with 300-byte segments, batches and frequent checkpoints recovered a contiguous,
  never-shrinking prefix at every reopen (7,538 keys), with the log held between 12 and 47
  segments (before the reclamation fix, 1,847).

**Measured on the development machine** (Apple silicon, macOS, whose `fsync` is a full
flush):

| Workload | Result |
|---|---|
| 1 thread, `put` | 325 commits/s, one sync per commit |
| 8 threads, `put` | 1,101 commits/s, mean group 4.1 |
| 32 threads, `put` | 3,335 commits/s, mean group 16 |
| Clone of a 100k × 1 KiB snapshot map (per commit) | 19 ms with owned bytes → 11 ms with shared bytes |
| 1 thread, `put`, store of 1k / 100k / 1M keys × 100 B (release) | 300 / 185 / 54 commits/s (see 0.2.0) |

**What is not there** (see the milestones): Raft does not drive the durable store; there
is no network transport and no election timer; the runtime and the reactor are not
connected; the snapshot map is still cloned whole per commit (O(entries)); nothing
checkpoints automatically; the B+ tree does not rebalance on delete and the value store
is not wired into the WAL or snapshot formats; the only size limit is the `u32` record
framing (enforced: an oversized commit is refused with `InvalidInput`); `Options` has one
setting, and there is no logging or metrics beyond `Store::stats`. Recovery is
point-in-time at the end of the log: a damaged record in the last segment ends it (a probe
lost 96 of 100 acknowledged keys to one flipped bit), and a lost newest segment looks like
a shorter log. A store whose only checkpoint is damaged refuses to open even while its log
still starts at record 1 and could rebuild everything. On a filesystem without file
locks, `open` goes ahead unlocked. Failure paths are tested by forcing failures
in-process; a real `ENOSPC` was reproduced only by hand, on a tiny `tmpfs`. (All 0.3.0.)

**Verify a checkout with:**

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
cargo test --all-features       # about a minute; Unix-only (the crash tests use kill -9)
```

CI (`.github/workflows/ci.yml`) runs the tests on Linux and macOS with and without
`experimental`, and formatting, clippy and rustdoc (both feature sets) and `cargo package`
on Linux. Locally, run `cargo package` last, or clean afterwards (`cargo clean -p
keystory`): its verification build shares `target/` and can leave the default-feature
library looking fresh when it is not.

**Working conventions:**

- Every module's doc comment states what it is, what it is not, and what is tracked here.
  Keep that true when you change behaviour.
- Every bug fix lands with a regression test whose name says what it guards; every
  claim in this file's status block is backed by a named test or a measurement.
- The public API is a promise from 1.0: keep it small. A new building block goes behind
  `experimental` until `Store` uses it; internals stay private.
- A change to an on-disk format bumps that file's version and is listed in
  `CHANGELOG.md`; a build never guesses at a version it does not know.
- Dependencies: none by default, `mio` for `experimental`, by decision. Adding one is a
  roadmap entry, not a footnote.
- `unsafe` is confined to the raw-waker vtable in `src/rt.rs`, each block annotated;
  `#![deny(unsafe_code)]` enforces it everywhere else.
- Target the latest stable Rust: bump `rust-toolchain.toml` and `rust-version` together.
- When a milestone item lands, tick it off below and record it in `CHANGELOG.md`; keep
  the status block current at every hand-off.

## Road to 1.0

### What 1.0 promises

1.0 is the point where a user can build on keystory without reading its source. It is
reached when all of the following hold, each backed by a named test or a measurement:

1. **A stable API.** From 1.0 the public API changes only compatibly within 1.x (semver,
   checked by `cargo-semver-checks` in CI). It is deliberately small: `Store`, `Options`,
   `Snapshot`, `Op`, `Stats`, and the `checker` test oracle. Everything else is private or
   behind the `experimental` feature, which promises nothing.
2. **Stable, versioned files.** Every file keystory writes carries a format version. 1.x
   reads every format an earlier 1.x wrote and refuses a newer one with `Unsupported`,
   never misreads it; a golden-fixture suite (a store written by each release, opened by
   every later one) proves it on every change.
3. **A written durability contract, proven by fault injection.** What an acknowledged write
   survives (a process crash; power loss, on a filesystem that honours `fsync`), what
   recovery does with a torn tail and with corruption, what `ENOSPC` and `EIO` do, the
   single-owner rule. Each clause is backed by a test that injects the fault through a
   filesystem seam, not only by forcing it in-process.
4. **No performance cliffs.** A commit costs O(log n) in the number of keys (today O(n):
   see 0.2.0), recovery needs memory proportional to the dataset rather than to the log,
   and the log is reclaimed without the caller remembering to checkpoint. Numbers are
   published together with the benchmark that produced them.
5. **Complete documentation.** Every public item documented (enforced by `missing_docs`),
   a guide-level README, and the on-disk format and durability contract written down.
6. **A decided scope** (next section).

### Decision needed: what 1.0 contains

- **(A) Recommended: 1.0 is the single-node engine.** Milestones 0.2 and 0.3, then
  stabilisation. Replication stays behind `experimental` and lands during 1.x as additive
  work: its record types and state files are new format versions, which the compatibility
  policy lets a later 1.x add.
- **(B) 1.0 is the replicated store**, the original north star. Milestones 0.4 to 0.6 come
  before stabilisation.

The case for (A): the single-node path is bounded and mostly understood, while replication
is the largest and most open-ended body of work left (transport, timers, persisted Raft
state, snapshot installation, membership changes, a client protocol, multi-process fault
testing), and all of it builds on a single-node engine whose API and formats should be
stable first. Under either scope, 0.2 and 0.3 come next.

### Milestones

| Version | Theme | Status |
|---------|-------|--------|
| 0.1.0 | First release: a curated API, versioned formats, the review's fixes, release engineering | **ready to release** |
| 0.2.0 | Scale the engine: O(log n) commits, bounded-memory recovery, automatic checkpoints | planned |
| 0.3.0 | Hardening: fault injection, torn write vs corruption, fuzzed decoders, soak | planned |
| 0.4.0 | Raft on the durable engine | planned (scope B; otherwise 1.x) |
| 0.5.0 | Transport and timers; the runtime driven by the reactor | planned (scope B; otherwise 1.x) |
| 0.6.0 | Cluster operations: membership, snapshot installation, linearizable reads | planned (scope B; otherwise 1.x) |
| 1.0.0 | Stabilisation: API and format freeze | planned |

A milestone is **DONE** when its checklist is complete, CI is green, this file and
`CHANGELOG.md` are updated, and the release is tagged.

#### 0.1.0 — first release

Publish what exists, with a public surface small enough to keep, file formats that can
evolve, and the fixes from the pre-release review (see History). Everything but the
release itself is done on the `release-0.1.0` branch.

- [x] **Public API narrowed** to `Store`, `Options`, `Snapshot`, `Op`, `Stats` and
  `checker`; `engine`, `wal`, `checkpoint` (renamed from `snapshot`), `rcu` and `types` are
  private.
- [x] **`experimental` feature** for the building blocks `Store` does not use: `raft`, `rt`,
  `asyncio`, `btree_store`, `valuestore`, `epoch_rcu`. `mio` is optional, so the default
  build has **no dependencies** (and no `unsafe`).
- [x] **`Options`** replaces `open_with(dir, u64)`; `Stats` and `Op` are `#[non_exhaustive]`.
- [x] **Read API:** `Store::snapshot()` returns a consistent point-in-time view for
  multi-key reads, so `get_at` is gone; `get_with_index` is now `get_with_version`,
  `put_batch` is now `apply_batch`, `term()` is removed until replication gives it a
  meaning, and `Op::put` / `Op::delete` construct ops.
- [x] **Versioned files:** every WAL segment starts with a 12-byte header (magic, format
  version, and a CRC of both, so a damaged version field is not mistaken for a newer
  format). An unknown version is refused with `Unsupported`, never misread: in the WAL,
  and in checkpoints, which check their CRC before their version and do not fall back to
  `snap.prev` past a newer-format `snap.dat`.
- [x] **Review fixes**, each with a regression test that fails when the fix is reverted: a
  checkpoint after a fall-back recovery no longer destroys the only good generation; a
  CRC-valid record that does not decode is corruption, not a torn tail; a new store
  directory is `fsync`'d into its parent; recovery streams the log instead of collecting
  it in memory. From the second pass: the first checkpoint after a restart reclaims the
  log (a process that checkpointed once per run never did); a store opened through a
  relative path no longer follows `chdir`; a failed rotation removes the segment it
  half-created, and one that finds the name taken poisons the log; a short checkpoint of
  a newer format is `Unsupported`, not damage; snapshot iterators are a named type that
  borrows only the snapshot.
- [x] **Error contract** documented on `Store`: which `io::ErrorKind` means what.
- [x] **Lints:** `missing_docs` (every public item documented); `unsafe_code` denied outside
  `rt`.
- [x] **Release engineering:** crates.io metadata (the name is free), `CHANGELOG.md`,
  crate docs with the file formats and the compatibility policy, doc examples run as
  tests (the README's example is compiled too), CI testing both feature sets, plus
  doctests and `cargo package`.
- [ ] **Release** (needs the owner): review and merge the branch, set the date in
  `CHANGELOG.md`, tag `v0.1.0`, `cargo publish`, and mark this milestone **DONE**.

*Exit criteria:* CI green on Linux and macOS for both feature sets; `cargo package` clean;
the README quick start compiles (it is a doctest).

#### 0.2.0 — scale the single-node engine

Measured on the development machine (one writer, 100-byte values, release build, macOS on
Apple silicon, whose `fsync` is a full flush):

| Keys in the store | Commits/s | Per commit |
|---|---|---|
| 1,000 | 300 | 3.3 ms (the `fsync` floor) |
| 10,000 | 266 | 3.8 ms |
| 100,000 | 185 | 5.4 ms |
| 1,000,000 | 54 | 18.5 ms |

Every group commit clones the whole snapshot map, so past roughly 100k keys the clone, not
the disk, sets the commit rate.

- **A structurally shared (persistent) ordered map** for the snapshot, so a commit costs
  O(log n): path copying over `Arc`'d nodes, hand-written under the dependency policy.
  *Exit:* per-commit latency at 1M keys within noise of the `fsync` floor.
- **Streaming checkpoint load.** `load` reads the whole file before decoding it, so peak
  memory at open is the file plus the map; decode record by record instead.
- **Automatic checkpoints.** A log-size threshold in `Options`, checked after each group
  commit and acted on off the commit path, so the log stays bounded without the caller
  scheduling `checkpoint()`.
- **Configurable limits** in `Options`: maximum key, value and batch sizes (today only the
  `u32` record framing bounds them).
- **Fewer copies on the write path:** a value is copied into the `Op`, again into the WAL
  record, and again into the shared bytes of the map; drop the record copy.
- *Exit:* the table above becomes a benchmark in the repository and is re-measured;
  recovery memory is bounded by the dataset.

#### 0.3.0 — hardening

- **Fault injection.** A crate-private filesystem seam with a test implementation that
  fails writes, `fsync`s, renames and directory syncs on command, and simulates a crash
  that loses unsynced data. It covers `ENOSPC`/`EIO` mid-append (today forced in-process,
  and reproduced once by hand on a 64 KiB tmpfs), crash points inside a checkpoint, failed
  segment creation, and the directory-entry durability that 0.1.0 can only check through a
  test hook.
- **Torn write vs corruption in the last segment.** Recovery is point-in-time: the first
  bad record in the last WAL segment ends the log. That is right for a torn tail, but a bit
  flip in acknowledged data looks the same (probe: one flipped bit early in the last
  segment lost 96 of 100 acknowledged keys). Candidates: mark group-commit boundaries so
  that damage followed by a later complete group is recognised as corruption (and refused,
  with a repair tool); etcd-style torn-sector detection; an opt-in strict mode. A lost
  newest segment is the same problem one level up: nothing records how long the log
  should be. A small manifest (the last durable index, the segment range) would detect
  both. Needs a design note before code.
- **Recovery from the log alone.** When no checkpoint is usable (`snap.dat` damaged and
  no `snap.prev`, as between a store's first and second checkpoints) `open` refuses,
  though the log may still start at record 1 and hold every commit; the gap check already
  proves such a log complete. Rebuild from it instead (review, second pass: probe
  recovered all 15 keys that way).
- **Locking.** On a filesystem that cannot lock files, `open` goes ahead unlocked. Decide
  whether to refuse instead, with an explicit opt-out in `Options`.
- **Fuzzed and property-tested decoders** for the WAL and checkpoint formats (the tests use
  hand-rolled xorshift generators today); see the dev-dependency decision below.
- **Soak and Miri in CI:** a scheduled long-running workload with periodic `kill -9`, and
  Miri over `rt` (it passes as a one-off today).
- **Observability:** more counters in `Stats` (bytes written, records replayed at open,
  torn bytes dropped, recovery time) and a hook for events such as a dropped torn tail.
- **Platforms:** decide Windows' tier. The library builds there; the crash harness is
  Unix-only (`kill -9`).

#### 0.4.0 — Raft on the durable engine

Persist `term` and `votedFor` (a small durable state file), keep per-entry terms in WAL
records (the field exists; it is always 1 today), support truncating an uncommitted suffix
(a new record type), make `Store` the applied state machine, let a checkpoint double as
the Raft snapshot, and add `InstallSnapshot` for followers that fell behind the log.
*Exit:* a node killed with `kill -9` and restarted loses no committed entry, and the
in-process replication suite passes over durable nodes.

#### 0.5.0 — transport and timers

Register wakers with the `mio` reactor and poll it whenever the runtime's ready queue is
empty; Unix-domain-socket RPC (decided in Phase 4); election timeouts and heartbeats.
*Exit:* a multi-process cluster survives `kill -9` of its leader and a partitioned
minority, as checked by the linearisability oracle.

#### 0.6.0 — cluster operations

Membership changes (single-server changes or joint consensus), linearizable reads without
a log write (ReadIndex or leader leases), a client protocol with leader redirection, and
snapshot transfer.

#### 1.0.0 — stabilisation

- API review and freeze: `io::Error` with documented kinds (as now) or a crate error enum
  -- an operating-system error keeps its own kind, which can coincide with one keystory
  gives a meaning (`EINVAL` is `InvalidInput`), so the kind alone cannot prove which case
  occurred; `RangeBounds`-based ranges; iterators or `Vec`s from `Store`'s scans; naming.
- Format freeze and the golden-fixture compatibility suite.
- `cargo-semver-checks` in CI against the last published release.
- The durability contract, the benchmarks and the docs complete.
- The standalone modules integrated or removed: the B+ tree (rebalancing on delete, or
  retirement once the persistent map lands), the value store (`BlobRef`s in WAL and
  checkpoint records for values above a threshold), the epoch-RCU model.

### Open decisions (for the owner)

1. **1.0 scope:** (A) single-node or (B) replicated; see above.
2. **Dev-dependencies.** "`mio` only" was decided for what users compile. Does it extend to
   test and bench tooling (`proptest`, `criterion`, or `libfuzzer-sys` in a separate
   `fuzz/` crate that keystory itself never depends on)? Recommendation: keep runtime
   dependencies minimal and allow test tooling that never reaches a user's build.
3. **Windows:** tier 2 (builds, unit tests in CI) or unsupported.
4. **Error type for 1.0:** see 1.0.0.

## Design decisions in force

Recorded so later work does not re-litigate them.

- **Durable before observable.** Ops are appended to the WAL and `fsync`'d before the new
  snapshot is published; a caller is never acknowledged before that `fsync`.
- **Only the logical commit index orders events.** Nothing depends on wall-clock time,
  which is what makes recovery deterministic. `Op::apply` is idempotent.
- **Authoritative state is an immutable snapshot** behind `RcuSwap` (an
  `RwLock<Arc<Snapshot>>`): readers load an `Arc` and never take the commit path. Keys
  and values inside the map are `Arc<[u8]>`; the API boundary uses owned `Vec<u8>`, and
  a `Snapshot` lends its bytes out as `&[u8]`.
- **Group commit.** Writers queue tickets; one flusher commits everything queued with one
  WAL append and one `fsync`, then publishes one snapshot; the rest wait on a condvar.
  Each `put`/`delete` keeps its own index. `apply_batch` is one index, one record,
  all-or-nothing.
- **WAL.** Per-record CRC; single-op records keep the Phase-1 layout, batches use tag 3.
  A torn tail is legitimate only in the last segment and is truncated at open;
  corruption elsewhere is an error. Within the last segment recovery is point-in-time:
  the first bad record ends the log. A record with a valid CRC that does not decode is
  corruption wherever it is. Segment creation and rotation `fsync` the directory. The
  log is reclaimed only by checkpoints, never at open. Past the checkpoint, replay
  requires the index to continue without a gap. Recovery streams the log.
- **Versioned files.** A WAL segment starts with a 12-byte header (`KSWL`, a `u32`
  version, a CRC of both) written and `fsync`'d before the segment's directory entry is
  synced or any record appended; every future segment format keeps that header. A
  checkpoint starts with `KSN1` and a version and ends with a CRC of everything before
  it, and a reader checks that CRC before the version. So an intact file of an unknown
  version is `Unsupported` (never misread, never "repaired", and no grounds for falling
  back to `snap.prev`), while damage is `InvalidData`. A segment shorter than its header,
  or header-sized but not a valid header, is a creation a crash interrupted, and `open`
  rewrites it; a bad header in front of records is corruption.
- **Failed writes.** A failed append (WAL or blob log) is rolled back to the last durable
  record, so no unacknowledged bytes stay in the log and an index is never logged twice;
  if the rollback fails too, the log is *poisoned* -- writes refused, reads served --
  until a reopen, whose torn-tail repair takes over. The one rule a failed rotation keeps:
  no newer segment may exist beside the one being appended to (a later torn tail there
  would look like corruption). So it removes the segment it half-created (and syncs the
  removal), and poisons the log if it cannot, or if the next name is already taken. A
  write that poisons the log has an unknown outcome until the reopen. A commit that the
  `u32` framing cannot hold is refused before it joins a group, so it fails only its own
  caller.
- **One owner per directory.** `Store::open` holds an exclusive lock on `LOCK` (std
  `File::try_lock`) for the store's lifetime; it is taken before recovery touches the log.
  On a filesystem that cannot lock, `open` goes ahead unlocked (open question, 0.3.0).
  Every directory `open` creates is `fsync`'d into its parent, and the directory is
  canonicalised at open, so the store never follows the process's working directory or a
  re-pointed symlink.
- **Checkpoints.** Streamed body, tmp + `fsync` + rename + directory `fsync`. The previous
  checkpoint is retained as `snap.prev`, and the segments it covered are deleted only by
  the *next* checkpoint, so a fall-back always has a complete log. Across a restart the
  boundary is recomputed: recovery notes the segment holding the first record the loaded
  checkpoint does not cover, and everything below it is what the next `snap.prev` covers.
  After a fall-back
  recovery, the next checkpoint replaces the damaged `snap.dat` instead of demoting it
  over `snap.prev`. Checkpoints exclude flushers only for the instant that pins the
  snapshot and rotates the WAL, and are serialised with each other.
- **Reads.** `get`, `scan`, `range_scan` and `snapshot` are served from the snapshot map;
  the B+ tree is a standalone module, not an index the store maintains. A `Snapshot`'s
  scans return `Entries`, a named, double-ended iterator over a range of the map that
  borrows the snapshot and nothing else; a prefix scan is the range from the prefix to
  its exclusive upper bound.
- **Errors.** `std::io::Error` throughout, with the kinds documented on `Store` as a
  contract: `WouldBlock` (locked), `InvalidData` (damage recovery will not repair),
  `Unsupported` (another format version), `InvalidInput` (a commit too large), `Other`
  (poisoned). Operating-system errors pass through with their own kinds, which can
  coincide with these. Whether 1.0 keeps this or adopts a crate error enum is open.
- **Public API.** `Store`, `Options`, `Snapshot` (with `Entries`), `Op`, `Stats` and
  `checker`;
  everything else is private or behind `experimental`. `Stats` and `Op` are
  `#[non_exhaustive]` and `Options` has private fields, so each can grow compatibly.
- **Raft driver.** Protocol logic is real (majority election, log matching, majority
  commit, whole-cluster quorum); the transport is in-process and synchronous. A leader
  is sticky until it fails or loses its quorum; reads are leader reads that first catch
  every live follower up. The FSM keeps `votedFor` across same-term appends, skips
  redelivered entries and replaces conflicting ones, and commits only up to the last
  entry an `AppendEntries` covered (Raft §5.3). Catch-up always reaches the target index,
  and only nodes whose log matches the leader's there apply the committed prefix.
- **Dependencies and `unsafe`.** No dependency by default; `mio` behind `experimental`.
  `unsafe` lives only in the raw-waker vtable, which is itself `experimental`, and the
  compiler enforces that.
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
milestone 0.4.0. *Revised in Phase 6:* elections ran on every write, reads could panic on
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

### Pre-release review and 0.1.0 preparation (2026-09-26)

A second review, aimed at what a first release and a 1.0 need rather than at the modules
one by one: the public surface, the on-disk formats' ability to evolve, performance at
scale, and the durability edge cases the audit left. The phase numbering ends here; the
Phase 7 backlog became milestones 0.2 to 0.6 and 1.0 above. Suspected bugs were confirmed
with throwaway probes against the pre-release code first, as in the audit:

- *Checkpoint after a fall-back (probe: store unopenable).* When `snap.dat` was damaged,
  recovery used `snap.prev` -- and the next checkpoint's first step renamed the damaged
  file over it. A crash before the second rename (or a failure of it) left no usable
  checkpoint and an unopenable store. The next checkpoint now replaces the damaged file
  in place (`a_checkpoint_after_a_fallback_keeps_the_good_generation`,
  `discarding_a_damaged_latest_keeps_the_good_previous_generation`).
- *CRC-valid but undecodable records (probe: truncated).* Such a record -- which no torn
  write produces -- was treated as a torn tail, and it and every record after it were cut
  off at open. It is now corruption, refused without truncating anything
  (`a_checksummed_record_that_does_not_decode_is_corruption_not_a_torn_tail`).
- *Directory durability.* `open` created a missing store directory without syncing its
  parent, so a power cut could lose the directory and every `fsync`'d write inside it.
  Every directory created is now synced into its parent
  (`creating_a_store_syncs_every_new_directory_entry`). The value store and the B+ tree
  use the same helper.
- *Bit rot in the last segment (probe: 96 of 100 acknowledged keys lost to one flipped
  bit).* Not a bug but the documented point-in-time rule, whose reach -- the rest of the
  last segment -- was wider than its description suggested. Distinguishing a torn write
  from corruption needs a design (0.3.0); the rule is now stated in the crate docs.
- *Scale (measured).* A single writer's commit rate falls from 300/s at 1k keys to 54/s at
  1M, because each group commit clones the snapshot map. This made the persistent map
  the first item of 0.2.0.

Built for the release: versioned WAL segment headers (with a header CRC, after the first
design would have reported a flipped version bit as a newer format), `Unsupported` for
unknown versions in the WAL and in checkpoints (which now check their CRC before their
version, and no longer fall back past a newer-format file), streaming recovery, cleanup of
a half-created segment after a failed rotation, a narrowed public API with `Options` and
`Snapshot` views, the `experimental` feature, `missing_docs` and `unsafe_code` lints, the
error contract, crate docs, `CHANGELOG.md`, and a CI that covers both feature sets,
doctests and packaging (`a_segment_of_another_format_version_is_refused`,
`another_format_version_is_refused_without_falling_back`,
`an_interrupted_segment_creation_is_repaired_at_open`,
`a_store_in_another_format_version_is_refused`,
`a_snapshot_is_a_consistent_point_in_time_view`). Each fix's regression test was checked
to fail with its fix reverted.

*Second pass.* A `kill -9` crash loop and an independent adversarial review of the change
set found no sequence of crashes that loses an acknowledged write, and these defects:

- *Reclamation across restarts (soak and probe: 4, 7, 10, ... 19 segments over six runs).*
  The boundary a checkpoint reclaims to was forgotten at every open, so only a process's
  second checkpoint reclaimed anything; one checkpoint per run never did, and every open
  read the whole history. Recovery now works it out
  (`one_checkpoint_per_open_still_reclaims_the_log`).
- *Relative paths (probe: 3 of 13 acknowledged writes recovered).* `Store` resolved its
  directory afresh for every new segment, checkpoint and deletion, so after a `chdir` it
  wrote into another directory -- and could delete another store's segments. The
  directory is pinned at open, in the value store too
  (`a_store_opened_by_a_relative_path_stays_where_it_was_opened`).
- *A rotation that found its segment name taken* kept appending beside a newer segment,
  so a later torn tail made the store unopenable; it now poisons the log. The rotation's
  failure paths, untested before (the claim above that every fix had a test was not true
  of them), are now driven through a small fault seam
  (`a_rotation_that_finds_its_segment_name_taken_poisons_the_log`,
  `a_rotation_whose_header_fails_removes_the_half_created_segment`,
  `a_rotation_that_cannot_undo_or_finish_its_segment_poisons_the_log`).
- *A short checkpoint of a newer format* was taken for damage, because the decoder
  checked this version's minimum length first; the shared layout (magic, CRC, version)
  is now checked before anything version-specific.
- *Snapshot iterators* captured the lifetimes of their bound arguments (edition 2024's
  `impl Trait` capture rules), so one could not outlive them; they are now a named type,
  `Entries` (`iterators_outlive_the_bounds_they_were_made_from`).
- *Documentation*: a write that poisons the log has an unknown outcome, not "not
  applied"; operating-system errors can share a kind with keystory's own; a lost newest
  segment looks like a shorter log; the lock is skipped where a filesystem cannot lock;
  CI tested only one feature set, and the cross-process lock claim had only an
  in-process test (`a_store_owned_by_another_process_is_refused_until_it_dies`).

Recovery from the log alone, when no checkpoint is usable but the log still starts at
record 1, went to 0.3.0.

---

*Convention: when a milestone is released, update the status block, tick its checklist,
record it in `CHANGELOG.md`, mark it **DONE**, then pause for approval before starting
the next.*
