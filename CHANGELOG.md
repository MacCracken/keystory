# Changelog

All notable changes to keystory are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html): before 1.0, a minor release
may change the public API or an on-disk format, and says so here.

## [0.1.0] - Unreleased

The first release: the single-node engine, with a deliberately small public API and
versioned on-disk formats.

### The engine

- `Store`: durable writes (`put`, `delete`, `apply_batch`), each appended to a segmented,
  CRC-guarded write-ahead log and `fsync`'d before it becomes visible; group commit
  (concurrent writers share appends and `fsync`s); atomic batches; one owner per
  directory, enforced by a lock.
- Reads from immutable snapshots that never block a writer: `get`, `get_with_version`,
  `scan`, `range_scan`, and `snapshot()` for a consistent view across many reads, whose
  scans return a double-ended `Entries` iterator that borrows the snapshot and nothing else.
- `checkpoint()`: an atomically published checkpoint, written while writers keep running,
  with the previous generation retained as a fall-back; each checkpoint reclaims the log
  the previous one covers. Recovery loads the latest usable checkpoint and streams the log
  written since.
- `Options` (the WAL segment size), `Stats` (commit counters), `Op` (`Op::put`,
  `Op::delete`).
- `checker`: an offline sequential-consistency oracle for concurrent workloads.
- The `experimental` feature: a Raft state machine with an in-process cluster driver and
  an async facade, a cooperative runtime, a `mio` reactor, a B+ tree, an off-heap value
  store and an epoch-RCU model. These are tested building blocks that `Store` does not
  use yet, with no stability promise. Without the feature, keystory has no dependencies.

### On-disk formats

- WAL segments begin with a 12-byte header: `KSWL`, format version 1, and a CRC of both.
  Checkpoints (`snap.dat`, `snap.prev`) are `KSN1`, format version 1.
- A file in a format version this build does not know is refused with
  `ErrorKind::Unsupported`, never misread.

### Known limitations

- The whole live dataset is held in memory, and every commit clones the snapshot's map,
  at a cost linear in the number of keys: one writer measured about 300 commits/s with
  1,000 keys and 54 with 1,000,000.
- Nothing checkpoints automatically; call `Store::checkpoint` to bound the log.
- Recovery is point-in-time at the end of the log: a damaged record in the last WAL
  segment ends the log, whether a crash tore it or the medium corrupted it later, and a
  lost newest segment would look the same.
- On a filesystem without file locks, `open` goes ahead without the directory lock.
- Tested on Linux and macOS.

### Changes from the pre-release repository

For anyone who built keystory from its repository before this release:

- **API.** The `engine`, `wal`, `snapshot`, `rcu` and `types` modules are private, and
  with them everything they held that is not re-exported at the crate root (the `Entry`
  and `Shared` re-exports included). `Snapshot`'s fields are private: use its methods,
  where `scan_prefix` and `range` are now `scan` and `range_scan` and return `Entries`.
  `Op::apply` is internal, and the unused `checker::WriteRec` is gone. The experimental
  modules need the `experimental` feature. `Store::put_batch` is now `apply_batch` and
  `Store::get_with_index` is now `get_with_version`; `Store::get_at` is gone (read from
  `Store::snapshot` instead), and so is `Store::term`. `Store::open_with` takes `Options`.
  `Stats` and `Op` are `#[non_exhaustive]`.
- **Format.** WAL segments now start with a versioned header, so a store written before
  this release does not open (`InvalidData`).
- **Fixed:**
  - A checkpoint taken after recovery had fallen back to `snap.prev` renamed the damaged
    `snap.dat` over it, so an interrupted checkpoint could leave the store unopenable.
  - A WAL record with a valid CRC that did not decode was cut off as a torn tail, with
    every record after it, instead of being reported as corruption.
  - A store opened through a relative path followed the process's working directory: after
    a `chdir`, new segments and checkpoints went to another directory, and reopening the
    store lost the writes in them. The directory is now pinned at `open`.
  - Only the second checkpoint of a process reclaimed any log, so a process that
    checkpointed once per run never reclaimed it, and every open read all of it.
  - A newly created store directory was not `fsync`'d into its parent directory.
  - Recovery collected the whole log in memory before applying it; it now streams.

[0.1.0]: https://github.com/MacCracken/keystory/releases/tag/v0.1.0
