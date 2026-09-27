# keystory

A crash-resilient, embedded key/value store in Rust, built from scratch in
approval-gated phases and documented honestly at every gate. The single-node engine is
real, tested under a genuine `kill -9`, and has no dependencies. Replication (a Raft
state machine with an in-process cluster driver) and async I/O (a cooperative runtime
and a `mio` reactor) are working, tested building blocks behind the `experimental`
feature, not yet wired to the engine. `ROADMAP.md` holds the state at handoff, the road
to 1.0, the design decisions in force, and a condensed history.

## What it does

- **Durable writes.** Every `put`/`delete` is appended to a segmented, CRC-guarded
  write-ahead log and `fsync`'d *before* it becomes visible. A torn trailing record
  is detected and truncated on the next open, never trusted. A failed append (a full
  disk, say) is rolled back, so an I/O error never costs an acknowledged write, and
  recovery refuses a log with records missing from its middle rather than skipping them.
- **One owner per directory.** `open` locks the store directory; a second open, in this
  process or another, fails with `WouldBlock` instead of corrupting the live store (on a
  filesystem without file locks, `open` goes ahead unlocked). The store stays in the
  directory it opened, whatever the process's working directory later becomes.
- **Group commit and batches.** Concurrent writers share one WAL append and one
  `fsync` per group (measured: 32 threads reach about 10× the single-thread rate);
  `apply_batch` applies several ops under one index, all-or-nothing under a crash.
- **Snapshot reads.** Readers load an immutable, version-stamped snapshot and never
  take the commit path, so they never block a writer and never see a torn state;
  `Store::snapshot` hands one out for any number of mutually consistent reads.
- **Checkpoint + WAL-tail recovery.** A checkpoint is streamed to a temp file,
  `fsync`'d and renamed into place while writers keep running; the previous
  checkpoint is kept as a fall-back. `open` loads the latest usable checkpoint and
  replays only the newer WAL records.
- **Versioned files.** Every file names its format version; one this build does not
  know is refused with `Unsupported`, never misread.
- **A checkable consistency story.** An offline MVCC oracle (`keystory::checker`)
  validates every read a multi-threaded workload records against the commit order the
  engine assigned.
- **Raft, in process** (`experimental`). A pure Raft state machine (election, log
  matching, majority commit) driven synchronously by an in-process cluster: no lost
  updates across a leader failure, no minority commit. No sockets, no timers, no
  persistence yet.

## Quick start

```toml
[dependencies]
keystory = "0.1"
```

```rust,no_run
use keystory::{Op, Store};

fn main() -> std::io::Result<()> {
    let store = Store::open("/tmp/keystory-demo")?;
    let index = store.put("user:42", "alice")?; // durable once this returns
    assert_eq!(store.get("user:42"), Some(b"alice".to_vec()));
    assert_eq!(store.get_with_version("user:42").map(|(_, v)| v), Some(index));

    // One commit, one index, all or nothing.
    store.apply_batch([Op::put("user:43", "bob"), Op::delete("user:42")])?;

    // A consistent view for many reads, unaffected by later writes.
    let view = store.snapshot();
    for (key, value) in view.scan("user:") {
        println!("{} = {}", String::from_utf8_lossy(key), String::from_utf8_lossy(value));
    }

    store.checkpoint()?; // fold the WAL into a checkpoint; writers are not blocked
    Ok(())
}
```

The crate documentation carries the same walk-through as a tested doctest, along with
the error contract, the directory's files and their format versions, and the
compatibility policy. The project targets the latest stable Rust (edition 2024); the
exact toolchain is pinned in `rust-toolchain.toml` and `rustup` installs it on first use.

## Features

| Feature | Adds | Dependencies |
|---------|------|--------------|
| *(default)* | The engine: `Store`, `Options`, `Snapshot` (and its `Entries` iterator), `Op`, `Stats`, and the `checker` oracle | none |
| `experimental` | `raft`, `rt`, `asyncio` (Unix), `btree_store`, `valuestore`, `epoch_rcu`: tested, but no stability promise | `mio` |

## Layout

| Path | Role |
|------|------|
| `src/engine.rs` | `Store` and `Options`: commit lock, WAL append + `fsync`, RCU publish; `open` = checkpoint + WAL tail |
| `src/wal.rs` | Segmented, CRC-guarded write-ahead log with versioned segment headers: torn-tail repair, batch records, one-`fsync` group append |
| `src/checkpoint.rs` | Durable checkpoint file (streamed body, atomic publish, previous generation retained) |
| `src/rcu.rs` | `RcuSwap<T>`: the snapshot cell readers load lock-free of the commit path |
| `src/types.rs` | `Op`, `Snapshot`: the deterministic state model and its read view |
| `src/checker.rs` | Offline MVCC sequential-consistency oracle (Jepsen-lite) |
| `src/raft/` | *experimental:* pure Raft FSM, in-process synchronous cluster driver (sticky leader, leader reads), async facade |
| `src/btree_store.rs` | *experimental:* ordered B+ tree with a CRC-guarded document format (standalone) |
| `src/valuestore.rs` | *experimental:* off-heap blob log for large values, ids stable across compaction (not yet wired in) |
| `src/epoch_rcu.rs` | *experimental:* thread-safe epoch-based reclamation model (not the hot path) |
| `src/rt.rs` | *experimental:* cooperative single-threaded async runtime with hand-built wakers |
| `src/asyncio.rs` | *experimental:* `mio` reactor: real kernel readiness with deadlines (Unix only) |
| `src/main.rs` | `keystory-crash-runner`, the SIGKILL harness used by `tests/crash_recover.rs` |
| `tests/` | Crash recovery (real `kill -9`), Jepsen-lite linearisability, replication (`experimental`) |

## Policies

- **Dependencies.** Phases 1 to 4 were std-only, and so is the default build. Phase 5
  added `mio` for real non-blocking I/O, and only `experimental` pulls it in: no
  consensus crate, no runtime, no serde.
- **`unsafe`** is confined to the raw-waker vtable in `src/rt.rs`, each block annotated,
  and the compiler enforces it (`#![deny(unsafe_code)]` everywhere else). `rt` is
  `experimental`, so the default build contains no `unsafe` at all.
- **Logical time only.** The commit index orders everything; nothing depends on the
  wall clock, which is what makes recovery deterministic.
- **Say what is proven.** `ROADMAP.md` lists what the tests prove and what they
  deliberately do not; every fixed bug has a named regression test.

## Status and known gaps

Version 0.1.0 is being prepared: the first release, with a deliberately small public
API and versioned on-disk formats (see `CHANGELOG.md`). The gaps, all scheduled in
`ROADMAP.md`:

- The whole live dataset is held in memory, and every commit clones the snapshot's map:
  a single writer manages about 300 commits/s on a small store (the `fsync` floor) but 54
  at a million keys (0.2.0).
- Nothing checkpoints automatically; call `Store::checkpoint` to bound the log (0.2.0).
- Recovery is point-in-time at the end of the log: a damaged record in the last WAL
  segment ends the log, whether a crash tore it or a bit flip damaged it, and a lost
  newest segment would look the same (0.3.0).
- Replication is not wired to the durable engine, and there is no network transport
  (0.4.0 and later).

## Development

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
cargo test --all-features       # about a minute; Unix-only (the crash tests use kill -9)
```

CI (`.github/workflows/ci.yml`) runs the test suite on Linux and macOS, with and without
`experimental`; formatting, clippy and rustdoc (for both feature sets) and a packaging
check, which builds the crate as a publish would, run on Linux.

## License

Licensed under either of the [Apache License, Version 2.0](LICENSE-APACHE) or the
[MIT license](LICENSE-MIT) at your option.
