# keystory

A crash-resilient key/value store engine in Rust, built from scratch in
approval-gated phases and documented honestly at every gate. The single-node
engine is real and tested under a genuine `kill -9`; the replication and async
layers are working in-process models that are not yet wired to it. `ROADMAP.md`
holds the state at handoff, the backlog, the design decisions in force, and a
condensed history.

## What it does

- **Durable writes.** Every `put`/`delete` is appended to a segmented, CRC-guarded
  write-ahead log and `fsync`'d *before* it becomes visible. A torn trailing record
  is detected and truncated on the next open, never trusted.
- **Group commit and batches.** Concurrent writers share one WAL append and one
  `fsync` per group (measured: 32 threads reach about 10× the single-thread rate);
  `put_batch` applies several ops under one index, all-or-nothing under a crash.
- **Snapshot reads.** Readers load an immutable, version-stamped snapshot and never
  take the commit path, so they never block a writer and never see a torn state.
- **Checkpoint + WAL-tail recovery.** A checkpoint is streamed to a temp file,
  `fsync`'d and renamed into place while writers keep running; the previous
  checkpoint is kept as a fall-back. `open` loads the latest usable checkpoint and
  replays only the newer WAL records.
- **A checkable consistency story.** An offline MVCC oracle validates every read a
  multi-threaded workload records against the commit order the engine assigned.
- **Raft, in process.** A pure Raft state machine (election, log matching, majority
  commit) driven synchronously by an in-process cluster: no lost updates across a
  leader failure, no minority commit. No sockets, no timers, no persistence yet.

## Quick start

The project targets the latest stable Rust (edition 2024); the exact toolchain is
pinned in `rust-toolchain.toml` and `rustup` installs it on first use.

```bash
cargo test --all-targets
```

The suite includes four tests that spawn `keystory-crash-runner`, write keys, and
`kill -9` the child (one of them mid-write) before recovering from disk, plus a
Jepsen-lite concurrency test, so it takes about a minute and is Unix-only.

```rust
use keystory::{Op, Store};

fn main() -> std::io::Result<()> {
    let store = Store::open("/tmp/keystory-demo")?;
    let index = store.put(b"user:42", b"alice")?; // durable once this returns
    assert_eq!(store.get(b"user:42"), Some(b"alice".to_vec()));
    assert_eq!(store.get_with_index(b"user:42").map(|(_, v)| v), Some(index));
    store.put_batch([
        Op::Put { key: b"user:43".to_vec(), value: b"bob".to_vec() },
        Op::Delete { key: b"user:42".to_vec() },
    ])?; // one commit, one index, all or nothing
    store.checkpoint()?; // fold the WAL into a snapshot; writers are not blocked
    Ok(())
}
```

## Layout

| Path | Role |
|------|------|
| `src/engine.rs` | `Store`: commit lock, WAL append + `fsync`, RCU publish; `open` = checkpoint + WAL tail |
| `src/wal.rs` | Segmented, CRC-guarded write-ahead log: torn-tail repair, batch records, one-`fsync` group append |
| `src/snapshot.rs` | Durable checkpoint file (streamed body, atomic publish, previous generation retained) |
| `src/rcu.rs` | `RcuSwap<T>`: the snapshot cell readers load lock-free of the commit path |
| `src/types.rs` | `Op`, `Entry`, `Snapshot`: the deterministic state model |
| `src/checker.rs` | Offline MVCC sequential-consistency oracle (Jepsen-lite) |
| `src/raft/` | Pure Raft FSM, in-process synchronous cluster driver (sticky leader, leader reads), async facade |
| `src/btree_store.rs` | Ordered B+ tree with a CRC-guarded document format (standalone, not used by `Store`) |
| `src/valuestore.rs` | Off-heap blob log for large values, ids stable across compaction (primitive, not yet wired in) |
| `src/epoch_rcu.rs` | Thread-safe epoch-based reclamation model (not the hot path) |
| `src/rt.rs` | Cooperative single-threaded async runtime with hand-built wakers |
| `src/asyncio.rs` | `mio` reactor: real kernel readiness with deadlines (Unix only) |
| `src/main.rs` | `keystory-crash-runner`, the SIGKILL harness used by `tests/crash_recover.rs` |
| `tests/` | Crash recovery (real `kill -9`), Jepsen-lite linearisability, replication |

## Policies

- **One dependency.** Phases 1 to 4 were std-only. Phase 5 added `mio` for real
  non-blocking I/O and nothing else: no consensus crate, no runtime, no serde.
- **`unsafe`** is confined to the raw-waker vtable in `src/rt.rs`, each block annotated.
- **Logical time only.** The commit index orders everything; nothing depends on the
  wall clock, which is what makes recovery deterministic.
- **Say what is proven.** Each phase in `ROADMAP.md` lists what its tests prove and
  what they deliberately do not.

## Status and known gaps

Phases 1 to 6 are complete; Phase 6 was a full review followed by consolidation, and
`ROADMAP.md` records exactly what the tests prove and what was measured. The headline
gaps, all in the Phase 7 backlog there: the Raft layer does not yet use the durable
store, there is no network transport, the runtime and the reactor are not connected,
and the snapshot map is still cloned whole (though not its bytes) on every commit.

## Development

```bash
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
```

CI (`.github/workflows/ci.yml`) runs all of the above plus the test suite on Linux
and macOS.

## License

Licensed under either of the [Apache License, Version 2.0](LICENSE-APACHE) or the
[MIT license](LICENSE-MIT) at your option.
