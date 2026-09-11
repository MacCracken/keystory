# keystory

A crash-resilient key/value store engine in Rust, built from scratch in
approval-gated phases and documented honestly at every gate. The single-node
engine is real and tested under a genuine `kill -9`; the replication and async
layers are working in-process models that are not yet wired to it. `ROADMAP.md`
is the design log, the threat/failure audit of each phase, and the open backlog.

## What it does

- **Durable writes.** Every `put`/`delete` is appended to a segmented, CRC-guarded
  write-ahead log and `fsync`'d *before* it becomes visible. A torn trailing record
  is detected and dropped on replay, never trusted.
- **Snapshot reads.** Readers load an immutable, version-stamped snapshot and never
  take the commit lock, so they never block a writer and never see a torn state.
- **Checkpoint + WAL-tail recovery.** A checkpoint is written tmp + `fsync` +
  atomic rename; `open` loads the last checkpoint and replays only the newer WAL
  records.
- **A checkable consistency story.** An offline MVCC oracle validates every read a
  multi-threaded workload records against the commit order the engine assigned.
- **Raft, in process.** A pure Raft state machine (election, log matching, majority
  commit) driven synchronously by an in-process cluster: no lost updates across a
  leader failure, no minority commit. No sockets, no timers, no persistence yet.

## Quick start

The toolchain is pinned in `rust-toolchain.toml`; `rustup` installs it on first use.

```bash
cargo test --all-targets
```

The suite includes three tests that spawn `keystory-crash-runner`, write keys, and
`kill -9` the child before recovering from disk, plus a Jepsen-lite concurrency
test, so it takes about a minute and is Unix-only.

```rust
use keystory::Store;

fn main() -> std::io::Result<()> {
    let store = Store::open("/tmp/keystory-demo")?;
    let index = store.put(b"user:42", b"alice")?; // durable once this returns
    assert_eq!(store.get(b"user:42"), Some(b"alice".to_vec()));
    assert_eq!(store.get_with_index(b"user:42").map(|(_, v)| v), Some(index));
    store.checkpoint()?; // fold the WAL into a snapshot
    Ok(())
}
```

## Layout

| Path | Role |
|------|------|
| `src/engine.rs` | `Store`: commit lock, WAL append + `fsync`, RCU publish; `open` = checkpoint + WAL tail |
| `src/wal.rs` | Segmented, CRC-guarded write-ahead log with torn-tail detection |
| `src/snapshot.rs` | Durable checkpoint file (streamed body, atomic publish) |
| `src/rcu.rs` | `RcuSwap<T>`: the snapshot cell readers load lock-free of the commit path |
| `src/types.rs` | `Op`, `Entry`, `Snapshot`: the deterministic state model |
| `src/checker.rs` | Offline MVCC sequential-consistency oracle (Jepsen-lite) |
| `src/raft/` | Pure Raft FSM, in-process synchronous cluster driver, async facade |
| `src/btree_store.rs` | Ordered B+ tree with a CRC-guarded document format |
| `src/valuestore.rs` | Off-heap blob log for large values (primitive, not yet wired in) |
| `src/epoch_rcu.rs` | Epoch-based reclamation model (not the hot path) |
| `src/rt.rs` | Cooperative single-threaded async runtime with hand-built wakers |
| `src/asyncio.rs` | `mio` reactor proving real kernel readiness (Unix only) |
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

Phases 1 to 5 are complete. A full review on 2026-09-11 found bugs and design gaps
that the phase gates missed; they are being worked through as Phase 6 in
`ROADMAP.md`. The headline items: the Raft layer does not yet use the durable store,
every commit clones the whole map, there is no group commit, and the B-tree range
index is being reconsidered.

## Development

```bash
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
cargo +1.81 check --all-targets   # MSRV
```

CI (`.github/workflows/ci.yml`) runs all of the above plus the test suite on Linux
and macOS.

## License

Licensed under either of the [Apache License, Version 2.0](LICENSE-APACHE) or the
[MIT license](LICENSE-MIT) at your option.
