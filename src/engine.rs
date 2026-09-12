//! # The store engine (single node)
//!
//! `Store` is the public entry point. It composes the other primitives:
//!
//! * the **authoritative state** is a [`RcuSwap`] of `Snapshot` (internally
//!   `Arc<Snapshot>`) -- a version-stamped, immutable, parallel-reader view whose keys
//!   and values are reference-counted, so the per-commit clone costs O(entries), not
//!   O(bytes).
//! * every mutation is **durable-then-published**: the ops are appended to the WAL and
//!   `fsync`'d *before* the new snapshot is published into the RCU. A crash before the
//!   append has no effect; a crash after it is recovered on the next open.
//! * commits are **grouped**: concurrent writers queue their ops; one of them flushes
//!   everything queued as one WAL append and one `fsync`, then publishes one snapshot
//!   carrying all of them (each op keeps its own commit index), while the others wait
//!   on a condvar and return as soon as their ops are durable. Throughput scales with concurrency instead of paying one `fsync` per
//!   writer. [`Store::put_batch`] applies several ops under one index, all-or-nothing.
//! * **reads** (`get`, `scan`, `range_scan`, `get_at`) load a frozen `Arc<Snapshot>` and
//!   never touch the commit lock, so they never block a writer and never observe a
//!   half-updated state. A reader that loads while a commit is in flight sees either
//!   the pre- or the post-commit snapshot -- both are legitimate points in a sequential
//!   history, which is exactly what the Jepsen-lite checker enforces.
//! * a **checkpoint** pins the current snapshot and rotates the WAL during an instant
//!   of exclusivity with the flushers, then writes the snapshot file with writers
//!   running. The previous
//!   checkpoint is retained, and the WAL segments it covers are deleted only by the
//!   *next* checkpoint, so recovery can fall back to it with a complete log.
//!
//! On `open`, recovery is *snapshot + WAL tail*: load the latest usable checkpoint (O(1)
//! in the log length), then replay only the WAL records newer than it. The WAL is
//! reclaimed by [`Store::checkpoint`] and **never** at open: until a checkpoint
//! re-persists it, the recovered state lives only in memory.
//!
//! ## What is deliberately not here (see `ROADMAP.md`)
//! * No replication: `term` is always `1`. The Raft layer in [`crate::raft`] does not
//!   yet drive this engine; it keeps its own in-memory logs and state.
//! * No async API and no network. The cooperative runtime in [`crate::rt`] and the
//!   `mio` reactor are not connected to the store.
//! * The snapshot map is still cloned whole on every commit (O(entries)); a
//!   structurally shared map would make that O(log n).

use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use crate::rcu::RcuSwap;
use crate::snapshot as checkpoint;
use crate::types::{Op, Snapshot};
use crate::wal::{self, Record, Wal};

/// Default write-ahead-log segment rotation size.
const DEFAULT_MAX_SEG_BYTES: u64 = 1 << 20; // 1 MiB

/// Commit counters, for observability and tests.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Commits acknowledged (one per `put`, `delete` or `put_batch` call).
    pub commits: u64,
    /// WAL appends, each ending in one `fsync`. `commits / wal_syncs` is the mean
    /// group size; it exceeds 1 only when writers ran concurrently.
    pub wal_syncs: u64,
    /// Checkpoints written.
    pub checkpoints: u64,
}

/// A queued commit: the ops of one caller plus the slot its result is delivered to.
struct Pending {
    ops: Vec<Op>,
    ticket: Arc<Ticket>,
}

/// The result slot of a queued commit. `io::Error` is not `Clone`, so a failure is
/// carried as its kind and message and rebuilt for each caller of the failed group.
#[derive(Default)]
struct Ticket {
    result: Mutex<Option<Result<u64, (io::ErrorKind, String)>>>,
}

impl Ticket {
    fn set(&self, r: Result<u64, (io::ErrorKind, String)>) {
        *self.result.lock().expect("ticket lock poisoned") = Some(r);
    }

    fn take(&self) -> Option<io::Result<u64>> {
        self.result
            .lock()
            .expect("ticket lock poisoned")
            .take()
            .map(|r| r.map_err(|(kind, msg)| io::Error::new(kind, msg)))
    }
}

/// The commit queue: pending commits plus the flag that says a flush (or a checkpoint's
/// instant of exclusivity) is in progress. Waiters sleep on [`Store::flushed`].
struct CommitQueue {
    pending: VecDeque<Pending>,
    flushing: bool,
}

/// Marks a flush in progress for its lifetime and, on drop -- including an unwind --
/// clears the flag and wakes every waiter, so a panic inside a flush cannot wedge the
/// store.
struct FlushGuard<'a> {
    store: &'a Store,
}

impl Drop for FlushGuard<'_> {
    fn drop(&mut self) {
        let mut q = self
            .store
            .queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        q.flushing = false;
        self.store.flushed.notify_all();
    }
}

/// Checkpoint bookkeeping, held only by checkpoints.
struct CheckpointState {
    /// The WAL boundary drawn by the previous checkpoint: every record newer than the
    /// retained `snap.prev` lives in a segment numbered at or above it. Unknown after a
    /// restart, in which case the next checkpoint reclaims nothing (conservative).
    prev_boundary: Option<u32>,
}

/// A single-node, crash-recoverable key/value store.
///
/// `Store` is `Send + Sync`: the authoritative snapshot is a `RcuSwap<Snapshot>`
/// (interior-mutable) and the WAL and commit queue sit behind mutexes, so one `Store`
/// may live behind an `Arc` and be shared across threads.
pub struct Store {
    /// Store directory (snapshot + WAL segments live here).
    dir: std::path::PathBuf,
    /// Commits waiting for a flusher, and the flush-in-progress flag (see `commit_ops`).
    queue: Mutex<CommitQueue>,
    /// Signalled whenever a flush (or a checkpoint's exclusive instant) ends: settled
    /// writers return, unsettled ones compete to flush next.
    flushed: Condvar,
    /// Authoritative, version-stamped, parallel-reader state.
    snap: RcuSwap<Snapshot>,
    /// Write-ahead log (one append + fsync per group). Interior-mutable so `put` is
    /// `&self`.
    wal: Mutex<Wal>,
    /// Serialises checkpoints and remembers the previous one's WAL boundary.
    ckpt: Mutex<CheckpointState>,
    /// Current term. Always `1` on a single node; reserved for replication.
    term: u64,
    /// Highest commit index published so far, mirrored for a lockless `index()`.
    index: AtomicU64,
    commits: AtomicU64,
    wal_syncs: AtomicU64,
    checkpoints: AtomicU64,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("dir", &self.dir)
            .field("index", &self.index.load(Ordering::Acquire))
            .field("term", &self.term)
            .finish()
    }
}

impl Store {
    /// Open (or create) a store at `dir`, recovering from snapshot + WAL tail.
    pub fn open(dir: impl AsRef<std::path::Path>) -> io::Result<Self> {
        Self::open_with(dir, DEFAULT_MAX_SEG_BYTES)
    }

    /// Open with a custom WAL segment-rotation size (tests use this to force turns).
    pub fn open_with(dir: impl AsRef<std::path::Path>, max_seg_bytes: u64) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        // 1) Fast path: load the latest usable checkpoint, if any. First boot -> None.
        let base = checkpoint::load(&dir)?;

        // 2) Replay the WAL, applying only records strictly newer than the checkpoint.
        //    A torn tail is the one append a crash interrupted: never acknowledged, so
        //    never applied; `Wal::open` below truncates it so new appends start on a
        //    clean boundary. Corruption anywhere else is an error, not something to skip.
        let (records, _torn) = wal::replay(&dir)?;
        let (mut data, mut last_index) = match base {
            Some(s) => (s.data, s.index),
            None => (Default::default(), 0),
        };
        for r in records {
            if r.index > last_index {
                for op in &r.ops {
                    op.apply(&mut data, r.index);
                }
                last_index = r.index;
            }
            // A record at/below the checkpoint index is already reflected; skip it.
        }

        // 3) Resume the log. It is reclaimed by `checkpoint()`, never here: the recovered
        //    state is only in memory until a checkpoint re-persists it, so deleting the
        //    log at open would turn the next crash into data loss.
        let wal = Wal::open(&dir, max_seg_bytes)?;

        // 4) Publish the recovered state as the initial authoritative snapshot.
        Ok(Store {
            dir,
            queue: Mutex::new(CommitQueue {
                pending: VecDeque::new(),
                flushing: false,
            }),
            flushed: Condvar::new(),
            snap: RcuSwap::new(Snapshot {
                index: last_index,
                data,
            }),
            wal: Mutex::new(wal),
            ckpt: Mutex::new(CheckpointState {
                prev_boundary: None,
            }),
            term: 1,
            index: AtomicU64::new(last_index),
            commits: AtomicU64::new(0),
            wal_syncs: AtomicU64::new(0),
            checkpoints: AtomicU64::new(0),
        })
    }

    /// Current term. Always `1` on a single node.
    pub fn term(&self) -> u64 {
        self.term
    }

    /// The highest commit index published so far.
    pub fn index(&self) -> u64 {
        self.index.load(Ordering::Acquire)
    }

    /// Commit counters since this `Store` was opened.
    pub fn stats(&self) -> Stats {
        Stats {
            commits: self.commits.load(Ordering::Relaxed),
            wal_syncs: self.wal_syncs.load(Ordering::Relaxed),
            checkpoints: self.checkpoints.load(Ordering::Relaxed),
        }
    }

    /// Number of live keys, as of the most recent published snapshot.
    pub fn len(&self) -> usize {
        self.snap.load().len()
    }

    /// True when no live keys are present.
    pub fn is_empty(&self) -> bool {
        self.snap.load().is_empty()
    }

    /// Write `key -> value`, durable and linearised. Returns the commit index.
    pub fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> io::Result<u64> {
        self.commit_ops(vec![Op::Put {
            key: key.as_ref().to_vec(),
            value: value.as_ref().to_vec(),
        }])
    }

    /// Delete `key`, durable and linearised. Returns the commit index.
    pub fn delete(&self, key: impl AsRef<[u8]>) -> io::Result<u64> {
        self.commit_ops(vec![Op::Delete {
            key: key.as_ref().to_vec(),
        }])
    }

    /// Apply several ops as **one** commit: one index, one WAL record, all-or-nothing
    /// under a crash (the record's CRC covers every op), and visible at once. Ops apply
    /// in order, so a later op in the batch sees an earlier one. An empty batch commits
    /// nothing and returns the current index.
    pub fn put_batch(&self, ops: impl IntoIterator<Item = Op>) -> io::Result<u64> {
        let ops: Vec<Op> = ops.into_iter().collect();
        if ops.is_empty() {
            return Ok(self.index());
        }
        self.commit_ops(ops)
    }

    /// Read `key` from a freshly-loaded snapshot. `None` means absent/deleted.
    ///
    /// This is the lock-free read fast path: no commit lock, just an RCU load.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.snap.load().get(key).map(|e| e.value.to_vec())
    }

    /// Read `key` with its per-key commit index (its "version").
    ///
    /// The primitive MVCC / historical reads build on: the index of the *write* that
    /// produced the current value.
    pub fn get_with_index(&self, key: &[u8]) -> Option<(Vec<u8>, u64)> {
        self.snap
            .load()
            .get(key)
            .map(|e| (e.value.to_vec(), e.version))
    }

    /// Point read together with the **snapshot index** the value was observed at,
    /// i.e. the MVCC read point `(value_or_none, snapshot_index)`. A concurrent
    /// reader may observe an older snapshot than the leader; the checker uses that
    /// index to validate the observed value deterministically.
    pub fn get_at(&self, key: &[u8]) -> (Option<Vec<u8>>, u64) {
        let snap = self.snap.load();
        (snap.get(key).map(|e| e.value.to_vec()), snap.index)
    }

    /// Snapshot-consistent prefix scan: live keys with the given prefix, byte order.
    pub fn scan(&self, prefix: impl AsRef<[u8]>) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.snap
            .load()
            .scan_prefix(prefix.as_ref())
            .into_iter()
            .map(|(k, e)| (k.to_vec(), e.value.to_vec()))
            .collect()
    }

    /// Snapshot-consistent ordered range scan over `[lo, hi)`: `lo` inclusive, `hi`
    /// exclusive, empty when `lo >= hi`. Served in O(log n + k) straight from the
    /// snapshot's ordered map, like every other read: no extra index, no extra lock.
    pub fn range_scan(
        &self,
        lo: impl AsRef<[u8]>,
        hi: impl AsRef<[u8]>,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.snap
            .load()
            .range(lo.as_ref(), hi.as_ref())
            .into_iter()
            .map(|(k, e)| (k.to_vec(), e.value.to_vec()))
            .collect()
    }

    // ---- core commit path ----

    /// Queue `ops` as one commit and wait for it to be durable and published.
    ///
    /// Group commit: every writer enqueues its ticket. If no flush is in progress it
    /// becomes the flusher, takes everything queued (its own ticket included) and
    /// commits the lot as one group; otherwise it sleeps on the condvar. When a flush
    /// ends, every waiter wakes: those whose ticket was settled return, the rest compete
    /// to flush next, so the next group holds everything that arrived during the last
    /// flush. No thread is ever acknowledged before the `fsync` that made its ops
    /// durable.
    fn commit_ops(&self, ops: Vec<Op>) -> io::Result<u64> {
        let ticket = Arc::new(Ticket::default());
        let mut q = self.queue.lock().expect("commit queue poisoned");
        q.pending.push_back(Pending {
            ops,
            ticket: Arc::clone(&ticket),
        });
        loop {
            if let Some(done) = ticket.take() {
                return done; // an earlier flusher committed this ticket's ops
            }
            if !q.flushing {
                q.flushing = true;
                let group: Vec<Pending> = q.pending.drain(..).collect();
                drop(q);
                let _flush = FlushGuard { store: self };
                debug_assert!(!group.is_empty(), "our own ticket is still queued");
                match self.flush_group(&group) {
                    Ok(indices) => {
                        for (p, idx) in group.iter().zip(indices) {
                            p.ticket.set(Ok(idx));
                        }
                    }
                    Err(e) => {
                        let err = (e.kind(), e.to_string());
                        for p in &group {
                            p.ticket.set(Err(err.clone()));
                        }
                    }
                }
                return ticket.take().expect("the flusher settles its own ticket");
            }
            q = self.flushed.wait(q).expect("commit queue poisoned");
        }
    }

    /// Wait until no flush is in progress and claim exclusivity; the returned guard
    /// releases it (and wakes waiters) when dropped. Writers keep queueing meanwhile.
    fn exclusive_instant(&self) -> FlushGuard<'_> {
        let mut q: MutexGuard<'_, CommitQueue> = self.queue.lock().expect("commit queue poisoned");
        while q.flushing {
            q = self.flushed.wait(q).expect("commit queue poisoned");
        }
        q.flushing = true;
        drop(q);
        FlushGuard { store: self }
    }

    /// Commit one group (the caller holds flush exclusivity): copy-on-write the snapshot, apply every
    /// pending commit at its own index, append all records with one `fsync`, then
    /// publish the new snapshot. Returns each pending commit's index, in order.
    fn flush_group(&self, group: &[Pending]) -> io::Result<Vec<u64>> {
        // Copy-on-write the current snapshot and apply the ops on the copy, so the
        // published RCU value is a *new* immutable object.
        let cur = self.snap.load();
        let mut data = cur.data.clone();
        let mut index = cur.index;
        let mut records = Vec::with_capacity(group.len());
        let mut indices = Vec::with_capacity(group.len());
        for p in group {
            index += 1;
            for op in &p.ops {
                op.apply(&mut data, index);
            }
            records.push(Record {
                term: self.term,
                index,
                ops: p.ops.clone(),
            });
            indices.push(index);
        }

        // Durable BEFORE publish: a crash here means the ops are recovered on the next
        // open, not lost, and not half-observable. One fsync for the whole group.
        self.wal
            .lock()
            .expect("wal lock poisoned")
            .append_many(&records)?;
        self.wal_syncs.fetch_add(1, Ordering::Relaxed);
        self.commits
            .fetch_add(group.len() as u64, Ordering::Relaxed);

        // Publish the new authoritative, version-stamped snapshot.
        self.snap.store(Arc::new(Snapshot { index, data }));
        self.index.store(index, Ordering::Release);
        Ok(indices)
    }

    /// Durably checkpoint the current state and reclaim the WAL it makes redundant.
    ///
    /// Excludes flushes only for the instant it takes to pin the snapshot and rotate the
    /// WAL, so writers keep committing while the snapshot file is written. The previous checkpoint is
    /// retained as `snap.prev`; the segments *it* covered are deleted now, and the
    /// segments this one covers survive until the next checkpoint -- so a fall-back to
    /// `snap.prev` always has a complete log to replay. Checkpoints are serialised with
    /// each other. Returns the commit index reflected in the checkpoint.
    pub fn checkpoint(&self) -> io::Result<u64> {
        let mut state = self.ckpt.lock().expect("checkpoint lock poisoned");
        let (cur, boundary) = {
            let _exclusive = self.exclusive_instant();
            let cur = self.snap.load();
            let boundary = self
                .wal
                .lock()
                .expect("wal lock poisoned")
                .rotate_segment()?;
            (cur, boundary)
        };
        checkpoint::write(&self.dir, &cur)?; // durable tmp + fsync + renames; writers run meanwhile
        self.checkpoints.fetch_add(1, Ordering::Relaxed);
        if let Some(prev) = state.prev_boundary.replace(boundary) {
            self.wal
                .lock()
                .expect("wal lock poisoned")
                .remove_segments_before(prev)?;
        }
        Ok(cur.index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    /// A unique temp dir per test, removed on drop (even when the test panics).
    struct TmpDir(PathBuf);

    impl TmpDir {
        fn new(suffix: &str) -> TmpDir {
            static SEQ: AtomicU64 = AtomicU64::new(0);
            let d = std::env::temp_dir().join(format!(
                "ks-engine-{}-{}-{}",
                suffix,
                std::process::id(),
                SEQ.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = std::fs::remove_dir_all(&d);
            TmpDir(d)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn tmp_store(suffix: &str, max_seg: u64) -> (TmpDir, Store) {
        let d = TmpDir::new(suffix);
        let s = Store::open_with(d.path(), max_seg).expect("open store");
        (d, s)
    }

    fn put(k: &str, v: &str) -> Op {
        Op::Put {
            key: k.as_bytes().to_vec(),
            value: v.as_bytes().to_vec(),
        }
    }

    #[test]
    fn put_get_roundtrip() {
        let (_d, s) = tmp_store("pg", 1 << 20);
        assert!(s.get(b"k").is_none());
        assert_eq!(s.put(b"k", b"v").unwrap(), 1, "first commit is index 1");
        assert_eq!(s.get(b"k"), Some(b"v".to_vec()));
        assert_eq!(
            s.get_with_index(b"k").unwrap().1,
            1,
            "version = commit index"
        );
        assert_eq!(
            s.stats(),
            Stats {
                commits: 1,
                wal_syncs: 1,
                checkpoints: 0
            }
        );
    }

    #[test]
    fn delete_then_get_is_none() {
        let (_d, s) = tmp_store("del", 1 << 20);
        s.put(b"k", b"v").unwrap();
        s.delete(b"k").unwrap();
        assert!(s.get(b"k").is_none(), "deleted key absent");
        assert!(s.is_empty(), "store empty after deleting its only key");
    }

    #[test]
    fn scan_returns_prefix_in_order() {
        let (_d, s) = tmp_store("scan", 1 << 20);
        for k in ["b", "aa", "ab", "ac", "ba"] {
            s.put(k, k).unwrap();
        }
        let got: Vec<_> = s.scan("a").into_iter().map(|(k, _)| k).collect();
        assert_eq!(got, vec![b"aa".to_vec(), b"ab".to_vec(), b"ac".to_vec()]);
    }

    #[test]
    fn get_at_reports_snapshot_index() {
        let (_d, s) = tmp_store("getat", 1 << 20);
        s.put(b"k", b"1").unwrap();
        s.put(b"k", b"2").unwrap();
        let (val, snap_idx) = s.get_at(b"k");
        assert_eq!(val, Some(b"2".to_vec()));
        assert!(snap_idx >= 2, "read observed a snapshot >= the 2nd commit");
    }

    #[test]
    fn crash_recovery_replays_wal() {
        let (d, s) = tmp_store("crash-wal", 1 << 20);
        for i in 0..100 {
            s.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
        // No checkpoint: recover by replaying the WAL in a fresh Store.
        drop(s);
        let s2 = Store::open(d.path()).unwrap();
        assert_eq!(s2.len(), 100, "all commits survive WAL replay");
        for i in 0..100 {
            assert_eq!(
                s2.get(format!("k{i}").as_bytes()),
                Some(format!("v{i}").into_bytes())
            );
        }
    }

    #[test]
    fn crash_recovery_from_snapshot_then_wal_tail() {
        let (d, s) = tmp_store("crash-snap", 1 << 20);
        for i in 0..200 {
            s.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
        assert_eq!(
            s.checkpoint().unwrap(),
            200,
            "checkpoint reflects 200 commits"
        );
        // WAL tail after the checkpoint, plus a cross-boundary rewrite + delete.
        for i in 200..250 {
            s.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
        s.put(b"k10", b"overwritten").unwrap();
        s.delete(b"k100").unwrap();
        drop(s);

        // Recover purely from the checkpoint + WAL tail.
        let s2 = Store::open(d.path()).unwrap();
        assert_eq!(s2.len(), 249, "200 + 50 tail - 1 delete");
        for i in 0..200 {
            let k = format!("k{i}");
            if k == "k10" {
                assert_eq!(s2.get(b"k10"), Some(b"overwritten".to_vec()));
            } else if k == "k100" {
                assert!(s2.get(b"k100").is_none(), "deleted in the WAL tail");
            } else {
                assert_eq!(s2.get(k.as_bytes()), Some(format!("v{i}").into_bytes()));
            }
        }
        assert_eq!(s2.get(b"k100"), None, "deleted key must be gone");
        for i in 200..250 {
            assert_eq!(
                s2.get(format!("k{i}").as_bytes()),
                Some(format!("v{i}").into_bytes())
            );
        }
    }

    /// Two checkpoints reclaim the segments the first one covered, keep the previous
    /// generation on disk, and a fresh open replays nothing it does not need.
    #[test]
    fn checkpoints_reclaim_wal_one_generation_behind() {
        let (d, s) = tmp_store("chkpt-trunc", 1 << 20);
        for i in 0..50 {
            s.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
        assert_eq!(wal::list_segments(d.path()).unwrap().len(), 1);
        s.checkpoint().unwrap();
        // The first checkpoint drew a boundary but reclaimed nothing: the log it covers
        // is what a fall-back to it (as `snap.prev`, later) would replay.
        assert_eq!(wal::list_segments(d.path()).unwrap().len(), 2);
        for i in 50..60 {
            s.put(format!("k{i}").as_bytes(), b"v").unwrap();
        }
        s.checkpoint().unwrap();
        assert!(d.path().join(checkpoint::SNAPSHOT_PREV).exists());
        assert_eq!(
            wal::list_segments(d.path()).unwrap().len(),
            2,
            "the segment covered by the first checkpoint is gone; the one covered by the \
             second is retained for a fall-back"
        );
        let loaded = checkpoint::load(d.path()).unwrap();
        assert_eq!(
            loaded.map(|s| s.len()),
            Some(60),
            "latest snapshot covers 60 keys"
        );
        let fresh = Store::open(d.path()).unwrap();
        assert_eq!(fresh.len(), 60, "recovered from snapshot");
        assert_eq!(fresh.stats().checkpoints, 0, "stats are per open");
    }

    /// Regression (Phase 6): opening a store must never discard the WAL. Two reopens
    /// with no checkpoint in between used to lose every WAL-recovered key.
    #[test]
    fn reopen_without_checkpoint_keeps_wal_data() {
        let (d, s) = tmp_store("reopen", 1 << 20);
        for i in 0..100 {
            s.put(format!("k{i}"), format!("v{i}")).unwrap();
        }
        drop(s);
        let s = Store::open(d.path()).unwrap();
        assert_eq!(s.len(), 100, "first reopen replays the WAL");
        s.put(b"k100", b"v100").unwrap();
        drop(s);
        let s = Store::open(d.path()).unwrap();
        assert_eq!(
            s.len(),
            101,
            "second reopen still has everything: the log was kept"
        );
        drop(s);
        let s = Store::open(d.path()).unwrap();
        assert_eq!(s.len(), 101, "and so does the third");
        assert_eq!(s.get(b"k42"), Some(b"v42".to_vec()));
        assert_eq!(s.get(b"k100"), Some(b"v100".to_vec()));
        assert_eq!(s.index(), 101);
    }

    /// A torn tail (a crash mid-append) is truncated at open, and the log keeps working:
    /// later appends land after the intact records and replay cleanly.
    #[test]
    fn open_repairs_torn_tail_then_keeps_appending() {
        let (d, s) = tmp_store("torn", 1 << 20);
        for i in 0..3 {
            s.put(format!("k{i}"), b"v").unwrap();
        }
        drop(s);
        let mut segs = wal::list_segments(d.path()).unwrap();
        segs.sort();
        let last = segs.last().expect("a segment exists").clone();
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&last)
                .unwrap();
            // A length header claiming 127 bytes, followed by only three: a partial record.
            f.write_all(&[0x7F, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03])
                .unwrap();
        }
        let s = Store::open(d.path()).expect("open repairs the torn tail");
        assert_eq!(
            s.len(),
            3,
            "intact records survive, the torn one is dropped"
        );
        for i in 3..5 {
            s.put(format!("k{i}"), b"v").unwrap();
        }
        drop(s);
        let s = Store::open(d.path()).unwrap();
        assert_eq!(s.len(), 5, "records appended after the repair replay too");
        assert_eq!(s.index(), 5);
    }

    /// A batch is one commit: one index, one record, visible at once, and recoverable.
    #[test]
    fn put_batch_is_one_commit() {
        let (d, s) = tmp_store("batch", 1 << 20);
        s.put(b"c", b"old").unwrap();
        let idx = s
            .put_batch([
                put("a", "1"),
                put("b", "2"),
                Op::Delete { key: b"c".to_vec() },
            ])
            .unwrap();
        assert_eq!(idx, 2, "the batch took exactly one index");
        assert_eq!(s.index(), 2);
        assert_eq!(s.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(s.get_with_index(b"b"), Some((b"2".to_vec(), 2)));
        assert_eq!(s.get(b"c"), None);
        assert_eq!(s.put_batch([]).unwrap(), 2, "an empty batch is a no-op");
        assert_eq!(s.stats().commits, 2);
        drop(s);
        let s = Store::open(d.path()).unwrap();
        assert_eq!(s.len(), 2);
        assert_eq!(s.index(), 2);
        assert_eq!(s.get(b"b"), Some(b"2".to_vec()));
    }

    /// A batch record cut short by a crash applies none of its ops: no partial batch
    /// is ever recovered.
    #[test]
    fn torn_batch_record_applies_nothing() {
        let (d, s) = tmp_store("torn-batch", 1 << 20);
        s.put(b"a", b"1").unwrap();
        let before = {
            let mut segs = wal::list_segments(d.path()).unwrap();
            segs.sort();
            (segs[0].clone(), std::fs::metadata(&segs[0]).unwrap().len())
        };
        s.put_batch([put("b", "2"), put("c", "3")]).unwrap();
        drop(s);
        // Chop the batch record in half, as a crash mid-write would.
        let (seg, clean) = before;
        let full = std::fs::metadata(&seg).unwrap().len();
        let f = std::fs::OpenOptions::new().write(true).open(&seg).unwrap();
        f.set_len(clean + (full - clean) / 2).unwrap();
        drop(f);
        let s = Store::open(d.path()).expect("torn batch is dropped at open");
        assert_eq!(s.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(s.get(b"b"), None, "no half of a batch is applied");
        assert_eq!(s.get(b"c"), None);
        assert_eq!(s.index(), 1);
    }

    /// Concurrent writers are grouped: many commits share far fewer WAL syncs, every
    /// write is present afterwards, and the indices form one contiguous sequence.
    #[test]
    fn concurrent_writers_are_group_committed() {
        const THREADS: u64 = 8;
        const PER_THREAD: u64 = 100;
        let (d, s) = tmp_store("group", 1 << 20);
        let s = Arc::new(s);
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let s = Arc::clone(&s);
                std::thread::spawn(move || {
                    let mut got = Vec::new();
                    for i in 0..PER_THREAD {
                        got.push(s.put(format!("t{t}-{i}"), format!("{i}")).unwrap());
                    }
                    got
                })
            })
            .collect();
        let mut indices: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().expect("writer thread"))
            .collect();
        indices.sort_unstable();
        assert_eq!(
            indices,
            (1..=THREADS * PER_THREAD).collect::<Vec<_>>(),
            "every commit got a distinct, contiguous index"
        );
        let st = s.stats();
        assert_eq!(st.commits, THREADS * PER_THREAD);
        assert!(
            st.wal_syncs < st.commits,
            "{} writers on {} commits should share syncs, got {} syncs",
            THREADS,
            st.commits,
            st.wal_syncs
        );
        for t in 0..THREADS {
            for i in 0..PER_THREAD {
                assert_eq!(
                    s.get(format!("t{t}-{i}").as_bytes()),
                    Some(format!("{i}").into_bytes())
                );
            }
        }
        drop(s);
        let s = Store::open(d.path()).unwrap();
        assert_eq!(s.len() as u64, THREADS * PER_THREAD, "all recovered");
    }

    /// Checkpoints run concurrently with writers (and with each other) and lose
    /// nothing: a fresh open sees every write, from whichever generation it recovers.
    #[test]
    fn checkpoint_concurrent_with_writes_loses_nothing() {
        const THREADS: u64 = 4;
        const PER_THREAD: u64 = 150;
        let (d, s) = tmp_store("ckpt-race", 4096); // small segments: plenty of rotation
        let s = Arc::new(s);
        let writers: Vec<_> = (0..THREADS)
            .map(|t| {
                let s = Arc::clone(&s);
                std::thread::spawn(move || {
                    for i in 0..PER_THREAD {
                        s.put(format!("w{t}-{i:04}"), vec![t as u8; 64]).unwrap();
                    }
                })
            })
            .collect();
        let checkpointers: Vec<_> = (0..2)
            .map(|_| {
                let s = Arc::clone(&s);
                std::thread::spawn(move || {
                    for _ in 0..6 {
                        s.checkpoint().expect("checkpoint under load");
                        std::thread::sleep(std::time::Duration::from_millis(15));
                    }
                })
            })
            .collect();
        for h in writers.into_iter().chain(checkpointers) {
            h.join().expect("thread");
        }
        let final_index = s.index();
        assert_eq!(final_index, THREADS * PER_THREAD);
        assert_eq!(s.stats().checkpoints, 12);
        drop(s);
        let s = Store::open(d.path()).unwrap();
        assert_eq!(
            s.len() as u64,
            THREADS * PER_THREAD,
            "every write recovered"
        );
        assert_eq!(s.index(), final_index);
        for t in 0..THREADS {
            assert_eq!(
                s.get(format!("w{t}-{:04}", PER_THREAD - 1).as_bytes()),
                Some(vec![t as u8; 64])
            );
        }
    }

    /// Regression (Phase 6): a corrupt latest checkpoint falls back to the retained
    /// previous one, and the WAL kept since then completes the state.
    #[test]
    fn corrupt_latest_checkpoint_falls_back_to_previous_plus_wal() {
        let (d, s) = tmp_store("fallback", 1 << 20);
        for i in 0..10 {
            s.put(format!("k{i:02}"), b"1").unwrap();
        }
        s.checkpoint().unwrap();
        for i in 10..20 {
            s.put(format!("k{i:02}"), b"2").unwrap();
        }
        s.checkpoint().unwrap();
        for i in 20..25 {
            s.put(format!("k{i:02}"), b"3").unwrap();
        }
        drop(s);
        let latest = d.path().join(checkpoint::SNAPSHOT_NAME);
        let mut bytes = std::fs::read(&latest).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        std::fs::write(&latest, &bytes).unwrap();

        let s = Store::open(d.path()).expect("recovers via snap.prev + WAL");
        assert_eq!(
            s.len(),
            25,
            "10 from snap.prev, 15 replayed from the retained WAL"
        );
        assert_eq!(s.index(), 25);
        assert_eq!(s.get(b"k15"), Some(b"2".to_vec()));
        assert_eq!(s.get(b"k24"), Some(b"3".to_vec()));
    }

    #[test]
    fn store_is_sync_shareable_across_threads() {
        let (_d, s) = tmp_store("sync", 1 << 20);
        let s = Arc::new(s);
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        s.put(b"hot", "0").unwrap();
        // 8 readers hammer get(); share ONE stop flag.
        let mut handles = vec![];
        for _t in 0..8 {
            let s = Arc::clone(&s);
            let stop = Arc::clone(&stop);
            handles.push(std::thread::spawn(move || {
                let mut last = 0u32;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    if let Some(v) = s.get(b"hot") {
                        let n = std::str::from_utf8(&v).unwrap().parse::<u32>().unwrap();
                        // A reader may see a stale snapshot but never a "regression".
                        assert!(n >= last, "get never regressed");
                        last = n;
                    }
                }
            }));
        }
        // One writer drives the value forward, then signals stop.
        let s_w = Arc::clone(&s);
        let stop_w = Arc::clone(&stop);
        let writer = std::thread::spawn(move || {
            for i in 0..5000u32 {
                s_w.put(b"hot", i.to_string()).unwrap();
            }
            stop_w.store(true, std::sync::atomic::Ordering::Relaxed);
        });
        for h in handles {
            h.join().unwrap();
        }
        writer.join().unwrap();
        assert_eq!(s.get(b"hot"), Some(b"4999".to_vec()));
    }

    #[test]
    fn range_scan_is_ordered_and_half_open() {
        // Stable, ascending 5-byte keys: 'k' + big-endian u32 index.
        let k = |i: u32| -> Vec<u8> {
            let mut v = b"k".to_vec();
            v.extend_from_slice(&i.to_be_bytes());
            v
        };
        // Read a key's index via explicit bytes ('k' + 4 big-endian bytes).
        let idx_of = |kk: &Vec<u8>| -> u32 {
            let mut n = [0u8; 4];
            n.copy_from_slice(&kk[1..5]);
            u32::from_be_bytes(n)
        };
        let (_d, s) = tmp_store("range", 1 << 20);
        for i in 1..=50u32 {
            s.put(k(i), vec![i as u8]).expect("put");
        }
        let got = s.range_scan(k(20), k(30));
        let idx: Vec<u32> = got.iter().map(|(kk, _v)| idx_of(kk)).collect();
        assert_eq!(
            idx,
            (20u32..30u32).collect::<Vec<u32>>(),
            "range must be [k20,k30) in ascending order"
        );
        assert!(
            idx.windows(2).all(|w| w[0] < w[1]),
            "output stays ascending"
        );
        assert_eq!(idx.first(), Some(&20u32), "lo inclusive");
        assert_eq!(idx.last(), Some(&29u32), "hi exclusive");
        // A deleted interior key drops out of a later range scan.
        s.delete(k(25)).unwrap();
        let after = s.range_scan(k(20), k(30));
        let aidx: Vec<u32> = after.iter().map(|(kk, _v)| idx_of(kk)).collect();
        assert!(!aidx.contains(&25), "a deleted key drops out of the range");
    }

    #[test]
    fn range_scan_is_empty_for_inverted_or_equal_bounds() {
        let (_d, s) = tmp_store("range-empty", 1 << 20);
        s.put(b"a", b"1").unwrap();
        s.put(b"b", b"2").unwrap();
        assert!(s.range_scan(b"b", b"a").is_empty(), "inverted bounds");
        assert!(s.range_scan(b"a", b"a").is_empty(), "empty interval");
        assert_eq!(
            s.range_scan(b"a", b"b"),
            vec![(b"a".to_vec(), b"1".to_vec())]
        );
    }

    /// A full-range scan equals the prefix scan of the same snapshot after a mix of puts
    /// and deletes: both are views of one immutable map.
    #[test]
    fn range_scan_matches_prefix_scan_after_deletes() {
        let (_d, s) = tmp_store("range-vs-scan", 1 << 20);
        // 100 zero-padded keys in ascending order.
        for i in 0..100u32 {
            s.put(format!("{i:03}"), vec![i as u8]).expect("put");
        }
        // Delete every 5th key (0,5,10,...).
        for i in (0..100u32).step_by(5) {
            s.delete(format!("{i:03}")).expect("del");
        }
        let via_range = s.range_scan(b"000", b"zzz");
        let via_prefix: Vec<(Vec<u8>, Vec<u8>)> = s.scan("");
        assert_eq!(via_range, via_prefix, "one snapshot, two views");
        // 100 - 20 deleted = 80 live keys, all ascending, none deleted.
        let keys: Vec<Vec<u8>> = via_range.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys.len(), 80, "100 minus 20 deleted equals 80 live keys");
        assert!(keys.windows(2).all(|w| w[0] < w[1]), "ascending");
        assert!(
            keys.iter().all(|k| !(0..100u32)
                .step_by(5)
                .any(|j| k == format!("{j:03}").as_bytes())),
            "no deleted key survives"
        );
    }
}
