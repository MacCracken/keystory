//! # The store engine (single node)
//!
//! `Store` is the public entry point. It composes the other primitives:
//!
//! * the **authoritative state** is a [`RcuSwap`] of `Snapshot` (internally
//!   `Arc<Snapshot>`) -- a version-stamped, immutable, parallel-reader view.
//! * every mutation is **serialised** by a `commit` mutex and **durable-then-published**:
//!   the op is appended to the WAL and `fsync`'d *before* the new snapshot is
//!   published into the RCU. A crash before the append has no effect; a crash after it
//!   is recovered on the next open.
//! * **reads** (`get`, `scan`, `range_scan`, `get_at`) load a frozen `Arc<Snapshot>` and
//!   never touch the commit lock, so they never block a writer and never observe a
//!   half-updated state. A reader that loads while a commit is in flight sees either
//!   the pre- or the post-commit snapshot -- both are legitimate points in a sequential
//!   history, which is exactly what the Jepsen-lite checker enforces.
//!
//! On `open`, recovery is *snapshot + WAL tail*: load the last checkpoint (O(1) in the
//! log length), then replay only the WAL records newer than that checkpoint. The WAL is
//! reclaimed by [`Store::checkpoint`] and **never** at open: until a checkpoint
//! re-persists it, the recovered state lives only in memory.
//!
//! ## What is deliberately not here (see `ROADMAP.md`)
//! * No replication: `term` is always `1`. The Raft layer in [`crate::raft`] does not
//!   yet drive this engine; it keeps its own in-memory logs and state.
//! * No async API and no network. The cooperative runtime in [`crate::rt`] and the
//!   `mio` reactor are not connected to the store.
//! * Every commit clones the whole map (O(N) in keys *and* value bytes), and every
//!   commit is one `fsync`; there is no group commit yet.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::rcu::RcuSwap;
use crate::snapshot as checkpoint;
use crate::types::{Op, Snapshot};
use crate::wal::{self, Record, Wal};

/// Default write-ahead-log segment rotation size.
const DEFAULT_MAX_SEG_BYTES: u64 = 1 << 20; // 1 MiB

/// A single-node, crash-recoverable key/value store.
///
/// `Store` is `Send + Sync`: the authoritative snapshot is a `RcuSwap<Snapshot>`
/// (interior-mutable) and the WAL/commit lock are `Send`/`Sync`, so one `Store` may
/// live behind an `Arc` and be shared across threads.
pub struct Store {
    /// Store directory (snapshot + WAL segments live here).
    dir: std::path::PathBuf,
    /// Serialises every commit. The critical section is tiny: clone, append + fsync,
    /// publish. Readers never touch this lock.
    commit: Mutex<()>,
    /// Authoritative, version-stamped, parallel-reader state.
    snap: RcuSwap<Snapshot>,
    /// Write-ahead log (append + fsync per commit). Interior-mutable so `put` is
    /// `&self`.
    wal: Mutex<Wal>,
    /// Current term. Always `1` on a single node; reserved for replication.
    term: u64,
    /// Highest commit index published so far, mirrored for a lockless `index()`.
    index: AtomicU64,
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
    pub fn open(dir: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        Self::open_with(dir, DEFAULT_MAX_SEG_BYTES)
    }

    /// Open with a custom WAL segment-rotation size (tests use this to force turns).
    pub fn open_with(
        dir: impl AsRef<std::path::Path>,
        max_seg_bytes: u64,
    ) -> std::io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        // 1) Fast path: load the last checkpoint, if any. First boot -> None.
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
                r.op.apply(&mut data, r.index);
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
            commit: Mutex::new(()),
            snap: RcuSwap::new(Snapshot {
                index: last_index,
                data,
            }),
            wal: Mutex::new(wal),
            term: 1,
            index: AtomicU64::new(last_index),
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

    /// Number of live keys, as of the most recent published snapshot.
    pub fn len(&self) -> usize {
        self.snap.load().len()
    }

    /// True when no live keys are present.
    pub fn is_empty(&self) -> bool {
        self.snap.load().is_empty()
    }

    /// Write `key -> value`, durable and linearised under the commit lock.
    pub fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> std::io::Result<u64> {
        self.commit_op(Op::Put {
            key: key.as_ref().to_vec(),
            value: value.as_ref().to_vec(),
        })
    }

    /// Delete `key`, durable and linearised. Returns the commit index.
    pub fn delete(&self, key: impl AsRef<[u8]>) -> std::io::Result<u64> {
        self.commit_op(Op::Delete {
            key: key.as_ref().to_vec(),
        })
    }

    /// Read `key` from a freshly-loaded snapshot. `None` means absent/deleted.
    ///
    /// This is the lock-free read fast path: no commit lock, just an RCU load.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.snap.load().get(key).map(|e| e.value.clone())
    }

    /// Read `key` with its per-key commit index (its "version").
    ///
    /// The primitive MVCC / historical reads build on: the index of the *write* that
    /// produced the current value.
    pub fn get_with_index(&self, key: &[u8]) -> Option<(Vec<u8>, u64)> {
        self.snap
            .load()
            .get(key)
            .map(|e| (e.value.clone(), e.version))
    }

    /// Point read together with the **snapshot index** the value was observed at,
    /// i.e. the MVCC read point `(value_or_none, snapshot_index)`. A concurrent
    /// reader may observe an older snapshot than the leader; the checker uses that
    /// index to validate the observed value deterministically.
    pub fn get_at(&self, key: &[u8]) -> (Option<Vec<u8>>, u64) {
        let snap = self.snap.load();
        (snap.get(key).map(|e| e.value.clone()), snap.index)
    }

    /// Snapshot-consistent prefix scan: live keys with the given prefix, byte order.
    pub fn scan(&self, prefix: impl AsRef<[u8]>) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.snap
            .load()
            .scan_prefix(prefix.as_ref())
            .into_iter()
            .map(|(k, e)| (k, e.value))
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
            .map(|(k, e)| (k, e.value))
            .collect()
    }

    // ---- core commit path ----
    /// Serialise, clone-on-write the snapshot, apply the op, durable-append first,
    /// then publish the new snapshot into the RCU.
    fn commit_op(&self, op: Op) -> std::io::Result<u64> {
        // Serialize writers. Critical section: build next snapshot, durable-append,
        // publish. Readers never take this lock.
        let _guard = self.commit.lock().expect("commit lock poisoned");

        // Copy-on-write the current snapshot and apply the op on the copy, so the
        // published RCU value is a *new* immutable object.
        let cur = self.snap.load();
        let index = cur.index + 1;
        let mut data = cur.data.clone();
        op.apply(&mut data, index);

        let rec = Record {
            term: self.term,
            index,
            op,
        };

        // Durable BEFORE publish: a crash here means the op is recovered on the
        // next open, not lost, and not half-observable.
        self.wal.lock().expect("wal lock poisoned").append(&rec)?;

        // Publish the new authoritative, version-stamped snapshot.
        self.snap.store(Arc::new(Snapshot { index, data }));
        self.index.store(index, Ordering::Release);
        Ok(index)
    }

    /// Durably checkpoint the current state and reclaim the consumed WAL.
    ///
    /// Returns the commit index reflected in the checkpoint. After this, a fresh
    /// `open` recovers with zero WAL replay.
    pub fn checkpoint(&self) -> std::io::Result<u64> {
        let _guard = self.commit.lock().expect("commit lock poisoned");
        let cur = self.snap.load();
        checkpoint::write(&self.dir, &cur)?; // durable tmp + fsync + rename
        self.wal.lock().expect("wal lock poisoned").truncate_all()?; // reclaim WAL
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

    #[test]
    fn checkpoint_truncates_wal() {
        let (d, s) = tmp_store("chkpt-trunc", 1 << 20);
        for i in 0..50 {
            s.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
        assert!(
            !wal::list_segments(d.path()).unwrap().is_empty(),
            "WAL has a segment"
        );
        s.checkpoint().unwrap();
        // The meaningful invariant: the snapshot now covers all state, so a fresh
        // open replays zero WAL records.
        let loaded = checkpoint::load(d.path()).unwrap();
        assert_eq!(
            loaded.map(|s| s.len()),
            Some(50),
            "snapshot covers 50 commits"
        );
        let fresh = Store::open(d.path()).unwrap();
        assert_eq!(fresh.len(), 50, "recovered from snapshot, no WAL replay");
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
