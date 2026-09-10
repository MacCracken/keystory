//! # The store engine (Phase 1, single node)
//!
//! `Store` is the public entry point. It composes the other primitives:
//!
//! * the **authoritative state** is a [`RcuSwap`](crate::rcu::RcuSwap) of `Snapshot`
//!   (internally `Arc<Snapshot>`) -- a version-stamped, immutable, parallel-reader view.
//! * every mutation is **serialised** by a `commit` mutex and **dured-then-published**:
//!   the op is appended to the WAL and `fsync`'d *before* the new snapshot is
//!   published into the RCU. A crash before the append has no effect; a crash after it
//!   is recovered on the next open.
//! * **reads** (`get`, `scan`, `get_at`) load a frozen `Arc<Snapshot>` and never touch
//!   the commit lock, so they never block a writer and never observe a half-updated
//!   state. A reader that loads while a commit is in flight sees either the pre- or the
//!   post-commit snapshot -- both are legitimate points in a sequential history, which
//!   is exactly what the Jepsen-lite checker enforces.
//!
//! On `open`, recovery is *snapshot + WAL tail*: load the last checkpoint (O(1) in the
//! log length), replay only WAL records newer than that checkpoint, then truncate the
//! consumed WAL.
//!
//! ## What is deliberately not here yet
//! * No consensus / fault tolerance -- single node, `term == 1`. (Phase 2: Raft.)
//! * No async, no network. (Phase 3.)
//! * Snapshotting is O(N) and each commit clones the map (O(N)); a log-structured
//!   engine would amortise this. Noted as a Phase-2 open question.

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
      /// Current term. Always `1` on a single node; reserved for Raft in Phase 2.
    term: u64,
      /// Highest commit index published so far, mirrored for a lockless `index()`.
    index: Arc<AtomicU64>,
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

          // 2) Replay only WAL records strictly newer than the checkpoint index.
        let (records, _dropped) = wal::replay(&dir)?;
        let mut data = base.as_ref().map(|s| s.data.clone()).unwrap_or_default();
        let mut last_index = base.as_ref().map(|s| s.index).unwrap_or(0);
        for r in records {
            if r.index > last_index {
                r.op.apply(&mut data, r.index);
                last_index = r.index;
             }
              // A record at/below the checkpoint index is already reflected; skip it.
            }
              // `_dropped == Some(..)` means a torn tail was discarded at open, which
               // is correct: a torn record is never applied.

            // 3) The durable content is now in memory; reclaim the consumed WAL so the
            //    next boot is again fast. A later `checkpoint()` will persist it again.
        for p in wal::list_segments(&dir)? {
            let _ = std::fs::remove_file(&p);
            }

          // 4) Publish the recovered state as the initial authoritative snapshot.
        let snap = Snapshot { index: last_index, data };
        let index = Arc::new(AtomicU64::new(last_index));
        let wal = Wal::open(&dir, max_seg_bytes)?;
        Ok(Store {
            dir,
            commit: Mutex::new(()),
            snap: RcuSwap::new(snap),
            wal: Mutex::new(wal),
            term: 1,
            index,
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
        self.commit_op(Op::Delete { key: key.as_ref().to_vec() })
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
        self.snap.load().get(key).map(|e| (e.value.clone(), e.version))
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
        let p = prefix.as_ref();
        self.snap
              .load()
              .scan_prefix(p)
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

        let rec = Record { term: self.term, index, op: op.clone() };

              // Durable BEFORE publish: a crash here means the op is recovered on the
               // next open, not lost, and not half-observable.
        self.wal.lock().expect("wal lock poisoned").append(&rec)?;

              // Publish the new authoritative, version-stamped snapshot.
        let next = Arc::new(Snapshot { index, data });
        self.snap.store(next);
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

        /// A unique temp-dir helper mirroring the WAL tests (avoids std TempDir).
    fn tmp_store(suffix: &str, max_seg: u64) -> (std::path::PathBuf, Store) {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir()
              .join(format!("ks-engine-{}-{}-{}", suffix, std::process::id(), seq));
        let s = Store::open_with(&dir, max_seg).expect("open store");
         (dir, s)
        }

       #[test]
    fn put_get_roundtrip() {
        let (_d, s) = tmp_store("pg", 1 << 20);
        assert!(s.get(b"k").is_none());
        assert_eq!(s.put(b"k", b"v").unwrap(), 1, "first commit is index 1");
        assert_eq!(s.get(b"k"), Some(b"v".to_vec()));
        assert_eq!(s.get_with_index(b"k").unwrap().1, 1, "version = commit index");
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
            s.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes()).unwrap();
             }
             // No checkpoint: recover by replaying the WAL in a fresh Store.
        drop(s);
        let s2 = Store::open(&d).unwrap();
        assert_eq!(s2.len(), 100, "all commits survive WAL replay");
        for i in 0..100 {
            assert_eq!(s2.get(format!("k{i}").as_bytes()), Some(format!("v{i}").into_bytes()));
            }
        std::fs::remove_dir_all(&d).ok();
     }

       #[test]
    fn crash_recovery_from_snapshot_then_wal_tail() {
        let (d, s) = tmp_store("crash-snap", 1 << 20);
        for i in 0..200 {
            s.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes()).unwrap();
            }
        assert_eq!(s.checkpoint().unwrap(), 200, "checkpoint reflects 200 commits");
             // WAL tail after the checkpoint, plus a cross-boundary rewrite + delete.
        for i in 200..250 {
            s.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes()).unwrap();
             }
        s.put(b"k10", b"overwritten").unwrap();
        s.delete(b"k100").unwrap();
        drop(s);

             // Recover purely from the checkpoint + WAL tail.
        let s2 = Store::open(&d).unwrap();
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
            assert_eq!(s2.get(format!("k{i}").as_bytes()), Some(format!("v{i}").into_bytes()));
             }
        std::fs::remove_dir_all(&d).ok();
     }

       #[test]
    fn checkpoint_truncates_wal() {
        let (d, s) = tmp_store("chkpt-trunc", 1 << 20);
        for i in 0..50 {
            s.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes()).unwrap();
             }
        assert!(!wal::list_segments(&d).unwrap().is_empty(), "WAL has a segment");
        s.checkpoint().unwrap();
             // The meaningful invariant: the snapshot now covers all state, so a fresh
             // open replays zero WAL records.
        let loaded = checkpoint::load(&d).unwrap();
        assert_eq!(loaded.map(|s| s.len()), Some(50), "snapshot covers 50 commits");
        let fresh = Store::open(&d).unwrap();
        assert_eq!(fresh.len(), 50, "recovered from snapshot, no WAL replay");
        std::fs::remove_dir_all(&d).ok();
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
}
