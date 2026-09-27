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
//!   on a condvar and return as soon as their ops are durable. Throughput scales with
//!   concurrency instead of paying one `fsync` per writer. [`Store::apply_batch`] applies
//!   several ops under one index, all-or-nothing.
//! * **reads** (`get`, `scan`, `range_scan`, `snapshot`) load a frozen `Arc<Snapshot>` and
//!   never touch the commit lock, so they never block a writer and never observe a
//!   half-updated state. A reader that loads while a commit is in flight sees either
//!   the pre- or the post-commit snapshot -- both are legitimate points in a sequential
//!   history, which is exactly what the Jepsen-lite checker enforces.
//! * a **checkpoint** pins the current snapshot and rotates the WAL during an instant
//!   of exclusivity with the flushers, then writes the snapshot file with writers
//!   running. The previous checkpoint is retained, and the WAL segments it covers are
//!   deleted only by the *next* checkpoint, so recovery can fall back to it with a
//!   complete log.
//!
//! On `open`, recovery is *snapshot + WAL tail*: load the latest usable checkpoint (O(1)
//! in the log length), then stream only the WAL records newer than it, which must
//! continue its index without a gap (a missing record is corruption, and `open` fails
//! rather than silently losing it). The WAL is reclaimed by [`Store::checkpoint`] and
//! **never** at open: until a checkpoint re-persists it, the recovered state lives only
//! in memory.
//!
//! A store directory has one owner at a time: `open` takes an exclusive lock on its
//! `LOCK` file (released when the `Store` is dropped, or by the OS if the process dies),
//! so a second `open` -- in this process or another -- fails with `WouldBlock` instead
//! of repairing the live owner's log out from under it and reusing its indices.
//!
//! ## What is deliberately not here (see `ROADMAP.md`)
//! * No replication: every record's term is `1`. The Raft layer (the `experimental`
//!   feature) does not yet drive this engine; it keeps its own in-memory logs and state.
//! * No async API and no network. The cooperative runtime and the `mio` reactor (also
//!   `experimental`) are not connected to the store.
//! * The snapshot map is still cloned whole on every commit (O(entries)); a
//!   structurally shared map would make that O(log n).
//! * Nothing checkpoints automatically: the log grows until [`Store::checkpoint`] runs.

use std::collections::VecDeque;
use std::fs::{File, OpenOptions, TryLockError};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use crate::checkpoint::{self, Replaced};
use crate::rcu::RcuSwap;
use crate::types::{Op, Snapshot};
use crate::wal::{self, Record, Wal};

/// Default write-ahead-log segment rotation size.
const DEFAULT_WAL_SEGMENT_SIZE: u64 = 1 << 20; // 1 MiB

/// The lock file that makes a store directory single-owner (see the module docs).
const LOCK_NAME: &str = "LOCK";

/// The Raft term stamped on every WAL record. A single node never holds an election, so
/// it is always 1 until replication gives it a meaning (`ROADMAP.md`, 0.4.0).
const SINGLE_NODE_TERM: u64 = 1;

/// Settings for [`Store::open_with`].
///
/// The defaults suit most uses. Each setter returns the updated options, so they chain:
///
/// ```
/// use keystory::{Options, Store};
///
/// # fn main() -> std::io::Result<()> {
/// # let dir = std::env::temp_dir().join(format!("keystory-doc-options-{}", std::process::id()));
/// # let _ = std::fs::remove_dir_all(&dir);
/// let store = Store::open_with(&dir, Options::new().wal_segment_size(4 << 20))?;
/// # drop(store);
/// # std::fs::remove_dir_all(&dir)?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Options {
    wal_segment_size: u64,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            wal_segment_size: DEFAULT_WAL_SEGMENT_SIZE,
        }
    }
}

impl Options {
    /// The default options.
    pub fn new() -> Options {
        Options::default()
    }

    /// The size in bytes past which the write-ahead log starts a new segment file
    /// (default 1 MiB). Checkpoints reclaim the log a whole segment at a time, so smaller
    /// segments give space back sooner, at the cost of more files. A group commit larger
    /// than a segment makes one oversized segment rather than being split.
    pub fn wal_segment_size(mut self, bytes: u64) -> Options {
        self.wal_segment_size = bytes;
        self
    }
}

/// Commit counters since the [`Store`] was opened, for observability and tests.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Stats {
    /// Commits acknowledged (one per `put`, `delete` or `apply_batch` call).
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
    /// Where the log that the next `snap.prev` needs begins: every record newer than the
    /// checkpoint the next checkpoint will retain as `snap.prev` lives in a segment
    /// numbered at or above this one, so that checkpoint may delete every segment below
    /// it. Each checkpoint's rotation draws it for the next; after a restart, recovery works
    /// it out from where the first record the loaded checkpoint does not cover lives.
    prev_boundary: u32,
    /// Recovery fell back past a damaged `snap.dat`, so the next checkpoint must replace
    /// that file rather than demote it over the good `snap.prev` (see
    /// [`Replaced::Discard`]).
    latest_damaged: bool,
}

/// A single-node, crash-recoverable key/value store.
///
/// Every write is durable when it returns: it was appended to the write-ahead log and
/// `fsync`'d before any reader could see it. Reads never wait for writers: each is served
/// from an immutable [`Snapshot`], and [`Store::snapshot`] hands one out to keep for many
/// consistent reads. `Store` is `Send + Sync`; share one behind an `Arc`, and concurrent
/// writers will share log appends and `fsync`s (group commit).
///
/// A directory holds one store and has one owner at a time: [`Store::open`] locks it
/// until the `Store` is dropped (on a filesystem that cannot lock files, `open` goes ahead
/// without the lock). The store keeps working in the directory it opened, whatever the
/// process's working directory later becomes. The whole live dataset is held in memory.
///
/// # Errors
///
/// Every fallible method returns a [`std::io::Error`]. The kinds keystory itself reports
/// mean:
///
/// | Kind | From | Meaning |
/// |------|------|---------|
/// | `WouldBlock` | `open` | Another `Store`, in this process or another, owns the directory. |
/// | `InvalidData` | `open` | The files are damaged in a way recovery will not repair on its own: corruption before the tail of the last WAL segment, records missing from the middle of the log, or no usable checkpoint. The log and checkpoints are left as they were. |
/// | `Unsupported` | `open` | A file was written in a format version this build does not read (see the crate docs). |
/// | `InvalidInput` | writes | The commit is too large for a log record (4 GiB); nothing was written. |
/// | `Other` | writes, `checkpoint` | The log is *poisoned*: an earlier write failed and could not be rolled back. Writes are refused until the store is reopened; reads go on, and reopening recovers every acknowledged write. |
///
/// Any other error is the operating system's, passed through (`StorageFull`,
/// `PermissionDenied`, ...). A write that fails that way was not applied -- unless it
/// also poisoned the log, in which case its outcome is unknown until the store is
/// reopened: its records may or may not have reached the disk. An operating-system error
/// keeps the kind the OS gave it, which can coincide with one of the kinds above (`EINVAL`
/// is `InvalidInput`, for instance), so the kind alone does not prove which case occurred;
/// the message does.
pub struct Store {
    /// Store directory (snapshot + WAL segments live here).
    dir: PathBuf,
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
    /// Serialises checkpoints and carries what one tells the next.
    ckpt: Mutex<CheckpointState>,
    /// Highest commit index published so far, mirrored for a lockless `index()`.
    index: AtomicU64,
    commits: AtomicU64,
    wal_syncs: AtomicU64,
    checkpoints: AtomicU64,
    /// Holds the directory's exclusive lock. Declared last so it is dropped (and the
    /// lock released) only after the WAL is closed.
    _lock: File,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("dir", &self.dir)
            .field("index", &self.index.load(Ordering::Acquire))
            .finish()
    }
}

impl Store {
    /// Open the store in `dir`, creating the directory and an empty store if there is
    /// none, and recover its state: the latest usable checkpoint plus the log written
    /// since. Uses the default [`Options`].
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Store> {
        Self::open_with(dir, Options::default())
    }

    /// [`Store::open`] with explicit [`Options`].
    pub fn open_with(dir: impl AsRef<Path>, options: Options) -> io::Result<Store> {
        // A directory created here must survive a crash as surely as the data written
        // into it, and its entry lives in its parent.
        wal::create_dir_all_durably(dir.as_ref())?;
        // Pin the directory: every later path (a new segment, a checkpoint, a deletion) is
        // resolved from it, so a relative path must not follow the process's working
        // directory, nor a symlink that is re-pointed while the store is open.
        let dir = std::fs::canonicalize(dir.as_ref())?;

        // 0) Own the directory before touching it: recovery below truncates what looks
        //    like a torn tail, which in a live owner's log is an append in flight.
        let lock = lock_dir(&dir)?;

        // 1) Fast path: load the latest usable checkpoint, if any. First boot -> None.
        let loaded = checkpoint::load(&dir)?;
        let base = loaded.snapshot.unwrap_or_else(Snapshot::empty);
        let (mut data, base_index) = (base.data, base.index);

        // 2) Stream the WAL, applying only records strictly newer than the checkpoint. A
        //    torn tail is the one append a crash interrupted: never acknowledged, so never
        //    applied; `Wal::open` below truncates it so new appends start on a clean
        //    boundary. Corruption anywhere else is an error, not something to skip.
        let mut last_index = base_index;
        // The segment holding the first record the checkpoint does not cover: everything
        // below it is covered, which is what lets the first checkpoint reclaim it.
        let mut first_uncovered = None;
        wal::replay(&dir, |segment, r| {
            if r.index <= last_index {
                if last_index == base_index {
                    return Ok(()); // at/below the checkpoint index: already reflected
                }
                return Err(corrupt_log(format!(
                    "WAL record {} follows record {last_index}: the log is out of order",
                    r.index
                )));
            }
            // Past the checkpoint, the log must continue its index without a gap: a
            // missing record (a lost or deleted segment) is data loss, never skippable.
            if r.index != last_index + 1 {
                return Err(corrupt_log(format!(
                    "WAL is missing records {}..={} (found {} after {last_index})",
                    last_index + 1,
                    r.index - 1,
                    r.index
                )));
            }
            for op in &r.ops {
                op.apply(&mut data, r.index);
            }
            last_index = r.index;
            first_uncovered.get_or_insert(segment);
            Ok(())
        })?;

        // 3) Resume the log. It is reclaimed by `checkpoint()`, never here: the recovered
        //    state is only in memory until a checkpoint re-persists it, so deleting the
        //    log at open would turn the next crash into data loss. The next checkpoint
        //    retains the loaded one as `snap.prev` (or keeps it there, after a fall-back),
        //    so it may reclaim every segment below the first uncovered record -- or below
        //    the current segment, if the checkpoint covers the whole log.
        let wal = Wal::open(&dir, options.wal_segment_size)?;
        let prev_boundary = first_uncovered.unwrap_or_else(|| wal.current_segment());

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
                prev_boundary,
                latest_damaged: loaded.latest_damaged,
            }),
            index: AtomicU64::new(last_index),
            commits: AtomicU64::new(0),
            wal_syncs: AtomicU64::new(0),
            checkpoints: AtomicU64::new(0),
            _lock: lock,
        })
    }

    /// The highest commit index published so far: the index of the latest acknowledged
    /// commit, or 0 for a store that has never been written.
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

    /// Delete `key`, durable and linearised. Returns the commit index. Deleting an absent
    /// key is still a commit, and changes nothing.
    pub fn delete(&self, key: impl AsRef<[u8]>) -> io::Result<u64> {
        self.commit_ops(vec![Op::Delete {
            key: key.as_ref().to_vec(),
        }])
    }

    /// Apply several ops as **one** commit: one index, one WAL record, all-or-nothing
    /// under a crash (the record's CRC covers every op), and visible at once. Ops apply
    /// in order, so a later op in the batch sees an earlier one. An empty batch commits
    /// nothing and returns the current index.
    pub fn apply_batch(&self, ops: impl IntoIterator<Item = Op>) -> io::Result<u64> {
        let ops: Vec<Op> = ops.into_iter().collect();
        if ops.is_empty() {
            return Ok(self.index());
        }
        self.commit_ops(ops)
    }

    /// The value of `key`, or `None` if it is absent (never written, or deleted).
    ///
    /// Served from the latest snapshot, without touching the commit path.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<Vec<u8>> {
        self.snap.load().get(key).map(<[u8]>::to_vec)
    }

    /// The value of `key` together with its *version*: the commit index of the write that
    /// produced it (see [`Snapshot::get_with_version`]).
    pub fn get_with_version(&self, key: impl AsRef<[u8]>) -> Option<(Vec<u8>, u64)> {
        self.snap
            .load()
            .get_with_version(key)
            .map(|(value, version)| (value.to_vec(), version))
    }

    /// Every live key that starts with `prefix`, and its value, in byte order, from one
    /// snapshot.
    pub fn scan(&self, prefix: impl AsRef<[u8]>) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.snap
            .load()
            .scan(prefix)
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect()
    }

    /// Every live key in the half-open range `[lo, hi)`, and its value, in byte order,
    /// from one snapshot; empty when `lo >= hi`.
    pub fn range_scan(
        &self,
        lo: impl AsRef<[u8]>,
        hi: impl AsRef<[u8]>,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.snap
            .load()
            .range_scan(lo, hi)
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect()
    }

    /// The latest snapshot: a consistent, point-in-time view of every key, for reads that
    /// must agree with each other. It is unaffected by later writes, costs one reference
    /// count to take, and never blocks a writer however long it is held (though it keeps
    /// the data it references alive).
    ///
    /// ```
    /// # fn main() -> std::io::Result<()> {
    /// # let dir = std::env::temp_dir().join(format!("keystory-doc-snapshot-{}", std::process::id()));
    /// # let _ = std::fs::remove_dir_all(&dir);
    /// let store = keystory::Store::open(&dir)?;
    /// store.put("stock:apples", "3")?;
    /// let view = store.snapshot();
    /// store.put("stock:apples", "2")?;
    /// assert_eq!(view.get("stock:apples"), Some(&b"3"[..])); // as of the snapshot
    /// assert_eq!(view.index() + 1, store.index());
    /// # drop(store);
    /// # std::fs::remove_dir_all(&dir)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.snap.load()
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
        // Refuse ops the WAL cannot frame before they join a group: an oversized commit
        // must fail its own caller, never the concurrent writers it would share a flush with.
        wal::check_ops(&ops)?;
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
                term: SINGLE_NODE_TERM,
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
    /// WAL, so writers keep committing while the snapshot file is written. The previous
    /// checkpoint is retained as `snap.prev`; the segments *it* covered are deleted now,
    /// and the segments this one covers survive until the next checkpoint -- so a
    /// fall-back to `snap.prev` always has a complete log to replay. Checkpoints are
    /// serialised with each other. Returns the commit index reflected in the checkpoint.
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
        // After a fall-back recovery `snap.dat` is the damaged file: replace it, and never
        // demote it over the good `snap.prev` that recovery used.
        let replaced = if state.latest_damaged {
            Replaced::Discard
        } else {
            Replaced::Retain
        };
        checkpoint::write(&self.dir, &cur, replaced)?; // writers run meanwhile
        state.latest_damaged = false;
        self.checkpoints.fetch_add(1, Ordering::Relaxed);
        // Every segment below the old boundary holds only records the checkpoint now
        // retained as `snap.prev` covers; the new boundary is for the next checkpoint.
        let covered = std::mem::replace(&mut state.prev_boundary, boundary);
        self.wal
            .lock()
            .expect("wal lock poisoned")
            .remove_segments_before(covered)?;
        Ok(cur.index)
    }
}

/// Take the store directory's exclusive lock, returning the handle that holds it. A lock
/// held by another `Store` fails with `WouldBlock`; on a platform without file locking
/// the store opens unlocked rather than not at all.
fn lock_dir(dir: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(dir.join(LOCK_NAME))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!(
                "store directory {} is already open (locked by another Store)",
                dir.display()
            ),
        )),
        Err(TryLockError::Error(e)) if e.kind() == io::ErrorKind::Unsupported => Ok(file),
        Err(TryLockError::Error(e)) => Err(e),
    }
}

/// An `InvalidData` error for a log that recovery refuses to replay.
fn corrupt_log(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A unique temp dir per test, removed on drop (even when the test panics). Not
    /// created: `Store::open` creates it.
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
        let s = Store::open_with(d.path(), Options::new().wal_segment_size(max_seg))
            .expect("open store");
        (d, s)
    }

    fn put(k: &str, v: &str) -> Op {
        Op::put(k, v)
    }

    fn corrupt(path: &Path) {
        let mut bytes = std::fs::read(path).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        std::fs::write(path, &bytes).unwrap();
    }

    #[test]
    fn put_get_roundtrip() {
        let (_d, s) = tmp_store("pg", 1 << 20);
        assert!(s.get(b"k").is_none());
        assert_eq!(s.put(b"k", b"v").unwrap(), 1, "first commit is index 1");
        assert_eq!(s.get(b"k"), Some(b"v".to_vec()));
        assert_eq!(
            s.get_with_version(b"k").unwrap().1,
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

    /// A snapshot is one point in the commit order: later writes do not reach it, every
    /// read on it agrees with every other, and it names the index it reflects.
    #[test]
    fn a_snapshot_is_a_consistent_point_in_time_view() {
        let (_d, s) = tmp_store("snapshot-view", 1 << 20);
        s.put("a", "1").unwrap();
        s.put("b", "2").unwrap();
        let view = s.snapshot();
        s.put("a", "changed").unwrap();
        s.delete("b").unwrap();
        s.put("c", "3").unwrap();

        assert_eq!(view.index(), 2);
        assert_eq!(view.get("a"), Some(&b"1"[..]));
        assert_eq!(view.get_with_version("b"), Some((&b"2"[..], 2)));
        assert_eq!(view.get("c"), None);
        assert_eq!(
            view.iter().collect::<Vec<_>>(),
            vec![(&b"a"[..], &b"1"[..]), (&b"b"[..], &b"2"[..])]
        );
        let now = s.snapshot();
        assert_eq!((now.index(), now.len()), (5, 2));
        assert_eq!(
            now.range_scan("a", "z").map(|(k, _)| k).collect::<Vec<_>>(),
            vec![&b"a"[..], &b"c"[..]]
        );
        assert_eq!(s.get_with_version("a"), Some((b"changed".to_vec(), 3)));
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
            loaded.snapshot.map(|s| s.len()),
            Some(60),
            "latest snapshot covers 60 keys"
        );
        drop(s);
        let fresh = Store::open(d.path()).unwrap();
        assert_eq!(fresh.len(), 60, "recovered from snapshot");
        assert_eq!(fresh.stats().checkpoints, 0, "stats are per open");
    }

    /// Regression (pre-release review, second pass): only the second checkpoint of a
    /// process reclaimed anything, because the boundary it needed was forgotten at every
    /// open -- so a process that checkpointed once per run never reclaimed its log (probe:
    /// 4, 7, 10, ... 19 segments over six runs, and every open read all of them). Recovery
    /// now works the boundary out, and the log stays bounded run after run.
    #[test]
    fn one_checkpoint_per_open_still_reclaims_the_log() {
        let d = TmpDir::new("reclaim-per-open");
        let mut segments = Vec::new();
        for run in 0..6 {
            let s = Store::open_with(d.path(), Options::new().wal_segment_size(4096)).unwrap();
            for i in 0..200 {
                s.put(format!("run{run}-k{i:03}"), "value").unwrap();
            }
            s.checkpoint().unwrap();
            drop(s);
            segments.push(wal::list_segments(d.path()).unwrap().len());
        }
        assert!(
            segments[5] <= segments[1],
            "the log grows run after run: {segments:?} segments"
        );
        let s = Store::open(d.path()).unwrap();
        assert_eq!((s.len(), s.index()), (1200, 1200), "and nothing was lost");
        drop(s);

        // A fall-back to the retained generation still finds every record it needs.
        std::fs::remove_file(d.path().join(checkpoint::SNAPSHOT_NAME)).unwrap();
        let s = Store::open(d.path()).expect("snap.prev plus the log kept since it");
        assert_eq!((s.len(), s.index()), (1200, 1200));
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
    fn a_batch_is_one_commit() {
        let (d, s) = tmp_store("batch", 1 << 20);
        s.put(b"c", b"old").unwrap();
        let idx = s
            .apply_batch([put("a", "1"), put("b", "2"), Op::delete("c")])
            .unwrap();
        assert_eq!(idx, 2, "the batch took exactly one index");
        assert_eq!(s.index(), 2);
        assert_eq!(s.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(s.get_with_version(b"b"), Some((b"2".to_vec(), 2)));
        assert_eq!(s.get(b"c"), None);
        assert_eq!(s.apply_batch([]).unwrap(), 2, "an empty batch is a no-op");
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
        s.apply_batch([put("b", "2"), put("c", "3")]).unwrap();
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
        corrupt(&d.path().join(checkpoint::SNAPSHOT_NAME));

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

    /// Regression (pre-release review): the first checkpoint after a fall-back recovery
    /// renamed the damaged `snap.dat` over the good `snap.prev`, so a crash before it
    /// published the new checkpoint left the store unopenable (probe: "crc mismatch" on
    /// every later open). Now the damaged file is replaced in place: even with the new
    /// checkpoint lost, the good generation and the WAL kept since it recover everything.
    #[test]
    fn a_checkpoint_after_a_fallback_keeps_the_good_generation() {
        let (d, s) = tmp_store("fallback-ckpt", 1 << 20);
        for i in 0..10 {
            s.put(format!("a{i}"), b"1").unwrap();
        }
        s.checkpoint().unwrap(); // index 10
        for i in 0..10 {
            s.put(format!("b{i}"), b"2").unwrap();
        }
        s.checkpoint().unwrap(); // snap.dat at 20, snap.prev at 10
        for i in 0..5 {
            s.put(format!("c{i}"), b"3").unwrap();
        }
        drop(s);
        corrupt(&d.path().join(checkpoint::SNAPSHOT_NAME));

        let s = Store::open(d.path()).expect("falls back to snap.prev + WAL");
        assert_eq!((s.len(), s.index()), (25, 25));
        assert_eq!(s.checkpoint().unwrap(), 25);
        drop(s);

        let latest = checkpoint::load(d.path()).unwrap();
        assert_eq!(
            latest.snapshot.map(|s| s.index),
            Some(25),
            "the new checkpoint"
        );
        // As if a crash had struck before the new checkpoint was published:
        std::fs::remove_file(d.path().join(checkpoint::SNAPSHOT_NAME)).unwrap();
        let s = Store::open(d.path()).expect("the good generation survived the checkpoint");
        assert_eq!((s.len(), s.index()), (25, 25));
        assert_eq!(s.get("c4"), Some(b"3".to_vec()));
    }

    /// Regression (2026-09-26 audit): recovery used to apply whatever records it found
    /// past the checkpoint, so a log missing records (a lost segment) opened "fine" with
    /// those commits silently gone. A gap or a backwards index is now an error, while
    /// records the checkpoint already covers may still be skipped.
    #[test]
    fn open_refuses_a_log_with_missing_or_reordered_records() {
        fn write_log(dir: &Path, indices: &[u64]) {
            let mut w = Wal::open(dir, 1 << 20).unwrap();
            for &i in indices {
                w.append(&Record::single(1, i, put(&format!("k{i}"), "v")))
                    .unwrap();
            }
        }
        for (name, log) in [("gap", &[1u64, 2, 7, 8][..]), ("backwards", &[1, 2, 3, 2])] {
            let d = TmpDir::new(name);
            write_log(d.path(), log);
            let err = Store::open(d.path()).expect_err("a broken log must not open");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{name}: {err}");
        }

        // With a checkpoint at 5, records 3 and 4 are covered; 6 must follow directly.
        let covered = |log: &[u64]| {
            let d = TmpDir::new("covered");
            std::fs::create_dir_all(d.path()).unwrap();
            let mut snap = Snapshot::empty();
            snap.index = 5;
            checkpoint::write(d.path(), &snap, Replaced::Retain).unwrap();
            write_log(d.path(), log);
            Store::open(d.path()).map(|s| s.index())
        };
        assert_eq!(covered(&[3, 4, 6, 7]).unwrap(), 7);
        assert!(covered(&[3, 4, 7]).is_err(), "record 6 is missing");
    }

    /// Regression (2026-09-26 audit): nothing stopped a second `Store` from opening a
    /// live store's directory; it "repaired" the owner's log and reused its indices, so
    /// an acknowledged write vanished at the next open. The directory is now locked.
    #[test]
    fn a_second_open_of_a_live_store_is_refused() {
        let (d, s) = tmp_store("lock", 1 << 20);
        s.put(b"k", b"v").unwrap();
        let err = Store::open(d.path()).expect_err("the directory is owned");
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        s.put(b"k2", b"v2").unwrap(); // the owner is undisturbed
        drop(s);
        let s = Store::open(d.path()).expect("the lock is released on drop");
        assert_eq!(s.len(), 2);
    }

    /// Regression (pre-release review): `open` created a missing store directory without
    /// syncing its parent, so after a power cut the directory -- and every acknowledged
    /// write inside it -- could vanish, though each write had been `fsync`'d. Every
    /// directory `open` creates is now synced into its parent, outermost first.
    #[test]
    fn creating_a_store_syncs_every_new_directory_entry() {
        let root = TmpDir::new("durable-dirs");
        let dir = root.path().join("a").join("b");
        wal::SYNCED_DIRS.with(|synced| synced.borrow_mut().clear());
        let s = Store::open(&dir).unwrap();
        let synced = wal::SYNCED_DIRS.with(|synced| synced.borrow().clone());
        let parents = [
            root.path().parent().unwrap().to_path_buf(),
            root.path().to_path_buf(),
            root.path().join("a"),
        ];
        let at: Vec<usize> = parents
            .iter()
            .map(|p| {
                synced
                    .iter()
                    .position(|s| s == p)
                    .unwrap_or_else(|| panic!("{} was never synced: {synced:?}", p.display()))
            })
            .collect();
        assert!(at.is_sorted(), "outermost first: {at:?}");
        s.put("k", "v").unwrap();
        drop(s);
        assert_eq!(Store::open(&dir).unwrap().get("k"), Some(b"v".to_vec()));
    }

    /// A commit whose WAL append fails is not published, and a log that cannot roll the
    /// failure back refuses later commits -- but nothing acknowledged is lost, and a
    /// reopen recovers and resumes at the next index.
    #[test]
    fn a_failed_append_is_never_published_and_loses_nothing_acknowledged() {
        let (d, s) = tmp_store("poisoned", 1 << 20);
        for i in 0..5 {
            s.put(format!("k{i}"), b"v").unwrap();
        }
        s.wal.lock().unwrap().break_for_test();
        assert!(s.put(b"k5", b"v").is_err());
        assert_eq!(s.get(b"k5"), None, "a failed commit is not visible");
        let err = s
            .put(b"k6", b"v")
            .expect_err("the poisoned log refuses writes");
        assert!(err.to_string().contains("reopen"), "{err}");
        assert!(
            s.checkpoint().is_err(),
            "a checkpoint cannot rotate a poisoned log"
        );
        assert_eq!((s.len(), s.index()), (5, 5), "reads keep working");
        drop(s);
        let s = Store::open(d.path()).unwrap();
        assert_eq!(
            (s.len(), s.index()),
            (5, 5),
            "every acknowledged write recovered"
        );
        assert_eq!(s.put(b"k5", b"v").unwrap(), 6, "and the index resumes");
    }

    /// A store in another format version is refused with `Unsupported` at open, before
    /// anything is changed on disk.
    #[test]
    fn a_store_in_another_format_version_is_refused() {
        let (d, s) = tmp_store("format", 1 << 20);
        s.put("k", "v").unwrap();
        drop(s);
        let seg = wal::list_segments(d.path()).unwrap().remove(0);
        let mut bytes = std::fs::read(&seg).unwrap();
        let newer = wal::header_for(wal::SEGMENT_VERSION + 1);
        bytes[..newer.len()].copy_from_slice(&newer);
        std::fs::write(&seg, &bytes).unwrap();
        let err = Store::open(d.path()).expect_err("a newer format");
        assert_eq!(err.kind(), io::ErrorKind::Unsupported, "{err}");
        assert_eq!(std::fs::read(&seg).unwrap(), bytes, "left as it was");
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
