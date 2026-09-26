//! Off-heap value storage ("large-value handling").
//!
//! A large value dominates the on-disk footprint and the snapshot. The off-heap store spills such
//! values to an append-only **blob log** (`values/blobs`); what a WAL entry or snapshot record then
//! carries is a small [`BlobRef`] handle, **not the bytes**. Reads are *random*: fetching one
//! value seeks to that record and reads only its bytes, never the rest of the log -- the std-only
//! stand-in for a zero-copy / mmap read.
//!
//! **Compaction = GC.** [`ValueStore::compact`] rewrites the log keeping only the blobs whose ids
//! are live, dropping superseded ones, atomically (tmp + fsync + rename) so a crash mid-compact
//! leaves the original log intact. Ids are **stable across compaction**: a handle stays valid,
//! because reads resolve the id through an in-memory index (rebuilt from the log at open) rather
//! than trusting the handle's recorded offset. Ids are also **never reused**: a compacted log
//! ends with a *fence* record carrying the id high-water mark, so a reopen keeps counting past
//! every id ever handed out even when compaction dropped the highest ones. A handle to a dropped
//! blob therefore reads as absent -- never as some newer blob that inherited its id.
//!
//! **Failed writes.** A failed `put` is rolled back (the log is cut back to its last intact
//! record) so the next append starts on a clean boundary; if the rollback fails too, the store is
//! *poisoned* and refuses writes until it is reopened, where the torn-tail repair takes over.
//!
//! *Honest scope:* this is a tested primitive that the live [`crate::Store`] does **not**
//! use yet. WAL entries and snapshot records still carry value bytes inline and the in-RAM
//! state still materialises values; wiring `BlobRef`s into the on-disk formats is tracked
//! in `ROADMAP.md`.

use crate::crc::Crc;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// A blob id.
pub type BlobId = u64;

/// A handle to a value in the blob store: its id, the byte offset it was written at, and its
/// length. Small and cheap to copy -- this is what an on-disk log/snapshot would carry instead of
/// the bytes. Only `id` (and `len`, as a check) identify the blob; `offset` records where it was
/// appended and goes stale after a compaction, which is fine because reads resolve the id.
/// `Default` (`0,0,0`) denotes "no value".
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct BlobRef {
    pub id: BlobId,
    pub offset: u64,
    pub len: u64,
}

const REL_PATH: &str = "values/blobs";
const REC_HDR: usize = 16; // id:8 BE + len:8 BE
const REC_TRAIL: usize = 4; // crc32 over (header || payload)
/// The reserved id of a *fence* record. It is never allocated to a blob; its 8-byte
/// big-endian payload is the id high-water mark (`next_id`) when a compaction wrote it.
const FENCE_ID: BlobId = BlobId::MAX;

/// An off-heap, append-only, compactable value store rooted at `dir`.
///
/// All methods take `&self` and use a [`Mutex`], so a store can be shared behind an `Arc`;
/// a compaction holds that lock for its whole duration, so no concurrent `put` can be lost.
pub struct ValueStore {
    inner: Mutex<Inner>,
    path: PathBuf,
}

struct Inner {
    /// The live log, open for append (writes) and positioned reads.
    file: File,
    /// The next id a `put` allocates: one above the highest id ever written.
    next_id: u64,
    /// End of the intact log (the running append offset).
    end: u64,
    /// `id -> (offset, len)` for every record in the log, rebuilt at open.
    index: BTreeMap<BlobId, (u64, u64)>,
    /// Why the store refuses writes, once a failed append could not be rolled back.
    poisoned: Option<String>,
}

impl Inner {
    /// `Ok` unless an earlier failure poisoned the store.
    fn check_usable(&self) -> io::Result<()> {
        match &self.poisoned {
            None => Ok(()),
            Some(why) => Err(io::Error::other(format!(
                "the value store refuses writes after an earlier failure: {why}; reopen it \
                 to recover"
            ))),
        }
    }

    /// Undo a failed append: cut the log back to the end of its last intact record, so
    /// the next record starts on a clean boundary (the handle is `O_APPEND`, so the next
    /// write lands exactly there). If the cut fails too, poison the store.
    fn roll_back(&mut self, cause: &io::Error) {
        if let Err(e) = self
            .file
            .set_len(self.end)
            .and_then(|()| self.file.sync_all())
        {
            self.poisoned = Some(format!(
                "an append failed ({cause}) and could not be rolled back ({e})"
            ));
        }
    }
}

impl ValueStore {
    /// Open (or create) a value store. A pre-existing blob log is scanned once at open to rebuild
    /// the id index, the id counter and the end offset; a torn trailing record (a crash
    /// mid-append) is truncated away so later appends start on a clean boundary.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<ValueStore> {
        let root = dir.as_ref();
        let path = root.join(REL_PATH);
        let values_dir = path.parent().expect("REL_PATH has a parent directory");
        let created = !path.exists();
        fs::create_dir_all(values_dir)?;
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)?;
        if created {
            // A new log's directory entries must be as durable as the blobs put into it.
            crate::wal::fsync_dir(values_dir)?;
            crate::wal::fsync_dir(root)?;
        }
        let mut index = BTreeMap::new();
        let mut fence = 0;
        let end = scan_records(&path, |id, offset, payload| {
            if id == FENCE_ID {
                if let Ok(mark) = <[u8; 8]>::try_from(payload) {
                    fence = fence.max(u64::from_be_bytes(mark));
                }
            } else {
                index.insert(id, (offset, payload.len() as u64));
            }
        })?;
        if file.metadata()?.len() > end {
            file.set_len(end)?; // drop a torn tail
            file.sync_all()?;
        }
        let next_id = index.keys().next_back().map_or(0, |id| id + 1).max(fence);
        Ok(ValueStore {
            inner: Mutex::new(Inner {
                file,
                next_id,
                end,
                index,
                poisoned: None,
            }),
            path,
        })
    }

    /// Append `value` durably and return its handle. The record is fsync'd *before* the id is
    /// handed out, so a returned handle is durable. A failed append leaves nothing behind.
    #[allow(clippy::should_implement_trait)]
    pub fn put(&self, value: &[u8]) -> io::Result<BlobRef> {
        let mut g = self.inner.lock().unwrap();
        g.check_usable()?;
        let id = g.next_id;
        if id == FENCE_ID {
            return Err(io::Error::other("blob id space exhausted"));
        }
        let offset = g.end;
        if let Err(e) = write_record(&mut g.file, id, value).and_then(|()| g.file.sync_all()) {
            g.roll_back(&e);
            return Err(e);
        }
        let len = value.len() as u64;
        g.end += (REC_HDR + REC_TRAIL) as u64 + len;
        g.next_id = id + 1;
        g.index.insert(id, (offset, len));
        Ok(BlobRef { id, offset, len })
    }

    /// Read a blob by handle, verifying its CRC. Random access: seeks to the record the index
    /// holds for `ref_to.id` and reads only its bytes. Returns `None` if the id is unknown, the
    /// handle's length disagrees with the log, or the record is torn or corrupt; a genuine I/O
    /// failure is an error, not an absence. The allocation is bounded by the indexed length,
    /// never by whatever a damaged header claims.
    pub fn get(&self, ref_to: &BlobRef) -> io::Result<Option<Vec<u8>>> {
        let mut g = self.inner.lock().unwrap();
        let Some(&(offset, len)) = g.index.get(&ref_to.id) else {
            return Ok(None);
        };
        if len != ref_to.len {
            return Ok(None);
        }
        // Positioned read on the shared handle: appends use O_APPEND, so moving the read
        // position cannot misplace a later write.
        g.file.seek(SeekFrom::Start(offset))?;
        let mut hdr = [0u8; REC_HDR];
        if !read_or_eof(&mut g.file, &mut hdr)? {
            return Ok(None);
        }
        let disk_id = u64::from_be_bytes(hdr[0..8].try_into().expect("8 bytes"));
        let disk_len = u64::from_be_bytes(hdr[8..16].try_into().expect("8 bytes"));
        if disk_id != ref_to.id || disk_len != len {
            return Ok(None); // the header on disk disagrees with the index: damaged
        }
        let mut payload = vec![0u8; len as usize];
        if !read_or_eof(&mut g.file, &mut payload)? {
            return Ok(None);
        }
        let mut crcb = [0u8; REC_TRAIL];
        if !read_or_eof(&mut g.file, &mut crcb)? {
            return Ok(None);
        }
        let mut crc = Crc::new();
        crc.update(&hdr);
        crc.update(&payload);
        if crc.finish() != u32::from_be_bytes(crcb) {
            return Ok(None);
        }
        Ok(Some(payload))
    }

    /// Compact the log: rewrite it keeping only the blobs whose ids are in `live`, dropping
    /// superseded ones. Ids are preserved, so every handle to a live blob stays valid, and the
    /// new log ends with a fence carrying the id high-water mark, so no dropped id is ever
    /// reused. Streams record by record (O(one record) memory), then publishes atomically (tmp,
    /// fsync, rename, then a directory fsync); a crash mid-compaction leaves the original log
    /// intact, and so does any error. A log damaged before its end is refused (`InvalidData`)
    /// rather than rewritten without the live blobs past the damage. The store's lock is held
    /// throughout, so no concurrent `put` can slip into the old log and be lost. Returns the
    /// new end offset.
    pub fn compact(&self, live: &BTreeSet<BlobId>) -> io::Result<u64> {
        let mut g = self.inner.lock().unwrap();
        g.check_usable()?;
        let tmp = self.path.with_extension("tmp");
        let _ = fs::remove_file(&tmp);
        // Opened for read + append up front: once the rename publishes it, this very handle
        // becomes the store's, so nothing can fail between the old log going and the new one
        // being usable.
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create_new(true)
            .open(&tmp)?;
        let mut out = BufWriter::new(&file);
        let mut index = BTreeMap::new();
        let mut end = 0u64;
        let mut failed = None;
        let scanned = scan_records(&self.path, |id, _old_offset, payload| {
            if failed.is_some() || id == FENCE_ID || !live.contains(&id) {
                return;
            }
            match write_record(&mut out, id, payload) {
                Ok(()) => {
                    index.insert(id, (end, payload.len() as u64));
                    end += (REC_HDR + REC_TRAIL + payload.len()) as u64;
                }
                Err(e) => failed = Some(e),
            }
        })?;
        if let Some(e) = failed {
            return Err(e);
        }
        if scanned != g.end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "blob log {} is damaged at byte {scanned} of {}; refusing to compact away \
                     the blobs after it",
                    self.path.display(),
                    g.end
                ),
            ));
        }
        write_record(&mut out, FENCE_ID, &g.next_id.to_be_bytes())?;
        end += (REC_HDR + REC_TRAIL + 8) as u64;
        out.flush()?;
        drop(out);
        file.sync_all()?;
        fs::rename(&tmp, &self.path)?;
        // The compacted log is the live one from here on, whatever happens next.
        g.file = file;
        g.index = index;
        g.end = end;
        if let Some(dir) = self.path.parent() {
            crate::wal::fsync_dir(dir)?;
        }
        Ok(end)
    }

    /// The current end-of-log offset (high-water mark).
    pub fn end(&self) -> u64 {
        self.inner.lock().unwrap().end
    }

    /// The next blob id a `put` would allocate.
    pub fn next_id(&self) -> u64 {
        self.inner.lock().unwrap().next_id
    }

    /// Number of blobs currently in the log.
    pub fn count(&self) -> usize {
        self.inner.lock().unwrap().index.len()
    }

    /// The blob-log path (under the store root), for diagnostics / reopen.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Swap the log handle for a read-only one, so the next append fails and so does its
    /// rollback: the poisoning path, on demand.
    #[cfg(test)]
    fn break_for_test(&self) {
        self.inner.lock().unwrap().file =
            File::open(&self.path).expect("reopen the blob log read-only");
    }
}

/// Write one record -- `[id:8 BE][len:8 BE][payload][crc32(header||payload):4 BE]` -- to `w`.
/// The CRC folds incrementally, so the payload is never copied.
fn write_record(w: &mut impl Write, id: BlobId, payload: &[u8]) -> io::Result<()> {
    let hdr = id.to_be_bytes();
    let lenb = (payload.len() as u64).to_be_bytes();
    let mut crc = Crc::new();
    crc.update(&hdr);
    crc.update(&lenb);
    crc.update(payload);
    w.write_all(&hdr)?;
    w.write_all(&lenb)?;
    w.write_all(payload)?;
    w.write_all(&crc.finish().to_be_bytes())
}

/// `read_exact`, except that running out of file (a torn record) is `Ok(false)` rather than
/// an error: only a genuine I/O failure propagates.
fn read_or_eof(r: &mut impl Read, buf: &mut [u8]) -> io::Result<bool> {
    match r.read_exact(buf) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(e),
    }
}

/// Walk the log at `path` record by record, handing each intact `(id, offset, payload)` to
/// `sink` in order, with one record in memory at a time. Stops at the first torn or corrupt
/// record and returns the offset where the intact log ends.
fn scan_records(path: &Path, mut sink: impl FnMut(BlobId, u64, &[u8])) -> io::Result<u64> {
    let file_len = fs::metadata(path)?.len();
    let mut rd = BufReader::new(File::open(path)?);
    let mut pos = 0u64;
    loop {
        if pos + (REC_HDR + REC_TRAIL) as u64 > file_len {
            return Ok(pos);
        }
        let mut hdr = [0u8; REC_HDR];
        rd.read_exact(&mut hdr)?;
        let id = u64::from_be_bytes(hdr[0..8].try_into().expect("8 bytes"));
        let len = u64::from_be_bytes(hdr[8..16].try_into().expect("8 bytes"));
        // A damaged length can be anything up to `u64::MAX`: bound it with checked
        // arithmetic, so it ends the log instead of overflowing.
        let next = len
            .checked_add((REC_HDR + REC_TRAIL) as u64)
            .and_then(|total| pos.checked_add(total));
        let (Some(next), Ok(len)) = (next, usize::try_from(len)) else {
            return Ok(pos);
        };
        if next > file_len {
            return Ok(pos); // torn tail (or a damaged length): the log ends here
        }
        let mut payload = vec![0u8; len];
        rd.read_exact(&mut payload)?;
        let mut crcb = [0u8; REC_TRAIL];
        rd.read_exact(&mut crcb)?;
        let mut crc = Crc::new();
        crc.update(&hdr);
        crc.update(&payload);
        if crc.finish() != u32::from_be_bytes(crcb) {
            return Ok(pos); // corrupt record: stop, never trust what follows
        }
        sink(id, pos, &payload);
        pos = next;
    }
}

/// One-shot CRC over a header/payload pair, for tests that forge records.
#[cfg(test)]
fn record_crc(hdr: &[u8], payload: &[u8]) -> u32 {
    let mut v = hdr.to_vec();
    v.extend_from_slice(payload);
    crate::crc::crc32(&v)
}

#[cfg(test)]
mod test {
    use super::*;
    /// Per-test unique temp dir; cleaned at drop. Mirrors the crash_recover helper.
    struct T {
        path: PathBuf,
    }
    impl T {
        fn new() -> T {
            static C: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            let n = C.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let p = std::env::temp_dir().join(format!("ks-vs-{n}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&p);
            std::fs::create_dir_all(&p).unwrap();
            T { path: p }
        }
        fn root(&self) -> &Path {
            &self.path
        }
    }
    impl Drop for T {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    /// A large value round-trips through the off-heap store.
    #[test]
    fn put_get_round_trip_large_value() {
        let t = T::new();
        let v = ValueStore::open(t.root()).unwrap();
        let big: Vec<u8> = (0..1_000_000).map(|i| i as u8).collect();
        let r = v.put(&big).expect("put");
        assert_eq!(v.get(&r).unwrap(), Some(big));
    }

    /// Two values live side by side; reading one never needs the other (random access).
    #[test]
    fn random_access_reads_one_record() {
        let t = T::new();
        let v = ValueStore::open(t.root()).unwrap();
        let a: Vec<u8> = vec![1; 50_000];
        let b: Vec<u8> = vec![2; 50_000];
        let ra = v.put(&a).unwrap();
        let rb = v.put(&b).unwrap();
        assert_eq!(v.get(&ra).unwrap(), Some(a));
        assert_eq!(v.get(&rb).unwrap(), Some(b));
        assert_eq!(
            v.get(&BlobRef {
                id: 999,
                offset: 10_000_000,
                len: 4
            })
            .unwrap(),
            None
        );
    }

    /// Compaction keeps only live blobs and drops the superseded ones; the file shrinks.
    #[test]
    fn compact_drops_superseded_blobs() {
        let t = T::new();
        let v = ValueStore::open(t.root()).unwrap();
        let mut refs = Vec::new();
        for i in 0..10 {
            refs.push(v.put(&(i as u64).to_be_bytes()).unwrap());
        }
        let live: BTreeSet<BlobId> = refs.iter().skip(7).map(|r| r.id).collect();
        let before = std::fs::metadata(v.path()).unwrap().len();
        v.compact(&live).expect("compact");
        let after = std::fs::metadata(v.path()).unwrap().len();
        assert!(after < before, "compact shrinks: {before} -> {after}");
        assert_eq!(v.count(), 3);
    }

    /// A compacted store, reopened, recovers the compacted log.
    #[test]
    fn compact_survives_reopen() {
        let t = T::new();
        let r;
        {
            let v = ValueStore::open(t.root()).unwrap();
            r = v.put(&[7, 7, 7, 7]).unwrap();
            v.compact(&BTreeSet::from([r.id])).unwrap();
        }
        let v2 = ValueStore::open(t.root()).unwrap();
        assert_eq!(v2.get(&r).unwrap(), Some(vec![7, 7, 7, 7]));
    }

    /// A blob survives a reopen purely from the on-disk log.
    #[test]
    fn survives_reopen() {
        let t = T::new();
        let r;
        {
            let v = ValueStore::open(t.root()).unwrap();
            r = v.put(&[0xde, 0xad, 0xbe, 0xef]).unwrap();
        }
        let v2 = ValueStore::open(t.root()).unwrap();
        assert_eq!(v2.get(&r).unwrap(), Some(vec![0xde, 0xad, 0xbe, 0xef]));
    }

    /// Regression (Phase 6): compaction used to renumber ids, so every outstanding handle
    /// but one went dead. Ids are stable now: live handles keep working across the
    /// compaction and a reopen, dropped ones read as absent, and new ids keep counting up.
    #[test]
    fn compaction_keeps_ids_and_handles_valid() {
        let t = T::new();
        let v = ValueStore::open(t.root()).unwrap();
        let a = v.put(b"aaaa").unwrap();
        let b = v.put(b"bb").unwrap();
        let c = v.put(b"cccccc").unwrap();
        v.compact(&BTreeSet::from([a.id, c.id])).unwrap();
        assert_eq!(v.get(&a).unwrap(), Some(b"aaaa".to_vec()));
        assert_eq!(v.get(&b).unwrap(), None, "dropped by compaction");
        assert_eq!(v.get(&c).unwrap(), Some(b"cccccc".to_vec()));
        let d = v.put(b"d").unwrap();
        assert_eq!(
            d.id,
            c.id + 1,
            "ids keep counting past the highest ever used"
        );
        drop(v);
        let v2 = ValueStore::open(t.root()).unwrap();
        assert_eq!(v2.get(&a).unwrap(), Some(b"aaaa".to_vec()));
        assert_eq!(v2.get(&c).unwrap(), Some(b"cccccc".to_vec()));
        assert_eq!(v2.get(&d).unwrap(), Some(b"d".to_vec()));
        assert_eq!(v2.next_id(), d.id + 1);
    }

    /// A torn trailing record is truncated at open, and appends continue cleanly after it.
    #[test]
    fn torn_tail_is_truncated_on_open() {
        let t = T::new();
        let a;
        {
            let v = ValueStore::open(t.root()).unwrap();
            a = v.put(b"first").unwrap();
        }
        let path = t.root().join(REL_PATH);
        let clean = std::fs::metadata(&path).unwrap().len();
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&1u64.to_be_bytes()).unwrap();
            f.write_all(&500u64.to_be_bytes()).unwrap();
            f.write_all(b"only a few bytes of a 500-byte value")
                .unwrap();
        }
        let v = ValueStore::open(t.root()).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), clean);
        assert_eq!(v.get(&a).unwrap(), Some(b"first".to_vec()));
        let b = v.put(b"second").unwrap();
        assert_eq!(b.id, a.id + 1);
        drop(v);
        let v2 = ValueStore::open(t.root()).unwrap();
        assert_eq!(v2.get(&b).unwrap(), Some(b"second".to_vec()));
        assert_eq!(v2.count(), 2);
    }

    /// Regression (2026-09-26 audit): `next_id` was rebuilt at open from the highest id
    /// left in the log, so a compaction that dropped the highest blobs followed by a reopen
    /// handed their ids out again -- and a stale handle to a dropped blob silently read the
    /// new blob's bytes. The compacted log's fence keeps the high-water mark.
    #[test]
    fn ids_are_never_reused_after_compaction_and_reopen() {
        let t = T::new();
        let (a, b);
        {
            let v = ValueStore::open(t.root()).unwrap();
            a = v.put(b"aa").unwrap();
            b = v.put(b"bb").unwrap(); // the highest id, about to be dropped
            v.compact(&BTreeSet::from([a.id])).unwrap();
        }
        let v = ValueStore::open(t.root()).unwrap();
        assert_eq!(v.next_id(), b.id + 1, "the fence survives the reopen");
        let c = v.put(b"cc").unwrap();
        assert!(c.id > b.id, "a dropped id is never handed out again");
        assert_eq!(v.get(&b).unwrap(), None, "a stale handle stays absent");
        // Dropping everything, twice over, still leaves the mark in place.
        v.compact(&BTreeSet::new()).unwrap();
        v.compact(&BTreeSet::new()).unwrap();
        drop(v);
        let v = ValueStore::open(t.root()).unwrap();
        assert_eq!((v.count(), v.next_id()), (0, c.id + 1));
    }

    /// Regression (2026-09-26 audit): a failed `put` left its partial record in the log,
    /// so the next put was indexed at the wrong offset (unreadable at once) and the partial
    /// record cut every later blob off at the next open. The rollback prevents both.
    #[test]
    fn a_failed_put_is_rolled_back_and_later_blobs_survive() {
        let t = T::new();
        let v = ValueStore::open(t.root()).unwrap();
        let a = v.put(b"first").unwrap();
        {
            // What a short write leaves behind, then the rollback a failed put runs.
            let mut g = v.inner.lock().unwrap();
            g.file.write_all(&[0xAB; 11]).unwrap();
            g.roll_back(&io::Error::other("simulated short write"));
            assert!(g.poisoned.is_none());
        }
        let b = v.put(b"second").unwrap();
        assert_eq!(
            v.get(&b).unwrap(),
            Some(b"second".to_vec()),
            "readable at once"
        );
        drop(v);
        let v = ValueStore::open(t.root()).unwrap();
        assert_eq!(v.get(&a).unwrap(), Some(b"first".to_vec()));
        assert_eq!(
            v.get(&b).unwrap(),
            Some(b"second".to_vec()),
            "and after a reopen"
        );
    }

    /// When the rollback fails as well, the store is poisoned: puts and compactions refuse,
    /// reads go on, and a reopen recovers every acknowledged blob.
    #[test]
    fn a_failed_rollback_poisons_the_store_until_reopen() {
        let t = T::new();
        let v = ValueStore::open(t.root()).unwrap();
        let a = v.put(b"first").unwrap();
        v.break_for_test();
        assert!(v.put(b"lost").is_err());
        let err = v.put(b"refused").expect_err("poisoned");
        assert!(err.to_string().contains("reopen"), "{err}");
        assert!(v.compact(&BTreeSet::from([a.id])).is_err());
        assert_eq!(v.get(&a).unwrap(), Some(b"first".to_vec()), "reads go on");
        drop(v);
        let v = ValueStore::open(t.root()).unwrap();
        assert_eq!(v.count(), 1);
        let b = v.put(b"second").unwrap();
        assert_eq!(b.id, a.id + 1);
    }

    /// Regression (2026-09-26 audit): compaction scanned only up to the first damaged
    /// record, so live blobs after it were silently dropped from the rewritten log. A log
    /// damaged before its end is now refused, and left as it was.
    #[test]
    fn compaction_refuses_a_damaged_log_instead_of_dropping_live_blobs() {
        let t = T::new();
        let v = ValueStore::open(t.root()).unwrap();
        let a = v.put(b"aaaa").unwrap();
        let b = v.put(b"bbbb").unwrap();
        let c = v.put(b"cccc").unwrap();
        let mut bytes = std::fs::read(v.path()).unwrap();
        bytes[b.offset as usize + REC_HDR] ^= 0xFF; // flip a byte of b's payload
        std::fs::write(v.path(), &bytes).unwrap();
        let err = v
            .compact(&BTreeSet::from([a.id, b.id, c.id]))
            .expect_err("damage before the end of the log");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            std::fs::read(v.path()).unwrap(),
            bytes,
            "the log is untouched"
        );
        assert_eq!(
            v.get(&c).unwrap(),
            Some(b"cccc".to_vec()),
            "c is still there"
        );
    }

    /// Regression (2026-09-26 audit): a length field of all ones (erased or damaged media)
    /// overflowed the bounds arithmetic at open -- a panic in debug builds, a wrapped
    /// bound and a huge allocation in release. It now simply ends the intact log.
    #[test]
    fn an_all_ones_length_ends_the_log_instead_of_overflowing() {
        let t = T::new();
        let a;
        {
            let v = ValueStore::open(t.root()).unwrap();
            a = v.put(b"kept").unwrap();
        }
        let path = t.root().join(REL_PATH);
        let clean = std::fs::metadata(&path).unwrap().len();
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0xFF; REC_HDR + REC_TRAIL + 8]).unwrap();
        }
        let v = ValueStore::open(t.root()).expect("open survives the damaged header");
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            clean,
            "torn tail cut"
        );
        assert_eq!(v.get(&a).unwrap(), Some(b"kept".to_vec()));
    }

    /// A damaged length header must not drive the allocation: the read is bounded by the
    /// index and the record is reported absent, not read as garbage.
    #[test]
    fn damaged_length_header_reads_as_absent() {
        let t = T::new();
        let v = ValueStore::open(t.root()).unwrap();
        let r = v.put(b"payload").unwrap();
        let path = t.root().join(REL_PATH);
        let mut bytes = std::fs::read(&path).unwrap();
        // Claim an absurd length (the top byte of the 8-byte BE length field).
        bytes[8] = 0x7F;
        let payload = b"payload";
        let crc = record_crc(&bytes[..REC_HDR], payload);
        let n = bytes.len();
        bytes[n - 4..].copy_from_slice(&crc.to_be_bytes()); // even with a "valid" CRC
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(v.get(&r).unwrap(), None);
        // And a reopen treats the damaged record as the end of the intact log.
        drop(v);
        let v2 = ValueStore::open(t.root()).unwrap();
        assert_eq!(v2.count(), 0);
    }
}
