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
//! than trusting the handle's recorded offset.
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
}

impl ValueStore {
    /// Open (or create) a value store. A pre-existing blob log is scanned once at open to rebuild
    /// the id index, the id counter and the end offset; a torn trailing record (a crash
    /// mid-append) is truncated away so later appends start on a clean boundary.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<ValueStore> {
        let path = dir.as_ref().join(REL_PATH);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)?;
        let mut index = BTreeMap::new();
        let end = scan_records(&path, |id, offset, payload| {
            index.insert(id, (offset, payload.len() as u64));
        })?;
        if file.metadata()?.len() > end {
            file.set_len(end)?; // drop a torn tail
            file.sync_all()?;
        }
        let next_id = index.keys().next_back().map_or(0, |id| id + 1);
        Ok(ValueStore {
            inner: Mutex::new(Inner {
                file,
                next_id,
                end,
                index,
            }),
            path,
        })
    }

    /// Append `value` durably and return its handle. The record is fsync'd *before* the id is
    /// handed out, so a returned handle is durable.
    #[allow(clippy::should_implement_trait)]
    pub fn put(&self, value: &[u8]) -> io::Result<BlobRef> {
        let mut g = self.inner.lock().unwrap();
        let id = g.next_id;
        let offset = g.end;

        // Record: [id:8 BE][len:8 BE][payload][crc32(header||payload):4 BE]. The CRC folds
        // incrementally, so the value is never copied.
        let hdr = id.to_be_bytes();
        let lenb = (value.len() as u64).to_be_bytes();
        let mut crc = Crc::new();
        crc.update(&hdr);
        crc.update(&lenb);
        crc.update(value);
        g.file.write_all(&hdr)?;
        g.file.write_all(&lenb)?;
        g.file.write_all(value)?;
        g.file.write_all(&crc.finish().to_be_bytes())?;
        g.file.sync_all()?;

        let len = value.len() as u64;
        g.end += (REC_HDR + REC_TRAIL) as u64 + len;
        g.next_id = id + 1;
        g.index.insert(id, (offset, len));
        Ok(BlobRef { id, offset, len })
    }

    /// Read a blob by handle, verifying its CRC. Random access: seeks to the record the index
    /// holds for `ref_to.id` and reads only its bytes. Returns `None` if the id is unknown, the
    /// handle's length disagrees with the log, or the record is torn or corrupt. The allocation
    /// is bounded by the indexed length, never by whatever a damaged header claims.
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
        if g.file.read_exact(&mut hdr).is_err() {
            return Ok(None);
        }
        let disk_id = u64::from_be_bytes(hdr[0..8].try_into().expect("8 bytes"));
        let disk_len = u64::from_be_bytes(hdr[8..16].try_into().expect("8 bytes"));
        if disk_id != ref_to.id || disk_len != len {
            return Ok(None); // the header on disk disagrees with the index: damaged
        }
        let mut payload = vec![0u8; len as usize];
        if g.file.read_exact(&mut payload).is_err() {
            return Ok(None);
        }
        let mut crcb = [0u8; REC_TRAIL];
        if g.file.read_exact(&mut crcb).is_err() {
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
    /// superseded ones. Ids are preserved, so every handle to a live blob stays valid. Streams
    /// record by record (O(one record) memory), then publishes atomically (tmp, fsync, rename,
    /// then a directory fsync); a crash mid-compaction leaves the original log intact. The
    /// store's lock is held throughout, so no concurrent `put` can slip into the old log and
    /// be lost. Returns the new end offset.
    pub fn compact(&self, live: &BTreeSet<BlobId>) -> io::Result<u64> {
        let mut g = self.inner.lock().unwrap();
        let tmp = self.path.with_extension("tmp");
        let _ = fs::remove_file(&tmp);
        let mut out = BufWriter::new(File::create(&tmp)?);
        let mut index = BTreeMap::new();
        let mut end = 0u64;
        scan_records(&self.path, |id, _old_offset, payload| {
            if !live.contains(&id) {
                return;
            }
            let hdr = id.to_be_bytes();
            let lenb = (payload.len() as u64).to_be_bytes();
            let mut crc = Crc::new();
            crc.update(&hdr);
            crc.update(&lenb);
            crc.update(payload);
            // Writes into a BufWriter over a fresh file only fail on I/O errors, which
            // surface at `flush`/`sync_all` below.
            let _ = out.write_all(&hdr);
            let _ = out.write_all(&lenb);
            let _ = out.write_all(payload);
            let _ = out.write_all(&crc.finish().to_be_bytes());
            index.insert(id, (end, payload.len() as u64));
            end += (REC_HDR + REC_TRAIL + payload.len()) as u64;
        })?;
        out.flush()?;
        let f = out.into_inner().map_err(|e| e.into_error())?;
        f.sync_all()?;
        drop(f);
        fs::rename(&tmp, &self.path)?;
        if let Some(dir) = self.path.parent() {
            crate::wal::fsync_dir(dir)?;
        }
        g.file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.path)?;
        g.index = index;
        g.end = end;
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
        let total = (REC_HDR + REC_TRAIL) as u64 + len;
        if pos + total > file_len {
            return Ok(pos); // torn tail (or a damaged length): the log ends here
        }
        let mut payload = vec![0u8; len as usize];
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
        pos += total;
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
