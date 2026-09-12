//! # Write-ahead log
//!
//! Append-only, `fsync`-able, segmented WAL.
//!
//! ## Record layout (little-endian)
//!
//! ```text
//!  offset  size  field
//!    0      4    length           # bytes of `payload`
//!    4      L    payload:
//!                     tag   u8   1 = Put, 2 = Delete, 3 = Batch
//!                     term  u64  Raft term (1 on a single node)
//!                     index u64  logical commit index
//!                  tag 1 / 2 (one op):
//!                     k_len u32
//!                     key       [u8]
//!                     v_len u32
//!                     val       [u8]   (empty for Delete)
//!                  tag 3 (one atomic commit of several ops):
//!                     n     u32
//!                     n x { sub_tag u8 (1/2), k_len u32, key, v_len u32, val }
//!    4+L     4   crc32           # IEEE CRC-32 over `payload`
//! ```
//!
//! A single-op commit uses tag 1/2, byte-for-byte the Phase-1 layout. A `Batch`
//! record is one commit index applied all-or-nothing: the CRC covers every op, so a
//! crash mid-record loses the whole batch and never a prefix of it.
//!
//! ## Crash model
//!
//! Every [`Wal::append_many`] ends with one `fsync`, so a record that is fully on disk
//! is durable. Under a `SIGKILL` mid-write the trailing partial record -- a
//! half-written `length`, `payload`, or `crc` -- is detected on replay by either (a)
//! an out-of-bounds/undersized length, or (b) a CRC mismatch, and is discarded. Hence
//! recovery loses **at most the records of the last un-`fsync`-ed group**, none of
//! which were ever acknowledged to a caller.
//!
//! A torn tail is only legitimate in the **last** segment: a crash interrupts at most
//! one append. A bad record in an earlier segment, with later segments present, is not
//! a crash artefact but corruption, and [`replay`] refuses to skip past it.
//!
//! ## Segmentation, repair, truncation
//!
//! A segment rotates into a new file once it would exceed `Wal::max_seg_bytes`.
//! Names embed a 10-digit monotone sequence so a sort yields the append order.
//! [`Wal::open`] resumes the latest segment, first truncating any torn tail it finds
//! so new records always start on a clean record boundary. Creating or rotating a
//! segment `fsync`s the directory, so the new file's existence is as durable as its
//! bytes. The engine reclaims space with [`Wal::rotate_segment`] plus
//! [`Wal::remove_segments_before`] (see `Store::checkpoint`); nothing else ever
//! deletes the log.

use std::fs::{File, OpenOptions, create_dir_all, remove_file};
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};

use crate::crc::crc32;
use crate::types::Op;

/// Minimum payload size: any "length" smaller than this is a torn tail.
const MIN_PAYLOAD: u32 = 1 + 8 + 8 + 4 + 4; // tag + term + index + k_len + v_len

const TAG_PUT: u8 = 1;
const TAG_DELETE: u8 = 2;
const TAG_BATCH: u8 = 3;

/// One log record: a commit index and the ops applied at it, all-or-nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub term: u64,
    pub index: u64,
    /// The ops of this commit. One for an ordinary put/delete; several for a batch.
    pub ops: Vec<Op>,
}

impl Record {
    /// A single-op record.
    pub fn single(term: u64, index: u64, op: Op) -> Record {
        Record {
            term,
            index,
            ops: vec![op],
        }
    }
}

/// Where [`replay`] found a torn (partial or corrupt) trailing record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TornTail {
    /// The segment holding the torn record.
    pub segment: PathBuf,
    /// Byte offset of the torn record's first byte; every byte before it is intact.
    pub offset: u64,
}

/// Durable, segmented WAL rooted at a directory.
pub struct Wal {
    dir: PathBuf,
    file: File,
    seg_seq: u32,
    /// Bytes written to the current segment so far (for rotation).
    seg_bytes: u64,
    /// Rotation threshold.
    pub max_seg_bytes: u64,
}

impl Wal {
    /// Open the log in `dir`, creating the directory and a first segment if needed.
    ///
    /// Resumes the latest existing segment. If that segment ends in a torn record (a
    /// crash mid-append), the tail is truncated first, so the next append lands on a
    /// clean boundary and a later [`replay`] sees one contiguous, intact log. Earlier
    /// segments are left untouched for the caller to [`replay`].
    pub fn open(dir: impl AsRef<Path>, max_seg_bytes: u64) -> io::Result<Wal> {
        let dir = dir.as_ref().to_path_buf();
        create_dir_all(&dir)?;
        let mut segs = list_segments(&dir)?;
        segs.sort();
        let (seq, path) = match segs.last() {
            Some(p) => (segment_seq(p).expect("listed segments parse"), p.clone()),
            None => (1, segment_path(&dir, 1)),
        };
        let resumed = path.exists();
        if resumed {
            repair_tail(&path)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        if !resumed {
            fsync_dir(&dir)?; // the new segment's directory entry is durable too.
        }
        let seg_bytes = file.metadata()?.len();
        Ok(Wal {
            dir,
            file,
            seg_seq: seq,
            seg_bytes,
            max_seg_bytes: max_seg_bytes.max(MIN_PAYLOAD as u64 + 8),
        })
    }

    /// Append one record and `fsync` it.
    ///
    /// Invariant: *after `append` returns, the record is durable and will be replayed
    /// after any crash.*
    pub fn append(&mut self, rec: &Record) -> io::Result<()> {
        self.append_many(std::slice::from_ref(rec))
    }

    /// Append several records with **one** `fsync` -- the group-commit primitive. The
    /// records are written as one contiguous buffer; a crash mid-write leaves an intact
    /// prefix of the group (each record is individually framed and checksummed), and no
    /// caller is acknowledged before the `fsync` returns.
    pub fn append_many(&mut self, recs: &[Record]) -> io::Result<()> {
        if recs.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::new();
        for rec in recs {
            let payload = encode_payload(rec);
            buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            buf.extend_from_slice(&payload);
            buf.extend_from_slice(&crc32(&payload).to_le_bytes());
        }
        // Rotate first if this group would overflow the current segment (a group larger
        // than a segment simply makes an oversized segment).
        if self.seg_bytes + buf.len() as u64 > self.max_seg_bytes && self.seg_bytes > 0 {
            self.rotate_segment()?;
        }
        self.file.write_all(&buf)?;
        self.file.sync_all()?; // fsync -> durable
        self.seg_bytes += buf.len() as u64;
        Ok(())
    }

    /// The sequence number of the segment currently being appended to.
    pub fn current_segment(&self) -> u32 {
        self.seg_seq
    }

    /// Close the current segment and start a fresh one, returning the new segment's
    /// sequence number: every record appended from now on lives in a segment numbered
    /// at or above it. Used by checkpoints to draw a boundary in the log.
    pub fn rotate_segment(&mut self) -> io::Result<u32> {
        self.file.sync_all()?;
        self.seg_seq += 1;
        let path = segment_path(&self.dir, self.seg_seq);
        self.file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)?;
        self.seg_bytes = 0;
        fsync_dir(&self.dir)?; // the new segment's directory entry is durable too.
        Ok(self.seg_seq)
    }

    /// Delete every segment numbered below `seq` (never the current one). Call only for
    /// segments a durable checkpoint fully covers. Returns how many were removed.
    pub fn remove_segments_before(&mut self, seq: u32) -> io::Result<usize> {
        let mut removed = 0;
        for p in list_segments(&self.dir)? {
            if segment_seq(&p).is_some_and(|s| s < seq && s != self.seg_seq) {
                remove_file(&p)?;
                removed += 1;
            }
        }
        if removed > 0 {
            fsync_dir(&self.dir)?;
        }
        Ok(removed)
    }

    /// Durably flush the current on-disk state without appending a new record.
    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    /// Delete every segment file in this log, then start a fresh one. Call only after
    /// a durable snapshot that fully covers their contents.
    pub fn truncate_all(&mut self) -> io::Result<()> {
        let boundary = self.rotate_segment()?;
        self.remove_segments_before(boundary)?;
        Ok(())
    }
}

/// Replay every segment in `dir` in filename (i.e. append) order.
///
/// Returns `(records, torn)`. A torn trailing record in the **last** segment is dropped
/// and reported in `torn`: the records before it are complete and durable, and that is
/// not an error. A bad record in any *earlier* segment is corruption, not a crash tail,
/// and is returned as an `InvalidData` error rather than silently truncating history.
pub fn replay(dir: impl AsRef<Path>) -> io::Result<(Vec<Record>, Option<TornTail>)> {
    let mut files = list_segments(dir.as_ref())?;
    files.sort();
    let mut out = Vec::new();
    for (i, path) in files.iter().enumerate() {
        if let Some(offset) = scan_segment(path, |r| out.push(r))? {
            let later = files.len() - 1 - i;
            if later > 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "WAL segment {} is corrupt at byte {offset} but {later} later segment(s) \
                         exist; refusing to replay past corruption",
                        path.display()
                    ),
                ));
            }
            return Ok((
                out,
                Some(TornTail {
                    segment: path.clone(),
                    offset,
                }),
            ));
        }
    }
    Ok((out, None))
}

/// Walk one segment, handing each intact record to `sink` in order. Returns the byte
/// offset of the first torn or undecodable record, or `None` if the segment ends
/// cleanly. Every read is bounded, so a truncated file yields an offset, not a panic.
fn scan_segment(path: &Path, mut sink: impl FnMut(Record)) -> io::Result<Option<u64>> {
    let len = std::fs::metadata(path)?.len();
    let mut f = BufReader::new(File::open(path)?);
    let mut pos: u64 = 0;
    loop {
        if pos + 4 > len {
            // Fewer than 4 header bytes remain: a clean end when we are exactly at
            // `len`, otherwise an overhang to drop.
            return Ok(if pos == len { None } else { Some(pos) });
        }
        let mut lb = [0u8; 4];
        f.read_exact(&mut lb)?; // safe: pos + 4 <= len
        let payload_len = u64::from(u32::from_le_bytes(lb));
        if payload_len < u64::from(MIN_PAYLOAD) {
            return Ok(Some(pos)); // short/corrupt header => torn tail
        }
        let total = 4 + payload_len + 4;
        if pos + total > len {
            return Ok(Some(pos)); // record not fully present => torn tail
        }
        let mut pbuf = vec![0u8; payload_len as usize];
        f.read_exact(&mut pbuf)?;
        let mut cb = [0u8; 4];
        f.read_exact(&mut cb)?;
        if crc32(&pbuf) != u32::from_le_bytes(cb) {
            return Ok(Some(pos)); // crc mismatch => torn tail
        }
        match decode_payload(&pbuf) {
            Ok(rec) => sink(rec),
            Err(_) => return Ok(Some(pos)), // undecodable => torn tail
        }
        pos += total;
    }
}

/// Truncate a torn trailing record off `path`, if there is one. Returns the offset the
/// file was cut to, or `None` if it was already clean.
fn repair_tail(path: &Path) -> io::Result<Option<u64>> {
    match scan_segment(path, |_| {})? {
        Some(off) => {
            let f = OpenOptions::new().write(true).open(path)?;
            f.set_len(off)?;
            f.sync_all()?;
            Ok(Some(off))
        }
        None => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

fn op_tag(op: &Op) -> u8 {
    match op {
        Op::Put { .. } => TAG_PUT,
        Op::Delete { .. } => TAG_DELETE,
    }
}

/// `k_len, key, v_len, val` (an empty value for a delete).
fn encode_op_body(op: &Op, b: &mut Vec<u8>) {
    let (key, value): (&[u8], &[u8]) = match op {
        Op::Put { key, value } => (key, value),
        Op::Delete { key } => (key, &[]),
    };
    b.extend_from_slice(&(key.len() as u32).to_le_bytes());
    b.extend_from_slice(key);
    b.extend_from_slice(&(value.len() as u32).to_le_bytes());
    b.extend_from_slice(value);
}

fn encode_payload(rec: &Record) -> Vec<u8> {
    let mut b = Vec::new();
    match rec.ops.as_slice() {
        [op] => {
            b.push(op_tag(op));
            b.extend_from_slice(&rec.term.to_le_bytes());
            b.extend_from_slice(&rec.index.to_le_bytes());
            encode_op_body(op, &mut b);
        }
        ops => {
            b.push(TAG_BATCH);
            b.extend_from_slice(&rec.term.to_le_bytes());
            b.extend_from_slice(&rec.index.to_le_bytes());
            b.extend_from_slice(&(ops.len() as u32).to_le_bytes());
            for op in ops {
                b.push(op_tag(op));
                encode_op_body(op, &mut b);
            }
        }
    }
    b
}

/// A bounded cursor over a payload: every read checks the remaining length.
struct Cursor<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize, what: &str) -> io::Result<&'a [u8]> {
        if self.at + n > self.b.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, what.to_string()));
        }
        let s = &self.b[self.at..self.at + n];
        self.at += n;
        Ok(s)
    }
    fn u8(&mut self, what: &str) -> io::Result<u8> {
        Ok(self.take(1, what)?[0])
    }
    fn u32(&mut self, what: &str) -> io::Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4, what)?.try_into().expect("4 bytes"),
        ))
    }
    fn u64(&mut self, what: &str) -> io::Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8, what)?.try_into().expect("8 bytes"),
        ))
    }
    fn remaining(&self) -> usize {
        self.b.len() - self.at
    }
}

fn decode_op(tag: u8, c: &mut Cursor<'_>) -> io::Result<Op> {
    let k_len = c.u32("k_len")? as usize;
    let key = c.take(k_len, "key body")?.to_vec();
    let v_len = c.u32("v_len")? as usize;
    let value = c.take(v_len, "val body")?;
    match tag {
        TAG_PUT => Ok(Op::Put {
            key,
            value: value.to_vec(),
        }),
        TAG_DELETE => Ok(Op::Delete { key }),
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "bad tag")),
    }
}

fn decode_payload(b: &[u8]) -> io::Result<Record> {
    let mut c = Cursor { b, at: 0 };
    let tag = c.u8("tag")?;
    let term = c.u64("term")?;
    let index = c.u64("index")?;
    let ops = match tag {
        TAG_PUT | TAG_DELETE => vec![decode_op(tag, &mut c)?],
        TAG_BATCH => {
            let n = c.u32("batch count")? as usize;
            // Each op needs at least a tag and two lengths; a count beyond that is bogus.
            if n == 0 || n > c.remaining() / 9 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "batch count"));
            }
            let mut ops = Vec::with_capacity(n);
            for _ in 0..n {
                let sub = c.u8("sub tag")?;
                ops.push(decode_op(sub, &mut c)?);
            }
            ops
        }
        _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "bad tag")),
    };
    if c.remaining() != 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "trailing bytes"));
    }
    Ok(Record { term, index, ops })
}

// ---------------------------------------------------------------------------
// Filesystem helpers
// ---------------------------------------------------------------------------

fn segment_path(dir: &Path, seq: u32) -> PathBuf {
    dir.join(format!("wal-{seq:010}.log"))
}

/// The sequence number embedded in a segment file name, if it is one.
fn segment_seq(path: &Path) -> Option<u32> {
    let name = path.file_name()?.to_str()?;
    name.strip_prefix("wal-")?
        .strip_suffix(".log")?
        .parse::<u32>()
        .ok()
}

/// Every segment file in `dir`, in no particular order (callers sort).
pub fn list_segments(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut vec: Vec<PathBuf> = Vec::new();
    if !dir.exists() {
        return Ok(vec);
    }
    for e in std::fs::read_dir(dir)? {
        let p = e?.path();
        if segment_seq(&p).is_some() {
            vec.push(p);
        }
    }
    Ok(vec)
}

/// `fsync` a directory so a just-created, renamed or deleted entry is durable: POSIX
/// makes no promise about the entry until the directory itself is synced. A no-op on
/// platforms where a directory cannot be opened as a file.
pub(crate) fn fsync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Op;

    fn recs_clean(dir: &Path) -> Vec<Record> {
        let (r, tail) = replay(dir).expect("replay");
        assert!(tail.is_none(), "clean log must not report a torn tail");
        r
    }

    fn dir(pid_suffix: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("ks-{pid_suffix}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn put(index: u64, key: &[u8], value: &[u8]) -> Record {
        Record::single(
            1,
            index,
            Op::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            },
        )
    }

    fn append_junk(seg: &Path) {
        let mut f = OpenOptions::new().append(true).open(seg).unwrap();
        // A plausible length header (0xFF) followed by far fewer bytes than it claims.
        f.write_all(&[0xFF, 0x00, 0x00, 0x00, 0x01, 0x00]).unwrap();
    }

    fn only_segment(d: &Path) -> PathBuf {
        let segs = list_segments(d).unwrap();
        assert_eq!(segs.len(), 1, "expected exactly one segment, got {segs:?}");
        segs[0].clone()
    }

    #[test]
    fn roundtrip_put_and_delete() {
        let d = dir("wal-rt");
        let mut w = Wal::open(&d, 4096).unwrap();
        w.append(&put(1, b"a", b"1")).unwrap();
        w.append(&Record::single(1, 2, Op::Delete { key: b"a".into() }))
            .unwrap();
        w.append(&put(3, b"z", b"\x00\x01\xff")).unwrap();
        let got = recs_clean(&d);
        assert_eq!(got.len(), 3);
        assert_eq!(got[0], put(1, b"a", b"1"));
        assert_eq!(
            got[1],
            Record::single(1, 2, Op::Delete { key: b"a".into() })
        );
        assert_eq!(got[2], put(3, b"z", b"\x00\x01\xff"));
        std::fs::remove_dir_all(&d).ok();
    }

    /// A batch record carries several ops under one index and decodes exactly; a
    /// single-op record still uses the original tag-1/2 layout.
    #[test]
    fn batch_record_round_trips() {
        let d = dir("wal-batch");
        let mut w = Wal::open(&d, 4096).unwrap();
        let batch = Record {
            term: 1,
            index: 1,
            ops: vec![
                Op::Put {
                    key: b"a".to_vec(),
                    value: b"1".to_vec(),
                },
                Op::Delete { key: b"b".to_vec() },
                Op::Put {
                    key: b"c".to_vec(),
                    value: vec![0xAB; 300],
                },
            ],
        };
        w.append_many(&[batch.clone(), put(2, b"d", b"4")]).unwrap();
        assert_eq!(encode_payload(&put(2, b"d", b"4"))[0], TAG_PUT);
        assert_eq!(encode_payload(&batch)[0], TAG_BATCH);
        let got = recs_clean(&d);
        assert_eq!(got, vec![batch, put(2, b"d", b"4")]);
        std::fs::remove_dir_all(&d).ok();
    }

    /// A malformed batch payload (count larger than the bytes can hold) is rejected by
    /// the bounded decoder rather than trusted.
    #[test]
    fn oversized_batch_count_is_rejected() {
        let mut payload = vec![TAG_BATCH];
        payload.extend_from_slice(&1u64.to_le_bytes());
        payload.extend_from_slice(&1u64.to_le_bytes());
        payload.extend_from_slice(&1_000_000u32.to_le_bytes());
        payload.extend_from_slice(&[TAG_PUT, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert!(decode_payload(&payload).is_err());
    }

    #[test]
    fn torn_tail_is_dropped_cleanly() {
        let d = dir("wal-torn");
        let mut w = Wal::open(&d, 1024).unwrap();
        w.append(&put(1, b"k", b"v")).unwrap();
        let seg = only_segment(&d);
        let clean_len = std::fs::metadata(&seg).unwrap().len();
        append_junk(&seg); // simulate a torn tail: junk without fsync.
        let (got, dropped) = replay(&d).unwrap();
        assert_eq!(got.len(), 1, "only the durable record survives");
        let torn = dropped.expect("a torn tail must be detected");
        assert_eq!(torn.segment, seg);
        assert_eq!(
            torn.offset, clean_len,
            "the tail starts where the intact records end"
        );
        std::fs::remove_dir_all(&d).ok();
    }

    /// Reopening resumes the latest segment and repairs its torn tail, so records
    /// appended afterwards follow the intact ones and replay cleanly.
    #[test]
    fn open_repairs_torn_tail_and_resumes_the_segment() {
        let d = dir("wal-repair");
        {
            let mut w = Wal::open(&d, 4096).unwrap();
            w.append(&put(1, b"a", b"1")).unwrap();
        }
        let seg = only_segment(&d);
        let clean_len = std::fs::metadata(&seg).unwrap().len();
        append_junk(&seg);
        {
            let mut w = Wal::open(&d, 4096).unwrap();
            assert_eq!(
                std::fs::metadata(&seg).unwrap().len(),
                clean_len,
                "open truncates the torn tail"
            );
            w.append(&put(2, b"b", b"2")).unwrap();
        }
        assert_eq!(
            only_segment(&d),
            seg,
            "the same segment was resumed, not a new one"
        );
        let got = recs_clean(&d);
        assert_eq!(got, vec![put(1, b"a", b"1"), put(2, b"b", b"2")]);
        std::fs::remove_dir_all(&d).ok();
    }

    /// A bad record in a non-final segment is corruption, not a crash tail: replay must
    /// refuse rather than silently drop every later segment.
    #[test]
    fn corruption_before_a_later_segment_is_an_error() {
        let d = dir("wal-corrupt");
        let mut w = Wal::open(&d, 64).unwrap(); // tiny threshold forces rotation
        for i in 0..10u64 {
            w.append(&put(i + 1, format!("k{i}").as_bytes(), b"v"))
                .unwrap();
        }
        let mut segs = list_segments(&d).unwrap();
        segs.sort();
        assert!(segs.len() >= 2, "expected rotation");
        let first = &segs[0];
        let mut bytes = std::fs::read(first).unwrap();
        bytes[10] ^= 0xFF; // flip a byte inside the first record's payload
        std::fs::write(first, &bytes).unwrap();
        let err = replay(&d).expect_err("corruption followed by later segments is an error");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn rotation_and_replay_order() {
        let d = dir("wal-rot");
        let mut w = Wal::open(&d, 64).unwrap(); // tiny threshold forces rotation
        for i in 0..50u64 {
            w.append(&put(i + 1, format!("k{i}").as_bytes(), &i.to_le_bytes()))
                .unwrap();
        }
        let got = recs_clean(&d);
        assert_eq!(got.len(), 50);
        for (i, r) in got.iter().enumerate() {
            assert_eq!(r.index, (i + 1) as u64);
        }
        assert!(list_segments(&d).unwrap().len() >= 2, "expected rotation");
        std::fs::remove_dir_all(&d).ok();
    }

    /// `rotate_segment` draws a boundary: records before it live in lower-numbered
    /// segments, which `remove_segments_before` deletes while keeping the rest intact.
    #[test]
    fn rotate_then_remove_segments_before_boundary() {
        let d = dir("wal-boundary");
        let mut w = Wal::open(&d, 1 << 20).unwrap();
        for i in 1..=5u64 {
            w.append(&put(i, b"k", &i.to_le_bytes())).unwrap();
        }
        let boundary = w.rotate_segment().unwrap();
        for i in 6..=8u64 {
            w.append(&put(i, b"k", &i.to_le_bytes())).unwrap();
        }
        assert_eq!(list_segments(&d).unwrap().len(), 2);
        assert_eq!(w.remove_segments_before(boundary).unwrap(), 1);
        assert_eq!(w.current_segment(), boundary);
        let got = recs_clean(&d);
        assert_eq!(
            got.iter().map(|r| r.index).collect::<Vec<_>>(),
            vec![6, 7, 8],
            "only the records after the boundary remain"
        );
        assert_eq!(
            w.remove_segments_before(boundary + 5).unwrap(),
            0,
            "the current segment is never removed"
        );
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn crc_detects_bit_flip() {
        // A flipped byte in a committed record's body must fail its crc guard.
        let good = put(42, b"key", b"value");
        let payload = encode_payload(&good);
        assert_eq!(crc32(&payload), crc32(&payload));
        let mut bad = payload.clone();
        bad[4] ^= 0xFF; // flip a term byte
        assert_ne!(crc32(&payload), crc32(&bad));
    }
}
