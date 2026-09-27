//! # Write-ahead log
//!
//! Append-only, `fsync`-able, segmented WAL.
//!
//! ## Segment layout (little-endian)
//!
//! ```text
//!  offset  size  field
//!    0      4    magic     b"KSWL"
//!    4      4    version   u32 = 1: the record layout below
//!    8      4    crc32     IEEE CRC-32 over bytes 0..8
//!   12      ...  records, back to back
//! ```
//!
//! Every version of the format starts with this 12-byte header, so any build can tell a
//! segment written in another version (intact header, unknown version: refused with
//! `Unsupported`, never read as though it were this one) from a damaged one (header CRC
//! mismatch).
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
//! a crash artefact but corruption, and [`replay`] refuses to skip past it. Within the
//! last segment recovery is *point-in-time*: the first record that is cut short or fails
//! its CRC ends the log, whether a crash tore it or a later bit flip damaged it (telling
//! the two apart is tracked in `ROADMAP.md`, under 0.3.0). A record whose CRC is valid but
//! which does not decode is neither: no torn write produces one, so it is corruption
//! wherever it is.
//!
//! A segment is born with its header written and `fsync`'d before its directory entry is
//! synced and before any record is appended to it. A segment shorter than its header, or
//! exactly header-sized but not a valid header, is therefore one whose creation a crash
//! interrupted: it holds no record, [`replay`] reports a torn tail at offset 0, and
//! [`Wal::open`] writes the header afresh. A bad header in front of records is corruption.
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
//!
//! ## Failed writes
//!
//! A failed append (a short write, `ENOSPC`, a failed `fsync`) is rolled back: the
//! segment is cut back to the end of its last durable record, so the next append again
//! starts on a clean boundary and a commit index that was never acknowledged is never
//! logged twice. A failed rotation removes the segment it half-created, so appends go on
//! in the current segment with no newer one beside it. If a rollback or that removal
//! fails too -- or a rotation finds the next segment's name already taken -- the on-disk
//! shape of the log is not what the `Wal` believes, and it is *poisoned*: every later
//! append or rotation fails until the store is reopened, where the repair above takes
//! over. Records that the `u32` framing cannot hold are refused with `InvalidInput`
//! before anything is written.

use std::fs::{File, OpenOptions, create_dir_all, remove_file};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::crc::crc32;
use crate::types::Op;

/// The first four bytes of every segment.
const SEGMENT_MAGIC: [u8; 4] = *b"KSWL";

/// The segment format this build writes, and the only one it reads.
pub(crate) const SEGMENT_VERSION: u32 = 1;

/// Magic, version and the header's CRC: where a segment's first record starts.
const HEADER_LEN: u64 = 12;

/// Minimum payload size: any "length" smaller than this is a torn tail.
const MIN_PAYLOAD: u32 = 1 + 8 + 8 + 4 + 4; // tag + term + index + k_len + v_len

/// The smallest whole record: length field, minimal payload, CRC.
const MIN_RECORD: u64 = 4 + MIN_PAYLOAD as u64 + 4;

/// The largest payload the record's `u32` length field can frame, in bytes.
pub(crate) const MAX_PAYLOAD: u64 = u32::MAX as u64;

const TAG_PUT: u8 = 1;
const TAG_DELETE: u8 = 2;
const TAG_BATCH: u8 = 3;

/// One log record: a commit index and the ops applied at it, all-or-nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Record {
    pub(crate) term: u64,
    pub(crate) index: u64,
    /// The ops of this commit. One for an ordinary put/delete; several for a batch.
    pub(crate) ops: Vec<Op>,
}

impl Record {
    /// A single-op record.
    #[cfg(test)]
    pub(crate) fn single(term: u64, index: u64, op: Op) -> Record {
        Record {
            term,
            index,
            ops: vec![op],
        }
    }
}

/// A point on a rotation's failure paths where a test can make I/O fail on demand.
/// Outside tests [`Wal::fault`] is a no-op, so these cost nothing; a filesystem seam that
/// covers every call is tracked in `ROADMAP.md` (0.3.0).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RotationFault {
    /// Creating the next segment fails before anything is created.
    Create,
    /// Writing or syncing the next segment's header fails, after the file was created.
    Header,
    /// Removing that half-created segment fails too.
    Cleanup,
    /// Syncing the directory after creating the next segment fails.
    DirSync,
}

/// Durable, segmented WAL rooted at a directory.
pub(crate) struct Wal {
    dir: PathBuf,
    file: File,
    seg_seq: u32,
    /// Bytes in the current segment so far, header included (for rotation and rollback).
    seg_bytes: u64,
    /// Rotation threshold.
    max_seg_bytes: u64,
    /// Why the log refuses writes, once a failed append could not be rolled back (see
    /// the module docs). `None` while the log is healthy.
    poisoned: Option<String>,
    /// The failure points a test has armed (see [`RotationFault`]).
    #[cfg(test)]
    faults: Vec<RotationFault>,
}

impl Wal {
    /// Open the log in `dir`, creating the directory and a first segment if needed.
    ///
    /// Resumes the latest existing segment. If that segment ends in a torn record (a
    /// crash mid-append), the tail is truncated first, so the next append lands on a
    /// clean boundary and a later [`replay`] sees one contiguous, intact log; if its
    /// header never became durable, the header is written afresh. Earlier segments are
    /// left untouched for the caller to [`replay`].
    pub(crate) fn open(dir: impl AsRef<Path>, max_seg_bytes: u64) -> io::Result<Wal> {
        let dir = dir.as_ref().to_path_buf();
        create_dir_all(&dir)?;
        let mut segs = list_segments(&dir)?;
        segs.sort();
        let (seq, path) = match segs.last() {
            Some(p) => (segment_seq(p).expect("listed segments parse"), p.clone()),
            None => (1, segment_path(&dir, 1)),
        };
        if path.exists() {
            repair_tail(&path)?;
        }
        let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
        let mut seg_bytes = file.metadata()?.len();
        if seg_bytes == 0 {
            // A new segment, or one whose creation a crash cut short: give it its header.
            file.write_all(&segment_header())?;
            file.sync_all()?;
            seg_bytes = HEADER_LEN;
        }
        // Make the segment's directory entry durable, whether it was just created or was
        // found after a crash that may have struck before its creator synced the directory.
        fsync_dir(&dir)?;
        Ok(Wal {
            dir,
            file,
            seg_seq: seq,
            seg_bytes,
            max_seg_bytes: max_seg_bytes.max(HEADER_LEN + MIN_RECORD),
            poisoned: None,
            #[cfg(test)]
            faults: Vec::new(),
        })
    }

    /// Append one record and `fsync` it.
    ///
    /// Invariant: *after `append` returns, the record is durable and will be replayed
    /// after any crash.*
    #[cfg(test)]
    pub(crate) fn append(&mut self, rec: &Record) -> io::Result<()> {
        self.append_many(std::slice::from_ref(rec))
    }

    /// Append several records with **one** `fsync` -- the group-commit primitive. The
    /// records are written as one contiguous buffer; a crash mid-write leaves an intact
    /// prefix of the group (each record is individually framed and checksummed), and no
    /// caller is acknowledged before the `fsync` returns.
    ///
    /// On failure nothing of the group stays in the log: the partial write is rolled
    /// back (or, if that fails too, the log is poisoned -- see the module docs), so the
    /// caller may retry the same indices. A record the framing cannot hold (no ops, or
    /// a payload above [`MAX_PAYLOAD`]) is refused with `InvalidInput` before any write.
    pub(crate) fn append_many(&mut self, recs: &[Record]) -> io::Result<()> {
        self.check_usable()?;
        if recs.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::new();
        for rec in recs {
            check_ops(&rec.ops)?;
            let payload = encode_payload(rec);
            let len = u32::try_from(payload.len()).expect("check_ops bounds the payload");
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(&payload);
            buf.extend_from_slice(&crc32(&payload).to_le_bytes());
        }
        // Rotate first if this group would overflow a segment that already holds records
        // (a group larger than a segment simply makes an oversized segment).
        if self.seg_bytes + buf.len() as u64 > self.max_seg_bytes && self.seg_bytes > HEADER_LEN {
            self.rotate_segment()?;
        }
        // fsync -> durable. A failure may leave part (or all) of `buf` in the file.
        if let Err(e) = self
            .file
            .write_all(&buf)
            .and_then(|()| self.file.sync_all())
        {
            self.roll_back(&e);
            return Err(e);
        }
        self.seg_bytes += buf.len() as u64;
        Ok(())
    }

    /// Undo a failed append: cut the segment back to the end of its last durable record,
    /// so the next append starts on a clean boundary and a retried index is never logged
    /// twice. If the cut itself fails, the log's on-disk shape is unknown: poison it.
    fn roll_back(&mut self, cause: &io::Error) {
        let cut = self
            .file
            .set_len(self.seg_bytes)
            .and_then(|()| self.file.seek(SeekFrom::Start(self.seg_bytes)))
            .and_then(|_| self.file.sync_all());
        if let Err(e) = cut {
            self.poisoned = Some(format!(
                "an append failed ({cause}) and could not be rolled back ({e})"
            ));
        }
    }

    /// Swap the segment handle for a read-only one, so the next append fails and so does
    /// its rollback: the poisoning path, on demand.
    #[cfg(test)]
    pub(crate) fn break_for_test(&mut self) {
        let path = segment_path(&self.dir, self.seg_seq);
        self.file = File::open(path).expect("reopen the current segment read-only");
    }

    /// `Ok` unless an earlier failure poisoned the log.
    fn check_usable(&self) -> io::Result<()> {
        match &self.poisoned {
            None => Ok(()),
            Some(why) => Err(io::Error::other(format!(
                "the WAL refuses writes after an earlier failure: {why}; reopen the store \
                 to recover"
            ))),
        }
    }

    /// The sequence number of the segment currently being appended to.
    pub(crate) fn current_segment(&self) -> u32 {
        self.seg_seq
    }

    /// Close the current segment and start a fresh one, returning the new segment's
    /// sequence number: every record appended from now on lives in a segment numbered
    /// at or above it. Used by checkpoints to draw a boundary in the log.
    ///
    /// The switch happens only once the new segment's header and directory entry are
    /// durable. If the segment cannot be created, whatever part of it was created is
    /// removed and appends continue in the current segment -- unless the failure leaves a
    /// newer segment beside the current one, which poisons the log.
    pub(crate) fn rotate_segment(&mut self) -> io::Result<u32> {
        self.check_usable()?;
        self.file.sync_all()?;
        let next = self.seg_seq.checked_add(1).ok_or_else(|| {
            io::Error::other("WAL segment sequence exhausted (u32::MAX segments)")
        })?;
        let path = segment_path(&self.dir, next);
        // Appending on in the current segment is safe only while no newer segment exists
        // beside it: a later torn tail in a segment that is not the last looks like
        // mid-log corruption, and recovery refuses it. Every failure below keeps to that.
        //
        // `create_new`: no segment above the current one should exist (open resumes the
        // highest), and one that does must not be clobbered.
        let created = self
            .fault(RotationFault::Create)
            .and_then(|()| OpenOptions::new().create_new(true).write(true).open(&path));
        let mut file = match created {
            Ok(file) => file,
            Err(e) => {
                // Normally nothing was created. But the name may already be taken (a stray
                // file, or a create that was retried after it succeeded), and then a newer
                // segment sits beside the current one: stop.
                if path.symlink_metadata().is_ok() {
                    self.poisoned = Some(format!(
                        "segment {} already exists beside the current one ({e})",
                        path.display()
                    ));
                }
                return Err(e);
            }
        };
        if let Err(e) = self
            .fault(RotationFault::Header)
            .and_then(|()| file.write_all(&segment_header()))
            .and_then(|()| file.sync_all())
        {
            drop(file);
            // A half-born segment must not outlive the failure. If it cannot be removed
            // durably, poison the log.
            if let Err(cleanup) = self
                .fault(RotationFault::Cleanup)
                .and_then(|()| remove_file(&path))
                .and_then(|()| fsync_dir(&self.dir))
            {
                self.poisoned = Some(format!(
                    "segment {} could not be created ({e}) nor removed ({cleanup})",
                    path.display()
                ));
            }
            return Err(e);
        }
        // The new segment's directory entry must be durable before records land in it.
        // If it may not be, stop: appending on in the old segment with an empty newer
        // one beside it would make a later torn tail look like mid-log corruption.
        if let Err(e) = self
            .fault(RotationFault::DirSync)
            .and_then(|()| fsync_dir(&self.dir))
        {
            self.poisoned = Some(format!(
                "segment {} was created but the directory could not be synced ({e})",
                path.display()
            ));
            return Err(e);
        }
        self.file = file;
        self.seg_seq = next;
        self.seg_bytes = HEADER_LEN;
        Ok(next)
    }

    /// Delete every segment numbered below `seq` (never the current one), oldest first,
    /// so an interrupted call never leaves a hole in the middle of the retained log.
    /// Call only for segments a durable checkpoint fully covers. Returns how many were
    /// removed.
    pub(crate) fn remove_segments_before(&mut self, seq: u32) -> io::Result<usize> {
        let mut doomed: Vec<(u32, PathBuf)> = list_segments(&self.dir)?
            .into_iter()
            .filter_map(|p| segment_seq(&p).map(|s| (s, p)))
            .filter(|&(s, _)| s < seq && s != self.seg_seq)
            .collect();
        doomed.sort();
        for (_, p) in &doomed {
            remove_file(p)?;
        }
        if !doomed.is_empty() {
            fsync_dir(&self.dir)?;
        }
        Ok(doomed.len())
    }

    /// Fail at `point` if a test armed it.
    #[cfg(test)]
    fn fault(&self, point: RotationFault) -> io::Result<()> {
        if self.faults.contains(&point) {
            return Err(io::Error::other(format!("injected failure: {point:?}")));
        }
        Ok(())
    }

    /// Outside tests no failure is ever injected.
    #[cfg(not(test))]
    #[inline(always)]
    fn fault(&self, _point: RotationFault) -> io::Result<()> {
        Ok(())
    }

    /// Arm (or, with an empty list, disarm) the failure points of [`Wal::fault`].
    #[cfg(test)]
    pub(crate) fn inject(&mut self, faults: &[RotationFault]) {
        self.faults = faults.to_vec();
    }
}

/// Replay every segment in `dir` in filename (i.e. append) order, handing each intact
/// record to `sink` together with the sequence number of the segment that holds it. The
/// sink may refuse a record by returning an error, which ends the replay with that error.
/// Records are streamed: memory stays at one record however long the log is.
///
/// Returns the byte offset at which a torn tail begins in the **last** segment, if it has
/// one: the records before it are complete and durable, and that is not an error. A bad
/// record in any *earlier* segment is corruption, not a crash tail, and is returned as an
/// `InvalidData` error rather than silently truncating history.
pub(crate) fn replay(
    dir: &Path,
    mut sink: impl FnMut(u32, Record) -> io::Result<()>,
) -> io::Result<Option<u64>> {
    let mut files = list_segments(dir)?;
    files.sort();
    for (i, path) in files.iter().enumerate() {
        let seq = segment_seq(path).expect("listed segments parse");
        if let Some(offset) = scan_segment(path, |r| sink(seq, r))? {
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
            return Ok(Some(offset));
        }
    }
    Ok(None)
}

/// Walk one segment, handing each intact record to `sink` in order. Returns the byte
/// offset of the first torn record -- 0 for a segment whose header never became durable --
/// or `None` if the segment ends cleanly. Every read is bounded, so a truncated file yields
/// an offset, not a panic.
///
/// Errors: a header of another format version is `Unsupported`; a bad header in front of
/// records, or a record whose CRC is valid but which does not decode, is `InvalidData`.
fn scan_segment(
    path: &Path,
    mut sink: impl FnMut(Record) -> io::Result<()>,
) -> io::Result<Option<u64>> {
    let len = std::fs::metadata(path)?.len();
    let mut f = BufReader::new(File::open(path)?);
    if len < HEADER_LEN {
        return Ok(Some(0)); // a creation cut short before its header was durable
    }
    let mut header = [0u8; HEADER_LEN as usize];
    f.read_exact(&mut header)?;
    let stored_crc = u32::from_le_bytes(header[8..12].try_into().expect("4 bytes"));
    if header[..4] != SEGMENT_MAGIC || crc32(&header[..8]) != stored_crc {
        if len == HEADER_LEN {
            return Ok(Some(0)); // header-sized but never a header: the same interrupted birth
        }
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{} is not an intact keystory WAL segment: bad header in front of {} bytes",
                path.display(),
                len - HEADER_LEN
            ),
        ));
    }
    let version = u32::from_le_bytes(header[4..8].try_into().expect("4 bytes"));
    if version != SEGMENT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "WAL segment {} has format version {version}; this build reads version \
                 {SEGMENT_VERSION}",
                path.display()
            ),
        ));
    }
    let mut pos = HEADER_LEN;
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
            Ok(rec) => sink(rec)?,
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "WAL segment {} holds a record at byte {pos} whose checksum is valid \
                         but which does not decode ({e}); no torn write produces one, so \
                         recovery refuses it rather than truncating the log there",
                        path.display()
                    ),
                ));
            }
        }
        pos += total;
    }
}

/// Truncate a torn trailing record off `path`, if there is one. A segment whose header
/// never became durable is cut to zero bytes, for [`Wal::open`] to write it afresh.
fn repair_tail(path: &Path) -> io::Result<()> {
    if let Some(off) = scan_segment(path, |_| Ok(()))? {
        let f = OpenOptions::new().write(true).open(path)?;
        f.set_len(off)?;
        f.sync_all()?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

/// The bytes every segment starts with.
fn segment_header() -> [u8; HEADER_LEN as usize] {
    header_for(SEGMENT_VERSION)
}

/// An intact segment header claiming `version`.
pub(crate) fn header_for(version: u32) -> [u8; HEADER_LEN as usize] {
    let mut header = [0u8; HEADER_LEN as usize];
    header[..4].copy_from_slice(&SEGMENT_MAGIC);
    header[4..8].copy_from_slice(&version.to_le_bytes());
    let crc = crc32(&header[..8]);
    header[8..].copy_from_slice(&crc.to_le_bytes());
    header
}

fn op_tag(op: &Op) -> u8 {
    match op {
        Op::Put { .. } => TAG_PUT,
        Op::Delete { .. } => TAG_DELETE,
    }
}

/// The exact encoded payload size of a record carrying `ops` (see the layout above).
fn payload_len(ops: &[Op]) -> u64 {
    let body = |op: &Op| -> u64 {
        let (k, v) = match op {
            Op::Put { key, value } => (key.len(), value.len()),
            Op::Delete { key } => (key.len(), 0),
        };
        4 + k as u64 + 4 + v as u64
    };
    match ops {
        [op] => 1 + 8 + 8 + body(op),
        ops => 1 + 8 + 8 + 4 + ops.iter().map(|op| 1 + body(op)).sum::<u64>(),
    }
}

/// Check that one commit's ops fit a WAL record: at least one op, and a payload the
/// `u32` length field can frame (which bounds every key, value and op count too).
/// Anything else would be written as a record that replay cannot decode -- silently
/// truncating the log at that point -- so it is refused with `InvalidInput` instead.
pub(crate) fn check_ops(ops: &[Op]) -> io::Result<()> {
    if ops.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "a WAL record needs at least one op",
        ));
    }
    let len = payload_len(ops);
    if len > MAX_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "a commit of {len} encoded bytes exceeds the WAL record limit of {MAX_PAYLOAD}"
            ),
        ));
    }
    Ok(())
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

/// The sequence number embedded in a segment file name, if it is one. Only the exact
/// shape [`segment_path`] writes (ten ASCII digits) counts, so the lexical order the
/// callers sort by is always the numeric append order.
fn segment_seq(path: &Path) -> Option<u32> {
    let name = path.file_name()?.to_str()?;
    let digits = name.strip_prefix("wal-")?.strip_suffix(".log")?;
    if digits.len() != 10 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse::<u32>().ok()
}

/// Every segment file in `dir`, in no particular order (callers sort).
pub(crate) fn list_segments(dir: &Path) -> io::Result<Vec<PathBuf>> {
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

#[cfg(test)]
thread_local! {
    /// Every directory [`fsync_dir`] synced on this thread: how tests observe directory
    /// durability, which no crash short of a power cut would otherwise reveal.
    pub(crate) static SYNCED_DIRS: std::cell::RefCell<Vec<PathBuf>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// `fsync` a directory so a just-created, renamed or deleted entry is durable: POSIX
/// makes no promise about the entry until the directory itself is synced. A no-op on
/// platforms where a directory cannot be opened as a file.
pub(crate) fn fsync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(test)]
    SYNCED_DIRS.with(|synced| synced.borrow_mut().push(dir.to_path_buf()));
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

/// [`create_dir_all`], then `fsync` the parent of every directory it created, outermost
/// first, so the new entries survive a crash: a directory's existence is recorded in its
/// parent, and POSIX promises nothing about that record until the parent is synced.
pub(crate) fn create_dir_all_durably(dir: &Path) -> io::Result<()> {
    let mut missing = Vec::new();
    let mut cur = Some(dir);
    while let Some(d) = cur {
        if d.as_os_str().is_empty() || d.exists() {
            break;
        }
        missing.push(d);
        cur = d.parent();
    }
    create_dir_all(dir)?;
    for d in missing.iter().rev() {
        match d.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => fsync_dir(parent)?,
            _ => fsync_dir(Path::new("."))?,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Op;

    /// Every record in the log plus where a torn tail begins, if anywhere.
    fn replay_all(dir: &Path) -> io::Result<(Vec<Record>, Option<u64>)> {
        let mut out = Vec::new();
        let torn = replay(dir, |_, r| {
            out.push(r);
            Ok(())
        })?;
        Ok((out, torn))
    }

    fn recs_clean(dir: &Path) -> Vec<Record> {
        let (r, tail) = replay_all(dir).expect("replay");
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

    /// A framed record whose CRC is valid, for payloads the encoder would never produce.
    fn framed(payload: &[u8]) -> Vec<u8> {
        let mut rec = (payload.len() as u32).to_le_bytes().to_vec();
        rec.extend_from_slice(payload);
        rec.extend_from_slice(&crc32(payload).to_le_bytes());
        rec
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
        let (got, torn) = replay_all(&d).unwrap();
        assert_eq!(got.len(), 1, "only the durable record survives");
        assert_eq!(
            torn,
            Some(clean_len),
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
        bytes[HEADER_LEN as usize + 10] ^= 0xFF; // flip a byte inside the first record's payload
        std::fs::write(first, &bytes).unwrap();
        let err = replay_all(&d).expect_err("corruption followed by later segments is an error");
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

    /// Regression (2026-09-26 audit): a failed append used to leave its partial bytes in
    /// the segment, so later acknowledged records landed after garbage and were cut off
    /// as a "torn tail" at the next open (and a retried index was logged twice). The
    /// rollback restores a clean boundary -- also on a rotated segment, whose handle is
    /// not in append mode and must be re-positioned.
    #[test]
    fn failed_append_is_rolled_back_so_a_retried_index_is_logged_once() {
        let d = dir("wal-rollback");
        let mut w = Wal::open(&d, 1 << 20).unwrap();
        w.append(&put(1, b"a", b"1")).unwrap();
        for (next, rotate) in [(2u64, false), (3, true)] {
            if rotate {
                w.rotate_segment().unwrap();
            }
            let clean = w.seg_bytes;
            // What a short write leaves behind: part of a record, never fsync'd.
            w.file.write_all(&[0x40, 0, 0, 0, 1, 2, 3]).unwrap();
            w.roll_back(&io::Error::other("simulated short write"));
            assert!(
                w.poisoned.is_none(),
                "a successful rollback keeps the log usable"
            );
            let seg = segment_path(&d, w.current_segment());
            assert_eq!(std::fs::metadata(&seg).unwrap().len(), clean, "cut back");
            w.append(&put(next, b"k", b"retried")).unwrap();
        }
        let got = recs_clean(&d);
        assert_eq!(
            got.iter().map(|r| r.index).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "every index is logged exactly once, after intact records only"
        );
        std::fs::remove_dir_all(&d).ok();
    }

    /// When even the rollback fails, the log is poisoned: appends and rotations refuse
    /// until a reopen, whose repair leaves a clean, appendable log.
    #[test]
    fn a_failed_rollback_poisons_the_log_until_reopen() {
        let d = dir("wal-poison");
        let mut w = Wal::open(&d, 1 << 20).unwrap();
        w.append(&put(1, b"a", b"1")).unwrap();
        w.break_for_test();
        assert!(w.append(&put(2, b"b", b"2")).is_err(), "the write fails");
        let err = w
            .append(&put(2, b"b", b"2"))
            .expect_err("a poisoned log refuses appends");
        assert!(err.to_string().contains("reopen"), "{err}");
        assert!(w.rotate_segment().is_err(), "and rotations");
        drop(w);
        let mut w = Wal::open(&d, 1 << 20).unwrap();
        w.append(&put(2, b"b", b"2")).unwrap();
        assert_eq!(recs_clean(&d), vec![put(1, b"a", b"1"), put(2, b"b", b"2")]);
        std::fs::remove_dir_all(&d).ok();
    }

    /// A rotation that fails before creating anything leaves the log exactly as it was:
    /// appends continue in the current segment, which is still the one reported, and the
    /// next rotation takes the number.
    #[test]
    fn a_rotation_that_creates_nothing_keeps_appending_to_the_current_segment() {
        let d = dir("wal-rot-fail");
        let mut w = Wal::open(&d, 1 << 20).unwrap();
        w.append(&put(1, b"a", b"1")).unwrap();
        w.inject(&[RotationFault::Create]);
        assert!(w.rotate_segment().is_err());
        assert_eq!(
            w.current_segment(),
            1,
            "no switch to a segment that does not exist"
        );
        assert!(
            w.poisoned.is_none(),
            "nothing was created, so nothing to clean up"
        );
        assert!(!segment_path(&d, 2).exists());
        w.append(&put(2, b"b", b"2")).unwrap();
        w.inject(&[]);
        assert_eq!(recs_clean(&d), vec![put(1, b"a", b"1"), put(2, b"b", b"2")]);
        assert_eq!(
            w.rotate_segment().unwrap(),
            2,
            "the next rotation takes the number"
        );
        std::fs::remove_dir_all(&d).ok();
    }

    /// Regression (pre-release review, second pass): a rotation that found the next
    /// segment's name already taken failed, and appends went on in the current segment
    /// with the newer one beside it -- so a crash tearing the current segment's tail left
    /// a store that refused to open, every acknowledged write intact. A taken name now
    /// poisons the log; a reopen resumes the newer segment, and nothing is lost.
    #[test]
    fn a_rotation_that_finds_its_segment_name_taken_poisons_the_log() {
        let d = dir("wal-rot-taken");
        let mut w = Wal::open(&d, 1 << 20).unwrap();
        w.append(&put(1, b"a", b"1")).unwrap();
        std::fs::write(segment_path(&d, 2), b"").unwrap(); // a stray, empty file
        let err = w.rotate_segment().expect_err("the name is taken");
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        let err = w
            .append(&put(2, b"b", b"2"))
            .expect_err("no append beside a newer segment");
        assert!(err.to_string().contains("already exists"), "{err}");
        drop(w);
        let mut w = Wal::open(&d, 1 << 20).unwrap();
        assert_eq!(w.current_segment(), 2, "the newer segment is resumed");
        w.append(&put(2, b"b", b"2")).unwrap();
        assert_eq!(recs_clean(&d), vec![put(1, b"a", b"1"), put(2, b"b", b"2")]);
        std::fs::remove_dir_all(&d).ok();
    }

    /// A rotation whose new segment cannot be given its header removes the half-created
    /// file (and syncs the removal), so appends continue in the current segment with no
    /// newer one beside it.
    #[test]
    fn a_rotation_whose_header_fails_removes_the_half_created_segment() {
        let d = dir("wal-rot-header");
        let mut w = Wal::open(&d, 1 << 20).unwrap();
        w.append(&put(1, b"a", b"1")).unwrap();
        w.inject(&[RotationFault::Header]);
        assert!(w.rotate_segment().is_err());
        assert!(
            !segment_path(&d, 2).exists(),
            "the half-created segment is gone"
        );
        assert!(w.poisoned.is_none());
        assert_eq!(w.current_segment(), 1);
        w.append(&put(2, b"b", b"2")).unwrap();
        w.inject(&[]);
        assert_eq!(w.rotate_segment().unwrap(), 2);
        w.append(&put(3, b"c", b"3")).unwrap();
        assert_eq!(
            recs_clean(&d).iter().map(|r| r.index).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        std::fs::remove_dir_all(&d).ok();
    }

    /// If the half-created segment cannot be removed either, or its directory entry
    /// cannot be synced, a newer segment may sit beside the current one: the log is
    /// poisoned, and a reopen recovers every acknowledged record.
    #[test]
    fn a_rotation_that_cannot_undo_or_finish_its_segment_poisons_the_log() {
        for (case, faults) in [
            (
                "cleanup",
                &[RotationFault::Header, RotationFault::Cleanup][..],
            ),
            ("dir-sync", &[RotationFault::DirSync][..]),
        ] {
            let d = dir(&format!("wal-rot-poison-{case}"));
            let mut w = Wal::open(&d, 1 << 20).unwrap();
            w.append(&put(1, b"a", b"1")).unwrap();
            w.inject(faults);
            assert!(w.rotate_segment().is_err(), "{case}");
            assert!(
                segment_path(&d, 2).exists(),
                "{case}: the newer segment remains"
            );
            assert!(w.poisoned.is_some(), "{case}");
            assert!(w.append(&put(2, b"b", b"2")).is_err(), "{case}: poisoned");
            drop(w);
            let mut w = Wal::open(&d, 1 << 20).unwrap();
            w.append(&put(2, b"b", b"2")).unwrap();
            assert_eq!(
                recs_clean(&d),
                vec![put(1, b"a", b"1"), put(2, b"b", b"2")],
                "{case}"
            );
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// Regression (2026-09-26 audit): a record the `u32` framing cannot hold used to be
    /// written with a truncated length (or, with no ops, as an undecodable batch) and
    /// then read back as a torn tail. Now it is refused before anything is written, and
    /// the size check agrees byte-for-byte with the encoder.
    #[test]
    fn records_the_framing_cannot_hold_are_refused_before_writing() {
        let d = dir("wal-framing");
        let mut w = Wal::open(&d, 1 << 20).unwrap();
        w.append(&put(1, b"a", b"1")).unwrap();
        let before = w.seg_bytes;
        let empty = Record {
            term: 1,
            index: 2,
            ops: vec![],
        };
        let err = w.append(&empty).expect_err("an empty record is refused");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(w.seg_bytes, before, "nothing was written");
        assert_eq!(recs_clean(&d), vec![put(1, b"a", b"1")]);

        let batch = Record {
            term: 1,
            index: 2,
            ops: vec![
                Op::Put {
                    key: b"k".to_vec(),
                    value: vec![7; 300],
                },
                Op::Delete {
                    key: b"gone".to_vec(),
                },
            ],
        };
        for rec in [
            put(9, b"key", b"value"),
            Record::single(1, 9, Op::Delete { key: b"x".to_vec() }),
            batch,
        ] {
            assert_eq!(payload_len(&rec.ops), encode_payload(&rec).len() as u64);
        }
        std::fs::remove_dir_all(&d).ok();
    }

    /// Only names `segment_path` writes count as segments: a stray `wal-1.log` or
    /// `wal-+000000001.log` would otherwise sort out of append order.
    #[test]
    fn stray_files_are_not_mistaken_for_segments() {
        let d = dir("wal-stray");
        let mut w = Wal::open(&d, 1 << 20).unwrap();
        w.append(&put(1, b"a", b"1")).unwrap();
        for name in [
            "wal-1.log",
            "wal-+000000001.log",
            "wal-00000000001.log",
            "wal-x.log",
        ] {
            std::fs::write(d.join(name), b"junk").unwrap();
        }
        assert_eq!(list_segments(&d).unwrap().len(), 1);
        assert_eq!(recs_clean(&d), vec![put(1, b"a", b"1")]);
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

    /// Every segment -- the first one `open` creates and every one a rotation creates --
    /// begins with the magic, the format version and their CRC, and nothing else is
    /// written before its first record.
    #[test]
    fn every_segment_starts_with_a_versioned_header() {
        let d = dir("wal-header");
        let mut w = Wal::open(&d, 1 << 20).unwrap();
        let first = only_segment(&d);
        assert_eq!(std::fs::read(&first).unwrap(), segment_header());
        let header = segment_header();
        assert_eq!(&header[..4], b"KSWL");
        assert_eq!(header[4..8], 1u32.to_le_bytes());
        assert_eq!(header[8..], crc32(&header[..8]).to_le_bytes());
        w.append(&put(1, b"a", b"1")).unwrap();
        w.rotate_segment().unwrap();
        w.append(&put(2, b"b", b"2")).unwrap();
        for seg in list_segments(&d).unwrap() {
            let bytes = std::fs::read(&seg).unwrap();
            assert_eq!(bytes[..12], header, "{}", seg.display());
        }
        assert_eq!(recs_clean(&d), vec![put(1, b"a", b"1"), put(2, b"b", b"2")]);
        std::fs::remove_dir_all(&d).ok();
    }

    /// A newest segment whose header never became durable -- empty, cut short, or
    /// header-sized but not a valid header -- is the trace of a creation a crash
    /// interrupted: it holds no record, replay treats it as a torn tail at offset 0, and
    /// `open` rewrites the header and keeps appending there.
    #[test]
    fn an_interrupted_segment_creation_is_repaired_at_open() {
        let mut bad_crc = segment_header();
        bad_crc[11] ^= 0x01;
        for (case, torn) in [
            ("empty", &b""[..]),
            ("short", &b"KSWL\x01"[..]),
            ("zeros", &[0u8; 12][..]),
            ("bad-crc", &bad_crc[..]),
        ] {
            let d = dir(&format!("wal-born-{case}"));
            {
                let mut w = Wal::open(&d, 1 << 20).unwrap();
                w.append(&put(1, b"a", b"1")).unwrap();
            }
            let newest = segment_path(&d, 2);
            std::fs::write(&newest, torn).unwrap();
            let (got, tail) = replay_all(&d).unwrap();
            assert_eq!((got.len(), tail), (1, Some(0)), "{case}");
            let mut w = Wal::open(&d, 1 << 20).unwrap();
            assert_eq!(
                w.current_segment(),
                2,
                "{case}: the newest segment is resumed"
            );
            assert_eq!(std::fs::read(&newest).unwrap(), segment_header(), "{case}");
            w.append(&put(2, b"b", b"2")).unwrap();
            assert_eq!(
                recs_clean(&d),
                vec![put(1, b"a", b"1"), put(2, b"b", b"2")],
                "{case}"
            );
            std::fs::remove_dir_all(&d).ok();
        }
    }

    /// A segment written in another format version -- an intact header naming a version
    /// this build does not know -- is refused with `Unsupported`, by replay and by `open`,
    /// and left exactly as it was: a store written by a newer keystory is never misread,
    /// nor "repaired" into this version's shape. A version field damaged in place fails
    /// the header CRC instead, and is corruption, not a newer format.
    #[test]
    fn a_segment_of_another_format_version_is_refused() {
        let d = dir("wal-version");
        {
            let mut w = Wal::open(&d, 1 << 20).unwrap();
            w.append(&put(1, b"a", b"1")).unwrap();
        }
        let seg = only_segment(&d);
        let original = std::fs::read(&seg).unwrap();
        let mut bytes = original.clone();
        bytes[..12].copy_from_slice(&header_for(2));
        std::fs::write(&seg, &bytes).unwrap();
        let err = replay_all(&d).expect_err("an unknown version");
        assert_eq!(err.kind(), io::ErrorKind::Unsupported, "{err}");
        let err = Wal::open(&d, 1 << 20).err().expect("open refuses it too");
        assert_eq!(err.kind(), io::ErrorKind::Unsupported, "{err}");
        assert_eq!(
            std::fs::read(&seg).unwrap(),
            bytes,
            "the segment is untouched"
        );

        let mut flipped = original;
        flipped[4] ^= 0x02; // version 1 -> 3, in place: the header CRC no longer matches
        std::fs::write(&seg, &flipped).unwrap();
        let err = replay_all(&d).expect_err("a damaged version field");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
        std::fs::remove_dir_all(&d).ok();
    }

    /// A damaged header in front of records is corruption, not an interrupted creation
    /// (a segment receives records only once its header is durable), even in the newest
    /// segment: refused, and left as it was.
    #[test]
    fn a_bad_header_in_front_of_records_is_corruption() {
        let d = dir("wal-bad-header");
        {
            let mut w = Wal::open(&d, 1 << 20).unwrap();
            w.append(&put(1, b"a", b"1")).unwrap();
        }
        let seg = only_segment(&d);
        let mut bytes = std::fs::read(&seg).unwrap();
        bytes[0] ^= 0xFF;
        std::fs::write(&seg, &bytes).unwrap();
        let err = replay_all(&d).expect_err("a bad header");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
        assert!(Wal::open(&d, 1 << 20).is_err());
        assert_eq!(
            std::fs::read(&seg).unwrap(),
            bytes,
            "the segment is untouched"
        );
        std::fs::remove_dir_all(&d).ok();
    }

    /// Regression (pre-release review): a record whose CRC is valid but whose payload does
    /// not decode (an unknown tag, say) was cut off as a torn tail, together with every
    /// record after it. A torn write cannot produce a valid CRC, so this is corruption:
    /// replay and `open` refuse it, and the log is not truncated.
    #[test]
    fn a_checksummed_record_that_does_not_decode_is_corruption_not_a_torn_tail() {
        let d = dir("wal-undecodable");
        {
            let mut w = Wal::open(&d, 1 << 20).unwrap();
            w.append(&put(1, b"a", b"1")).unwrap();
        }
        let seg = only_segment(&d);
        let mut payload = vec![9u8]; // no such tag
        payload.extend_from_slice(&1u64.to_le_bytes()); // term
        payload.extend_from_slice(&2u64.to_le_bytes()); // index
        payload.extend_from_slice(&[0; 8]); // k_len, v_len
        let mut bytes = std::fs::read(&seg).unwrap();
        bytes.extend_from_slice(&framed(&payload));
        bytes.extend_from_slice(&framed(&encode_payload(&put(3, b"c", b"3"))));
        std::fs::write(&seg, &bytes).unwrap();
        let err = replay_all(&d).expect_err("a valid CRC over an undecodable payload");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
        assert!(Wal::open(&d, 1 << 20).is_err());
        assert_eq!(std::fs::read(&seg).unwrap(), bytes, "nothing was truncated");
        std::fs::remove_dir_all(&d).ok();
    }

    /// Replay hands records over one at a time and stops at the first one the sink
    /// refuses, returning the sink's error.
    #[test]
    fn replay_stops_at_the_first_record_the_sink_refuses() {
        let d = dir("wal-sink");
        let mut w = Wal::open(&d, 1 << 20).unwrap();
        for i in 1..=5 {
            w.append(&put(i, b"k", b"v")).unwrap();
        }
        let mut seen = Vec::new();
        let err = replay(&d, |_, r| {
            if r.index == 3 {
                return Err(io::Error::other("refused"));
            }
            seen.push(r.index);
            Ok(())
        })
        .expect_err("the sink's error ends the replay");
        assert_eq!(err.to_string(), "refused");
        assert_eq!(seen, vec![1, 2]);
        std::fs::remove_dir_all(&d).ok();
    }
}
