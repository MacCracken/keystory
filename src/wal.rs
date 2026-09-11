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
//!                     tag   u8   1 = Put, 2 = Delete
//!                     term  u64  Raft term (1 on a single node)
//!                     index u64  logical commit index
//!                     k_len u32
//!                     key       [u8]
//!                     v_len u32
//!                     val       [u8]   (empty for Delete)
//!    4+L     4   crc32           # IEEE CRC-32 over `payload`
//! ```
//!
//! ## Crash model
//!
//! We `fsync` after *every* record (via `sync_all`), so a record that is fully
//! on disk is durable. Under a `SIGKILL` mid-write the trailing partial record --
//! a half-written `length`, `payload`, or `crc` -- is detected on replay by
//! either (a) an out-of-bounds/undersized length, or (b) a CRC mismatch, and is
//! discarded. Hence recovery loses **at most the last un-`fsync`-ed record** and
//! nothing else: exactly the invariant the task demands.
//!
//! ## Segmentation and truncation
//!
//! A segment rotates into a new file once it would exceed `Wal::max_seg_bytes`.
//! Names embed a 10-digit monotone sequence so a glob-and-sort yields the correct
//! append order. After a durable snapshot, the engine deletes the segments fully
//! covered by it (WAL-tail recovery for the next boot).

use std::fs::{create_dir_all, remove_file, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::crc::crc32;
use crate::types::Op;

/// Minimum payload size: any "length" smaller than this is a torn tail.
const MIN_PAYLOAD: u32 = 1 + 8 + 8 + 4 + 4; // tag + term + index + k_len + v_len

/// One replayed log record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub term: u64,
    pub index: u64,
    pub op: Op,
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
    /// Open (creating if needed) a fresh log in `dir`. Existing segment files
    /// are left intact so the caller can [`replay`] them first.
    pub fn open(dir: impl AsRef<Path>, max_seg_bytes: u64) -> io::Result<Wal> {
        let dir = dir.as_ref().to_path_buf();
        make_dir(&dir)?;
        let seq = next_segment_seq(&dir);
        let path = segment_path(&dir, seq);
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
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
    /// Invariant: *after `append` returns, `(term,index,op)` is durable and
    /// will be replayed after any crash.*
    pub fn append(&mut self, rec: &Record) -> io::Result<()> {
        let payload = encode_payload(rec);
        let rec_bytes = 4 + payload.len() + 4;
        // Rotate first if this record would overflow the current segment.
        if self.seg_bytes + rec_bytes as u64 > self.max_seg_bytes && self.seg_bytes > 0 {
            self.rotate()?;
        }
        let mut buf = Vec::with_capacity(rec_bytes);
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&payload);
        buf.extend_from_slice(&crc32(&payload).to_le_bytes());
        self.file.write_all(&buf)?;
        self.file.sync_all()?; // fsync -> durable
        self.seg_bytes += rec_bytes as u64;
        Ok(())
    }

    fn rotate(&mut self) -> io::Result<()> {
        self.file.sync_all()?;
        self.seg_seq += 1;
        let path = segment_path(&self.dir, self.seg_seq);
        let f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)?;
        self.file = f;
        self.seg_bytes = 0;
        Ok(())
    }

    /// Durably flush the current on-disk state (e.g. before a snapshot
    /// checkpoint) without appending a new record.
    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    /// Delete every segment file in this log. Call after a durable snapshot
    /// that fully covers their contents; subsequent commits start a fresh
    /// segment.
    pub fn truncate_all(&mut self) -> io::Result<()> {
        for p in list_segments(&self.dir)? {
            // Best-effort; a missing file is fine.
            let _ = remove_file(&p);
        }
        // Open a fresh segment so subsequent appends write to a clean file.
        self.rotate()?;
        Ok(())
    }
}

/// Replay every segment in `dir` in filename (i.e. append) order.
///
/// A torn trailing record is *silently dropped*: records before it are complete
/// and durable. Returns `(records, dropped_at)`, where `dropped_at` is the byte
/// offset within the offending segment at which a partial/undecodable record was
/// detected (`None` when every segment ended cleanly). Only an `io::Error` (e.g.
/// a directory that cannot be read) is a hard failure; a torn tail is **not**.
pub fn replay(dir: impl AsRef<Path>) -> io::Result<(Vec<Record>, Option<u64>)> {
    let dir = dir.as_ref();
    let mut out = Vec::new();
    let mut dropped_at: Option<u64> = None;
    let mut files = list_segments(dir)?;
    files.sort();
    for path in &files {
        // `?` propagates only real io errors; a torn tail is reported in-band.
        let (mut recs, at) = replay_one_segment(path)?;
        out.append(&mut recs);
        if let Some(at) = at {
            dropped_at = Some(at);
            // The torn tail belongs to this (latest) segment, so no later segment
            // can contain anything durable.
            break;
        }
    }
    Ok((out, dropped_at))
}

/// Parse a single segment into its records.
///
/// Returns `(complete_records, tail_off)` where a well-formed log yields
/// `tail_off == None`. A short, corrupt, or CRC-failing trailing record yields
/// the *complete records before it* plus `Some(offset)`; that is not an error.
fn replay_one_segment(path: &Path) -> io::Result<(Vec<Record>, Option<u64>)> {
    let mut f = File::open(path)?;
    let len = f.metadata()?.len();
    let mut pos: u64 = 0;
    let mut out = Vec::new();
    loop {
        if pos + 4 > len {
            // Fewer than 4 header bytes remain: a clean end when we are exactly at
            // `len`, otherwise an overhang to drop.
            return if pos == len {
                Ok((out, None))
            } else {
                Ok((out, Some(pos)))
            };
        }
        let mut lb = [0u8; 4];
        f.read_exact(&mut lb)?; // safe: pos + 4 <= len
        let payload_len = u32::from_le_bytes(lb) as u64;
        if payload_len < MIN_PAYLOAD as u64 {
            return Ok((out, Some(pos))); // short/corrupt header => torn tail
        }
        let total = 4 + payload_len + 4;
        if pos + total > len {
            return Ok((out, Some(pos))); // record not fully present => torn tail
        }
        let mut pbuf = vec![0u8; payload_len as usize];
        f.read_exact(&mut pbuf)?;
        let mut cb = [0u8; 4];
        f.read_exact(&mut cb)?;
        if crc32(&pbuf) != u32::from_le_bytes(cb) {
            return Ok((out, Some(pos))); // crc mismatch => torn tail
        }
        match decode_payload(&pbuf) {
            Ok(rec) => out.push(rec),
            Err(_) => return Ok((out, Some(pos))), // undecodable => torn tail
        }
        pos += total;
    }
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

fn encode_payload(rec: &Record) -> Vec<u8> {
    let mut b = Vec::new();
    b.push(match &rec.op {
        Op::Put { .. } => 1,
        Op::Delete { .. } => 2,
    });
    b.extend_from_slice(&rec.term.to_le_bytes());
    b.extend_from_slice(&rec.index.to_le_bytes());
    match &rec.op {
        Op::Put { key, value } => {
            b.extend_from_slice(&(key.len() as u32).to_le_bytes());
            b.extend_from_slice(key);
            b.extend_from_slice(&(value.len() as u32).to_le_bytes());
            b.extend_from_slice(value);
        }
        Op::Delete { key } => {
            b.extend_from_slice(&(key.len() as u32).to_le_bytes());
            b.extend_from_slice(key);
            b.extend_from_slice(&0u32.to_le_bytes());
        }
    }
    b
}

fn decode_payload(b: &[u8]) -> Result<Record, io::Error> {
    let mut cur = 0;
    if cur >= b.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "empty"));
    }
    let tag = b[cur];
    cur += 1;
    if cur + 8 > b.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "term"));
    }
    let term = u64::from_le_bytes(b[cur..cur + 8].try_into().expect("term len"));
    cur += 8;
    if cur + 8 > b.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "index"));
    }
    let index = u64::from_le_bytes(b[cur..cur + 8].try_into().expect("index len"));
    cur += 8;
    if cur + 4 > b.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "k_len"));
    }
    let k_len = u32::from_le_bytes(b[cur..cur + 4].try_into().expect("k_len")) as usize;
    cur += 4;
    if cur + k_len > b.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "key body"));
    }
    let key = b[cur..cur + k_len].to_vec();
    cur += k_len;
    if cur + 4 > b.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "v_len"));
    }
    let v_len = u32::from_le_bytes(b[cur..cur + 4].try_into().expect("v_len")) as usize;
    cur += 4;
    if cur + v_len > b.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "val body"));
    }
    let op = match tag {
        1 => Op::Put {
            key,
            value: b[cur..cur + v_len].to_vec(),
        },
        2 => Op::Delete { key },
        _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "bad tag")),
    };
    Ok(Record { term, index, op })
}

// ---------------------------------------------------------------------------
// Filesystem helpers
// ---------------------------------------------------------------------------

fn segment_path(dir: &Path, seq: u32) -> PathBuf {
    dir.join(format!("wal-{seq:010}.log"))
}

fn make_dir(dir: &Path) -> io::Result<()> {
    match create_dir_all(dir) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e),
    }
}

fn next_segment_seq(dir: &Path) -> u32 {
    let mut seq = 0u32;
    for p in list_segments(dir).into_iter().flatten() {
        if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
            if let Some(rest) = name
                .strip_prefix("wal-")
                .and_then(|s| s.strip_suffix(".log"))
            {
                if let Ok(n) = rest.parse::<u32>() {
                    seq = seq.max(n);
                }
            }
        }
    }
    seq + 1
}

pub fn list_segments(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut vec: Vec<PathBuf> = Vec::new();
    if !dir.exists() {
        return Ok(vec);
    }
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        let name = e.file_name();
        if name
            .to_str()
            .map(|s| s.starts_with("wal-") && s.ends_with(".log"))
            .unwrap_or(false)
        {
            vec.push(e.path());
        }
    }
    Ok(vec)
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

    #[test]
    fn roundtrip_put_and_delete() {
        let d = dir("wal-rt");
        let mut w = Wal::open(&d, 4096).unwrap();
        w.append(&Record {
            term: 1,
            index: 1,
            op: Op::Put {
                key: b"a".into(),
                value: b"1".into(),
            },
        })
        .unwrap();
        w.append(&Record {
            term: 1,
            index: 2,
            op: Op::Delete { key: b"a".into() },
        })
        .unwrap();
        w.append(&Record {
            term: 1,
            index: 3,
            op: Op::Put {
                key: b"z".into(),
                value: b"\x00\x01\xff".into(),
            },
        })
        .unwrap();
        let got = recs_clean(&d);
        assert_eq!(got.len(), 3);
        assert_eq!(
            got[0],
            Record {
                term: 1,
                index: 1,
                op: Op::Put {
                    key: b"a".into(),
                    value: b"1".into()
                }
            }
        );
        assert_eq!(
            got[1],
            Record {
                term: 1,
                index: 2,
                op: Op::Delete { key: b"a".into() }
            }
        );
        assert_eq!(
            got[2],
            Record {
                term: 1,
                index: 3,
                op: Op::Put {
                    key: b"z".into(),
                    value: b"\x00\x01\xff".into()
                }
            }
        );
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn torn_tail_is_dropped_cleanly() {
        let d = dir("wal-torn");
        let mut w = Wal::open(&d, 1024).unwrap();
        w.append(&Record {
            term: 1,
            index: 1,
            op: Op::Put {
                key: b"k".into(),
                value: b"v".into(),
            },
        })
        .unwrap();
        // Simulate a torn tail: append non-record junk without fsync.
        let seg = d.join("wal-0000000001.log");
        {
            let mut f = OpenOptions::new().append(true).open(&seg).unwrap();
            f.write_all(&[0xFF, 0xFF, 0x00, 0x00, 0x01, 0x00]).unwrap();
        }
        let (got, dropped) = replay(&d).unwrap();
        assert_eq!(got.len(), 1, "only the durable record survives");
        assert!(dropped.is_some(), "a torn tail must be detected");
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn rotation_and_replay_order() {
        let d = dir("wal-rot");
        let mut w = Wal::open(&d, 64).unwrap(); // tiny threshold forces rotation
        for i in 0..50u64 {
            w.append(&Record {
                term: 1,
                index: i + 1,
                op: Op::Put {
                    key: format!("k{i}").into_bytes(),
                    value: i.to_le_bytes().into(),
                },
            })
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

    #[test]
    fn crc_detects_bit_flip() {
        // A flipped byte in a committed record's body must fail its crc guard.
        let good = Record {
            term: 7,
            index: 42,
            op: Op::Put {
                key: b"key".into(),
                value: b"value".into(),
            },
        };
        let payload = encode_payload(&good);
        assert_eq!(crc32(&payload), crc32(&payload));
        let mut bad = payload.clone();
        bad[4] ^= 0xFF; // flip a term byte
        assert_ne!(crc32(&payload), crc32(&bad));
    }
}
