//! # Durable checkpoint (snapshot)
//!
//! A checkpoint materialises the entire live state into a single self-contained
//! file so recovery can start from a fast point instead of replaying the whole WAL.
//! The write protocol is the classic atomic-publish pattern:
//!
//! 1. stream the body to a temporary sibling file `snap.tmp`, folding the CRC as it goes;
//! 2. `fsync` the tmp file (durability of bytes on the storage medium);
//! 3. `rename(tmp, "snap.dat")` -- on the same filesystem this is atomic, so a reader
//!    (including a post-crash recovery) sees the *old* file or the *new* one, never
//!    a half-written one;
//! 4. `fsync` the directory so the rename itself is durable.
//!
//! ## On-disk format (little-endian, no external dependency)
//!
//! ```text
//!        [0.. 4]  magic     = b"KSN1"
//!        [ 4.. 8]  version   u32 = 1
//!        [ 8..16]  index     u64    -- commit index reflected in the snapshot
//!       [16..24]  count     u64    -- number of live entries
//!     then per entry, in key order:
//!        klen  u32 | key bytes
//!        vlen  u32 | value bytes
//!        vers  u64
//!      [end] crc32 u32 -- IEEE CRC-32 over everything written before this field
//! ```
//!
//! The body is a byte-faithful mirror of the committed `BTreeMap`, so recovery
//! (checkpoint + WAL-tail) reproduces state *deterministically*: it never depends on
//! wall-clock time, matching the crate-wide principle that only the logical commit
//! index orders events.
//!
//! ## Known limits (tracked in `ROADMAP.md`)
//! A checkpoint is a whole-map rewrite, O(N) in the live state, and only the latest
//! generation is kept: a corrupt `snap.dat` makes [`load`] fail rather than fall back
//! to an older checkpoint.

use std::collections::BTreeMap;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::crc::{Crc, crc32};
use crate::types::{Entry, Snapshot};

/// Magic prefix + format version. A mismatch is a corrupt/foreign file.
const MAGIC: &[u8] = b"KSN1";
const FORMAT: u32 = 1;

/// The on-disk name of a durable checkpoint within a store directory.
pub const SNAPSHOT_NAME: &str = "snap.dat";
/// The transient name written-then-renamed into place.
pub const SNAPSHOT_TMP: &str = "snap.tmp";

/// Load a checkpoint from `dir` if one exists.
///
/// `Ok(None)` means no snapshot file is present (first-ever start). `Ok(Some(_))`
/// means a well-formed, CRC-valid snapshot. A genuine `io::Error` is returned only
/// for I/O failures; a truncated/corrupt *body* is reported as `InvalidData`. A torn
/// checkpoint must never be applied -- unlike a WAL tail, we cannot tell which of its
/// bytes are complete -- so the caller ([`crate::Store::open`]) fails rather than
/// guessing.
pub fn load(dir: impl AsRef<Path>) -> io::Result<Option<Snapshot>> {
    let path = dir.as_ref().join(SNAPSHOT_NAME);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(&path)?;
    let snap =
        decode(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    Ok(Some(snap))
}

/// Write a checkpoint of `snap` to `dir` durably (tmp + fsync + atomic rename).
///
/// The body is streamed record by record with the CRC folded incrementally, so peak
/// allocation is O(one record), not O(whole snapshot). The whole body is fsynced
/// *before* the rename; the rename is atomic, so a crash anywhere on this path leaves
/// either the previous `snap.dat` or the new one, never a half-written file. A leftover
/// `snap.tmp` from a prior crash is harmless.
pub fn write(dir: impl AsRef<Path>, snap: &Snapshot) -> io::Result<()> {
    let dir = dir.as_ref();
    let tmp = dir.join(SNAPSHOT_TMP);
    let final_path = dir.join(SNAPSHOT_NAME);

    let f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)
        .map_err(|e| io::Error::other(format!("open tmp: {e}")))?;
    let mut out = BufWriter::new(f);
    let mut crc = Crc::new();
    write_body(snap, &mut out, &mut crc)?;
    out.write_all(&crc.finish().to_le_bytes())?;
    out.flush()?;
    // Durability: force bytes to the medium before the rename; otherwise a crash
    // after the rename but before a later sync could surface a half-flushed file.
    out.get_mut()
        .sync_all()
        .map_err(|e| io::Error::other(format!("fsync tmp: {e}")))?;
    drop(out); // close before the rename so later metadata queries observe the flushed size

    std::fs::rename(&tmp, &final_path).map_err(|e| io::Error::other(format!("rename: {e}")))?;
    // Make the rename itself durable: POSIX promises nothing about a directory entry
    // until the directory is synced. (A no-op off Unix.)
    crate::wal::fsync_dir(dir)?;
    Ok(())
}

/// Alias of [`write()`], kept for callers that named the streaming writer explicitly
/// (Phase 3). There is only one writer now, and it streams.
pub fn write_streaming(dir: impl AsRef<Path>, snap: &Snapshot) -> io::Result<()> {
    write(dir, snap)
}

/// Serialise a snapshot to its on-disk body (header + entries, no CRC).
pub fn encode(snap: &Snapshot) -> Vec<u8> {
    let mut b = Vec::new();
    let mut crc = Crc::new();
    write_body(snap, &mut b, &mut crc).expect("writing to a Vec cannot fail");
    b
}

/// The single definition of the body layout: header, then one record per entry in key
/// order. Every byte goes through `sink` and into `crc`, so the buffered ([`encode`]) and
/// streaming ([`write()`]) paths cannot drift apart.
fn write_body<W: Write>(snap: &Snapshot, sink: &mut W, crc: &mut Crc) -> io::Result<()> {
    let mut emit = |bytes: &[u8]| -> io::Result<()> {
        crc.update(bytes);
        sink.write_all(bytes)
    };
    emit(MAGIC)?;
    emit(&FORMAT.to_le_bytes())?;
    emit(&snap.index.to_le_bytes())?;
    emit(&(snap.data.len() as u64).to_le_bytes())?;
    for (k, e) in &snap.data {
        emit(&(k.len() as u32).to_le_bytes())?;
        emit(k)?;
        emit(&(e.value.len() as u32).to_le_bytes())?;
        emit(&e.value)?;
        emit(&e.version.to_le_bytes())?;
    }
    Ok(())
}

/// Parse the on-disk format. Verifies magic, version, count, and CRC; every read is
/// bounded so a truncated/corrupt file yields an error rather than a panic.
pub fn decode(bytes: &[u8]) -> Result<Snapshot, Box<dyn std::error::Error + Send + Sync>> {
    if bytes.len() < 4 + 4 + 8 + 8 + 4 {
        return Err("snapshot too short for header + crc".into());
    }
    if &bytes[..4] != MAGIC {
        return Err("bad magic: not a keystory checkpoint".into());
    }
    let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    if version != FORMAT {
        return Err(format!("unknown snapshot version {version}").into());
    }
    let index = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let count = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    if count > 50_000_000 {
        // Defensive bound: a corrupt/implausibly huge count must never OOM us.
        return Err(format!("implausible entry count {count}").into());
    }

    // Trailing crc covers bytes[..len-4]; the last 4 bytes are its value.
    let stored = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap());
    let body = &bytes[..bytes.len() - 4];
    if crc32(body) != stored {
        return Err("crc mismatch: truncated or corrupt checkpoint".into());
    }

    let mut pos = 24usize;
    let mut data: BTreeMap<Vec<u8>, Entry> = BTreeMap::new();
    for _ in 0..count {
        if pos + 4 > body.len() {
            return Err("corrupt entry: short key length".into());
        }
        let klen = u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + klen > body.len() {
            return Err("corrupt entry: short key body".into());
        }
        let key = body[pos..pos + klen].to_vec();
        pos += klen;
        if pos + 4 > body.len() {
            return Err("corrupt entry: short value length".into());
        }
        let vlen = u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + vlen > body.len() {
            return Err("corrupt entry: short value body".into());
        }
        let value = body[pos..pos + vlen].to_vec();
        pos += vlen;
        if pos + 8 > body.len() {
            return Err("corrupt entry: short version".into());
        }
        let version = u64::from_le_bytes(body[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let inserted = data.insert(key, Entry { value, version });
        debug_assert!(inserted.is_none(), "BTreeMap keys are unique/ordered");
    }
    Ok(Snapshot { index, data })
}

/// Full path to the checkpoint within `dir` (for tests / diagnostics).
pub fn snapshot_path(dir: impl AsRef<Path>) -> PathBuf {
    dir.as_ref().join(SNAPSHOT_NAME)
}

/// Remove any checkpoint and transient file (used by tests).
pub fn purge(dir: impl AsRef<Path>) -> io::Result<()> {
    for n in [SNAPSHOT_NAME, SNAPSHOT_TMP] {
        let p = dir.as_ref().join(n);
        if p.exists() {
            let _ = std::fs::remove_file(&p);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    // A fresh, unique temp dir per call. We avoid std::fs::TempDir (its place in
    // std is unsettled across toolchains) and use `temp_dir()` + a pid/seq suffix
    // instead, matching the pattern the WAL tests use.
    fn tmp() -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let d = std::env::temp_dir().join(format!("ks-snap-{}-{}", std::process::id(), seq));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn fill(snap: &mut Snapshot, n: usize) {
        for i in 0..n {
            let k = format!("k{i:04}").into_bytes();
            snap.data.insert(
                k,
                Entry {
                    value: format!("v{i:04}").into_bytes(),
                    version: i as u64 + 1,
                },
            );
        }
    }

    #[test]
    fn roundtrip_empty() {
        let d = tmp();
        write(&d, &Snapshot::empty()).unwrap();
        let got = load(&d).unwrap().expect("snapshot present");
        assert_eq!(got.index, 0);
        assert!(got.data.is_empty());
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn roundtrip_nonempty() {
        let d = tmp();
        let mut s = Snapshot::empty();
        fill(&mut s, 50);
        s.index = 42;
        write(&d, &s).unwrap();
        let got = load(&d).unwrap().unwrap();
        assert_eq!(got, s, "round-trip must be exact");
        assert_eq!(got.index, 42);
        assert!(got.get(b"k0010").is_some());
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn load_missing_is_none() {
        let d = tmp();
        assert!(load(&d).unwrap().is_none());
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn corrupt_crc_rejected() {
        let d = tmp();
        let mut s = Snapshot::empty();
        fill(&mut s, 10);
        s.index = 9;
        write(&d, &s).unwrap();
        // Flip a byte in the middle of the file to invalidate the trailing CRC.
        let p = snapshot_path(&d);
        let mut bytes = std::fs::read(&p).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        std::fs::write(&p, &bytes).unwrap();
        assert!(load(&d).is_err(), "corrupt checkpoint must be rejected");
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn atomicity_no_tmp_left_behind() {
        let d = tmp();
        let mut s = Snapshot::empty();
        fill(&mut s, 7);
        write(&d, &s).unwrap();
        // After a clean write only the final file exists; the tmp was renamed away.
        assert!(snapshot_path(&d).exists());
        assert!(!d.join(SNAPSHOT_TMP).exists());
        std::fs::remove_dir_all(&d).ok();
    }
    /// A large snapshot streamed through the writer decodes back to the identical map,
    /// proving the incremental CRC and per-record layout are consistent at scale.
    #[test]
    fn streaming_roundtrip_large() {
        let d = tmp();
        let mut s = Snapshot::empty();
        fill(&mut s, 50_000);
        s.index = 50_000;
        write_streaming(&d, &s).expect("stream write");
        let got = load(&d).expect("load");
        assert!(got.is_some(), "a streamed snapshot must be present");
        let got = got.unwrap();
        assert_eq!(got.index, 50_000, "index survives a streamed checkpoint");
        assert_eq!(got.data.len(), 50_000, "all entries survive");
        assert_eq!(s.data, got.data, "streamed decode == in-memory state");
    }
}
