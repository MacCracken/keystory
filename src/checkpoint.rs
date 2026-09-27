//! # Durable checkpoint
//!
//! A checkpoint materialises the entire live state into a single self-contained
//! file so recovery can start from a fast point instead of replaying the whole WAL.
//! The write protocol is the classic atomic-publish pattern, with one generation of
//! history kept:
//!
//! 1. stream the body to a temporary sibling file `snap.tmp`, folding the CRC as it goes;
//! 2. `fsync` the tmp file (durability of bytes on the storage medium);
//! 3. `rename("snap.dat", "snap.prev")` -- the previous checkpoint is retained;
//! 4. `rename(tmp, "snap.dat")` -- on the same filesystem this is atomic, so a reader
//!    (including a post-crash recovery) sees the *old* file or the *new* one, never
//!    a half-written one;
//! 5. `fsync` the directory so the renames themselves are durable.
//!
//! [`load`] prefers `snap.dat` and falls back to `snap.prev` when the latest file is
//! missing (a crash between steps 3 and 4) or damaged. The engine keeps every
//! WAL segment newer than `snap.prev` until the *next* checkpoint (see
//! `Store::checkpoint`), so a fall-back plus WAL replay still reconstructs the full
//! state. A damaged `snap.dat` with no usable `snap.prev` is an error, never an
//! empty store. After a fall-back, the next checkpoint skips step 3
//! ([`Replaced::Discard`]): demoting the damaged file would overwrite the only good
//! generation, and a crash before step 4 would then leave no usable checkpoint at all.
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
//! Every version of the format begins with the magic and the version and ends with a
//! CRC-32 of everything before it. A reader checks the CRC *before* the version, so a
//! damaged version field is damage (`InvalidData`, and a fall-back to `snap.prev`), while
//! an intact file of another version is refused with `Unsupported`: it was written by a
//! different keystory, not damaged, so it is not grounds for a fall-back either.
//!
//! The body is a byte-faithful mirror of the committed `BTreeMap`, so recovery
//! (checkpoint + WAL-tail) reproduces state *deterministically*: it never depends on
//! wall-clock time, matching the crate-wide principle that only the logical commit
//! index orders events.
//!
//! ## Known limits (tracked in `ROADMAP.md`, under 0.2.0)
//! A checkpoint is a whole-map rewrite, O(N) in the live state, and [`load`] reads the
//! whole file into memory before decoding it.

use std::collections::BTreeMap;
use std::io::{self, BufWriter, Write};
use std::path::Path;

use crate::crc::{Crc, crc32};
use crate::types::{Entry, Shared, Snapshot};

/// Magic prefix + format version. A mismatch is a corrupt/foreign file.
const MAGIC: &[u8] = b"KSN1";

/// The checkpoint format this build writes, and the only one it reads.
pub(crate) const FORMAT: u32 = 1;

/// The on-disk name of the latest durable checkpoint within a store directory.
pub(crate) const SNAPSHOT_NAME: &str = "snap.dat";
/// The previous checkpoint, kept as a fall-back until the next one replaces it.
pub(crate) const SNAPSHOT_PREV: &str = "snap.prev";
/// The transient name written-then-renamed into place.
pub(crate) const SNAPSHOT_TMP: &str = "snap.tmp";

/// What [`load`] recovered.
#[derive(Debug)]
pub(crate) struct Loaded {
    /// The most recent usable checkpoint; `None` when no checkpoint exists at all.
    pub(crate) snapshot: Option<Snapshot>,
    /// `snap.dat` exists but is damaged, so `snapshot` came from `snap.prev`, and the next
    /// [`write`] must not demote the damaged file over it.
    pub(crate) latest_damaged: bool,
}

/// What [`write`] does with the checkpoint it replaces.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Replaced {
    /// Keep it as `snap.prev`: the normal case.
    Retain,
    /// Overwrite it and leave `snap.prev` as it is: the current `snap.dat` is damaged, and
    /// `snap.prev` is the good generation recovery fell back to.
    Discard,
}

/// Load the most recent usable checkpoint from `dir`, if any.
///
/// `snapshot` is `None` when no checkpoint exists at all (first-ever start); otherwise it
/// is a well-formed, CRC-valid snapshot: `snap.dat` when it is intact, else `snap.prev`. A
/// torn checkpoint is never applied -- unlike a WAL tail, we cannot tell which of its
/// bytes are complete -- so a damaged `snap.dat` with no usable `snap.prev` is an
/// `InvalidData` error, and the caller ([`crate::Store::open`]) fails rather than
/// silently starting empty. A checkpoint in another format version is `Unsupported`, with
/// no fall-back. Genuine I/O failures are returned as they are.
pub(crate) fn load(dir: &Path) -> io::Result<Loaded> {
    let previous = || load_file(&dir.join(SNAPSHOT_PREV));
    match load_file(&dir.join(SNAPSHOT_NAME)) {
        Ok(Some(s)) => Ok(Loaded {
            snapshot: Some(s),
            latest_damaged: false,
        }),
        // No latest file: either a fresh store (no previous either) or a crash between
        // the two renames, which leaves only the previous generation.
        Ok(None) => Ok(Loaded {
            snapshot: previous()?,
            latest_damaged: false,
        }),
        Err(e) if e.kind() == io::ErrorKind::InvalidData => match previous()? {
            Some(prev) => Ok(Loaded {
                snapshot: Some(prev),
                latest_damaged: true,
            }),
            None => Err(e),
        },
        Err(e) => Err(e),
    }
}

/// Load and verify one checkpoint file; `Ok(None)` if it does not exist.
fn load_file(path: &Path) -> io::Result<Option<Snapshot>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path)?;
    let snap =
        decode(&bytes).map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", path.display())))?;
    Ok(Some(snap))
}

/// Write a checkpoint of `snap` to `dir` durably (tmp + fsync + atomic rename), keeping
/// the checkpoint it replaces as `snap.prev` unless told to discard it.
///
/// The body is streamed record by record with the CRC folded incrementally, so peak
/// allocation is O(one record), not O(whole snapshot). The whole body is fsynced
/// *before* the renames; each rename is atomic, so a crash anywhere on this path leaves
/// a complete previous or new checkpoint, never a half-written file. A leftover
/// `snap.tmp` from a prior crash is harmless.
pub(crate) fn write(dir: &Path, snap: &Snapshot, replaced: Replaced) -> io::Result<()> {
    let tmp = dir.join(SNAPSHOT_TMP);
    let latest = dir.join(SNAPSHOT_NAME);
    let prev = dir.join(SNAPSHOT_PREV);

    let f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)
        .map_err(|e| io::Error::new(e.kind(), format!("open {}: {e}", tmp.display())))?;
    let mut out = BufWriter::new(f);
    let mut crc = Crc::new();
    write_body(snap, &mut out, &mut crc)?;
    out.write_all(&crc.finish().to_le_bytes())?;
    out.flush()?;
    // Durability: force bytes to the medium before the rename; otherwise a crash
    // after the rename but before a later sync could surface a half-flushed file.
    out.get_mut()
        .sync_all()
        .map_err(|e| io::Error::new(e.kind(), format!("fsync {}: {e}", tmp.display())))?;
    drop(out); // close before the rename so later metadata queries observe the flushed size

    // Retain the previous generation (unless it is the damaged one), then publish.
    if replaced == Replaced::Retain && latest.exists() {
        std::fs::rename(&latest, &prev).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("rename {} to prev: {e}", latest.display()),
            )
        })?;
    }
    std::fs::rename(&tmp, &latest)
        .map_err(|e| io::Error::new(e.kind(), format!("rename {}: {e}", tmp.display())))?;
    // Make the renames themselves durable: POSIX promises nothing about a directory entry
    // until the directory is synced. (A no-op off Unix.)
    crate::wal::fsync_dir(dir)?;
    Ok(())
}

/// Serialise a snapshot to its on-disk body (header + entries, no CRC).
///
/// # Panics
///
/// If a key or value is longer than `u32::MAX` bytes, which the format cannot frame.
#[cfg(test)]
fn encode(snap: &Snapshot) -> Vec<u8> {
    let mut b = Vec::new();
    let mut crc = Crc::new();
    write_body(snap, &mut b, &mut crc).expect("keys and values fit the u32 framing");
    b
}

/// The single definition of the body layout: header, then one record per entry in key
/// order. Every byte goes through `sink` and into `crc`. A key or value too long for its
/// `u32` length field is refused with `InvalidInput` rather than written with a
/// truncated length, which would decode (CRC and all) as a different map. A [`crate::Store`]
/// never holds one: its WAL refuses such a commit.
fn write_body<W: Write>(snap: &Snapshot, sink: &mut W, crc: &mut Crc) -> io::Result<()> {
    let mut emit = |bytes: &[u8]| -> io::Result<()> {
        crc.update(bytes);
        sink.write_all(bytes)
    };
    let len32 = |len: usize| {
        u32::try_from(len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("a {len}-byte key or value exceeds the checkpoint's u32 framing"),
            )
        })
    };
    emit(MAGIC)?;
    emit(&FORMAT.to_le_bytes())?;
    emit(&snap.index.to_le_bytes())?;
    emit(&(snap.data.len() as u64).to_le_bytes())?;
    for (k, e) in &snap.data {
        emit(&len32(k.len())?.to_le_bytes())?;
        emit(k)?;
        emit(&len32(e.value.len())?.to_le_bytes())?;
        emit(&e.value)?;
        emit(&e.version.to_le_bytes())?;
    }
    Ok(())
}

/// The fixed header before the first entry: magic, version, index, count.
const HEADER_LEN: usize = 4 + 4 + 8 + 8;
/// The smallest encoded entry: an empty key and value (two `u32` lengths) and a version.
const MIN_ENTRY_LEN: usize = 4 + 4 + 8;

/// Parse the on-disk format. Verifies magic, CRC, version and count, in that order (see
/// the module docs); every read is bounded so a truncated/corrupt file yields an error
/// rather than a panic. The body must hold exactly `count` entries of distinct keys and
/// nothing after them. Damage is `InvalidData`; an intact file of another format
/// version is `Unsupported`.
fn decode(bytes: &[u8]) -> io::Result<Snapshot> {
    let corrupt = |msg: String| io::Error::new(io::ErrorKind::InvalidData, msg);
    // What every version shares -- magic, version, trailing CRC -- is checked before
    // anything this version's layout requires, so a short file of another version is
    // still recognised as one.
    if bytes.len() < MAGIC.len() + 4 + 4 {
        return Err(corrupt("too short to be a keystory checkpoint".into()));
    }
    if &bytes[..4] != MAGIC {
        return Err(corrupt("bad magic: not a keystory checkpoint".into()));
    }
    // Trailing crc covers bytes[..len-4]; the last 4 bytes are its value.
    let stored = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap());
    let body = &bytes[..bytes.len() - 4];
    if crc32(body) != stored {
        return Err(corrupt(
            "crc mismatch: truncated or corrupt checkpoint".into(),
        ));
    }
    let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    if version != FORMAT {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("checkpoint format version {version}; this build reads version {FORMAT}"),
        ));
    }
    if bytes.len() < HEADER_LEN + 4 {
        return Err(corrupt("snapshot too short for header + crc".into()));
    }
    let index = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let count = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    // The count can be no larger than the body has room for. This is the only bound
    // on it: a fixed cap would make a legitimately large store impossible to reopen.
    if count > ((body.len() - HEADER_LEN) / MIN_ENTRY_LEN) as u64 {
        return Err(corrupt(format!(
            "entry count {count} exceeds what the body can hold"
        )));
    }

    let mut pos = HEADER_LEN;
    let mut data: BTreeMap<Shared, Entry> = BTreeMap::new();
    for _ in 0..count {
        if pos + 4 > body.len() {
            return Err(corrupt("corrupt entry: short key length".into()));
        }
        let klen = u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + klen > body.len() {
            return Err(corrupt("corrupt entry: short key body".into()));
        }
        let key = Shared::from(&body[pos..pos + klen]);
        pos += klen;
        if pos + 4 > body.len() {
            return Err(corrupt("corrupt entry: short value length".into()));
        }
        let vlen = u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + vlen > body.len() {
            return Err(corrupt("corrupt entry: short value body".into()));
        }
        let value = Shared::from(&body[pos..pos + vlen]);
        pos += vlen;
        if pos + 8 > body.len() {
            return Err(corrupt("corrupt entry: short version".into()));
        }
        let version = u64::from_le_bytes(body[pos..pos + 8].try_into().unwrap());
        pos += 8;
        if data.insert(key, Entry { value, version }).is_some() {
            return Err(corrupt("corrupt entry: duplicate key".into()));
        }
    }
    if pos != body.len() {
        return Err(corrupt(format!(
            "{} trailing bytes after the last entry",
            body.len() - pos
        )));
    }
    Ok(Snapshot { index, data })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Op;
    use std::path::PathBuf;
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
            Op::Put {
                key: format!("k{i:04}").into_bytes(),
                value: format!("v{i:04}").into_bytes(),
            }
            .apply(&mut snap.data, i as u64 + 1);
        }
    }

    fn corrupt(path: &Path) {
        let mut bytes = std::fs::read(path).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        std::fs::write(path, &bytes).unwrap();
    }

    fn latest(d: &Path) -> PathBuf {
        d.join(SNAPSHOT_NAME)
    }

    /// The snapshot `load` returns, which must exist.
    fn loaded(d: &Path) -> Snapshot {
        load(d).unwrap().snapshot.expect("a checkpoint is present")
    }

    #[test]
    fn roundtrip_empty() {
        let d = tmp();
        write(&d, &Snapshot::empty(), Replaced::Retain).unwrap();
        let got = loaded(&d);
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
        write(&d, &s, Replaced::Retain).unwrap();
        let got = loaded(&d);
        assert_eq!(got, s, "round-trip must be exact");
        assert_eq!(got.index, 42);
        assert!(got.get("k0010").is_some());
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn load_missing_is_none() {
        let d = tmp();
        let got = load(&d).unwrap();
        assert!(got.snapshot.is_none());
        assert!(!got.latest_damaged);
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn corrupt_crc_rejected() {
        let d = tmp();
        let mut s = Snapshot::empty();
        fill(&mut s, 10);
        s.index = 9;
        write(&d, &s, Replaced::Retain).unwrap();
        // Flip a byte in the middle of the file to invalidate the trailing CRC.
        corrupt(&latest(&d));
        let err = load(&d).expect_err("corrupt checkpoint must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn atomicity_no_tmp_left_behind() {
        let d = tmp();
        let mut s = Snapshot::empty();
        fill(&mut s, 7);
        write(&d, &s, Replaced::Retain).unwrap();
        // After a clean write only the final file exists; the tmp was renamed away.
        assert!(latest(&d).exists());
        assert!(!d.join(SNAPSHOT_TMP).exists());
        std::fs::remove_dir_all(&d).ok();
    }

    /// The previous generation is retained, and is used when the latest is corrupt or
    /// missing; a corrupt latest with no previous is still an error.
    #[test]
    fn previous_generation_is_kept_and_used_as_fallback() {
        let d = tmp();
        let mut first = Snapshot::empty();
        fill(&mut first, 5);
        first.index = 5;
        write(&d, &first, Replaced::Retain).unwrap();
        assert!(!d.join(SNAPSHOT_PREV).exists(), "nothing to retain yet");

        let mut second = first.clone();
        fill(&mut second, 8);
        second.index = 8;
        write(&d, &second, Replaced::Retain).unwrap();
        assert!(
            d.join(SNAPSHOT_PREV).exists(),
            "the first checkpoint was retained"
        );
        assert_eq!(loaded(&d).index, 8, "latest wins when intact");

        corrupt(&latest(&d));
        let got = load(&d).unwrap();
        assert!(got.latest_damaged, "the fall-back is reported");
        assert_eq!(
            got.snapshot.expect("fell back to the previous generation"),
            first
        );

        std::fs::remove_file(latest(&d)).unwrap();
        let got = load(&d).unwrap();
        assert!(!got.latest_damaged, "a missing latest is not a damaged one");
        assert_eq!(
            got.snapshot.unwrap(),
            first,
            "a missing latest (crash between renames) also falls back"
        );

        corrupt(&d.join(SNAPSHOT_PREV));
        assert!(
            load(&d).is_err(),
            "no usable generation left: an error, not an empty store"
        );
        std::fs::remove_dir_all(&d).ok();
    }

    /// Regression (pre-release review): after recovery fell back past a damaged
    /// `snap.dat`, the next checkpoint renamed the damaged file over the good `snap.prev`
    /// before publishing -- so a crash between its two renames (or a failed second rename)
    /// left no usable checkpoint, and the store could not be opened. `Replaced::Discard`
    /// publishes over the damaged file and leaves the good generation where it is.
    #[test]
    fn discarding_a_damaged_latest_keeps_the_good_previous_generation() {
        let d = tmp();
        let mut good = Snapshot::empty();
        fill(&mut good, 3);
        good.index = 3;
        write(&d, &good, Replaced::Retain).unwrap();
        let mut newer = good.clone();
        fill(&mut newer, 6);
        newer.index = 6;
        write(&d, &newer, Replaced::Retain).unwrap();
        corrupt(&latest(&d));
        assert!(load(&d).unwrap().latest_damaged);

        let mut next = newer.clone();
        fill(&mut next, 9);
        next.index = 9;
        write(&d, &next, Replaced::Discard).unwrap();
        assert_eq!(loaded(&d), next, "the new checkpoint is the latest");
        assert_eq!(
            load_file(&d.join(SNAPSHOT_PREV)).unwrap(),
            Some(good),
            "the good generation is still the previous one"
        );
        std::fs::remove_dir_all(&d).ok();
    }

    /// A body with a valid CRC, for crafting checkpoints the writer would never produce.
    fn with_crc(mut body: Vec<u8>) -> Vec<u8> {
        let crc = crc32(&body);
        body.extend_from_slice(&crc.to_le_bytes());
        body
    }

    /// Overwrite the header's entry count.
    fn set_count(body: &mut [u8], count: u64) {
        body[16..24].copy_from_slice(&count.to_le_bytes());
    }

    /// Regression (2026-09-26 audit): `decode` refused any checkpoint of more than 50
    /// million entries, so a store that grew past that could checkpoint but never reopen.
    /// The count is now bounded by what the body can physically hold -- which still stops
    /// a bogus count cold, without capping a legitimate one.
    #[test]
    fn entry_count_is_bounded_by_the_body_not_a_fixed_cap() {
        let mut s = Snapshot::empty();
        fill(&mut s, 1000);
        s.index = 1000;
        assert_eq!(decode(&with_crc(encode(&s))).unwrap(), s);

        // One entry too many: within the size bound, so the entry parser runs out of bytes.
        let mut body = encode(&s);
        set_count(&mut body, 1001);
        let err = decode(&with_crc(body)).expect_err("one entry is missing");
        assert!(err.to_string().contains("corrupt entry"), "{err}");
        // Counts no body this size could hold are refused before any parsing.
        for bogus in [50_000_001, u64::MAX] {
            let mut body = encode(&s);
            set_count(&mut body, bogus);
            let err = decode(&with_crc(body)).expect_err("more entries than bytes");
            assert!(err.to_string().contains("count"), "{bogus}: {err}");
        }
    }

    /// A CRC-valid body must still be exactly `count` distinct entries: trailing bytes
    /// or a repeated key mean the writer was not ours (or was broken), never a map to trust.
    #[test]
    fn trailing_bytes_and_duplicate_keys_are_rejected() {
        let mut s = Snapshot::empty();
        fill(&mut s, 3);
        let mut body = encode(&s);
        body.extend_from_slice(&[0; 20]);
        let err = decode(&with_crc(body)).expect_err("trailing bytes");
        assert!(err.to_string().contains("trailing"), "{err}");

        let mut one = Snapshot::empty();
        fill(&mut one, 1);
        let mut body = encode(&one);
        let entry = body[HEADER_LEN..].to_vec();
        body.extend_from_slice(&entry); // the same key twice
        set_count(&mut body, 2);
        let err = decode(&with_crc(body)).expect_err("duplicate key");
        assert!(err.to_string().contains("duplicate"), "{err}");
    }

    /// An intact checkpoint of another format version is `Unsupported`, and `load` does
    /// not fall back past it to `snap.prev`: the store was written by a different
    /// keystory, and opening an older generation would hide that. A version field damaged
    /// in place fails the CRC first, so it is damage -- and does fall back.
    #[test]
    fn another_format_version_is_refused_without_falling_back() {
        let d = tmp();
        let mut s = Snapshot::empty();
        fill(&mut s, 2);
        write(&d, &s, Replaced::Retain).unwrap();
        write(&d, &s, Replaced::Retain).unwrap(); // a good snap.prev to fall back to

        let mut body = encode(&s);
        body[4..8].copy_from_slice(&2u32.to_le_bytes());
        std::fs::write(latest(&d), with_crc(body)).unwrap();
        let err = load(&d).expect_err("a newer format");
        assert_eq!(err.kind(), io::ErrorKind::Unsupported, "{err}");

        // Also when it is shorter than any version-1 file could be (review, second pass:
        // the length check used to come first, so this fell back past it).
        let mut short = MAGIC.to_vec();
        short.extend_from_slice(&2u32.to_le_bytes());
        std::fs::write(latest(&d), with_crc(short)).unwrap();
        let err = load(&d).expect_err("a short file of a newer format");
        assert_eq!(err.kind(), io::ErrorKind::Unsupported, "{err}");

        let mut bytes = with_crc(encode(&s));
        bytes[4] ^= 0x02; // in place: the trailing CRC no longer matches
        std::fs::write(latest(&d), bytes).unwrap();
        let got = load(&d).unwrap();
        assert!(got.latest_damaged, "damage, so recovery falls back");
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
        write(&d, &s, Replaced::Retain).expect("stream write");
        let got = loaded(&d);
        assert_eq!(got.index, 50_000, "index survives a streamed checkpoint");
        assert_eq!(got.data.len(), 50_000, "all entries survive");
        assert_eq!(s.data, got.data, "streamed decode == in-memory state");
        std::fs::remove_dir_all(&d).ok();
    }
}
