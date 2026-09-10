//! Off-heap value storage ("large-value handling").
//!
//! A large value dominates the on-disk footprint and the snapshot. The off-heap store spills such
//! values to an append-only **blob log** (`values/blobs`); what a WAL entry or snapshot record then
//! carries is a small [`BlobRef`] handle, **not the bytes**. Reads are *random* (`read_at`):
//! fetching one large value opens and seek-reads only that record -- it never touches the rest of
//! the store -- the std-only stand-in for a zero-copy / mmap read.
//!
//! **Compaction = GC.** [`ValueStore::compact`] rewrites the log keeping only the blobs whose ids
//! are live, dropping superseded ones, atomically (tmp + fsync + rename) so a crash mid-compact
//! leaves the original log intact.
//!
//! *Honest scope:* the in-RAM FSM still materialises values; the win here is the on-disk
//! representation (off-heap, random-access, compactable). Streaming the snapshot straight from the
//! blob log is a later step; async I/O is Phase 4.

use crate::crc::crc32;
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// A blob id.
pub type BlobId = u64;

/// A handle to a value in the blob store: its id, its byte offset, its length. Small and cheap to
/// copy -- this is what the on-disk log/snapshot carry instead of the bytes. `Default` (`0,0,0`)
/// denotes "no value".
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct BlobRef {
     pub id: BlobId,
     pub offset: u64,
     pub len: u64,
}

const REL_PATH: &str = "values/blobs";
const REC_HDR: usize = 16; // id:8 BE + len:8 BE
const REC_TRAIL: usize = 4; // crc32 over (header || payload)

fn u64_from_be(b: &[u8; 8]) -> u64 {
     u64::from_be_bytes(*b)
}

/// An off-heap, append-only, compactable value store rooted at `dir`.
///
/// All methods take `&self` and use a [`Mutex`], so a store can be shared behind an `Arc`.
pub struct ValueStore {
  inner: Mutex<Inner>,
  path: PathBuf,
}

struct Inner {
  file: Option<File>,
  next_id: u64,
  offset: u64, // running end offset
}

impl Inner {
     fn blank() -> Self {
        Self {file: None, next_id: 0, offset: 0}
     }
}

/// Open (or create) a value store. A pre-existing blob log is scanned once at open to rebuild the
/// monotonic id counter and end offset, so recovery is self-contained.
impl ValueStore {
    pub fn open(dir: impl AsRef<Path>) -> io::Result<ValueStore> {
         let path = PathBuf::from(dir.as_ref().as_os_str())
              .join(REL_PATH);
         if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).ok();
            }
         let mut inner = Inner::blank();
         inner.file = File::options()
                .read(true)
                .append(true)
                .create(true)
                .open(&path)
                .ok();
         inner.next_id = 0;
         inner.offset = 0;
         if inner.file.is_some() {
            scan_log(&mut inner).ok();
            }
         Ok(ValueStore {
               inner: Mutex::new(inner),
               path,
              })
    }

     fn ensure_file(g: &mut Inner, path: &Path) -> io::Result<()> {
         if g.file.is_none() {
             let f = File::options()
                 .read(true)
                 .append(true)
                 .create(true)
                 .open(path)?;
             g.file = Some(f);
         }
         Ok(())
     }

       /// Append `value` durably and return its handle. The record is fsync'd *before* the id is
       /// handed out, so a returned handle is durable.
      #[allow(clippy::should_implement_trait)]
     pub fn put(&self, value: &[u8]) -> io::Result<BlobRef> {
        let mut g = self.inner.lock().unwrap();
        Self::ensure_file(&mut g, &self.path)?;
        let id = g.next_id;
        let offset = g.offset;
        let file = g.file.as_mut()
            .ok_or_else(|| io::Error::other("value store: no file"))?;

           // Record: [id:8 BE][len:8 BE][payload][crc32(header||payload):4 BE].
        let hdr = id.to_be_bytes();
        let lenb = (value.len() as u64).to_be_bytes();
        file.write_all(&hdr)?;
        file.write_all(&lenb)?;
        file.write_all(value)?;
        let mut pre = hdr.to_vec();
        pre.extend_from_slice(&lenb);
        pre.extend_from_slice(value);
        file.write_all(&crc32(&pre).to_be_bytes())?;
        file.sync_all()?;
        g.offset += (REC_HDR + REC_TRAIL + value.len()) as u64;
        g.next_id = id + 1;
        Ok(BlobRef {
               id,
               offset,
               len: value.len() as u64,
                })
    }

       /// Read a blob by handle, verifying its CRC. Random access (`read_at`): touches only this
       /// blob's bytes. Returns `None` if the record is absent or torn.
    pub fn get(&self, ref_to: &BlobRef) -> io::Result<Option<Vec<u8>>> {
        let f = match File::open(&self.path) {
            Ok(f) => f,
            Err(_) => return Ok(None),
        };
        let mut f = f;
        f.seek(std::io::SeekFrom::Start(ref_to.offset)).unwrap_or(0);
        // Read the 16-byte header first so the payload size comes from disk, not the handle.
        let mut hdr = [0u8; REC_HDR];
        if f.read_exact(&mut hdr).is_err() {
            return Ok(None); // short/eof read: record not fully on disk
        }
        let id = u64_from_be(&hdr[0..8].try_into().unwrap());
        let len = u64_from_be(&hdr[8..16].try_into().unwrap()) as usize;
        let mut payload = vec![0u8; len];
        if f.read_exact(&mut payload).is_err() {
            return Ok(None);
        }
        let mut crcb = [0u8; REC_TRAIL];
        if f.read_exact(&mut crcb).is_err() {
            return Ok(None);
        }
        let got = u32::from_be_bytes(crcb);
        let mut pre = hdr.to_vec();
        pre.extend_from_slice(&payload);
        if crc32(&pre) != got || id != ref_to.id {
            return Ok(None); // corrupt or wrong handle: treat as absent
        }
        Ok(Some(payload))
    }

       /// Compact the log: rewrite it keeping only the blobs whose ids are in `live`, dropping
       /// superseded blobs. Atomic (tmp + fsync + rename); crash-safe. Returns the new end offset.
      pub fn compact(&self, live: &BTreeSet<BlobId>) -> io::Result<u64> {
        let tmp = self.path.with_extension("blobs.tmp");
        fs::remove_file(&tmp).ok();
        let mut out =
             File::create(&tmp).map_err(|e| io::Error::other(format!("vs tmp: {e}")))?;
        let mut end = 0u64;
        let mut next = 0u64;

        if let Ok(mut reader) = File::open(&self.path) {
            let mut data = Vec::new();
            reader.read_to_end(&mut data).ok();
            let mut pos = 0usize;
               while pos + REC_HDR + REC_TRAIL <= data.len() {
                    let id = u64_from_be(&data[pos..pos + 8].try_into().unwrap());
                    let len =
                         u64_from_be(&data[pos + 8..pos + 16].try_into().unwrap()) as usize;
                    let total = REC_HDR + len + REC_TRAIL;
                     if pos + total > data.len() {
                        break; // torn tail
                            }
                    let got =
                         u32::from_be_bytes(
                              data[pos + total - 4..pos + total].try_into().unwrap()
                             );
                    let pre = data[pos..pos + REC_HDR + len].to_vec();
                     if crc32(&pre) != got {
                        break; // corrupt record: stop (crash-safe)
                            }
                          // Rewrite the live ones with fresh id/offset; drop the dead ones.
                     if live.contains(&id) {
                        let mut b =
                             Vec::with_capacity(REC_HDR + REC_TRAIL + len);
                        b.extend_from_slice(&next.to_be_bytes());
                        b.extend_from_slice(&(len as u64).to_be_bytes());
                        b.extend_from_slice(
                             &data[pos + REC_HDR..pos + REC_HDR + len]
                             );
                        let p2 = b.clone();
                        b.extend_from_slice(&crc32(&p2).to_be_bytes());
                        out.write_all(&b)
                             .map_err(|e| io::Error::other(format!("vs compact write: {e}")))?;
                        end += b.len() as u64;
                        next += 1;
                             }
                    pos += total;
                      }
            }
        out.sync_all()
             .map_err(|e| io::Error::other(format!("vs compact fsync: {e}")))?;
        fs::rename(&tmp, &self.path)
             .map_err(|e| io::Error::other(format!("vs compact rename: {e}")))?;
        {
            let mut g = self.inner.lock().unwrap();
            g.file = None;
            g.next_id = next;
            g.offset = end;
            }
        Ok(end)
    }

       /// The current end-of-log offset (high-water mark).
      pub fn end(&self) -> u64 {
           self.inner.lock().unwrap().offset
        }

       /// The next blob id a `put` would allocate.
      pub fn next_id(&self) -> u64 {
           self.inner.lock().unwrap().next_id
        }

       /// The blob-log path (under the store root), for diagnostics / reopen.
      pub fn path(&self) -> &Path {
           &self.path
        }
}

/// Scan the existing log once, rebuilding `next_id` and `offset` for recovery. A torn tail is
/// skipped. O(number of records) at open.
fn scan_log(inner: &mut Inner) -> io::Result<()> {
     let fd = match inner.file.as_ref().and_then(|f| f.try_clone().ok()) {
        Some(f) => f,
        None => return Ok(()),
            };
     let mut rd = fd;
     let mut data = Vec::new();
     rd.read_to_end(&mut data).ok();
     let mut pos = 0usize;
     let mut next = 0u64;
     let mut end = 0u64;
      while pos + REC_HDR + REC_TRAIL <= data.len() {
            let len = u64_from_be(&data[pos + 8..pos + 16].try_into().unwrap()) as usize;
            let total = REC_HDR + len + REC_TRAIL;
             if pos + total > data.len() {
                break; // torn tail
                }
            let got =
                 u32::from_be_bytes(data[pos + total - 4..pos + total].try_into().unwrap());
            let pre = data[pos..pos + REC_HDR + len].to_vec();
             if crc32(&pre) != got {
                break; // corrupt: stop
                  }
            next += 1;
            end = (pos + total) as u64;
            pos += total;
             }
     inner.next_id = next;
     inner.offset = end;
     Ok(())
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
          static C: std::sync::atomic::AtomicUsize =
               std::sync::atomic::AtomicUsize::new(0);
          let n = C.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
          let p =
               std::env::temp_dir().join(format!("ks-vs-{n}-{}", std::process::id()));
          let _ = std::fs::remove_dir_all(&p);
          std::fs::create_dir_all(&p).unwrap();
           T {
               path: p,
                }
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
        assert_eq!(v.get(&BlobRef { id: 999, offset: 10_000_000, len: 4 }).unwrap(), None);
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
}





