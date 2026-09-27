//! Core, serialisation-friendly value types shared across the crate.
//!
//! Everything here is deterministic and free of wall-clock timestamps: the only
//! ordering concepts are the **term** (Raft election epoch -- always 1 on a
//! single node) and the **commit index** (a logical, monotone sequence number).
//! A logical index, not `SystemTime`, is what gives crash recovery its
//! determinism (see the crate-level docs and `ROADMAP.md`).

use std::collections::{BTreeMap, btree_map};
use std::iter::FusedIterator;
use std::ops::Bound;
use std::sync::Arc;

/// Owned bytes: the key/value representation at the API boundary and inside [`Op`].
pub type Bytes = Vec<u8>;

/// Immutable, reference-counted bytes: the representation inside a [`Snapshot`].
/// Every commit clones the snapshot's map; with shared keys and values that clone bumps
/// reference counts instead of copying every byte, so its cost is proportional to the
/// number of entries, not to the size of the data.
pub type Shared = Arc<[u8]>;

/// The materialised state: live keys to their entries, in byte order.
pub type Map = BTreeMap<Shared, Entry>;

/// A stored entry: a value and the commit index that wrote it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// The value's bytes, shared between every snapshot that holds them.
    pub value: Shared,
    /// The commit index at which this value was written: the per-key logical clock.
    pub version: u64,
}

/// One write: a key set to a value, or a key deleted.
///
/// [`Store::put`](crate::Store::put) and [`Store::delete`](crate::Store::delete) commit one
/// op each; [`Store::apply_batch`](crate::Store::apply_batch) commits several under one
/// commit index, all or nothing. Every op is idempotent.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Op {
    /// Set `key` to `value`, replacing any previous value.
    Put {
        /// The key.
        key: Bytes,
        /// The new value.
        value: Bytes,
    },
    /// Remove `key`. Deleting an absent key changes nothing.
    Delete {
        /// The key.
        key: Bytes,
    },
}

impl Op {
    /// A put of `key -> value`.
    pub fn put(key: impl Into<Bytes>, value: impl Into<Bytes>) -> Op {
        Op::Put {
            key: key.into(),
            value: value.into(),
        }
    }

    /// A delete of `key`.
    pub fn delete(key: impl Into<Bytes>) -> Op {
        Op::Delete { key: key.into() }
    }

    /// Apply this op to `map` at commit `index`, stamping `version = index`.
    ///
    /// Returns whether the visible map state changed:
    ///   * `Put` to a new key, or to a key whose current value differs, changes state.
    ///   * `Put` to a key with the identical value is a no-op (idempotence).
    ///   * `Delete` of a present key changes state; of an absent key does not.
    pub(crate) fn apply(&self, map: &mut Map, index: u64) -> bool {
        match self {
            Op::Put { key, value } => {
                if let Some(e) = map.get_mut(key.as_slice()) {
                    if &*e.value == value.as_slice() {
                        return false; // A same-value put is a true no-op: state and version unchanged.
                    }
                    e.value = Shared::from(value.as_slice());
                    e.version = index;
                } else {
                    map.insert(
                        Shared::from(key.as_slice()),
                        Entry {
                            value: Shared::from(value.as_slice()),
                            version: index,
                        },
                    );
                }
                true
            }
            Op::Delete { key } => map.remove(key.as_slice()).is_some(),
        }
    }
}

/// A consistent, point-in-time, read-only view of a whole store: every key and value as
/// of one commit index, unaffected by any later write.
///
/// [`Store::snapshot`](crate::Store::snapshot) hands one out; every read of a
/// [`Store`](crate::Store) is served from the latest one. Holding a snapshot never blocks
/// a writer: a commit publishes a new snapshot instead of changing this one, and the
/// data this one references stays alive until the last holder drops it. Keys and values
/// are borrowed from the snapshot, never copied; scans return an [`Entries`] iterator, in
/// byte order from either end.
///
/// A snapshot lives in memory. It is not a checkpoint (see
/// [`Store::checkpoint`](crate::Store::checkpoint)), and it survives no restart.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// Commit index of the last operation reflected in this snapshot.
    pub(crate) index: u64,
    /// Materialised key/state map (live entries only; deletes reflected by absence).
    pub(crate) data: Map,
}

impl Snapshot {
    /// The empty state at index 0 (a brand-new store).
    pub(crate) fn empty() -> Self {
        Snapshot {
            index: 0,
            data: BTreeMap::new(),
        }
    }

    /// The commit index this view reflects: every commit up to and including it, and
    /// none after.
    pub fn index(&self) -> u64 {
        self.index
    }

    /// Number of live keys.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// True when there are no live keys.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// The value of `key`, or `None` if it is absent (never written, or deleted).
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<&[u8]> {
        self.data.get(key.as_ref()).map(|e| &*e.value)
    }

    /// The value of `key` together with its *version*: the commit index of the write that
    /// produced it. A put of the value a key already holds changes nothing, so it leaves
    /// the version alone.
    pub fn get_with_version(&self, key: impl AsRef<[u8]>) -> Option<(&[u8], u64)> {
        self.data.get(key.as_ref()).map(|e| (&*e.value, e.version))
    }

    /// Every live key and its value, in byte order.
    pub fn iter(&self) -> Entries<'_> {
        Entries {
            inner: self.data.range::<[u8], _>(..),
        }
    }

    /// Every live key that starts with `prefix`, and its value, in byte order. O(log n)
    /// to find the first, then O(1) per key.
    pub fn scan(&self, prefix: impl AsRef<[u8]>) -> Entries<'_> {
        let prefix = prefix.as_ref();
        let inner = match prefix_end(prefix) {
            Some(end) => self
                .data
                .range::<[u8], _>((Bound::Included(prefix), Bound::Excluded(end.as_slice()))),
            None => self
                .data
                .range::<[u8], _>((Bound::Included(prefix), Bound::Unbounded)),
        };
        Entries { inner }
    }

    /// Every live key in the half-open range `[lo, hi)`, and its value, in byte order;
    /// nothing when `lo >= hi`. O(log n) to find the first, then O(1) per key.
    pub fn range_scan(&self, lo: impl AsRef<[u8]>, hi: impl AsRef<[u8]>) -> Entries<'_> {
        let (lo, hi) = (lo.as_ref(), hi.as_ref());
        // `BTreeMap::range` panics on an inverted range; the empty `[lo, lo)` stands in.
        let hi = if lo < hi { hi } else { lo };
        Entries {
            inner: self
                .data
                .range::<[u8], _>((Bound::Included(lo), Bound::Excluded(hi))),
        }
    }
}

/// The exclusive upper bound of the keys that start with `prefix`: `prefix` with its last
/// byte below `0xFF` incremented and everything after it dropped. `None` when no such
/// bound exists (an empty prefix, or one of `0xFF` bytes only): the range is unbounded.
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < u8::MAX {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

/// An iterator over some of a [`Snapshot`]'s live entries: `(key, value)` pairs borrowed
/// from the snapshot, in byte order from either end. [`Snapshot::iter`],
/// [`Snapshot::scan`] and [`Snapshot::range_scan`] return one; it borrows the snapshot and
/// nothing else, so it may outlive the bounds it was made from.
#[derive(Clone, Debug)]
pub struct Entries<'a> {
    inner: btree_map::Range<'a, Shared, Entry>,
}

impl<'a> Iterator for Entries<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, e)| (&**k, &*e.value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl DoubleEndedIterator for Entries<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|(k, e)| (&**k, &*e.value))
    }
}

impl FusedIterator for Entries<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(k: &[u8], v: &[u8]) -> Op {
        Op::put(k, v)
    }

    /// Every entry of an iterator, owned, for comparisons.
    fn owned<'a>(it: impl Iterator<Item = (&'a [u8], &'a [u8])>) -> Vec<(Vec<u8>, Vec<u8>)> {
        it.map(|(k, v)| (k.to_vec(), v.to_vec())).collect()
    }

    #[test]
    fn put_overwrite_changes_state() {
        let mut m = Map::new();
        put(b"k", b"a").apply(&mut m, 1);
        assert!(put(b"k", b"b").apply(&mut m, 2));
        let e = m.get(b"k".as_slice()).unwrap();
        assert_eq!(&*e.value, b"b");
        assert_eq!(e.version, 2);
    }

    #[test]
    fn idempotent_put_same_value_is_noop() {
        let mut m = Map::new();
        put(b"k", b"a").apply(&mut m, 1);
        assert!(!put(b"k", b"a").apply(&mut m, 2));
        assert_eq!(
            m.get(b"k".as_slice()).unwrap().version,
            1,
            "an idempotent put does not bump the version"
        );
    }

    #[test]
    fn delete_missing_is_noop() {
        let mut m = Map::new();
        assert!(!Op::delete("k").apply(&mut m, 1));
        put(b"k", b"v").apply(&mut m, 1);
        assert!(Op::delete("k").apply(&mut m, 2));
        assert!(!Op::delete("k").apply(&mut m, 3));
        assert!(m.is_empty());
    }

    /// The constructors build exactly the variants they name, from any byte-like input.
    #[test]
    fn op_constructors_take_any_bytes() {
        assert_eq!(
            Op::put("k", vec![1u8]),
            Op::Put {
                key: b"k".to_vec(),
                value: vec![1]
            }
        );
        assert_eq!(Op::delete(&b"k"[..]), Op::Delete { key: b"k".to_vec() });
        assert_eq!(Op::put(String::from("k"), "v"), Op::put(b"k", b"v"));
    }

    /// Cloning a map shares its keys and values instead of copying them: the substance of
    /// the Phase 6 per-commit cost fix.
    #[test]
    fn cloned_maps_share_bytes() {
        let mut s = Snapshot::empty();
        put(b"k", b"a-fairly-long-value").apply(&mut s.data, 1);
        let copy = s.data.clone();
        let (k1, e1) = s.data.iter().next().unwrap();
        let (k2, e2) = copy.iter().next().unwrap();
        assert!(Arc::ptr_eq(k1, k2), "keys are shared");
        assert!(Arc::ptr_eq(&e1.value, &e2.value), "values are shared");
    }

    #[test]
    fn point_reads_borrow_the_value_and_report_its_version() {
        let mut s = Snapshot::empty();
        put(b"a", b"1").apply(&mut s.data, 1);
        put(b"b", b"2").apply(&mut s.data, 2);
        put(b"a", b"3").apply(&mut s.data, 3);
        s.index = 3;
        assert_eq!(s.index(), 3);
        assert_eq!((s.len(), s.is_empty()), (2, false));
        assert_eq!(s.get("a"), Some(&b"3"[..]));
        assert_eq!(s.get_with_version(b"a"), Some((&b"3"[..], 3)));
        assert_eq!(s.get_with_version(String::from("b")), Some((&b"2"[..], 2)));
        assert_eq!(s.get("z"), None);
        assert_eq!(
            owned(s.iter()),
            vec![
                (b"a".to_vec(), b"3".to_vec()),
                (b"b".to_vec(), b"2".to_vec())
            ]
        );
        assert!(Snapshot::empty().is_empty());
    }

    #[test]
    fn scan_is_ordered_and_stops_after_the_prefix() {
        let mut s = Snapshot::empty();
        for k in [b"ab", b"aa", b"ba", b"ac"] {
            put(k, k).apply(&mut s.data, 1);
        }
        let keys: Vec<Vec<u8>> = owned(s.scan(b"a")).into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, [b"aa".to_vec(), b"ab".to_vec(), b"ac".to_vec()]);
        assert_eq!(s.scan("").count(), 4, "the empty prefix matches everything");
        assert_eq!(s.scan("c").count(), 0);
    }

    /// Prefixes ending in `0xFF` bytes still select exactly the keys that start with
    /// them, and a prefix of `0xFF`s alone runs to the end of the key space.
    #[test]
    fn scan_bounds_prefixes_that_end_in_ff() {
        let mut s = Snapshot::empty();
        let keys: [&[u8]; 8] = [
            b"a",
            b"a\xff",
            b"a\xff\x00",
            b"a\xff\xff",
            b"b",
            b"\xff",
            b"\xff\xff",
            b"\xff\xff\x01",
        ];
        for k in keys {
            put(k, b"v").apply(&mut s.data, 1);
        }
        let scanned = |p: &[u8]| -> Vec<Vec<u8>> { s.scan(p).map(|(k, _)| k.to_vec()).collect() };
        assert_eq!(
            scanned(b"a\xff"),
            [&b"a\xff"[..], b"a\xff\x00", b"a\xff\xff"]
        );
        assert_eq!(
            scanned(b"\xff"),
            [&b"\xff"[..], b"\xff\xff", b"\xff\xff\x01"]
        );
        assert_eq!(scanned(b"\xff\xff"), [&b"\xff\xff"[..], b"\xff\xff\x01"]);
        assert_eq!(scanned(b"").len(), keys.len());
        assert_eq!(prefix_end(b"ab"), Some(b"ac".to_vec()));
        assert_eq!(prefix_end(b"a\xff\xff"), Some(b"b".to_vec()));
        assert_eq!(prefix_end(b"\xff\xff"), None);
        assert_eq!(prefix_end(b""), None);
    }

    /// `scan` agrees with filtering every key by the prefix, on keys drawn from an
    /// alphabet heavy in the edge bytes `0x00` and `0xFF`, forwards and backwards.
    #[test]
    fn scan_matches_a_prefix_filter_on_edge_bytes() {
        let mut x = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let alphabet = [0x00u8, 0x01, 0x7F, 0xFE, 0xFF];
        let mut word = |max: u64| -> Vec<u8> {
            let len = next() % (max + 1);
            (0..len)
                .map(|_| alphabet[(next() % alphabet.len() as u64) as usize])
                .collect()
        };
        let mut s = Snapshot::empty();
        for _ in 0..400 {
            let k = word(4);
            put(&k, b"v").apply(&mut s.data, 1);
        }
        for _ in 0..300 {
            let p = word(3);
            let want: Vec<&[u8]> = s
                .iter()
                .map(|(k, _)| k)
                .filter(|k| k.starts_with(&p))
                .collect();
            let got: Vec<&[u8]> = s.scan(&p).map(|(k, _)| k).collect();
            assert_eq!(got, want, "prefix {p:?}");
            let mut back: Vec<&[u8]> = s.scan(&p).rev().map(|(k, _)| k).collect();
            back.reverse();
            assert_eq!(back, want, "prefix {p:?}, backwards");
        }
    }

    /// Regression (review, second pass): the iterators borrow the snapshot and nothing
    /// else, so one made from bounds that do not outlive the call can still be returned.
    /// This test is that it compiles: `use<'s>` lets the returned type hold the snapshot's
    /// lifetime and no other, which an iterator capturing its bounds (as the anonymous
    /// `impl Iterator` did under edition 2024's capture rules) would violate.
    #[test]
    fn iterators_outlive_the_bounds_they_were_made_from() {
        fn between<'s>(
            s: &'s Snapshot,
            lo: &str,
            hi: &[u8],
        ) -> impl Iterator<Item = &'s [u8]> + use<'s> {
            s.range_scan(lo, hi).map(|(k, _)| k)
        }
        fn under<'s>(s: &'s Snapshot, prefix: &str) -> impl Iterator<Item = &'s [u8]> + use<'s> {
            s.scan(prefix).map(|(k, _)| k)
        }
        let mut s = Snapshot::empty();
        for k in [b"a1", b"a2", b"b1"] {
            put(k, b"v").apply(&mut s.data, 1);
        }
        assert_eq!(between(&s, "a", b"b").count(), 2);
        assert_eq!(under(&s, "b").collect::<Vec<_>>(), [&b"b1"[..]]);
        assert_eq!(s.iter().next_back(), Some((&b"b1"[..], &b"v"[..])));
    }

    #[test]
    fn range_is_half_open_and_ordered() {
        let mut s = Snapshot::empty();
        for k in [b"d", b"b", b"a", b"c"] {
            put(k, k).apply(&mut s.data, 1);
        }
        let keys = |it| -> Vec<Vec<u8>> { owned(it).into_iter().map(|(k, _)| k).collect() };
        assert_eq!(
            keys(s.range_scan(b"b", b"d")),
            vec![b"b".to_vec(), b"c".to_vec()]
        );
        assert_eq!(s.range_scan(b"a", b"zz").count(), 4, "hi past the end");
        assert_eq!(s.range_scan(b"c", b"c").count(), 0, "empty interval");
        assert_eq!(
            s.range_scan(b"d", b"a").count(),
            0,
            "inverted bounds do not panic"
        );
    }
}
