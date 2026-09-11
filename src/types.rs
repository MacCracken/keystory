//! Core, serialisation-friendly value types shared across the crate.
//!
//! Everything here is deterministic and free of wall-clock timestamps: the only
//! ordering concepts are the **term** (Raft election epoch -- always 1 on a
//! single node) and the **commit index** (a logical, monotone sequence number).
//! A logical index, not `SystemTime`, is what gives crash recovery its
//! determinism (see the crate-level docs and `ROADMAP.md`).

use std::collections::BTreeMap;
use std::ops::Bound;

/// Bytes are the canonical key/value representation. `String` keys accepted by the
/// public API are coerced to `Bytes`.
pub type Bytes = Vec<u8>;

/// A stored entry. `version` is the commit index at which this entry's state was
/// written: the per-key logical clock that a historical `get_at(index)` read would
/// traverse (not implemented yet).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub value: Bytes,
    pub version: u64,
}

/// The mutating operations a commit can represent. Each is idempotent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Put { key: Bytes, value: Bytes },
    Delete { key: Bytes },
}

impl Op {
    /// Apply this op to `map` at commit `index`, stamping `version = index`.
    ///
    /// Returns whether the visible map state changed:
    ///   * `Put` to a new key, or to a key whose current value differs, changes state.
    ///   * `Put` to a key with the identical value is a no-op (idempotence).
    ///   * `Delete` of a present key changes state; of an absent key does not.
    pub fn apply(&self, map: &mut BTreeMap<Bytes, Entry>, index: u64) -> bool {
        match self {
            Op::Put { key, value } => {
                let changed = match map.get(key) {
                    Some(e) => e.value != *value,
                    None => true,
                };
                if changed {
                    map.insert(
                        key.clone(),
                        Entry {
                            value: value.clone(),
                            version: index,
                        },
                    );
                } // A same-value put is a true no-op: state and version unchanged.
                changed
            }
            Op::Delete { key } => map.remove(key).is_some(),
        }
    }
}

/// An immutable, version-stamped view of the entire state space.
///
/// A `Snapshot` is published into [`RcuSwap`](crate::rcu::RcuSwap) on every commit;
/// readers `load()` an `Arc<Snapshot>` and never see it mutated. It is the substrate
/// for MVCC reads and crash-recovery checkpoints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// Commit index of the last operation reflected in this snapshot.
    pub index: u64,
    /// Materialised key/state map (live entries only; deletes reflected by absence).
    pub data: BTreeMap<Bytes, Entry>,
}

impl Snapshot {
    /// The empty state at index 0 (a brand-new store).
    pub fn empty() -> Self {
        Snapshot {
            index: 0,
            data: BTreeMap::new(),
        }
    }

    /// Read a key. `None` means absent or deleted.
    pub fn get(&self, key: &[u8]) -> Option<&Entry> {
        self.data.get(key)
    }

    /// Snapshot-consistent prefix scan: every live key with the given prefix, in
    /// byte order. The returned `Vec` is stable for the lifetime of the snapshot
    /// that produced it; no later write can reach into it.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Entry)> {
        self.data
            .range::<[u8], _>((Bound::Included(prefix), Bound::Unbounded))
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Snapshot-consistent ordered range `[lo, hi)`: `lo` inclusive, `hi` exclusive,
    /// empty when `lo >= hi`. O(log n + k) on the underlying ordered map.
    pub fn range(&self, lo: &[u8], hi: &[u8]) -> Vec<(Vec<u8>, Entry)> {
        if lo >= hi {
            return Vec::new();
        }
        self.data
            .range::<[u8], _>((Bound::Included(lo), Bound::Excluded(hi)))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Number of live keys in this snapshot.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// True when the snapshot holds no live keys.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_overwrite_changes_state() {
        let mut m: BTreeMap<_, _> = BTreeMap::new();
        Op::Put {
            key: b"k".to_vec(),
            value: b"a".to_vec(),
        }
        .apply(&mut m, 1);
        assert!(
            Op::Put {
                key: b"k".to_vec(),
                value: b"b".to_vec()
            }
            .apply(&mut m, 2)
        );
        assert_eq!(m.get(b"k".as_slice()).unwrap().value, b"b".to_vec());
        assert_eq!(m.get(b"k".as_slice()).unwrap().version, 2);
    }

    #[test]
    fn idempotent_put_same_value_is_noop() {
        let mut m: BTreeMap<_, _> = BTreeMap::new();
        Op::Put {
            key: b"k".to_vec(),
            value: b"a".to_vec(),
        }
        .apply(&mut m, 1);
        assert!(
            !Op::Put {
                key: b"k".to_vec(),
                value: b"a".to_vec()
            }
            .apply(&mut m, 2)
        );
        assert_eq!(
            m.get(b"k".as_slice()).unwrap().version,
            1,
            "an idempotent put does not bump the version"
        );
    }

    #[test]
    fn delete_missing_is_noop() {
        let mut m: BTreeMap<_, _> = BTreeMap::new();
        assert!(!Op::Delete { key: b"k".to_vec() }.apply(&mut m, 1));
        Op::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        }
        .apply(&mut m, 1);
        assert!(Op::Delete { key: b"k".to_vec() }.apply(&mut m, 2));
        assert!(!Op::Delete { key: b"k".to_vec() }.apply(&mut m, 3));
        assert!(m.is_empty());
    }

    #[test]
    fn scan_prefix_is_ordered() {
        let mut s = Snapshot::empty();
        for k in [b"ab", b"aa", b"ba", b"ac"] {
            Op::Put {
                key: k.to_vec(),
                value: k.to_vec(),
            }
            .apply(&mut s.data, 1);
        }
        let got = s.scan_prefix(b"a");
        assert_eq!(
            got.iter().map(|(k, _)| &**k).collect::<Vec<_>>(),
            [b"aa", b"ab", b"ac"]
        );
    }

    #[test]
    fn range_is_half_open_and_ordered() {
        let mut s = Snapshot::empty();
        for k in [b"d", b"b", b"a", b"c"] {
            Op::Put {
                key: k.to_vec(),
                value: k.to_vec(),
            }
            .apply(&mut s.data, 1);
        }
        let keys = |v: Vec<(Vec<u8>, Entry)>| v.into_iter().map(|(k, _)| k).collect::<Vec<_>>();
        assert_eq!(
            keys(s.range(b"b", b"d")),
            vec![b"b".to_vec(), b"c".to_vec()]
        );
        assert_eq!(keys(s.range(b"a", b"zz")).len(), 4, "hi past the end");
        assert!(s.range(b"c", b"c").is_empty(), "empty interval");
        assert!(
            s.range(b"d", b"a").is_empty(),
            "inverted bounds do not panic"
        );
    }
}
