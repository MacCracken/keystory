//! # Sequential-consistency checker (Jepsen-lite)
//!
//! The user requirement is "sequential consistency of reads and writes". With a
//! **single writer per key** (each key's writes come from one worker, in its commit
//! order) plus a **globally monotone commit index** (our linearisation clock), the
//! invariant is exact and checkable:
//!
//! > A read of `key` that observes value `V` at snapshot index `r` is correct iff `V`
//! > equals the value the *latest* write to `key` whose commit index is `<= r` produced
//! > (`None` if that latest write was a delete, or if no write has happened yet).
//!
//! This is MVCC snapshot-read consistency, which is strong enough to imply a coherent
//! sequential history. The checker records every write and every read into in-memory
//! logs and validates *all* reads offline, keeping the hot read path free of extra
//! locks.
//!
//! This is deliberately "lite": real Jepsen also kills nodes and reconfigures a
//! cluster. That is Phase 2/3 (Raft). What we prove here is that a *single* node
//! never violates sequential consistency on its own -- the prerequisite for any
//! replication to be correct.
use std::collections::{BTreeMap, BTreeSet};

/// A value written by an op: either stored bytes, or "deleted/absent".
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Stored {
    Present(Vec<u8>),
    Deleted,
}

/// A recorded write event: `(commit_index, key, value)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteRec {
    pub index: u64,
    pub key: Vec<u8>,
    pub value: Stored,
}

/// A recorded read event: `(commit_index_of_snapshot_read, key, value_observed)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadRec {
     /// The index of the snapshot the value was read from.
    pub read_index: u64,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

/// A per-key, MVCC-aware consistency checker.
///
/// `writes` is per-key, sorted ascending by commit index. `reads` is the flattened log
/// of every observation. Both are `Mutex`-guarded so a multi-threaded workload can feed
/// the model concurrently; per-key write lists stay ordered because a key has a single
/// writer that commits in increasing-index order.
/// One write entry: its commit index and the value it left visible.
type IndexedWrite = (u64, Stored);

#[derive(Default)]
pub struct Model {
     /// key -> writes, kept sorted ascending by commit index.
    writes: std::sync::Mutex<BTreeMap<Vec<u8>, Vec<IndexedWrite>>>,
     /// Global log of every read observation, in any order.
    reads: std::sync::Mutex<Vec<ReadRec>>,
}

impl Model {
     /// New, empty model.
    pub fn new() -> Self {
        Model::default()
        }

       /// Record a write to `key` at `commit_index` storing `value` (or delete).
    pub fn record_write(&self, index: u64, key: impl Into<Vec<u8>>, value: Option<Vec<u8>>) {
        let k = key.into();
        let stored = match value {
            Some(v) => Stored::Present(v),
            None => Stored::Deleted,
          };
        let mut w = self.writes.lock().expect("writes mutex poisoned");
        w.entry(k).or_default().push((index, stored));
        }

       /// Record a read of `key` at `read_index` observing `value`
       /// (`None` = absent/deleted at that snapshot).
    pub fn record_read(&self, read_index: u64, key: impl Into<Vec<u8>>, value: Option<Vec<u8>>) {
        let mut r = self.reads.lock().expect("reads mutex poisoned");
        r.push(ReadRec {
            read_index,
            key: key.into(),
            value,
          });
        }
}

/// The result of validating a workload's recorded reads.
#[derive(Debug, Default, Clone)]
pub struct CheckResult {
     /// Reads whose observed value did NOT match the expected MVCC value.
    pub violations: Vec<(ReadRec, Option<Vec<u8>>)>,
     /// Number of reads checked.
    pub checked: usize,
}

/// A trait so the checker has a clean, testable seam.
pub trait CheckModel {
     /// Validate every recorded read against the per-key write logs.
    fn check(&self) -> CheckResult;
}

impl CheckModel for Model {
    fn check(&self) -> CheckResult {
        let writes = self.writes.lock().expect("writes mutex poisoned");
        let reads = self.reads.lock().expect("reads mutex poisoned");
        let mut result = CheckResult {
            violations: Vec::new(),
            checked: reads.len(),
          };
        for r in reads.iter() {
             // Expected = value of the latest write to this key at index <= read_index,
              // or None if the latest such write was a delete / none has happened.
            let expected = match writes.get(&r.key) {
                None => None,
                Some(log) => log
                          .iter()
                          .rev()
                          .find(|(i, _)| *i <= r.read_index)
                          .and_then(|(_, stored)| match stored {
                                Stored::Present(v) => Some(v.clone()),
                                Stored::Deleted => None,
                              }),
              };
            if r.value != expected {
                result.violations.push((r.clone(), expected));
              }
             }
        result
        }
}

/// Distinct keys observed by a set of read/write logs (for reporting the test matrix).
pub fn observed_keys(keys: &[Vec<u8>]) -> BTreeSet<Vec<u8>> {
    keys.iter().cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

       #[test]
    fn consistent_history_passes() {
        let m = Model::new();
        m.record_write(1, b"k", Some(b"v1".to_vec()));
        m.record_write(2, b"k", Some(b"v2".to_vec()));
        m.record_write(3, b"k", None); // delete
        m.record_read(1, b"k", Some(b"v1".to_vec())); // reads v1 at snapshot 1
        m.record_read(2, b"k", Some(b"v2".to_vec())); // reads v2
        m.record_read(3, b"k", None); // absent (deleted by index 3)
        m.record_read(0, b"k", None); // before any write: absent
        assert!(m.check().violations.is_empty());
        }

       #[test]
    fn a_stale_but_valid_read_passes() {
         // A reader that observes v1 at index 1 even though v3 is "current" is FINE:
         // MVCC snapshot-read consistency permits reading a past snapshot.
        let m = Model::new();
        m.record_write(1, b"k", Some(b"v1".to_vec()));
        m.record_write(2, b"k", Some(b"v2".to_vec()));
        m.record_write(3, b"k", Some(b"v3".to_vec()));
        m.record_read(1, b"k", Some(b"v1".to_vec()));
        m.record_read(3, b"k", Some(b"v3".to_vec()));
        assert!(m.check().violations.is_empty());
        }

       #[test]
    fn an_inconsistent_read_is_caught() {
        let m = Model::new();
        m.record_write(1, b"k", Some(b"v1".to_vec()));
        m.record_write(2, b"k", Some(b"v2".to_vec()));
         // A read at index 2 must see v2, not some unrecorded value:
        m.record_read(2, b"k", Some(b"vBROKEN".to_vec()));
        assert_eq!(m.check().violations.len(), 1, "the bad read must be flagged");
        }

       #[test]
    fn delete_then_reread_reflects_absence() {
        let m = Model::new();
        m.record_write(1, b"k", Some(b"v1".to_vec()));
        m.record_write(2, b"k", None);
        m.record_read(1, b"k", Some(b"v1".to_vec())); // v1 at index 1
        m.record_read(2, b"k", None); // absent at index 2
        m.record_read(3, b"k", None); // still absent (no newer write)
        assert!(m.check().violations.is_empty());
        }
}
