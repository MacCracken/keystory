//! An ordered B+ tree with a CRC-guarded document format -- the Phase 5 answer to
//! ROADMAP open question 2 ("log-structured vs B-tree").
//!
//! A fixed-order (`ORDER = 5`) B+ tree built by **splits on insert**: separators route to
//! children and values live only in leaves. The whole tree can be persisted as one
//! `crc32`-checked document (magic `BTR2`) via [`commit`], crash-safely (tmp + `fsync` +
//! rename), and reloaded via [`open`]; a torn or corrupted document is rejected.
//!
//! # What this *is* and isn't (honest)
//! * `insert`, `get`, `scan`, `range` and a point `delete` are implemented; deletion does
//!   **no** merge/borrow rebalancing, so a leaf may underflow (or empty) after deletes.
//! * The tree is an in-memory nested structure serialised whole: there are no fixed-size
//!   pages, no page ids and no per-page I/O, and each `commit` rewrites the entire document.
//! * `scan` and `range` are in-order walks of the *whole* tree, not separator-pruned
//!   descents, so they cost O(N) rather than O(log N + k); `len`/`is_empty` are O(N) too.
//! * The live `Store` keeps an instance as a *secondary* range index rebuilt on `open`;
//!   the persistence path here is not used by the store.
//!
//! The rebalancing and range-cost gaps are tracked in `ROADMAP.md`.

#[cfg(test)]
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::crc::crc32;

/// The B-tree order: the maximum number of children of an internal node. Every internal node has
/// between `MIN_CHILDREN` and `ORDER` children; every leaf between `MIN_LEAF_KEYS` and `ORDER - 1`
/// keys. Chosen small (`ORDER = 5`) so splits exercise on modest inputs.
pub const ORDER: usize = 5;
#[cfg(test)]
const MIN_CHILDREN: usize = 3; // ceil(ORDER / 2) for ORDER = 5
#[cfg(test)]
const MIN_LEAF_KEYS: usize = 2; // ceil(ORDER / 2) - 1 for ORDER = 5
const MAGIC: u32 = 0x4254_5252; // B T R 2

// ---------------- node structure ----------------

/// An internal node: `keys` has `children.len() - 1` entries interleaved between the child
/// subtrees; `keys[i]` is the least key in `children[i + 1]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Internal {
    keys: Vec<Vec<u8>>,
    children: Vec<Node>,
}

/// A leaf: a sorted, non-overlapping run of `(key, value)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Leaf {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

/// A B-tree node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Node {
    Internal(Internal),
    Leaf(Leaf),
}

impl Node {
    fn count(&self) -> usize {
        match self {
            Node::Leaf(l) => l.entries.len(),
            Node::Internal(i) => i.children.iter().map(Node::count).sum(),
        }
    }
}

/// A split produced by an overflowing node during a bottom-up insert.
struct Split {
    median: Vec<u8>,
    right: Node,
}

// ---------------- the B-tree ----------------

/// A disk-persisted, fixed-order B-tree mapping bytes to bytes.
#[derive(Debug, Clone)]
pub struct BTree {
    root: Node,
}

impl Default for BTree {
    fn default() -> BTree {
        BTree::new()
    }
}

impl BTree {
    /// An empty tree.
    pub fn new() -> BTree {
        BTree {
            root: Node::Leaf(Leaf {
                entries: Vec::new(),
            }),
        }
    }

    /// Insert (or replace) `key -> val`, splitting any overflowing node on the way back up.
    pub fn insert(&mut self, key: Vec<u8>, val: Vec<u8>) {
        if let Some(split) = insert_rec(&mut self.root, key, val) {
            // Root overflowed; lift a new single-key root over the two halves.
            let left = std::mem::replace(
                &mut self.root,
                Node::Leaf(Leaf {
                    entries: Vec::new(),
                }),
            );
            self.root = Node::Internal(Internal {
                keys: vec![split.median],
                children: vec![left, split.right],
            });
        }
    }

    /// Fetch the value for `key`, or `None` (linear within a leaf; leaves are at most `ORDER`).
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        get_rec(&self.root, key)
    }

    /// Scan keys in **ascending** order, yielding `(key, value)` pairs whose entry matches `pred`.
    pub fn scan<F>(&self, pred: F) -> Vec<(Vec<u8>, Vec<u8>)>
    where
        F: Fn(&(Vec<u8>, Vec<u8>)) -> bool,
    {
        let mut out = Vec::new();
        collect(&self.root, &mut out, &pred);
        out
    }

    /// The number of stored keys.
    pub fn len(&self) -> usize {
        match &self.root {
            Node::Leaf(l) => l.entries.len(),
            Node::Internal(i) => i.children.iter().map(Node::count).sum(),
        }
    }

    /// Whether the tree is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Delete `key` from the tree, if present. Returns whether it was removed.
    ///
    /// This is a **point erase with no rebalancing**: the key is removed from its
    /// leaf, which may then fall below `MIN_LEAF_KEYS`. The tree stays *correct* --
    /// routing by `min_leaf_key` and `get`/`scan` remain valid -- but it is no longer
    /// balanced after heavy deletions. Merging/borrowing to re-balance is a separate,
    /// explicitly deferred piece (see the module doc-comment).
    pub fn delete(&mut self, key: &[u8]) -> bool {
        delete_rec(&mut self.root, key)
    }

    /// Collect entries with `lo <= key < hi` in **ascending** order (an ordered range
    /// query). This is the range counterpart of [`BTree::scan`]; the store builds its
    /// range scans on it.
    pub fn range(&self, lo: &[u8], hi: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::new();
        collect(&self.root, &mut out, &|e: &(Vec<u8>, Vec<u8>)| {
            e.0.as_slice() >= lo && e.0.as_slice() < hi
        });
        out
    }

    #[cfg(test)]
    pub fn to_sorted_map(&self) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let mut map = BTreeMap::new();
        flatten(&self.root, &mut map);
        map
    }

    #[cfg(test)]
    pub fn assert_balanced(&self) {
        assert_invariants(&self.root, true);
    }
}

// ---------------- insert recursion ----------------

fn insert_rec(node: &mut Node, key: Vec<u8>, val: Vec<u8>) -> Option<Split> {
    match node {
        Node::Leaf(leaf) => {
            let pos = leaf
                .entries
                .partition_point(|entry| entry.0.as_slice() < key.as_slice());
            if pos < leaf.entries.len() && leaf.entries[pos].0 == key {
                leaf.entries[pos].1 = val;
            } else {
                leaf.entries.insert(pos, (key, val));
            }
            if leaf.entries.len() > ORDER - 1 {
                let mut entries = std::mem::take(&mut leaf.entries);
                let mid = entries.len() / 2;
                let median = entries[mid].0.clone();
                let right = entries.split_off(mid); // median stays in the right leaf (B+); the key is also copied up
                leaf.entries = entries;
                Some(Split {
                    median,
                    right: Node::Leaf(Leaf { entries: right }),
                })
            } else {
                None
            }
        }
        Node::Internal(inner) => {
            let i = inner
                .keys
                .partition_point(|k| k.as_slice() <= key.as_slice());
            match insert_rec(&mut inner.children[i], key, val) {
                Some(split) => {
                    inner.keys.insert(i, split.median);
                    inner.children.insert(i + 1, split.right);
                    if inner.children.len() > ORDER {
                        let mid = inner.keys.len() / 2;
                        let right_children = inner.children.split_off(mid + 1);
                        let right_keys = inner.keys.split_off(mid + 1);
                        inner.keys.remove(mid);
                        let right = Node::Internal(Internal {
                            keys: right_keys,
                            children: right_children,
                        });
                        // B+ invariant: parent separator = min leaf key of the right subtree, not keys[mid].
                        let mut sep = Vec::new();
                        min_leaf_key(&right, &mut sep);
                        Some(Split { median: sep, right })
                    } else {
                        None
                    }
                }
                None => None,
            }
        }
    }
}

// ---------------- read / scan recursion ----------------

/// The smallest leaf key in `node`'s subtree (the B+ routing key for that subtree).
fn min_leaf_key(node: &Node, out: &mut Vec<u8>) {
    match node {
        Node::Leaf(leaf) => {
            out.clear();
            out.extend_from_slice(&leaf.entries[0].0);
        }
        Node::Internal(inner) => min_leaf_key(&inner.children[0], out),
    }
}

fn get_rec(node: &Node, key: &[u8]) -> Option<Vec<u8>> {
    match node {
        Node::Leaf(leaf) => leaf
            .entries
            .iter()
            .find(|(k, _)| k.as_slice() == key)
            .map(|(_, v)| v.clone()),
        Node::Internal(inner) => {
            let i = inner.keys.partition_point(|k| k.as_slice() <= key); // B+ route: first key > key
            get_rec(&inner.children[i], key)
        }
    }
}

/// Route to the leaf holding `key` and erase its entry; returns `true` if removed.
/// No rebalancing: the leaf may end up smaller than `MIN_LEAF_KEYS`.
fn delete_rec(node: &mut Node, key: &[u8]) -> bool {
    match node {
        Node::Leaf(leaf) => {
            // The leaf is sorted, so a partition-point + equality check locates it.
            let pos = leaf.entries.partition_point(|(k, _)| k.as_slice() < key);
            if pos < leaf.entries.len() && leaf.entries[pos].0.as_slice() == key {
                leaf.entries.remove(pos);
                true
            } else {
                false
            }
        }
        Node::Internal(inner) => {
            // Route to the child that owns `key` (same `<=` rule as `get`/`insert`).
            let i = inner.keys.partition_point(|k| k.as_slice() <= key);
            delete_rec(&mut inner.children[i], key)
        }
    }
}

fn collect<F>(node: &Node, out: &mut Vec<(Vec<u8>, Vec<u8>)>, pred: &F)
where
    F: Fn(&(Vec<u8>, Vec<u8>)) -> bool,
{
    match node {
        Node::Leaf(leaf) => {
            for e in leaf.entries.iter() {
                if pred(e) {
                    out.push(e.clone());
                }
            }
        }
        Node::Internal(inner) => {
            for child in inner.children.iter() {
                collect(child, out, pred);
            }
        }
    }
}

#[cfg(test)]
fn flatten(node: &Node, out: &mut BTreeMap<Vec<u8>, Vec<u8>>) {
    match node {
        Node::Leaf(leaf) => {
            for (k, v) in leaf.entries.iter() {
                out.insert(k.clone(), v.clone());
            }
        }
        Node::Internal(inner) => {
            for child in inner.children.iter() {
                flatten(child, out);
            }
        }
    }
}

// ---------------- invariant checker (tests) ----------------

#[cfg(test)]
fn assert_invariants(node: &Node, is_root: bool) {
    match node {
        Node::Leaf(l) => {
            for w in l.entries.windows(2) {
                assert!(w[0].0 < w[1].0, "leaf entries not sorted: {w:?}");
            }
            assert!(l.entries.len() < ORDER, "leaf too full");
            if !is_root {
                assert!(
                    MIN_LEAF_KEYS <= l.entries.len(),
                    "leaf underflow: {}",
                    l.entries.len()
                );
            }
        }
        Node::Internal(i) => {
            assert_eq!(i.keys.len() + 1, i.children.len(), "keys/children mismatch");
            let n = i.children.len();
            assert!(n <= ORDER, "internal too many children: {n}");
            if !is_root {
                assert!(
                    MIN_CHILDREN <= n,
                    "internal underflow: {n} < {MIN_CHILDREN}"
                );
            }
            for w in i.keys.windows(2) {
                assert!(w[0] < w[1], "internal keys not increasing: {w:?}");
            }
            for c in i.children.iter() {
                assert_invariants(c, false);
            }
        }
    }
}

// ---------------- on-disk format + commit ----------------

fn node_page(node: &Node, out: &mut Vec<u8>) {
    let mut body = Vec::new();
    match node {
        Node::Leaf(leaf) => {
            body.push(0);
            write_u32(leaf.entries.len() as u32, &mut body);
            for (k, v) in leaf.entries.iter() {
                write_u32(k.len() as u32, &mut body);
                body.extend_from_slice(k);
                write_u32(v.len() as u32, &mut body);
                body.extend_from_slice(v);
            }
        }
        Node::Internal(inner) => {
            body.push(1);
            write_u32(inner.keys.len() as u32, &mut body);
            for k in inner.keys.iter() {
                write_u32(k.len() as u32, &mut body);
                body.extend_from_slice(k);
            }
            write_u32(inner.children.len() as u32, &mut body);
            for c in inner.children.iter() {
                let mut child = Vec::new();
                node_page(c, &mut child);
                write_u64(child.len() as u64, &mut body);
                body.extend_from_slice(&child);
            }
        }
    }
    let crc = crc32(&body);
    out.extend_from_slice(&MAGIC.to_le_bytes());
    out.extend_from_slice(&body);
    out.extend_from_slice(&crc.to_le_bytes());
}

fn write_u32(v: u32, bytes: &mut Vec<u8>) {
    bytes.extend_from_slice(&v.to_le_bytes());
}

fn write_u64(v: u64, bytes: &mut Vec<u8>) {
    bytes.extend_from_slice(&v.to_le_bytes());
}

fn read_u32(bytes: &[u8], pos: &mut usize) -> u32 {
    let v = u32::from_le_bytes([
        bytes[*pos],
        bytes[*pos + 1],
        bytes[*pos + 2],
        bytes[*pos + 3],
    ]);
    *pos += 4;
    v
}

fn read_u64(bytes: &[u8], pos: &mut usize) -> u64 {
    let v = u64::from_le_bytes(bytes[*pos..*pos + 8].try_into().expect("8-byte read"));
    *pos += 8;
    v
}

/// Parse a single page into a node, verifying the CRC; `None` on corruption.
fn parse_page(bytes: &[u8]) -> Option<Node> {
    if bytes.len() < 4 + 4 + 4 {
        return None;
    }
    let got_magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    if got_magic != MAGIC {
        return None;
    }
    let body = &bytes[4..bytes.len() - 4];
    let last4 = [
        bytes[bytes.len() - 4],
        bytes[bytes.len() - 3],
        bytes[bytes.len() - 2],
        bytes[bytes.len() - 1],
    ];
    let got_crc = u32::from_le_bytes(last4);
    if got_crc != crc32(body) {
        return None;
    }
    let mut pos = 0usize;
    let ty = body[pos];
    pos += 1;
    let node = match ty {
        0 => {
            let n = read_u32(body, &mut pos) as usize;
            let mut entries = Vec::with_capacity(n);
            for _ in 0..n {
                let kl = read_u32(body, &mut pos) as usize;
                let k = body[pos..pos + kl].to_vec();
                pos += kl;
                let vl = read_u32(body, &mut pos) as usize;
                let v = body[pos..pos + vl].to_vec();
                pos += vl;
                entries.push((k, v));
            }
            Node::Leaf(Leaf { entries })
        }
        1 => {
            let n = read_u32(body, &mut pos) as usize;
            let mut keys = Vec::with_capacity(n);
            for _ in 0..n {
                let kl = read_u32(body, &mut pos) as usize;
                keys.push(body[pos..pos + kl].to_vec());
                pos += kl;
            }
            let cn = read_u32(body, &mut pos) as usize;
            let mut children = Vec::with_capacity(cn);
            for _ in 0..cn {
                let clen = read_u64(body, &mut pos) as usize;
                let child_bytes = &body[pos..pos + clen];
                pos += clen;
                children.push(parse_page(child_bytes)?);
            }
            Node::Internal(Internal { keys, children })
        }
        _ => return None,
    };
    Some(node)
}

fn tree_document(root: &Node) -> Vec<u8> {
    let mut body = Vec::new();
    node_page(root, &mut body);
    let mut doc = Vec::new();
    doc.extend_from_slice(&MAGIC.to_le_bytes());
    doc.extend_from_slice(&body);
    doc.extend_from_slice(&crc32(&body).to_le_bytes());
    doc
}

/// Persist the current tree to `dir/btree.dat` crash-safely (temp + fsync + rename).
pub fn commit(dir: &Path, tree: &BTree) -> std::io::Result<()> {
    commit_document(dir, &tree_document(&tree.root))
}

/// Persist a raw document to `dir/btree.dat` crash-safely. Exposed so tests can exercise commit /
/// reload without going through a `BTree`.
pub fn commit_document(dir: &Path, doc: &[u8]) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let live = dir.join("btree.dat");
    let tmp = dir.join("btree.tmp");
    {
        let f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        let mut w = BufWriter::new(f);
        w.write_all(doc)?;
        w.flush()?;
        w.into_inner()?.sync_all()?;
    }
    std::fs::rename(&tmp, &live)?;
    Ok(())
}

/// Load a tree from `dir`, or an empty tree if no file exists; a CRC-mismatched or corrupt
/// document is rejected (`Err`), not silently recovered.
pub fn open(dir: &Path) -> std::io::Result<BTree> {
    let live = dir.join("btree.dat");
    if !live.exists() {
        return Ok(BTree::new());
    }
    let doc = std::fs::read(&live)?;
    if doc.len() < 4 + 4 {
        return Err(std::io::Error::other("btree.dat too short"));
    }
    let got_magic = u32::from_le_bytes([doc[0], doc[1], doc[2], doc[3]]);
    if got_magic != MAGIC {
        return Err(std::io::Error::other("btree.dat bad magic"));
    }
    let body = &doc[4..doc.len() - 4];
    let last4 = [
        doc[doc.len() - 4],
        doc[doc.len() - 3],
        doc[doc.len() - 2],
        doc[doc.len() - 1],
    ];
    let got_crc = u32::from_le_bytes(last4);
    if got_crc != crc32(body) {
        return Err(std::io::Error::other(
            "btree.dat CRC mismatch (torn/corrupt)",
        ));
    }
    match parse_page(body) {
        Some(root) => Ok(BTree { root }),
        None => Err(std::io::Error::other("btree.dat undecodable page")),
    }
}

// ---------------- tests ----------------

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::BTreeMap as BTreeMapT;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(1);

    /// Mints a unique, stable key token.
    fn key() -> Vec<u8> {
        SEQ.fetch_add(1, Ordering::Relaxed).to_le_bytes().to_vec()
    }

    fn fresh_dir() -> std::path::PathBuf {
        let p =
            std::env::temp_dir().join(format!("ks_bt_p5_{}", SEQ.fetch_add(1, Ordering::Relaxed)));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn drop_dir(p: &std::path::Path) {
        let _ = std::fs::remove_dir_all(p);
    }

    /// In-order dump for hand-diagnosing routing.
    #[allow(unused)]
    fn dump(node: &Node, depth: usize) {
        match node {
            Node::Leaf(leaf) => {
                let fb: Vec<u8> = leaf.entries.iter().map(|(k, _)| k[0]).collect();
                eprintln!(
                    "{}LEAF n={} fb={:?}",
                    "      ".repeat(depth),
                    leaf.entries.len(),
                    fb
                );
            }
            Node::Internal(inner) => {
                let fb: Vec<u8> = inner.keys.iter().map(|k| k[0]).collect();
                eprintln!(
                    "{}INT keys#={} fb={:?} children#={}",
                    "      ".repeat(depth),
                    inner.keys.len(),
                    fb,
                    inner.children.len()
                );
                for c in inner.children.iter() {
                    dump(c, depth + 1);
                }
            }
        }
    }

    #[test]
    fn leaf_splits_and_scan() {
        let mut t = BTree::new();
        let keys: Vec<Vec<u8>> = (0..300).map(|_| key()).collect();
        for (i, k) in keys.iter().enumerate() {
            t.insert(k.clone(), vec![i as u8, (i / 10) as u8]);
            t.assert_balanced();
        }
        assert_eq!(t.len(), 300, "expected 300 keys");
        for (i, k) in keys.iter().enumerate() {
            assert_eq!(
                t.get(k),
                Some(vec![i as u8, (i / 10) as u8]),
                "lookup key {i}"
            );
        }
        let all = t.scan(|_| true);
        assert_eq!(all.len(), 300);
        for w in all.windows(2) {
            assert!(w[0].0 < w[1].0, "scan not ascending");
        }
        t.assert_balanced();
    }

    #[test]
    fn persist_and_recover() {
        let dir = fresh_dir();
        let keys: Vec<Vec<u8>> = (0..150).map(|_| key()).collect();
        {
            let mut t = BTree::new();
            for (i, k) in keys.iter().enumerate() {
                t.insert(k.clone(), vec![b'v', i as u8]);
            }
            commit(&dir, &t).unwrap();
            assert!(dir.join("btree.dat").exists());
        }
        let t2 = open(&dir).expect("reopen after clean commit");
        assert_eq!(t2.len(), 150, "recovered tree lost data");
        for (i, k) in keys.iter().enumerate() {
            assert_eq!(t2.get(k), Some(vec![b'v', i as u8]), "recovered key {i}");
        }
        t2.assert_balanced();
        drop_dir(&dir);
    }

    #[test]
    fn corrupt_document_rejected() {
        let dir = fresh_dir();
        let mut t = BTree::new();
        for _ in 0..50 {
            t.insert(key(), vec![b'x']);
        }
        let doc = tree_document(&t.root);
        commit_document(&dir, &doc).unwrap();
        let mut raw = std::fs::read(dir.join("btree.dat")).unwrap();
        raw[doc.len() / 2] ^= 0x80;
        std::fs::write(dir.join("btree.dat"), &raw).unwrap();
        assert!(
            open(&dir).is_err(),
            "corrupt document must be rejected on open"
        );
        drop_dir(&dir);
    }

    #[test]
    fn empty_round_trips() {
        let dir = fresh_dir();
        let t = BTree::new();
        commit(&dir, &t).unwrap();
        let t2 = open(&dir).unwrap();
        assert!(t2.is_empty());
        drop_dir(&dir);
    }

    #[test]
    fn in_order_matches_btreemap() {
        let mut t = BTree::new();
        let mut expected = BTreeMapT::new();
        for i in 0..500 {
            let k = key();
            let v = vec![i as u8, (i / 100) as u8];
            t.insert(k.clone(), v.clone());
            expected.insert(k, v);
            t.assert_balanced();
        }
        assert_eq!(t.to_sorted_map(), expected);
    }

    #[test]
    fn one_key_get() {
        let mut t = BTree::new();
        t.insert(b"abc".to_vec(), b"1".to_vec());
        t.insert(b"def".to_vec(), b"2".to_vec());
        assert_eq!(t.get(b"abc"), Some(b"1".to_vec()));
        assert_eq!(t.get(b"def"), Some(b"2".to_vec()));
        assert_eq!(t.get(b"xyz"), None);
    }
    #[test]
    fn deleted_key_vanishes_and_lookups_stay_valid() {
        // Stable, ordered, big-endian numeric keys with a 0x55 prefix so they
        // don't collide with the minted `key()` sequence tokens used by other tests.
        let k = |i: u64| -> Vec<u8> {
            let mut k = vec![0x55u8];
            k.extend_from_slice(&i.to_be_bytes());
            k
        };
        let mut b = BTree::new();
        for i in 0..200u64 {
            b.insert(k(i), u32::try_from(i).unwrap().to_le_bytes().to_vec());
        }
        // Delete every 7th key; each must then vanish from get.
        let mut gone = 0usize;
        for i in 0..200u64 {
            if i % 7 == 0 {
                assert!(b.delete(k(i).as_slice()), "delete not-found for a live key");
                assert_eq!(b.get(k(i).as_slice()), None, "a deleted key is still found");
                gone += 1;
            }
        }
        // Deleting a never-present key is a no-op.
        assert!(
            !b.delete(k(99999).as_slice()),
            "deleting an absent key is a no-op"
        );
        assert_eq!(b.len(), 200 - gone, "len must shrink by the number deleted");
        assert_eq!(b.get(k(1).as_slice()), Some(1u32.to_le_bytes().to_vec()));
    }

    #[test]
    fn range_query_is_ordered_and_bounded() {
        // Deterministic, big-endian ordered keys so [lo,hi) is meaningful.
        let k = |i: u64| -> Vec<u8> {
            let mut k = vec![0x55u8];
            k.extend_from_slice(&i.to_be_bytes());
            k
        };
        let mut b = BTree::new();
        for i in 0..200u64 {
            b.insert(k(i), u32::try_from(i).unwrap().to_le_bytes().to_vec());
        }
        // Delete an interior key: it must drop out of the range.
        b.delete(k(52).as_slice());
        let got = b.range(k(50).as_slice(), k(57).as_slice()); // [50,57)
        let got_idx: Vec<u64> = got
            .iter()
            .map(|(k, _)| u64::from_be_bytes(k[1..].try_into().unwrap()))
            .collect();
        // 52 is gone, 57 is excluded: [50,51,53,54,55,56].
        assert_eq!(
            got_idx,
            vec![50, 51, 53, 54, 55, 56],
            "range excludes the deleted key and the hi bound"
        );
        assert!(
            got_idx.windows(2).all(|w| w[0] < w[1]),
            "range output must be ascending"
        );
    }
}
