//! An ordered B+ tree with a CRC-guarded document format -- the Phase 5 answer to
//! ROADMAP open question 2 ("log-structured vs B-tree").
//!
//! A fixed-order (`ORDER = 5`) B+ tree built by **splits on insert**: separators route to
//! children and values live only in leaves. The whole tree can be persisted as one
//! `crc32`-checked document (magic `BTR2`) via [`commit`], crash-safely (tmp + `fsync` +
//! rename), and reloaded via [`open`]; a torn or corrupted document is rejected.
//!
//! # What this *is* and isn't (honest)
//! * `insert`, `get`, `scan`, `range` and a point `delete` are implemented. Deletion does
//!   **no** merge/borrow rebalancing, so a leaf may underflow (or empty) after deletes;
//!   routing stays correct because separators are lower bounds, never leaf contents.
//! * `range` is a separator-pruned descent, O(log N + k); `scan` takes an arbitrary
//!   predicate and therefore walks the whole tree; `len` is O(1).
//! * The tree is an in-memory nested structure serialised whole: there are no fixed-size
//!   pages, no page ids and no per-page I/O, and each `commit` rewrites the entire document.
//! * The live `Store` does **not** use this module (Phase 6 removed it as a secondary
//!   index: the snapshot map already answers range queries). It stands alone, tested.
//!
//! Rebalancing on delete is tracked in `ROADMAP.md`.

#[cfg(test)]
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{self, BufWriter, Write};
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

/// An internal node: `keys` has `children.len() - 1` separators interleaved between the
/// child subtrees. `keys[i]` is greater than every key in `children[i]` and a lower bound
/// for every key in `children[i + 1]` (it was that subtree's least key when promoted;
/// deletes may since have removed it, which leaves routing unaffected).
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
    /// Number of stored keys, maintained on insert/delete so `len` is O(1).
    len: usize,
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
            len: 0,
        }
    }

    /// Insert (or replace) `key -> val`, splitting any overflowing node on the way back up.
    pub fn insert(&mut self, key: Vec<u8>, val: Vec<u8>) {
        let (inserted, split) = insert_rec(&mut self.root, key, val);
        if inserted {
            self.len += 1;
        }
        if let Some(split) = split {
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

    /// The number of stored keys (O(1)).
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the tree is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Delete `key` from the tree, if present. Returns whether it was removed.
    ///
    /// This is a **point erase with no rebalancing**: the key is removed from its
    /// leaf, which may then fall below `MIN_LEAF_KEYS` or even empty. The tree stays
    /// *correct* -- separators are lower bounds, so `get`/`range`/`scan` and later
    /// splits remain valid -- but it is no longer balanced after heavy deletions.
    /// Merging/borrowing to re-balance is a separate, explicitly deferred piece.
    pub fn delete(&mut self, key: &[u8]) -> bool {
        let removed = delete_rec(&mut self.root, key);
        if removed {
            self.len -= 1;
        }
        removed
    }

    /// Collect entries with `lo <= key < hi` in **ascending** order (an ordered range
    /// query), descending only into subtrees the separators say can hold such keys:
    /// O(log N + k). Empty when `lo >= hi`.
    pub fn range(&self, lo: &[u8], hi: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::new();
        if lo < hi {
            collect_range(&self.root, lo, hi, &mut out);
        }
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

/// Insert into the subtree at `node`. Returns whether a *new* key was added (as opposed
/// to a value replaced) and, if the node overflowed, the split to lift into the parent.
fn insert_rec(node: &mut Node, key: Vec<u8>, val: Vec<u8>) -> (bool, Option<Split>) {
    match node {
        Node::Leaf(leaf) => {
            let pos = leaf
                .entries
                .partition_point(|entry| entry.0.as_slice() < key.as_slice());
            let inserted = if pos < leaf.entries.len() && leaf.entries[pos].0 == key {
                leaf.entries[pos].1 = val;
                false
            } else {
                leaf.entries.insert(pos, (key, val));
                true
            };
            if leaf.entries.len() > ORDER - 1 {
                let mut entries = std::mem::take(&mut leaf.entries);
                let mid = entries.len() / 2;
                let median = entries[mid].0.clone();
                let right = entries.split_off(mid); // median stays in the right leaf (B+); the key is also copied up
                leaf.entries = entries;
                (
                    inserted,
                    Some(Split {
                        median,
                        right: Node::Leaf(Leaf { entries: right }),
                    }),
                )
            } else {
                (inserted, None)
            }
        }
        Node::Internal(inner) => {
            let i = inner
                .keys
                .partition_point(|k| k.as_slice() <= key.as_slice());
            let (inserted, split) = insert_rec(&mut inner.children[i], key, val);
            let Some(split) = split else {
                return (inserted, None);
            };
            inner.keys.insert(i, split.median);
            inner.children.insert(i + 1, split.right);
            if inner.children.len() > ORDER {
                let mid = inner.keys.len() / 2;
                let right_children = inner.children.split_off(mid + 1);
                let right_keys = inner.keys.split_off(mid + 1);
                // Promote the middle separator itself: it is greater than every key in
                // the left half and a lower bound for every key in the right half, so it
                // routes correctly without reading a leaf (which may be empty after deletes).
                let sep = inner.keys.remove(mid);
                (
                    inserted,
                    Some(Split {
                        median: sep,
                        right: Node::Internal(Internal {
                            keys: right_keys,
                            children: right_children,
                        }),
                    }),
                )
            } else {
                (inserted, None)
            }
        }
    }
}

// ---------------- read / scan recursion ----------------

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

/// Separator-pruned in-order walk for `[lo, hi)`: descends only into subtrees whose
/// key interval can intersect the range, so the cost is O(log N + k).
fn collect_range(node: &Node, lo: &[u8], hi: &[u8], out: &mut Vec<(Vec<u8>, Vec<u8>)>) {
    match node {
        Node::Leaf(leaf) => {
            let start = leaf.entries.partition_point(|(k, _)| k.as_slice() < lo);
            for (k, v) in &leaf.entries[start..] {
                if k.as_slice() >= hi {
                    break;
                }
                out.push((k.clone(), v.clone()));
            }
        }
        Node::Internal(inner) => {
            // `children[i]` holds keys in `[keys[i - 1], keys[i])`, unbounded at the ends.
            let first = inner.keys.partition_point(|k| k.as_slice() <= lo);
            for (i, child) in inner.children.iter().enumerate().skip(first) {
                if i > 0 && inner.keys[i - 1].as_slice() >= hi {
                    break;
                }
                collect_range(child, lo, hi, out);
            }
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

/// The deepest page nesting a document may have. A real tree never comes close (its height
/// grows only when the root splits, and each level multiplies the minimum key count by at
/// least three); the bound keeps a crafted document from exhausting the stack.
const MAX_DEPTH: usize = 64;

fn node_page(node: &Node, out: &mut Vec<u8>) -> io::Result<()> {
    let mut body = Vec::new();
    match node {
        Node::Leaf(leaf) => {
            body.push(0);
            write_u32(len32(leaf.entries.len())?, &mut body);
            for (k, v) in leaf.entries.iter() {
                write_u32(len32(k.len())?, &mut body);
                body.extend_from_slice(k);
                write_u32(len32(v.len())?, &mut body);
                body.extend_from_slice(v);
            }
        }
        Node::Internal(inner) => {
            body.push(1);
            write_u32(len32(inner.keys.len())?, &mut body);
            for k in inner.keys.iter() {
                write_u32(len32(k.len())?, &mut body);
                body.extend_from_slice(k);
            }
            write_u32(len32(inner.children.len())?, &mut body);
            for c in inner.children.iter() {
                let mut child = Vec::new();
                node_page(c, &mut child)?;
                write_u64(child.len() as u64, &mut body);
                body.extend_from_slice(&child);
            }
        }
    }
    let crc = crc32(&body);
    out.extend_from_slice(&MAGIC.to_le_bytes());
    out.extend_from_slice(&body);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(())
}

/// A length for a `u32` field, or `InvalidInput` when it does not fit. Never truncated: a
/// truncated length would still carry a valid CRC and decode as a different tree.
fn len32(len: usize) -> io::Result<u32> {
    u32::try_from(len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("a length of {len} exceeds the document's u32 framing"),
        )
    })
}

fn write_u32(v: u32, bytes: &mut Vec<u8>) {
    bytes.extend_from_slice(&v.to_le_bytes());
}

fn write_u64(v: u64, bytes: &mut Vec<u8>) {
    bytes.extend_from_slice(&v.to_le_bytes());
}

/// A bounded cursor over a page body: every read checks the remaining length, so a
/// length field that lies yields `None`, never an out-of-bounds panic.
struct Cursor<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.at.checked_add(n)?;
        let s = self.b.get(self.at..end)?;
        self.at = end;
        Some(s)
    }

    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }

    fn u32(&mut self) -> Option<usize> {
        let b = self.take(4)?.try_into().ok()?;
        usize::try_from(u32::from_le_bytes(b)).ok()
    }

    fn u64(&mut self) -> Option<usize> {
        let b = self.take(8)?.try_into().ok()?;
        usize::try_from(u64::from_le_bytes(b)).ok()
    }

    fn remaining(&self) -> usize {
        self.b.len() - self.at
    }
}

/// Parse a single page into a node, verifying its magic and CRC; `None` on corruption.
/// Every read is bounded and the shape is checked -- an internal node has at least two
/// children and exactly one more child than keys (routing indexes children by key
/// position), nesting stays under [`MAX_DEPTH`], and nothing follows a page's last field
/// -- so a malformed page is rejected even when its CRC is valid, never a panic.
fn parse_page(bytes: &[u8], depth: usize) -> Option<Node> {
    if depth > MAX_DEPTH || bytes.len() < 4 + 4 {
        return None;
    }
    let (magic, rest) = bytes.split_at(4);
    if u32::from_le_bytes(magic.try_into().ok()?) != MAGIC {
        return None;
    }
    let (body, crc) = rest.split_at(rest.len() - 4);
    if u32::from_le_bytes(crc.try_into().ok()?) != crc32(body) {
        return None;
    }
    let mut c = Cursor { b: body, at: 0 };
    let node = match c.u8()? {
        0 => {
            let n = c.u32()?;
            if n > c.remaining() / 8 {
                return None; // each entry needs two length fields: the count is bogus
            }
            let mut entries = Vec::with_capacity(n);
            for _ in 0..n {
                let kl = c.u32()?;
                let k = c.take(kl)?.to_vec();
                let vl = c.u32()?;
                let v = c.take(vl)?.to_vec();
                entries.push((k, v));
            }
            Node::Leaf(Leaf { entries })
        }
        1 => {
            let n = c.u32()?;
            if n > c.remaining() / 4 {
                return None; // each key needs a length field: the count is bogus
            }
            let mut keys = Vec::with_capacity(n);
            for _ in 0..n {
                let kl = c.u32()?;
                keys.push(c.take(kl)?.to_vec());
            }
            let cn = c.u32()?;
            if n == 0 || cn != n + 1 {
                return None;
            }
            let mut children = Vec::with_capacity(cn);
            for _ in 0..cn {
                let clen = c.u64()?;
                children.push(parse_page(c.take(clen)?, depth + 1)?);
            }
            Node::Internal(Internal { keys, children })
        }
        _ => return None,
    };
    (c.remaining() == 0).then_some(node)
}

fn tree_document(root: &Node) -> io::Result<Vec<u8>> {
    let mut body = Vec::new();
    node_page(root, &mut body)?;
    let mut doc = Vec::new();
    doc.extend_from_slice(&MAGIC.to_le_bytes());
    doc.extend_from_slice(&body);
    doc.extend_from_slice(&crc32(&body).to_le_bytes());
    Ok(doc)
}

/// Persist the current tree to `dir/btree.dat` crash-safely (temp + fsync + rename +
/// directory fsync). A key or value longer than the format's `u32` framing is refused
/// with `InvalidInput`.
pub fn commit(dir: &Path, tree: &BTree) -> io::Result<()> {
    commit_document(dir, &tree_document(&tree.root)?)
}

/// Persist a raw document to `dir/btree.dat` crash-safely: the bytes are fsync'd before the
/// rename, and the directory after it, so the new document is durable once this returns.
/// Exposed so tests can exercise commit / reload without going through a `BTree`.
pub fn commit_document(dir: &Path, doc: &[u8]) -> io::Result<()> {
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
    // POSIX promises nothing about the rename until the directory itself is synced.
    crate::wal::fsync_dir(dir)
}

/// Load a tree from `dir`, or an empty tree if no file exists; a CRC-mismatched or corrupt
/// document is rejected with an `InvalidData` error, not silently recovered.
pub fn open(dir: &Path) -> io::Result<BTree> {
    let live = dir.join("btree.dat");
    if !live.exists() {
        return Ok(BTree::new());
    }
    let corrupt =
        |why: &str| io::Error::new(io::ErrorKind::InvalidData, format!("btree.dat {why}"));
    let doc = std::fs::read(&live)?;
    if doc.len() < 4 + 4 {
        return Err(corrupt("is too short"));
    }
    let got_magic = u32::from_le_bytes([doc[0], doc[1], doc[2], doc[3]]);
    if got_magic != MAGIC {
        return Err(corrupt("has a bad magic"));
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
        return Err(corrupt("fails its CRC (torn or corrupt)"));
    }
    match parse_page(body, 0) {
        Some(root) => {
            let len = root.count();
            Ok(BTree { root, len })
        }
        None => Err(corrupt("has an undecodable page")),
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
        let doc = tree_document(&t.root).unwrap();
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

    /// Regression (Phase 6): emptying a leaf with deletes and then forcing the internal
    /// split that used to read that leaf's first key must not panic. The promoted
    /// separator is the parent's own middle key, so contents stay exact throughout.
    #[test]
    fn emptied_leaf_then_internal_split_does_not_panic() {
        let k = |i: u32| i.to_be_bytes().to_vec();
        let mut t = BTree::new();
        let mut expected = BTreeMapT::new();
        for i in 0..=11u32 {
            t.insert(k(i), vec![1]);
            expected.insert(k(i), vec![1]);
        }
        for i in [6u32, 7] {
            assert!(t.delete(&k(i)));
            expected.remove(&k(i));
        }
        for i in 12..=60u32 {
            t.insert(k(i), vec![2]);
            expected.insert(k(i), vec![2]);
        }
        assert_eq!(t.to_sorted_map(), expected);
        assert_eq!(t.len(), expected.len());
        for (key, v) in &expected {
            assert_eq!(t.get(key).as_ref(), Some(v));
        }
        assert_eq!(t.get(&k(6)), None);
    }

    /// The pruned range walk agrees with `BTreeMap::range` on random keys, including
    /// after deletes have left underfull and empty leaves behind.
    #[test]
    fn range_matches_btreemap_on_random_keys_after_deletes() {
        let mut x = 0x9E37_79B9u64;
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let mut t = BTree::new();
        let mut m = BTreeMapT::new();
        for _ in 0..3000 {
            let key = (next() % 100_000).to_be_bytes().to_vec();
            let v = vec![(next() & 0xff) as u8];
            t.insert(key.clone(), v.clone());
            m.insert(key, v);
        }
        let keys: Vec<Vec<u8>> = m.keys().cloned().collect();
        for (i, key) in keys.iter().enumerate() {
            if i % 3 != 0 {
                assert!(t.delete(key));
                m.remove(key);
            }
        }
        assert_eq!(t.len(), m.len());
        for _ in 0..200 {
            let a = (next() % 100_000).to_be_bytes().to_vec();
            let b = (next() % 100_000).to_be_bytes().to_vec();
            let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
            let want: Vec<(Vec<u8>, Vec<u8>)> = m
                .range(lo.clone()..hi.clone())
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            assert_eq!(t.range(&lo, &hi), want, "range [{lo:?}, {hi:?})");
        }
        assert!(
            t.range(b"\xff", b"\x00").is_empty(),
            "inverted bounds yield nothing"
        );
    }

    /// Wrap a page body as the codec does: magic, body, CRC over the body.
    fn page(body: &[u8]) -> Vec<u8> {
        let mut p = MAGIC.to_le_bytes().to_vec();
        p.extend_from_slice(body);
        p.extend_from_slice(&crc32(body).to_le_bytes());
        p
    }

    /// An internal-node body over the given (already wrapped) child pages.
    fn internal(keys: &[&[u8]], children: &[Vec<u8>]) -> Vec<u8> {
        let mut b = vec![1u8];
        b.extend_from_slice(&(keys.len() as u32).to_le_bytes());
        for k in keys {
            b.extend_from_slice(&(k.len() as u32).to_le_bytes());
            b.extend_from_slice(k);
        }
        b.extend_from_slice(&(children.len() as u32).to_le_bytes());
        for c in children {
            b.extend_from_slice(&(c.len() as u64).to_le_bytes());
            b.extend_from_slice(c);
        }
        b
    }

    /// Regression (2026-09-26 audit): the page parser trusted its length fields once the
    /// CRC matched, so a document that was CRC-valid but malformed -- as any buggy writer
    /// or hand-made file can be -- panicked `open` (slice out of range, a capacity
    /// overflow, unbounded recursion), and an internal node without children panicked the
    /// first `get`. Every such document is now an `InvalidData` error.
    #[test]
    fn crc_valid_but_malformed_documents_are_rejected_not_panicked() {
        let empty_leaf = page(&[0, 0, 0, 0, 0]);
        let mut lying_key = vec![0u8];
        lying_key.extend_from_slice(&1u32.to_le_bytes());
        lying_key.extend_from_slice(&1000u32.to_le_bytes()); // a 1000-byte key, absent
        let mut huge_count = vec![0u8];
        huge_count.extend_from_slice(&u32::MAX.to_le_bytes());
        let mut trailing = vec![0u8, 0, 0, 0, 0];
        trailing.push(0xEE);
        let mut deep = empty_leaf.clone();
        for _ in 0..=MAX_DEPTH {
            deep = page(&internal(&[b"k"], &[deep, empty_leaf.clone()]));
        }
        let cases: Vec<(&str, Vec<u8>)> = vec![
            ("a key longer than the page", page(&lying_key)),
            ("an entry count no page could hold", page(&huge_count)),
            ("trailing bytes", page(&trailing)),
            (
                "an internal node with no children",
                page(&internal(&[], &[])),
            ),
            (
                "more keys than children allow",
                page(&internal(
                    &[b"a", b"b"],
                    &[empty_leaf.clone(), empty_leaf.clone()],
                )),
            ),
            ("nesting past the depth bound", deep),
            ("an unknown page type", page(&[7])),
        ];
        for (what, root) in cases {
            let dir = fresh_dir();
            let mut doc = MAGIC.to_le_bytes().to_vec();
            doc.extend_from_slice(&root);
            doc.extend_from_slice(&crc32(&root).to_le_bytes());
            commit_document(&dir, &doc).unwrap();
            let err = open(&dir).expect_err(what);
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "{what}: {err}");
            drop_dir(&dir);
        }

        // The same helpers build a well-formed two-leaf tree, which loads and routes.
        let leaf = |k: &[u8]| {
            let mut b = vec![0u8];
            b.extend_from_slice(&1u32.to_le_bytes());
            b.extend_from_slice(&(k.len() as u32).to_le_bytes());
            b.extend_from_slice(k);
            b.extend_from_slice(&1u32.to_le_bytes());
            b.push(b'v');
            page(&b)
        };
        let root = page(&internal(&[b"m"], &[leaf(b"a"), leaf(b"z")]));
        let dir = fresh_dir();
        commit(&dir, &BTree::new()).unwrap();
        let mut doc = MAGIC.to_le_bytes().to_vec();
        doc.extend_from_slice(&root);
        doc.extend_from_slice(&crc32(&root).to_le_bytes());
        commit_document(&dir, &doc).unwrap();
        let t = open(&dir).expect("a well-formed document loads");
        assert_eq!((t.len(), t.get(b"z")), (2, Some(b"v".to_vec())));
        drop_dir(&dir);
    }

    #[test]
    fn len_tracks_inserts_replacements_and_deletes() {
        let mut t = BTree::new();
        assert!(t.is_empty());
        t.insert(b"a".to_vec(), b"1".to_vec());
        t.insert(b"a".to_vec(), b"2".to_vec()); // replacement, not growth
        t.insert(b"b".to_vec(), b"3".to_vec());
        assert_eq!(t.len(), 2);
        assert!(t.delete(b"a"));
        assert!(!t.delete(b"a"));
        assert_eq!(t.len(), 1);
        assert_eq!(t.get(b"b"), Some(b"3".to_vec()));
    }
}
