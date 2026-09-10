//! In-process synchronous Raft driver.
//!
//! A dependency-free driver that wires a set of Nodes into a cluster and runs the
//! replication protocol by hand, on one thread's call stack -- no sockets, no async,
//! no election timers.
//!
//! What this proves, and what it does not. The protocol logic is the real Raft:
//! quorum-based election (RequestVote + majority), log replication (AppendEntries + the
//! log-matching property), and commit only on a whole-cluster majority. Because the
//! quorum is fixed by the cluster size (never by the live subset), a minority partition
//! cannot elect a leader or commit -- that is the partition-tolerance property.
//!
//! What is simplified, stated plainly:
//!
//!   * Replication is driven synchronously inside a client call -- a put blocks until the
//!     write is replicated and committed, rather than by a background loop. This is
//!     "synchronous replication": correct, just not an async pipeline.
//!   * The transport is in-process: the catch-up routine fans the log out to each live
//!     peer by calling the peer's append_entries directly. There is no network layer.
//!   * Elections are deterministic (the election picks the live node with the most
//!     up-to-date log, tie-broken by lowest id) -- a stand-in for randomised timers.
//!     The driver, not the clock, drives liveness.
//!
//! The genuine guarantees that ARE real: no lost updates across failover, no split-brain
//! commit (a minority cannot commit), and convergence -- every committed write earns a
//! global index and every live node eventually applies it.
//!
//! See ROADMAP.md for the full scope and the phase that adds real async networking.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use crate::raft::node::{Node, NodeId};
use crate::types::{Bytes, Op};

/// A cluster-wide error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterError {
       /// No reachable majority, so no leader can be elected (a minority partition).
    NoLeader,
}

/// Shared, lock-based cluster state.
struct Inner {
     nodes: BTreeMap<NodeId, Node>,
     leader: Option<NodeId>,
        /// Failed / partitioned-away nodes: they do not vote, do not receive replicas, and
        /// (if one *was* leader) its leadership is forfeited.
     down: BTreeSet<NodeId>,
}

impl Inner {
        /// The set of live node ids.
    fn live(&self) -> BTreeSet<NodeId> {
        self.nodes
               .keys()
               .filter(|id| !self.down.contains(id))
               .copied()
               .collect()
        }

        /// The *whole-cluster* quorum: fixed by cluster size, never by the live subset.
    fn quorum(&self) -> usize {
        self.nodes.len() / 2 + 1
        }

        /// Ensure a live leader exists, re-electing on failover. Returns the leader id, or
        /// [`ClusterError::NoLeader`] when no live majority exists.
    fn ensure_leader(&mut self) -> Result<NodeId, ClusterError> {
        if self.leader.map_or(true, |l| self.down.contains(&l)) {
            self.leader = None;
            }
        match self.elect() {
            Some(l) => Ok(l),
            None => Err(ClusterError::NoLeader),
            }
        }

        /// A deterministic election that succeeds *iff* a live node can reach a
        /// whole-cluster quorum. The candidate is the live node with the most up-to-date
        /// log, tie-broken by lowest id, so it is at least as up-to-date as every other
        /// live node and all of them grant it.
    fn elect(&mut self) -> Option<NodeId> {
        let live = self.live();
        if self.quorum() > live.len() {
            return None; // a live minority cannot win a majority.
            }
        let mut cand: Option<NodeId> = None;
        let mut best: (u64, u64) = (0, 0);
        for &id in &live {
            let key = (self.nodes[&id].log.last_term(), self.nodes[&id].log.last_index());
            if key > best || (key == best && cand.map_or(true, |c| id < c)) {
                cand = Some(id);
                best = key;
                }
            }
        let cand = cand.unwrap();
        let cand_term = self.max_term() + 1;
         {
            let c = self.nodes.get_mut(&cand).unwrap();
            c.start_election();
            c.term = cand_term;
            }
         // Capture the candidate's log tip before the (mutable) voting loop, to avoid an
          // aliasing self through the `nodes` map.
        let cand_tip = {
            let c = &self.nodes[&cand];
             (c.log.last_term(), c.log.last_index())
            };
        let mut votes = 1usize; // the candidate votes for itself.
        for &id in &live {
            if id == cand {
                continue;
                }
            let granted = self
                  .nodes
                  .get_mut(&id)
                  .unwrap()
                  .request_vote(cand, cand_term, cand_tip.0, cand_tip.1);
            if granted {
                votes += 1;
                }
            }
        if votes >= self.quorum() {
            self.nodes.get_mut(&cand).unwrap().become_leader();
            self.leader = Some(cand);
            self.down.remove(&cand); // the (formerly-failed) leader rejoins.
            Some(cand)
            } else {
            self.leader = None;
            None
            }
        }

        /// The highest term observed by any node.
    fn max_term(&self) -> u64 {
        self.nodes.values().map(|n| n.term).max().unwrap_or(0)
        }

        /// Fail a node (forfeits its leadership; keeps its log so it can be revived).
    pub(super) fn fail(&mut self, id: NodeId) {
        if self.leader == Some(id) {
            self.leader = None;
            }
        self.down.insert(id);
        }

        /// Revive a failed node; it rejoins and the next `put` catches it up.
    pub(super) fn revive(&mut self, id: NodeId) {
        self.down.remove(&id);
        }
}

/// A cluster of `n` nodes ids `0..n`, all peers of one another.
pub struct RaftCluster {
     inner: Mutex<Inner>,
        /// An optional shared consistency checker, guarded by its own lock (it is `Sync`),
        /// so it records without contending with the cluster's write lock.
     model: Option<Arc<crate::checker::Model>>,
}

impl Default for RaftCluster {
    fn default() -> Self {
        Self::new(3)
        }
}

impl RaftCluster {
        /// A fresh cluster of `n` nodes, no leader yet.
    pub fn new(n: usize) -> Self {
        let nodes = (0..n as u64)
               .map(|i| {
                let peers: BTreeSet<NodeId> = (0..n as u64).collect();
                   (i, Node::new(i, peers))
                })
               .collect();
        RaftCluster {
            inner: Mutex::new(Inner {
                nodes,
                leader: None,
                down: BTreeSet::new(),
                }),
             model: None,
            }
        }

        /// A cluster wired to a shared [`Model`](crate::checker::Model): each `put`/`get`
        /// records into it, for post-hoc linearisability checking.
    pub fn with_model(n: usize, model: Arc<crate::checker::Model>) -> Self {
        let mut c = Self::new(n);
        c.model = Some(model);
        c
        }


        /// The id of the current leader, if any.
    pub fn leader(&self) -> Option<NodeId> {
        self.inner.lock().unwrap().leader
        }

        /// Fail a node.
    pub fn fail(&self, id: NodeId) {
        self.inner.lock().unwrap().fail(id);
        }

        /// Revive a failed node.
    pub fn revive(&self, id: NodeId) {
        self.inner.lock().unwrap().revive(id);
        }

        /// The leader's converged view of `key`. Returns `None` if absent. Panics if any
        /// live node disagrees -- the convergence invariant the cluster must always hold.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>, ClusterError> {
        let g = self.inner.lock().unwrap();
        let leader = g
              .leader
              .filter(|l| !g.down.contains(l))
              .or_else(|| g.live().into_iter().next())
              .ok_or(ClusterError::NoLeader)?;
        let kb = key.as_ref().to_vec();
        let v = g.nodes[&leader].get(&kb);
        for &id in &g.live() {
             assert_eq!(
                g.nodes[&id].get(&kb).as_deref(),
                v.as_deref(),
                  "live cluster nodes disagree on key {kb:?}"
                );
            }
        if let Some(m) = self.model.as_ref() {
            m.record_read(g.nodes[&leader].applied, kb.clone(), v.clone());
            }
        Ok(v)
        }

    /// A *put* replicated to a quorum and committed: the synchronous write.
    pub fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<u64, ClusterError> {
        cluster_put(self, key.as_ref().to_vec(), value.as_ref().to_vec())
    }

    /// A replicated delete (idempotent), committed on a quorum.
    pub fn delete(&self, key: impl AsRef<[u8]>) -> Result<u64, ClusterError> {
        cluster_delete(self, key.as_ref().to_vec())
    }

    /// A leader-side prefix scan of the converged state.
    pub fn scan(&self, prefix: impl AsRef<[u8]>) -> Result<Vec<(Bytes, Bytes)>, ClusterError> {
        let g = self.inner.lock().unwrap();
        let leader =
              g.leader.filter(|l| !g.down.contains(l)).ok_or(ClusterError::NoLeader)?;
        let pref = prefix.as_ref().to_vec();
        let out = g.nodes[&leader]
             .state
             .range(pref.clone()..)
             .take_while(|(k, _)| k.starts_with(&pref))
             .map(|(k, e)| (k.clone(), e.value.clone()))
             .collect();
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// The synchronous replicated write -- the heart of the driver.
// ---------------------------------------------------------------------------

/// A *put*: the leader proposes the op, replicates it (with log-matching catch-up) to every
/// **live** peer; on a *whole-cluster* majority it commits and the entry is applied to all
/// live nodes at one global index. On a minority it cannot reach quorum, so it returns
/// [`ClusterError::NoLeader`] and is *nowhere* applied -- no split-brain.
pub(crate) fn cluster_put(
     cluster: &RaftCluster,
     key: Bytes,
     value: Bytes,
) -> Result<u64, ClusterError> {
    let mut g = cluster.inner.lock().unwrap();
       // 1) Ensure a live leader (re-elects on failover).
    let leader = g.ensure_leader()?;
       // 2) The leader proposes the new entry.
    let idx = {
        let n = g.nodes.get_mut(&leader).unwrap();
        n.propose(Op::Put {
            key: key.clone(),
            value: value.clone(),
            })
        };
    let lterm = g.nodes[&leader].term;
       // 3) Catch every other live peer up to `idx`; count acceptors.
    let mut acks = 1usize; // the leader is a replica of its own log.
    for &id in &g.live() {
        if id == leader {
            continue;
            }
        if peer_catch_up(&mut g, leader, id, idx, lterm) {
            g.nodes.get_mut(&leader).unwrap().match_idx.insert(id, idx);
            acks += 1;
            }
        }
       // 4) Commit iff a whole-cluster majority holds the entry.
    if acks < g.quorum() {
        return Err(ClusterError::NoLeader);
        }
     {
        g.nodes.get_mut(&leader).unwrap().advance_commit();
        }
    let ci = g.nodes[&leader].commit_idx;
    assert_eq!(ci, idx, "a majority holds index {idx} => it is committed");
       // 5) Apply the newly committed entry to every live node, then record the op.
    apply_committed(&mut g, ci);
    if let Some(m) = cluster.model.as_ref() {
        m.record_write(ci, key, Some(value));
        }
    Ok(ci)
        }

/// A replicated, committed delete: same quorum discipline as `cluster_put`, but the entry
/// is an [`Op::Delete`]. On a minority it cannot commit and returns `ClusterError::NoLeader`.
pub(crate) fn cluster_delete(
    cluster: &RaftCluster,
    key: Bytes,
) -> Result<u64, ClusterError> {
    let mut g = cluster.inner.lock().unwrap();
    let leader = g.ensure_leader()?;
    let idx = {
        let n = g.nodes.get_mut(&leader).unwrap();
        n.propose(Op::Delete { key: key.clone() })
    };
    let lterm = g.nodes[&leader].term;
    let mut acks = 1usize;
    for &id in &g.live() {
        if id == leader {
            continue;
        }
        if peer_catch_up(&mut g, leader, id, idx, lterm) {
            g.nodes.get_mut(&leader).unwrap().match_idx.insert(id, idx);
            acks += 1;
        }
    }
    if acks < g.quorum() {
        return Err(ClusterError::NoLeader);
    }
    g.nodes.get_mut(&leader).unwrap().advance_commit();
    let ci = g.nodes[&leader].commit_idx;
    assert_eq!(ci, idx, "a majority holds index {idx} => it is committed");
    apply_committed(&mut g, ci);
    if let Some(m) = cluster.model.as_ref() {
        m.record_write(ci, key, None);
    }
    Ok(ci)
}

/// Push a peer's log up to `idx` (the leader's tip) via `AppendEntries`. In this driver
/// every peer's log is a *prefix* of the leader's (the last entry always committed to a
/// quorum, and a new leader is always the most-up-to-date live node), so the suffix always
/// applies cleanly. Returns whether the peer reached `idx`.
fn peer_catch_up(
     g: &mut Inner,
     leader: NodeId,
     peer: NodeId,
     idx: u64,
     lterm: u64,
) -> bool {
     loop {
        let start = g.nodes[&peer].log.last_index() + 1;
        if start > idx {
            return true;
            }
        let prev_idx = start - 1;
        let prev_term = g.nodes[&leader].log.term_of(prev_idx).unwrap_or(0);
        let leader_commit = g.nodes[&leader].commit_idx;
        let suffix: Vec<_> =
             (start..=idx).filter_map(|i| g.nodes[&leader].log.get(i).cloned()).collect();
        let res = g.nodes.get_mut(&peer).unwrap().append_entries(
             leader,
             lterm,
             prev_idx,
             prev_term,
               &suffix,
             leader_commit,
             );
         // `None` = accepted (a prefix peer). A `Some` mismatch cannot arise from a prefix
          // peer here, but if it did the follower already truncated the divergent tail, so
          // the next loop iteration resyncs from the corrected prefix.
        let _ = res;
        }
        }

/// Apply the committed suffix `[applied+1 ..= upto]` to every **live** node's state machine,
/// advancing each node's `commit_idx`/`applied` so the views converge.
fn apply_committed(g: &mut Inner, upto: u64) {
     let live = g.live();
     for n in g.nodes.values_mut() {
        if !live.contains(&n.id) {
            continue; // only live nodes participate.
             }
        n.commit_idx = n.commit_idx.max(upto);
        n.apply_committed();
        }
}



#[cfg(test)]
mod test {
    use super::*;

    /// A 3-node cluster elects a leader, replicates a batch, and every node converges.
    #[test]
    fn election_then_replication_converges() {
        let c = RaftCluster::new(3);
        for i in 0..50u64 {
            c.put(vec![b'k', i as u8], vec![i as u8]).expect("commits on a live quorum");
        }
        for i in 0..50u64 {
            assert_eq!(c.get(vec![b'k', i as u8]).unwrap(), Some(vec![i as u8]));
        }
        assert_eq!(c.leader(), Some(0));
    }

    /// Kill the leader; a new leader takes over and all committed writes still apply with no
    /// gap -- the no-lost-update property of failover.
    #[test]
    fn failover_does_not_lose_a_committed_write() {
        let c = RaftCluster::new(3);
        for i in 0..30u64 {
            c.put(vec![b'a'], vec![i as u8]).expect("commit before failover");
        }
        c.fail(0); // kill node 0 (the current leader).
        for i in 30..60 {
            c.put(vec![b'a'], vec![i as u8]).expect("commit after failover");
        }
        // Every live node independently applied every entry; the value is exactly the last one.
        assert_eq!(c.get(vec![b'a']).unwrap(), Some(vec![59]));
    }

    /// A minority (below quorum) cannot make progress: `put` returns `NoLeader`, the value is
    /// nowhere in the cluster, and no split-brain divergence occurs.
    #[test]
    fn minority_partition_cannot_either_elect_or_commit() {
        let c = RaftCluster::new(3);
        c.put(vec![b'a'], vec![1]).expect("seed");
        c.fail(0);
        c.fail(1);
        assert_eq!(
            c.put(vec![b'a'], vec![99]),
            Err(ClusterError::NoLeader),
            "a minority cannot commit"
        );
        c.fail(2);
        assert_eq!(c.put(vec![b'a'], vec![42]), Err(ClusterError::NoLeader));
        c.revive(0);         c.revive(1); // 2 of 3 => a quorum, so writes resume
        assert!(c.put(vec![b'a'], vec![7]).is_ok());
        assert_eq!(c.get(vec![b'a']).unwrap(), Some(vec![7]));
    }
}
