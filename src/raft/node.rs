//! # Raft node FSM -- the pure, deterministic core
//!
//! This module implements the **safety-critical** part of Raft as a *pure* state
//! machine: no clocks, no I/O, no threads. Given the same sequence of inputs it always
//! produces the same state, which is what makes the safety tests non-flaky.
//!
//! It holds the invariants that make a cluster correct:
//!
//!    * **Quorum-based election.** A candidate wins a term only if a *majority* of nodes
//!      grant it (quorum intersection). A node votes for at most one candidate per term.
//!    * **Log replication + the log-matching property.** Appends agree with the leader up
//!      to a contiguous prefix (prev-index / prev-term check); the leader's committed
//!      entries are replicated to a majority before they are applied.
//!    * **Leader-authoritative commit + apply.** An entry is committed (and applied to the
//!      state machine) only once it is replicated to a majority -- the condition that
//!      prevents split-brain divergent commits.
//!
//! *What this is NOT (stated honestly):* no async, no sockets, no wall-clock election
//! timers. Replication is driven synchronously by the cluster driver. A real
//! asynchronous, networking Raft is a later phase.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::types::{Entry, Op};

/// Node identifiers.
pub type NodeId = u64;
/// The state-machine key/value type: identical to the Phase-1 engine's map.
pub type Map = BTreeMap<Vec<u8>, Entry>;
/// A command in the replicated log. Reuses the idempotent Phase-1 op.
pub type Cmd = Op;

impl Op {
    /// Re-expose the state-machine application for the FSM (stamps `version == index`).
    pub(crate) fn apply_to(&self, map: &mut Map, index: u64) -> bool {
        self.apply(map, index)
    }
}

/// One entry in the replicated log. `self_term` is the term that produced it -- the
/// field reused from the Phase-1 `Record.term` (then a global, now the entry's own).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEntry {
    pub self_term: u64,
    pub cmd: Cmd,
}

impl LogEntry {
    pub fn new(term: u64, cmd: Cmd) -> Self {
        LogEntry {
            self_term: term,
            cmd,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// The replicated log: a contiguous sequence of `LogEntry`, 1-indexed by convention
/// (entry at log index `i` is stored at `entries[i - 1]`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Log {
    entries: Vec<LogEntry>,
}

impl Log {
    pub fn new() -> Self {
        Log {
            entries: Vec::new(),
        }
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when the log holds no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The index of the last entry, or `0` when empty.
    pub fn last_index(&self) -> u64 {
        self.entries.len() as u64
    }

    /// The term of the last entry, or `0` when empty.
    pub fn last_term(&self) -> u64 {
        self.entries.last().map(|e| e.self_term).unwrap_or(0)
    }

    /// The entry at a 1-based `index`, if in range.
    pub fn get(&self, index: u64) -> Option<&LogEntry> {
        if index == 0 {
            return None;
          }
        self.entries.get((index - 1) as usize)
    }

    /// The term of the entry at a 1-based `index`, if present.
    pub fn term_of(&self, index: u64) -> Option<u64> {
        self.get(index).map(|e| e.self_term)
    }

    /// Append `entries`, returning their newly assigned 1-based indices.
    pub fn append(&mut self, entries: &[LogEntry]) -> Vec<u64> {
        let base = self.entries.len() as u64;
        let mut out = Vec::with_capacity(entries.len());
        for (i, e) in entries.iter().enumerate() {
            out.push(base + 1 + i as u64);
            self.entries.push(e.clone());
        }
        out
    }

    /// Log-matching property: drop every entry at or after 1-based `from_index`, keeping
    /// exactly the entries with index `< from_index`.
    pub fn truncate_from(&mut self, from_index: u64) {
        // Keep entries with index < from_index, i.e. entries[0 .. from_index - 1].
        let keep = from_index.saturating_sub(1) as usize;
        self.entries.truncate(keep);
    }
}

/// A node's fully in-core Raft state (term + votedFor are the non-volatile subset).
#[derive(Clone, Debug)]
pub struct Node {
    /// Stable identity.
    pub id: NodeId,
    /// All cluster member ids **including this node** (used for quorum math).
    pub peers: BTreeSet<NodeId>,
    /// `currentTerm`.
    pub term: u64,
    /// `votedFor`.
    pub voted_for: Option<NodeId>,
      /// The leader this node is currently tracking (`None` when none / after a vote).
    pub leader_id: Option<NodeId>,
    /// Current role.
    pub role: Role,
    /// The replicated log.
    pub log: Log,
    /// Highest commit index the leader has advanced to.
    pub commit_idx: u64,
    /// Highest log index applied to the state machine.
    pub applied: u64,
    /// The state machine, a monotone function of the committed log.
    pub state: Map,
    /// For each peer, the next log index the leader will send it.
    pub next_idx: HashMap<NodeId, u64>,
    /// For each peer, the highest log index known replicated to it (leader only).
    pub match_idx: HashMap<NodeId, u64>,
}

impl Node {
    /// A brand-new node.
    pub fn new(id: NodeId, peers: BTreeSet<NodeId>) -> Self {
        Node {
            id,
            peers,
            term: 0,
            voted_for: None,
            leader_id: None,
            role: Role::Follower,
            log: Log::new(),
            commit_idx: 0,
            applied: 0,
            state: BTreeMap::new(),
            next_idx: HashMap::new(),
            match_idx: HashMap::new(),
        }
    }

    /// The quorum size for this node's cluster.
    pub fn quorum(&self) -> usize {
        self.peers.len() / 2 + 1
    }

    /// Start an election: become a candidate in a new term, vote for self, reset the
    /// leader bookkeeping.
    pub fn start_election(&mut self) {
        self.term += 1;
        self.voted_for = Some(self.id);
        self.role = Role::Candidate;
        self.next_idx.clear();
        self.match_idx.clear();
    }

    /// Promote to leader; initialise per-peer replication pointers to "nothing yet".
    pub fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.next_idx.clear();
        self.match_idx.clear();
        for &peer in &self.peers {
            self.next_idx.insert(peer, self.log.last_index() + 1);
            self.match_idx.insert(peer, 0);
        }
    }

    /// Handle a `RequestVote` from `from`. Returns whether the vote is granted.
    ///
    /// Grant only if `sender`'s term is at least ours and its log is at least as
    /// up-to-date as ours (the Raft eligibility restriction) -- the condition that keeps a
    /// minority partition from electing a stale leader.
    pub fn request_vote(
        &mut self,
        from: NodeId,
        sender_term: u64,
        sender_last_index: u64,
        sender_last_term: u64,
    ) -> bool {
        // A strictly higher term always wins; adopt it and step down.
        if sender_term > self.term {
            self.term = sender_term;
            self.voted_for = None;
            self.role = Role::Follower;
        }
        // A lower term is rejected outright.
        if sender_term < self.term {
            return false;
        }
        // Must not have voted already in this term.
        if self.voted_for.is_some() {
            return false;
        }
        // Eligibility: the candidate's log is at least as up-to-date as ours.
        let up_to_date =
            sender_last_term > self.log.last_term()
                || (sender_last_term == self.log.last_term()
                    && sender_last_index >= self.log.last_index());
        if !up_to_date {
            return false;
        }
        self.voted_for = Some(from);
        true
    }

    /// Handle an `AppendEntries` from leader `from`. Returns the index the leader should
    /// resume from on a (contiguity-)mismatch, else `None` (everything accepted).
    ///
    /// Updates `commit_idx` up to `leader_commit` (but never past a locally-known entry) --
    /// the mechanism by which a commit "propagates" to followers.
    pub fn append_entries(
        &mut self,
        from: NodeId,
        leader_term: u64,
        prev_index: u64,
        prev_term: u64,
        entries: &[LogEntry],
        leader_commit: u64,
    ) -> Option<u64> {
        // Stale leader: reject, and tell it to catch up to our tip.
        if leader_term < self.term {
            return Some(self.log.last_index() + 1);
        }
        self.term = leader_term;
        self.role = Role::Follower;
        self.voted_for = None;
        self.leader_id = Some(from); // record the leader we follow.

        // 1) Consistency check on the entry preceding the append (index 0 => empty prefix).
        let prev_ok = if prev_index == 0 {
            true
        } else {
            self.log.term_of(prev_index) == Some(prev_term)
        };
        if !prev_ok {
            // Find the largest probe < prev_index whose term matches, then truncate up to
            // it and ask the leader to resume from that point.
            let mut probe = prev_index;
            while probe > 0 && self.log.term_of(probe) != Some(prev_term) {
                probe -= 1;
            }
            if probe > 0 {
                self.log.truncate_from(probe);
            } else {
                self.log.truncate_from(1); // matched nothing; clear the log entirely.
            }
            return Some(self.log.last_index());
        }

        // 2) Append the new entries.
        for e in entries {
            self.log.append(std::slice::from_ref(e));
        }

        // 3) Commit up to the leader's commit, but never past what we actually hold.
        let safe = std::cmp::min(leader_commit, self.log.last_index());
        if safe > self.commit_idx {
            self.commit_idx = safe;
        }
        None
    }

    /// For a leader, advance `commit_idx` to the highest index that is (a) in this term
    /// and (b) replicated to a **majority** of nodes (via `match_idx`).
    ///
    /// Only entries whose `term` equals the current term are committed this way: Raft's
    /// rule that a leader does not infer a commit from an uncommitted entry of an earlier
    /// term.
    pub fn advance_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let q = self.quorum();
        for i in (self.commit_idx + 1)..=self.log.last_index() {
            if self.log.term_of(i) != Some(self.term) {
                continue;
            }
            let reps = self
                .match_idx
                .values()
                .filter(|&&m| m >= i)
                .count();
            if reps >= q {
                self.commit_idx = i;
            }
        }
    }

    /// Apply all committed-but-unapplied log entries to the state machine, in index
    /// order. Returns the number newly applied.
    ///
    /// The state machine is a monotone function of the committed log: past-`commit_idx`
    /// application is forbidden, and `Op::apply` is idempotent, so re-applying an entry
    /// (e.g. on restart) cannot corrupt the state.
    pub fn apply_committed(&mut self) -> usize {
        let mut n = 0;
        while self.applied < self.commit_idx {
            let idx = self.applied + 1;
            if let Some(e) = self.log.get(idx) {
                e.cmd.apply_to(&mut self.state, idx);
                self.applied += 1;
                n += 1;
            } else {
                break;
            }
        }
        n
    }

    /// A leader appends a new client command to its own log. Returns the new index. The
    /// caller replicates it (`append_entries` on peers) and commits (`advance_commit`).
    pub fn propose(&mut self, cmd: Cmd) -> u64 {
        assert!(matches!(self.role, Role::Leader), "only a leader may propose");
        let e = LogEntry::new(self.term, cmd);
        let idx = self.log.append(std::slice::from_ref(&e))[0];
        self.match_idx.insert(self.id, idx); // the leader is a replica of its own log.
        idx
    }

    /// A point read of the state machine (reflects everything applied up to `applied`).
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.state.get(key).map(|e| e.value.clone())
    }

    /// Number of live keys in the state machine.
    pub fn kv_len(&self) -> usize {
        self.state.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build `n` fresh nodes numbered `0..n`, all peers of one another.
    fn nodes(n: usize) -> Vec<Node> {
        let peers: BTreeSet<NodeId> = (0..n as u64).collect();
        (0..n as u64).map(|i| Node::new(i, peers.clone())).collect()
    }

    #[test]
    fn single_node_elects_commits_and_applies() {
        let mut ns = nodes(1);
        ns[0].start_election();
        ns[0].become_leader();
        assert_eq!(ns[0].quorum(), 1);

        let idx = ns[0].propose(Op::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        });
        assert_eq!(idx, 1, "first entry is log index 1");
        ns[0].apply_committed();
        assert_eq!(ns[0].kv_len(), 0, "nothing applied until committed");

        // A single node is itself a majority: commit immediately.
        ns[0].advance_commit();
        assert_eq!(ns[0].commit_idx, 1, "single member = its own quorum");
        let n = ns[0].apply_committed();
        assert_eq!(n, 1, "one entry applied");
        assert_eq!(ns[0].get(b"k"), Some(b"v".to_vec()));
    }

    #[test]
    fn three_node_election_grants_quorum_and_one_voter_per_term() {
        let mut ns = nodes(3);
        // Node 2 starts an election in term 1 and votes for itself.
        ns[2].start_election();
        assert_eq!(ns[2].term, 1, "first election is term 1");
        assert!(ns[2].voted_for == Some(2));
        // Peers 0 and 1 grant to node 2 (fresh empty logs are equally up-to-date).
        assert!(ns[0].request_vote(2, 1, 0, 0), "peer 0 grants");
        assert!(ns[1].request_vote(2, 1, 0, 0), "peer 1 grants");
        assert_eq!(ns[0].term, 1, "peers adopt the candidate's term");
        // A second candidate in the same term gets denied: one vote per node per term.
        assert!(!ns[1].request_vote(0, 1, 0, 0), "node 1 keeps its vote for 2");
        // Three grants incl. self = quorum of 3.
        assert!(ns[2].voted_for == Some(2));
    }

    #[test]
    fn stale_log_cannot_win_even_at_a_higher_term() {
        let mut ns = nodes(3);
        // Node 1 becomes leader and commits one entry.
        ns[1].start_election();
        ns[1].become_leader();
        ns[1].propose(Op::Delete { key: b"x".to_vec() });
        ns[1].advance_commit();
        // Node 0 asks to be leader in the NEXT term with an EMPTY log: must be denied,
        // because its log is behind node 1's. Quorum intersection forbids it.
        let stale = ns[1].request_vote(0, 2, 0, 0);
        assert!(
            !stale,
            "a node with an older log cannot win an election even at a higher term"
        );
    }

    #[test]
    fn log_mismatch_is_corrected() {
        let mut follower = Node::new(1, BTreeSet::from([0u64, 1]));
        // Follower's own entry 1 was written under term 1.
        follower.log.append(&[LogEntry::new(1, Op::Delete { key: b"y".to_vec() })]);
        // A term-2 leader tries to continue after a prev whose term it claims is 9:
        // that cannot match the follower's entry at index 1 (term 1).
        let reply = follower.append_entries(0, 2, 1, 9, &[], 0);
        assert_eq!(
            reply,
            Some(0),
            "mismatch => leader should resume from index 0"
        );
        assert_eq!(
            follower.log.last_index(), 0, "the divergent entry was truncated");
        
        assert_eq!(follower.role, Role::Follower, "it now follows the newer leader");
    }

    #[test]
    fn three_node_replication_reaches_quorum_commit() {
        let mut ns = nodes(3);
        ns[0].start_election();
        ns[0].become_leader();
        assert_eq!(ns[0].quorum(), 2, "quorum of 3 = 2");

        // Propose, then *synchronously replicate* to peers 1 and 2, and let their
        // acknowledgements update the leader's match indices.
        let idx = ns[0].propose(Op::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        });
        for p in [1u64, 2] {
            // Extract the leader's (immutable) append inputs first, so the follower's
            // mutable borrow of `ns[p]` below never aliases `ns[0]`.
            let id = ns[0].id;
            let term = ns[0].term;
            let entries =
                ns[0].log.get(idx).cloned().into_iter().collect::<Vec<_>>();
            let prev_term = ns[0].log.term_of(idx - 1).unwrap_or(0);
            let res = ns[p as usize]
                 .append_entries(id, term, idx - 1, prev_term, &entries, 0);
            assert!(res.is_none(), "the peer accepted the contiguous append");
            ns[0].match_idx.insert(p, idx);
        }

        ns[0].advance_commit();
        assert_eq!(ns[0].commit_idx, 1, "committed once 2 of 3 hold it");
        // Propagate the *commit index* to the followers via a no-new-entry append:
      // their logs already hold the entry, so each just advances commit_idx and will apply.
    for p in [1u64, 2] {
        let li = ns[0].id;
        let lterm = ns[0].term;
        let lci = ns[0].commit_idx;
        let res = ns[p as usize].append_entries(li, lterm, idx, lterm, &[], lci);
        assert!(res.is_none(), "the peer advanced its commit index");
      }
        for n in &mut ns {
            n.apply_committed();
        }
        for n in &ns {
            assert_eq!(
                n.get(b"k"),
                Some(b"v".to_vec()),
                "all nodes converged on the committed value"
            );
        }
    }

    #[test]
    fn committed_entries_apply_in_order_across_a_failover() {
        // Node 1 leads and commits two entries; we hand its log + commit to a fresh node 2
        // that "restarts" (empty state) and catches up -- the state must rebuild exactly.
        let mut ns = nodes(3);
        ns[1].start_election();
        ns[1].become_leader();
        ns[1].propose(Op::Put {
            key: b"a".to_vec(),
            value: b"1".to_vec(),
        });
        let i2 = ns[1].propose(Op::Put {
            key: b"b".to_vec(),
            value: b"2".to_vec(),
        });
        ns[1].match_idx.insert(2, i2); // stand-in: peer 2 replicated both entries.
        ns[1].advance_commit();
        assert_eq!(
            ns[1].commit_idx,
            i2,
        "both entries committed to a majority");
        ns[1].apply_committed();

        // A fresh node adopts node 1's log + commit and applies.
        let mut fresh = Node::new(2, ns[1].peers.clone());
        fresh.log = ns[1].log.clone();
        fresh.term = ns[1].term;
        fresh.commit_idx = ns[1].commit_idx;
        fresh.apply_committed();
        assert_eq!(fresh.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(fresh.get(b"b"), Some(b"2".to_vec()));
        assert_eq!(fresh.applied, ns[1].commit_idx);
    }
}
