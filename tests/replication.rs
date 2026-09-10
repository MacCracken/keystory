//! # Phase 2 -- replication, failover, and partition tolerance (Jepsen-lite)
//!
//! Drives the in-process `RaftCluster` through the failure modes a replicated KV store must
//! survive, using the same `Model` linearisability oracle as Phase 1:
//!
//! * **Convergence** -- every committed write is applied at one global index on every node.
//! * **Failover** -- killing the leader and continuing must not lose a committed update.
//! * **Partition tolerance** -- a minority that reaches no quorum commits nothing: no
//!   split-brain divergence, its writes error, and it sees no value it "committed".
//! * **Linearisability under concurrency** -- concurrent client threads leave a history the
//!   `Model` validates with zero violations.
//!
//! *Honest scope:* the "network" is in-process and replication is synchronous inside each
//! client call (not a true async, socket-based cluster, and not two network-separated
//! sub-clusters each running its own election). See `ROADMAP.md` for what this proves and
//! what it does not -- the phase that adds real async networking.

use keystore::checker::{CheckModel, Model};
use keystore::raft::{ClusterError, RaftCluster};
use std::sync::Arc;

const KEYS: u64 = 40;
const WRITES: u64 = 50;

fn key(n: u64) -> Vec<u8> {
    format!("k{n}").into_bytes()
}

fn value(n: u64, i: u64) -> Vec<u8> {
    format!("w{n}_{i}").into_bytes()
}

/// A 3-node cluster converges on every write: after the writes, all three nodes hold the
/// same value for each key.
#[test]
fn three_nodes_converge_on_writes_and_reads() {
    let c = RaftCluster::new(3);
    for i in 0..WRITES {
        for n in 0..KEYS {
            assert!(
                c.put(key(n), value(n, i)).is_ok(),
                "put on a healthy 3-node cluster commits"
            );
        }
    }
    // Every node must agree; `get` already asserts convergence, but verify explicitly.
    for n in 0..KEYS {
        let v = c.get(key(n)).expect("read on a live cluster");
        assert_eq!(v, Some(value(n, WRITES - 1)), "every node converged on the last write");
    }
    assert!(c.leader().is_some(), "a healthy cluster has a leader");
}

/// Kill the leader exactly once, mid-workload, and prove NO committed update is lost -- the
/// central failover guarantee. The shared `Model` oracle sees zero violations and every final
/// value is present.
///
/// (Failing a second leader would leave only a minority live -- which correctly cannot commit;
/// this test instead asserts that a single leader death loses nothing.)
#[test]
fn failover_preserves_committed_state() {
    let model = Arc::new(Model::new());
    let c = RaftCluster::with_model(3, model.clone());

    for i in 0..WRITES {
        for n in 0..KEYS {
            c.put(key(n), value(n, i)).expect("a healthy majority commits on a live quorum");
        }
        // Kill the leader exactly once, mid-workload; the next op re-elects and continues.
        if i == WRITES / 2 {
            let l = c.leader().expect("there is a leader");
            c.fail(l);
        }
    }

    for n in 0..KEYS {
        let v = c.get(key(n)).expect("read after failover");
        assert_eq!(v, Some(value(n, WRITES - 1)), "no committed update lost across failover");
    }

    let r = model.check();
    assert_eq!(
        r.violations.len(),
        0,
        "failover must not break linearisability; got {} violations",
        r.violations.len()
    );
    assert!(r.checked > 0, "we recorded reads to validate");
}

/// A minority (1 of 3 live nodes, quorum 2) cannot make progress: its `put` returns
/// `NoLeader`, touches no state, and -- crucially -- the node does not serve a value the
/// minority "committed". This is the split-brain guard.
#[test]
fn minority_partition_cannot_commit() {
    let c = RaftCluster::new(3);

    for i in 0..10 {
        c.put(key(0), value(0, i)).expect("healthy commit");
    }
    assert_eq!(c.get(key(0)).unwrap(), Some(value(0, 9)));

    // Kill the leader and one follower, leaving a lone (minority) node.
    let leader = c.leader().expect("a leader exists");
    let followers: Vec<_> = (0..3u64).filter(|&id| id != leader).collect();
    assert_eq!(followers.len(), 2);
    c.fail(followers[0]);
    c.fail(leader);

    assert_eq!(
        c.put(key(99), value(99, 0)),
        Err(ClusterError::NoLeader),
        "a minority cannot commit"
    );

    match c.get(key(99)) {
        Ok(Some(_)) => panic!("split-brain: a minority served a value it could not commit"),
        Ok(None) => {}
        Err(_) => {}
    }

    c.revive(followers[0]);
    c.revive(followers[1]);
    assert!(c.put(key(99), value(99, 7)).is_ok(), "after healing, a majority commits");
    assert_eq!(c.get(key(99)).unwrap(), Some(value(99, 7)), "healed cluster converged");
    assert_eq!(c.get(key(0)).unwrap(), Some(value(0, 9)), "prior committed state preserved");
}

/// Many concurrent client threads hitting a live 3-node cluster. Each commit is recorded in
/// the shared `Model` at its global commit index, and reads observe that index's value; the
/// oracle must find zero violations -- the exact invariant Phase 1 proved single-node, now
/// across nodes under a real `std::thread` workload.
#[test]
fn concurrent_writers_and_readers_stay_linearizable() {
    const CLIENTS: usize = 12;
    const OPS: usize = 200;

    let model = Arc::new(Model::new());
    let cluster = Arc::new(RaftCluster::with_model(3, model.clone()));

    let handles: Vec<_> = (0..CLIENTS)
        .map(|c| {
            let cl = Arc::clone(&cluster);
            std::thread::spawn(move || {
                for i in 0..OPS {
                    let n = (c as u64 + i as u64) % KEYS;
                    if i % 3 == 0 {
                        cl.put(key(n), value(n, i as u64)).expect("healthy commit under load");
                    } else {
                        let _ = cl.get(key(n));
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().expect("client thread panicked");
    }

    let r = model.check();
    assert_eq!(
        r.violations.len(),
        0,
        "concurrent multi-node workload must be linearizable; got {} violations",
        r.violations.len()
    );
    assert!(
        r.checked >= OPS * CLIENTS / 3,
        "we should have recorded on the order of (OPS * CLIENTS / 3) reads"
    );
}
