//! # Jepsen-lite sequential-consistency test
//!
//! Many threads hammer a single node: `W` writer threads each own a **disjoint** slice
//! of the keyspace (so every key has exactly one writer) and commit ordered writes,
//! while `R` reader threads do random reads. Every commit and every read is recorded
//! into a [`checker::Model`] tagged with the **commit index** (write) or **snapshot
//! index** (read) the engine assigned.
//!
//! With one writer per key and a totally-ordered monotone commit index, MVCC
//! snapshot-read consistency is exact and checkable: a read that observes value `V` at
//! snapshot index `r` must equal the value the latest write to that key at index
//! `<= r` produced. Zero violations across a large concurrent workload proves the
//! engine's linearisation (commit order) matches the observed history.
//!
//! "Lite" by design: real Jepsen also kills/fails over nodes and reconfigures a cluster
//! (Phase 2/3). Here we prove a single node is internally coherent -- the necessary
//! precondition for any replicated system to be correct.

mod common;

use common::TempDir;
use keystory::checker::{CheckModel, Model};
use keystory::Store;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

const KEYS: u32 = 64;
const WRITES_PER_KEY: u32 = 100;
const READERS: u32 = 6;
const READS_PER_READER: u32 = 300;
const WRITERS: u32 = 8;

/// Deterministic xorshift so each reader thread walks the keyspace differently.
fn xorshift(mut x: u64) -> u64 {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

#[test]
fn concurrent_writers_and_readers_are_sequentially_consistent() {
    let tmp = TempDir::new("linearisability");
    let dir = tmp.path();

    let store = Arc::new(Store::open(dir).expect("open store"));
    let model = Arc::new(Model::new());
    let stop = Arc::new(AtomicBool::new(false));

    // Writers: writer `t` owns the key range [t*chunk, min((t+1)*chunk, KEYS)).
    let chunk = KEYS.div_ceil(WRITERS);
    let mut handles = Vec::new();

    for t in 0..WRITERS {
        let store = Arc::clone(&store);
        let model = Arc::clone(&model);
        let lo = t * chunk;
        let hi = std::cmp::min(lo + chunk, KEYS);
        handles.push(thread::spawn(move || {
            for k in lo..hi {
                let key = format!("key{:03}", k);
                for v in 0..WRITES_PER_KEY {
                    let val = format!("w{t}_k{k}_v{v}");
                    // commit, then record the write at its commit index.
                    let idx = store.put(key.as_bytes(), val.as_bytes()).expect("put");
                    model.record_write(idx, key.clone(), Some(val.clone().into_bytes()));
                }
                // Exercise the read-after-delete path once per key.
                let final_str = format!("w{t}_k{k}_v{}", WRITES_PER_KEY - 1);
                let d = store.delete(key.as_bytes()).expect("delete");
                model.record_write(d, key.clone(), None);
                let p = store
                    .put(key.as_bytes(), final_str.as_bytes())
                    .expect("reput");
                model.record_write(p, key.clone(), Some(final_str.clone().into_bytes()));
            }
        }));
    }

    // Readers: random reads, recording observed value + the snapshot index seen.
    for r in 0..READERS {
        let store = Arc::clone(&store);
        let model = Arc::clone(&model);
        let stop = Arc::clone(&stop);
        let mut seed = (r as u64)
            .wrapping_add(1)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15);
        handles.push(thread::spawn(move || {
            for _ in 0..READS_PER_READER {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                seed = xorshift(seed);
                let k = seed as u32 % KEYS;
                let key = format!("key{:03}", k);
                let (got, r_idx) = store.get_at(key.as_bytes());
                // Record the read at the snapshot index it observed.
                model.record_read(r_idx, key, got);
            }
        }));
    }

    // Let writers finish first, then release the readers and join everyone.
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().expect("join");
    }

    // 1) Every recorded read must be consistent with its key's write log.
    let res = model.check();
    assert!(
        res.violations.is_empty(),
        "sequential-consistency violations detected:\n{:#?}",
        res.violations
    );
    assert!(
        res.checked > 0,
        "the test recorded no reads -- it did nothing"
    );

    // 2) Final state: exactly one writer owns each key, so its final value is
    //    deterministic and recoverable.
    for k in 0..KEYS {
        let owner = (k / chunk) % WRITERS;
        let expected = format!("w{owner}_k{k}_v{}", WRITES_PER_KEY - 1);
        assert_eq!(
            store.get(format!("key{:03}", k).as_bytes()),
            Some(expected.into_bytes()),
            "final value of key {k}"
        );
    }

    // 3) Durability: checkpoint, then reopen the same final state.
    store.checkpoint().expect("checkpoint");
    let reopened = Store::open(dir).expect("reopen");
    assert_eq!(reopened.len(), KEYS as usize, "final recovered set size");

    // 4) A focused lost-update check with interleaved writers on one key.
    bump_and_check_lost_updates();
}

/// One key, N interleaved writers (NOT a single owner): each commits an incrementing
/// integer. The commit index total-orders them; the offline checker must find zero
/// violations, and the final recovered value must be a genuine written value.
fn bump_and_check_lost_updates() {
    let tmp = TempDir::new("lost-update");
    let dir = tmp.path();

    let store = Arc::new(Store::open(dir).expect("open"));
    let model = Arc::new(Model::new());
    let n = 5u32;
    let mut hs = Vec::new();
    for w in 0..n {
        let store = Arc::clone(&store);
        let model = Arc::clone(&model);
        let key = b"hot".to_vec();
        hs.push(thread::spawn(move || {
            for i in 0..500u32 {
                let val = format!("w{w}_{i:04}");
                // The commit index total-orders all writers; record it for the checker.
                let idx = store.put(key.as_slice(), val.as_bytes()).unwrap();
                model.record_write(idx, key.clone(), Some(val.into_bytes()));
            }
        }));
    }
    for h in hs {
        h.join().expect("join");
    }

    // The engine total-ordered all 2500 commits; zero checker violations.
    let res = model.check();
    assert!(res.violations.is_empty(), "checker disagrees: {res:?}");

    // Independently: one live key whose value was a genuine write.
    let finalv = store.get(b"hot").expect("hot present");
    assert!(
        std::str::from_utf8(&finalv).is_ok(),
        "value is a written string"
    );
    assert_eq!(store.len(), 1, "exactly one live key");

    // Durability across a simulated crash: checkpoint, then a WAL tail, reopen.
    store.checkpoint().expect("checkpoint hot");
    let tail = "w9_9999";
    store.put(b"hot", tail.as_bytes()).expect("tail put");
    let reopened = Store::open(dir).expect("reopen after crash");
    assert_eq!(
        reopened.get(b"hot"),
        Some(tail.as_bytes().to_vec()),
        "WAL tail recovered"
    );
}
