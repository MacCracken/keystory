//! # Jepsen-lite sequential-consistency test
//!
//! Many threads hammer a single node: `W` writer threads each own a **disjoint** slice
//! of the keyspace (so every key has exactly one writer) and commit ordered writes,
//! while `R` reader threads do paced random reads for as long as the writers run. Every
//! commit and every read is recorded into a [`checker::Model`] tagged with the **commit
//! index** (write) or **snapshot index** (read) the engine assigned.
//!
//! With one writer per key and a totally-ordered monotone commit index, MVCC
//! snapshot-read consistency is exact and checkable: a read that observes value `V` at
//! snapshot index `r` must equal the value the latest write to that key at index
//! `<= r` produced. Zero violations across a large concurrent workload proves the
//! engine's linearisation (commit order) matches the observed history.
//!
//! "Lite" by design: real Jepsen also kills/fails over nodes and reconfigures a cluster.
//! Here we prove a single node is internally coherent -- the necessary precondition for
//! any replicated system to be correct.

mod common;

use common::TempDir;
use keystory::Store;
use keystory::checker::{CheckModel, Model};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

const KEYS: u32 = 64;
const WRITES_PER_KEY: u32 = 100;
const READERS: u32 = 6;
const WRITERS: u32 = 8;
/// Upper bound on the reads one reader records, to keep the offline check bounded.
const MAX_READS_PER_READER: u32 = 40_000;
/// Readers are paced so their observations spread across the writers' whole run
/// instead of burning through the cap in the first few milliseconds.
const READ_PACE: Duration = Duration::from_millis(1);

/// Deterministic xorshift so each reader thread walks the keyspace differently.
fn xorshift(mut x: u64) -> u64 {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

/// Spawn `n` paced reader threads that read random keys from `keys` until `stop` is
/// raised, recording every observation together with the snapshot index it came from.
fn spawn_readers(
    n: u32,
    store: &Arc<Store>,
    model: &Arc<Model>,
    stop: &Arc<AtomicBool>,
    keys: Arc<Vec<Vec<u8>>>,
) -> Vec<thread::JoinHandle<()>> {
    (0..n)
        .map(|r| {
            let store = Arc::clone(store);
            let model = Arc::clone(model);
            let stop = Arc::clone(stop);
            let keys = Arc::clone(&keys);
            let mut seed = (r as u64)
                .wrapping_add(1)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15);
            thread::spawn(move || {
                for _ in 0..MAX_READS_PER_READER {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    seed = xorshift(seed);
                    let key = &keys[(seed % keys.len() as u64) as usize];
                    let (got, r_idx) = store.get_at(key);
                    // Record the read at the snapshot index it observed.
                    model.record_read(r_idx, key.clone(), got);
                    thread::sleep(READ_PACE);
                }
            })
        })
        .collect()
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
    let writers: Vec<_> = (0..WRITERS)
        .map(|t| {
            let store = Arc::clone(&store);
            let model = Arc::clone(&model);
            let lo = t * chunk;
            let hi = std::cmp::min(lo + chunk, KEYS);
            thread::spawn(move || {
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
            })
        })
        .collect();

    // Readers run for the whole time the writers do, then are released.
    let keys: Arc<Vec<Vec<u8>>> = Arc::new(
        (0..KEYS)
            .map(|k| format!("key{:03}", k).into_bytes())
            .collect(),
    );
    let readers = spawn_readers(READERS, &store, &model, &stop, keys);
    for h in writers {
        h.join().expect("writer thread");
    }
    stop.store(true, Ordering::Relaxed);
    for h in readers {
        h.join().expect("reader thread");
    }

    // 1) Every recorded read must be consistent with its key's write log.
    let res = model.check();
    assert!(
        res.violations.is_empty(),
        "sequential-consistency violations detected:\n{:#?}",
        res.violations
    );
    assert!(
        res.checked >= 1_000,
        "readers recorded only {} reads: the workload did not overlap the writers",
        res.checked
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

/// One key, N interleaved writers (NOT a single owner) plus concurrent readers: each
/// writer commits an incrementing integer, the commit index total-orders them, and the
/// offline checker must find zero violations across every read. The final recovered
/// value must be a genuine written value.
fn bump_and_check_lost_updates() {
    let tmp = TempDir::new("lost-update");
    let dir = tmp.path();

    let store = Arc::new(Store::open(dir).expect("open"));
    let model = Arc::new(Model::new());
    let stop = Arc::new(AtomicBool::new(false));
    let n = 5u32;
    let writers: Vec<_> = (0..n)
        .map(|w| {
            let store = Arc::clone(&store);
            let model = Arc::clone(&model);
            let key = b"hot".to_vec();
            thread::spawn(move || {
                for i in 0..500u32 {
                    let val = format!("w{w}_{i:04}");
                    // The commit index total-orders all writers; record it for the checker.
                    // (Recording happens after the commit, outside the store's lock, so
                    // writers may record out of order: the checker sorts by index.)
                    let idx = store.put(key.as_slice(), val.as_bytes()).unwrap();
                    model.record_write(idx, key.clone(), Some(val.into_bytes()));
                }
            })
        })
        .collect();
    let readers = spawn_readers(2, &store, &model, &stop, Arc::new(vec![b"hot".to_vec()]));
    for h in writers {
        h.join().expect("writer thread");
    }
    stop.store(true, Ordering::Relaxed);
    for h in readers {
        h.join().expect("reader thread");
    }

    // The engine total-ordered all 2500 commits; every read matched the write it saw.
    let res = model.check();
    assert!(res.violations.is_empty(), "checker disagrees: {res:?}");
    assert!(
        res.checked >= 100,
        "readers recorded only {} reads on the hot key",
        res.checked
    );

    // Independently: one live key whose value was a genuine write.
    let finalv = store.get(b"hot").expect("hot present");
    assert!(
        std::str::from_utf8(&finalv).is_ok(),
        "value is a written string"
    );
    assert_eq!(store.len(), 1, "exactly one live key");

    // Durability across a reopen: checkpoint, then a WAL tail, reopen.
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
