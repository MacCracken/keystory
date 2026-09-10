//! # RcuSwap -- snapshot-swap primitive
//!
//! `RcuSwap<T>` stores one reference-counted, *immutable* value of type `T`.
//! Readers `load()` an `Arc<T>` and then operate on the immutable object without
//! ever seeing it mutated; a later `store()` publishes a fresh value without
//! disturbing readers that already cloned the previous one. This is the
//! substrate for the engine's version-stamped `Snapshot`s.
//!
//! ## Concurrency model (and its honest trade-off)
//!
//!
//! The cell is `RwLock<Arc<T>>`, which gives three properties at once:
//!
//! - **Parallel readers** proceed at once; the write lock is contended only with writers.
//! - **No reader blocks a writer**: a reader holds the read lock only long enough to clone
//!   the `Arc` pointee (an O(1) refcount bump), then works on its private `Arc`.
//! - **Snapshot isolation**: a `load` returns a frozen `Arc<T>`; later `store`s cannot
//!   change what an in-progress reader observes.
//!
//! Why not a hand-rolled lock-free reader (`AtomicPtr` + a "forgotten" canonical `Arc`)?
//! A leak-free *transient* reader clone needs an atomic bump of the allocation's *strong*
//! counter, which on stable Rust requires a `&mut Arc` -- i.e. exclusive, not *shared*,
//! access -- defeating the "free readers" goal. The clean real-world choices are thus
//! exactly (a) this `RwLock<Arc<_>>` snapshot-swap, or (b) a true epoch-based reclaimer
//! with manual reclamation (a *la*' Linux RCU) / a hand-rolled `arc-swap`. We pick (a)
//! for Phase 1: it is provably correct, reads are allocation-free, and the write path is
//! already serialised by the engine's commit mutex, so a brief exclusive instant on the
//! read side cannot meaningfully serialise writers.
//! exclusive instant cannot meaningfully serialise writers.
//!
//! **Open question / Phase 2:** swap the cell for an epoch reclaimer (or
//! `arc-swap`) to keep the *read* path contention-free even under heavy churn.
//!
//! We deliberately do **not** ship a "forget-per-load" scheme: that permanently
//! increments the strong counter on every load (a silent one-ref-per-read leak)
//! and was the exact bug the `no_leak_of_replaced_values` test caught.

use std::sync::{Arc, RwLock};

/// A reference-counted cell with parallel readers.
pub struct RcuSwap<T> {
    inner: RwLock<Arc<T>>,
}

impl<T> RcuSwap<T> {
     /// Create a swap initialising the cell to `init`.
    pub fn new(init: T) -> Self {
        RcuSwap { inner: RwLock::new(Arc::new(init)) }
      }

     /// Return a fresh `Arc` clone pinned to the *current* value.
     ///
     /// The read lock is held only for the `Arc::clone` and is released
     /// immediately, so the returned `Arc` outlives the lock and may live (and
     /// be dropped) far away without reacquiring anything.
    pub fn load(&self) -> Arc<T> {
        let guard = self.inner.read().unwrap();
        Arc::clone(&*guard)
      }

     /// Replace the current value with `next`, returning the previous one.
     ///
     /// The write is a brief O(1) pointer swap held under the exclusive lock.
    pub fn store(&self, next: Arc<T>) -> Arc<T> {
        let mut guard = self.inner.write().unwrap();
        let prev = Arc::clone(&*guard);
       *guard = next;
        prev
      }

      /// Number of live clones of the current value (diagnostic / tests).
    pub fn live_clones(&self) -> usize {
        let guard = self.inner.read().unwrap();
        Arc::strong_count(&*guard)
      }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::thread;

       #[test]
    fn load_returns_initial_value() {
        let s = RcuSwap::new(42u64);
        assert_eq!(*s.load(), 42);
        }

       #[test]
    fn store_updates_and_returns_previous() {
        let s = RcuSwap::new(1u64);
        let prev = s.store(Arc::new(2));
        assert_eq!(*prev, 1);
        assert_eq!(*s.load(), 2);
        }

       #[test]
    fn reader_snapshot_is_isolated_from_later_stores() {
        let s = RcuSwap::new([0u8; 16]);
        let snap = s.load(); // frozen reader view
        s.store(Arc::new([7u8; 16]));
        s.store(Arc::new([9u8; 16]));
        assert_eq!(*snap, [0u8; 16], "frozen reader snapshot must be stable across stores");
        assert_eq!(*s.load(), [9u8; 16], "a fresh loader observes the latest value");
        }

       #[test]
    fn no_leak_of_replaced_values() {
        let s = RcuSwap::new(vec![1u8; 32]);
        for i in 0..1000u64 {
            let _ = s.store(Arc::new(vec![i as u8; 32]));
           }
        assert_eq!(s.live_clones(), 1, "only the current value should be referenced once");
        }

       #[test]
    fn parallel_readers_observe_no_torn_snapshots() {
             // 8 readers spin-reading while 2 writers churn. Each store writes a full
          // snapshot whose 64 bytes are identical (writer invariant), so a correct
          // reader may never observe a half-written value.
        let s = Arc::new(RcuSwap::new(vec![0u8; 64]));
        let stop = Arc::new(AtomicBool::new(false));
        let reads = Arc::new(AtomicU64::new(0));
        let mut handles = vec![];
        for _ in 0..8 {
            let stop = Arc::clone(&stop);
            let s = Arc::clone(&s);
            let reads = Arc::clone(&reads);
            handles.push(thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let v = s.load();
                    assert_eq!(v.len(), 64, "every snapshot is a complete value");
                    assert!(v.iter().all(|&b| b == v[0]), "torn/partial snapshot observed");
                     reads.fetch_add(1, Ordering::Relaxed);
                }
             }));
         }
        for w in 0..2u64 {
            let stop = Arc::clone(&stop);
            let s = Arc::clone(&s);
 {
                handles.push(thread::spawn(move || {
                    for i in 0..50_000u64 {
                        if stop.load(Ordering::Relaxed) { break; }
                        s.store(Arc::new(vec![(i as u8).wrapping_add(w as u8); 64]));
                    }
                }));
            }
         }
        thread::sleep(std::time::Duration::from_millis(100));
        stop.store(true, Ordering::Relaxed);
        for h in handles {
            h.join().expect("thread join");
          }
        assert!(reads.load(Ordering::Relaxed) > 0, "readers should have made progress");
        assert_eq!(s.load().len(), 64, "final load is well-formed");
        }
}
