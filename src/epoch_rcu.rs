//! Epoch-based reclamation (the model behind an RCU read path), Phase 5 (2/3).
//!
//! This closes open question #3 — *"should `RcuSwap` move to true epoch-based
//! reclamation so readers never take a write lock?"* — by implementing the
//! reclamation mechanism and proving, by construction, the property that makes it
//! attractive: **an old version is not freed until its grace period has elapsed, i.e.
//! every reader that began its critical section *before* the publish has since exited.**
//!
//! ## Model, and its honest scope
//!
//! A `Rcu` holds one *current* version as an `Arc<T>`.
//!
//! * [`Rcu::snapshot`] enters a critical section (recording the reader's *entry epoch*)
//!   and clones the current version. The reader never blocks on a concurrent
//!   [`Rcu::publish`]: the clone it already holds is untouched by later publications.
//! * [`Rcu::publish`] advances the epoch and *defers* the old version into a
//!   retirement queue, tagged with the epoch at which it was retired.
//! * [`Rcu::reclaim`] frees a deferred version only when **every active reader entered
//!   at or after the retiree's epoch** — i.e. no live reader could still be holding it.
//!
//! The grace-period test is a *quiescent-state* check on the minimum active entry epoch:
//! a retiree tagged `E` is free once the minimum entry epoch of all active readers has
//! passed `E` (no active reader began before the publish that retired it). This is the
//! textbook epoch-relation and is race-free for the cooperative, single-producer model
//! this system runs under. A production, async-safe variant would *additionally* guard
//! a reader that parks inside its critical section and a writer preempted mid-publish --
//! the same care a kernel RCU takes -- and is out of scope here: that is exactly why epoch
//! reclamation is **demonstrated and reasoned about** below rather than wired into the hot
//! `RcuSwap` path, whose correctness is already established by the snapshot test.
//!
//! **Observability is measured, not claimed.** A retired version is kept as a *strong*
//! reference in the queue, so its `Drop` (and `Arc` decrement) is deferred until
//! `reclaim` drops it. The tests assert this directly: a version's observer `Drop` fires
//! only after quiescence, and `pending()` / `reclaim()` counts track the queue.
//!
#![allow(clippy::arc_with_non_send_sync)]

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

// This is a single-producer, cooperative *model* of epoch reclamation: the retiree is
// an `Arc<Box<dyn Any>>` (not Send/Sync) held only behind one `Mutex`, driven from a
// single logical producer. A production variant would bound `T: Send + Sync` and use raw
// slots; that boundary is out of scope here, but the grace-period logic is the substance.

// A deferred version, tagged with the epoch at which it was retired.
type Retiree = (u64, Arc<Box<dyn std::any::Any>>);

/// A monotonic epoch counter plus a quiescent-state tracker for deferred reclamation.
#[derive(Debug, Default)]
pub struct EpochManager {
      /// Monotonic epoch; advanced on every publish.
    epoch: AtomicU64,
      /// The entry epoch of every reader currently inside a critical section.
    active: Mutex<Vec<u64>>,
      /// Deferred handles, each tagged with the epoch at which it was retired.
    retired: Mutex<Vec<Retiree>>,
}

impl EpochManager {
    pub fn new() -> Self {
        EpochManager::default()
         }

       /// The current epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
          }

       /// Enter a critical section: records this reader's entry epoch. Any version
       /// retired at or after this epoch cannot be freed while the returned guard lives.
    pub fn enter(&self) -> Reader<'_> {
        let e = self.epoch.load(Ordering::Acquire);
        self.active.lock().unwrap().push(e);
        Reader { mgr: self, entry: e }
          }

       /// Retire an old version: defer it, tagged with the just-advanced epoch. It is not
       /// freed until its grace period elapses. Returns the new epoch.
    pub fn retire(&self, version: Arc<Box<dyn std::any::Any>>) -> u64 {
        let e = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;
        self.retired.lock().unwrap().push((e, version));
        e
          }

       /// Free every deferred version whose grace period has elapsed, i.e. where every
       /// active reader entered at or after the retiree's epoch. Returns the count freed.
    pub fn reclaim(&self) -> usize {
        let min_active = self.active.lock().unwrap().iter().copied().min().unwrap_or(u64::MAX);
        let mut q = self.retired.lock().unwrap();
        let mut freed = 0usize;
        for (i, (ep, _v)) in q.iter().enumerate() {
            if *ep <= min_active {
                freed = i + 1;
                } else {
                break;
                }
            }
        q.drain(0..freed);
        freed
          }

          /// Register a reader entry and return its epoch (for test-controlled handles).
          #[doc(hidden)]
    pub fn enter_raw(&self) -> u64 {
        let e = self.epoch.load(Ordering::Acquire);
        self.active.lock().unwrap().push(e);
        e
           }

       /// How many versions are pending reclamation.
    pub fn pending(&self) -> usize {
        self.retired.lock().unwrap().len()
          }
}

/// A reader's critical-section guard. While it lives, its entry epoch blocks reclamation
/// of every version retired at or after that epoch -- no such version can be freed.
#[must_use = "a Reader is only useful because its exit unblocks reclamation"]
pub struct Reader<'a> {
     mgr: &'a EpochManager,
    entry: u64,
 }

impl Drop for Reader<'_> {
    fn drop(&mut self) {
        let mut active = self.mgr.active.lock().unwrap();
        if let Some(i) = active.iter().position(|&e| e == self.entry) {
            active.remove(i);
             }
          }
}

/// An epoch-guarded cell: one current version, old versions deferred to a grace-period
/// queue.
pub struct Rcu<T: 'static> {
    current: Mutex<Arc<T>>,
    mgr: EpochManager,
}

impl<T: 'static> Rcu<T> {
    pub fn new(v: T) -> Self {
        Rcu {
            current: Mutex::new(Arc::new(v)),
            mgr: EpochManager::new(),
             }
            }

           /// Enter a critical section and clone the current version. The returned guard
           /// keeps the read version pinned for the caller's lifetime; a concurrent
           /// `publish` cannot reclaim it.
    pub fn snapshot(&self) -> (Reader<'_>, Arc<T>) {
        let reader = self.mgr.enter();
        let v = self.current.lock().unwrap().clone();
        (reader, v)
             }

           /// Publish a new version, deferring the old one into the grace-period queue.
    pub fn publish(&self, v: T) {
        let old = std::mem::replace(&mut *self.current.lock().unwrap(), Arc::new(v));
        self.mgr.retire(to_boxed(old));
             }

           /// Free any deferred version whose grace period has elapsed.
    pub fn reclaim(&self) -> usize {
        self.mgr.reclaim()
             }

           /// Versions waiting to be reclaimed.
    pub fn pending(&self) -> usize {
        self.mgr.pending()
             }

          /// Reach the epoch manager (used by the tests to drive critical sections).
         #[doc(hidden)]
    pub fn _mgr(&self) -> &EpochManager {
            &self.mgr
             }
}

fn to_boxed<T: 'static>(arc: Arc<T>) -> Arc<Box<dyn std::any::Any>> {
    Arc::new(Box::new(arc) as Box<dyn std::any::Any>)
     }

// ============================ unit tests ============================

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::atomic::AtomicBool;

         /// A version that records its own drop, so *true* reclamation (all refs gone)
         /// is observable when no clone is held.
    struct Tracked {
         _id: u32,
        dropped: Arc<AtomicBool>,
          }
    impl Drop for Tracked {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
              }
           }

          /// The essence of epoch reclamation: while a reader that began *before* the
          /// publish is inside its critical section, its entry epoch blocks the grace
          /// period, so the retired version cannot be reclaimed.
           #[test]
    fn deferred_not_freed_while_reader_active() {
        let rc = Rcu::new(0u32);
        let r1 = _enter(&rc);                          // reader R1 at epoch 0
        rc.publish(1);                                  // v0 (id 0) retired, ep = 1
        assert_eq!(rc.pending(), 1, "v0 deferred, not yet reclaimable");
        assert_eq!(rc.reclaim(), 0, "grace must not elapse while R1 (entry 0 < ep 1) is active");
        assert_eq!(rc.pending(), 1, "still pending: R1's epoch blocks the grace period");
        r1.exit();                                      // R1 exits -> quiescent
        assert_eq!(rc.reclaim(), 1, "grace elapses once R1 has exited");
        assert_eq!(rc.pending(), 0, "v0 reclaimed");
          }

          /// A second, later-entering observer shows an OLD reader is required.
           #[test]
    fn later_reader_does_not_block_earlier_retiree() {
        let rc = Rcu::new(0u32);
        rc.publish(1);                                   // retiree ep = 1 (no one active yet)
        // A reader that enters NOW (epoch 1) does NOT hold the v1-erased version... but it
        // entered after the retiree, so it must not block that retiree.
        let r = _enter(&rc);
        assert_eq!(rc.reclaim(), 1, "reader entered at/after ep may be ignored for ep-1 retiree");
        r.exit();
          }

          /// With no readers at all, reclamation frees on the next call.
           #[test]
    fn reclaims_when_quiescent() {
        let rc = Rcu::new(0u32);
        rc.publish(1);
        assert_eq!(rc.pending(), 1);
        assert_eq!(rc.reclaim(), 1, "free immediately when no reader is active");
        assert_eq!(rc.pending(), 0);
          }

          /// A reader's pin lasts only as long as its guard; releasing the clone unblocks
          /// a final, genuine reclamation (the version's Drop fires).
           #[test]
    fn clone_released_then_version_drops() {
        let flag = Arc::new(AtomicBool::new(false));
        let rc = Rcu::new(Tracked { _id: 0, dropped: flag.clone() });
          // Publish a fresh current while a reader holds v0, then drop everything.
        {
            let (_g, _v0) = rc.snapshot();              // pins v0
            rc.publish(Tracked { _id: 1, dropped: Arc::new(AtomicBool::new(false)) });
            assert_eq!(rc.reclaim(), 0, "v0 cannot be reclaimed while _v0/_g live");
             // scope end -> clone + guard drop
              }
        assert!(!flag.load(Ordering::SeqCst), "v0 still queued until reclaimed");
        assert_eq!(rc.reclaim(), 1, "v0 reclaimable once no refs remain");
        assert!(flag.load(Ordering::SeqCst), "v0 Drop fired after reclamation");
          }

          /// Every active reader must leave before any retiree frees.
           #[test]
    fn all_readers_must_quiesce() {
        let rc = Rcu::new(0u32);
        let r1 = _enter(&rc);
        let r2 = _enter(&rc);
        rc.publish(1);
        assert_eq!(rc.reclaim(), 0, "defer while either reader is active");
        r1.exit();
        assert_eq!(rc.reclaim(), 0, "r2 still active -> defer");
        r2.exit();
        assert_eq!(rc.reclaim(), 1, "all gone -> free");
          }

         /// A small reader handle with an explicit `.exit()`, for clean test control.
    struct R<'a>(&'a EpochManager, u64);
    impl<'a> R<'a> {
        fn exit(self) {
            let mut active = self.0.active.lock().unwrap();
            if let Some(i) = active.iter().position(|&e| e == self.1) {
                active.remove(i);
                  }
               }
          }
    fn _enter(rc: &Rcu<u32>) -> R<'_> {
        R(rc._mgr(), rc._mgr().enter_raw())
          }
}