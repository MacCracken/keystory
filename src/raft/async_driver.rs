//! Async end-to-end Raft -- the same replication logic reached through an [`async fn`] API that
//! runs on the std-only, no-deps cooperative runtime in [`crate::rt`].
//!
//! # Design: an async API over a synchronous core, honestly scoped
//! There is no asynchronous I/O source yet (non-blocking sockets need `libc`; see the Phase-4
//! scope note). So this driver does not await a real I/O boundary -- it awaits a modeled one
//! ([`crate::rt::yield_once`], a one-shot cooperative yield) *between* the synchronous replicated
//! steps. This proves the *programming model* end-to-end: a workload is written as one async
//! block, the cooperative scheduler drives every `.await` point, and the replicated result is
//! identical to the synchronous driver's. The runtime, the futures, and the wakers are all real
//! (no runtime crate); only the I/O *source* is modeled.
//!
//! Each async function takes **owned** [`Arc`]s so its future is `'static` and can be handed to
//! [`Scheduler::block_on`], which consumes a `'static` future. The cluster is wired to a shared
//! [`Model`](crate::checker::Model) via [`RaftCluster::with_model`], so the replicated results
//! are checked for linearisability exactly as in the synchronous tests.

use std::sync::Arc;

use crate::raft::{ClusterError, RaftCluster};
use crate::rt::{Scheduler, yield_once};

/// Run a single `'static` async workload to completion on a fresh cooperative scheduler.
///
/// `work` is an async block closing over `Arc` handles; it is polled by the runtime until every
/// `.await` resolves. This is the honest "async end-to-end" entry point.
pub fn run<F>(work: F) -> F::Output
where
     F: std::future::Future + 'static,
     F::Output: Send + 'static,
     {
     Scheduler::new().block_on(work)
      }

/// Replicated put through the async API: model the async boundary, then apply synchronous quorum
/// replication. Returns the committed index. Behaves identically to [`RaftCluster::put`], but
/// reached the async way; the wired `Model` is checked exactly as in the synchronous driver.
pub async fn put(
     cluster: Arc<RaftCluster>,
     key: Vec<u8>,
     val: Vec<u8>,
       ) -> Result<u64, ClusterError> {
       // Model the I/O boundary (no real source yet) so the .await is genuinely driven.
    yield_once().await;
    cluster.put(&key, &val)
      }

/// Replicated read through the async API; `None` if the key was never written or was deleted.
pub async fn get(cluster: Arc<RaftCluster>, key: Vec<u8>) -> Result<Option<Vec<u8>>, ClusterError> {
     yield_once().await;
     cluster.get(&key)
       }

/// Replicated scan over a key prefix through the async API.
pub async fn scan(
     cluster: Arc<RaftCluster>,
     prefix: Vec<u8>,
       ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ClusterError> {
     yield_once().await;
     cluster.scan(&prefix)
       }

#[cfg(test)]
mod test {
    use super::{get, put, run};
    use crate::checker::{CheckModel, Model};
    use crate::raft::RaftCluster;
    use std::sync::Arc;

        /// A whole async workload -- many puts, then reads -- runs on the cooperative runtime and
        /// converges exactly like the synchronous driver, with the shared Model reporting zero
        /// linearisability violations.
        #[test]
    fn async_workload_converges() {
        let model = Arc::new(Model::new());
        let cluster = Arc::new(RaftCluster::with_model(3, model.clone()));
        let cluster_check = cluster.clone();

         // One async block, driven by the runtime.
        run(async move {
           for i in 0..60u64 {
               let c = cluster_check.clone();
               let key: Vec<u8> = vec![b'k', i as u8];
               let val: Vec<u8> = vec![b'v', i as u8];
                put(c, key, val).await.unwrap();
              }
            for _ in 0..20 {
                 let c = cluster_check.clone();
                 let got = get(c, vec![b'k', 0u8]).await.unwrap();
                 assert_eq!(got, Some(vec![b'v', 0u8]));
                }
              });

        let r = model_check_check(&model);
        assert!(r.violations.is_empty(), "async workload must be linearizable");
        assert!(r.checked > 0, "we recorded reads to validate");
     }

        /// An async workload that fails over mid-run still converges -- async changes nothing about
        /// the Raft guarantee; the Model stays clean.
        #[test]
    fn async_failover_then_converges() {
        let model = Arc::new(Model::new());
        let cluster = Arc::new(RaftCluster::with_model(3, model.clone()));

        let c1 = cluster.clone();
        run(async move {
             for i in 0..30u64 {
                 let c = c1.clone();
                 let key: Vec<u8> = vec![b'f', i as u8];
                 let val: Vec<u8> = vec![b'f', i as u8];
                 put(c, key, val).await.unwrap();
                  }
              });

              // Force failover: kill node 3 (the deterministic leader), then keep writing through
              // a new leader.
        cluster.fail(3);
        assert!(cluster.leader().is_some(),
        "a quorum (2 of 3) remains, so a leader re-emerges");

        let c2 = cluster.clone();
        run(async move {
             for i in 0..20u64 {
                 let c = c2.clone();
                 let key: Vec<u8> = vec![b'g', i as u8];
                 let val: Vec<u8> = vec![b'g', i as u8];
                 put(c.clone(), key, val).await.unwrap();
                 let _ = get(c, vec![b'x', 1u8]).await.unwrap();
                  }
              });

        let r = model_check_check(&model);
        assert!(r.violations.is_empty(), "post-failover async workload must be linearizable");
     }

      /// Run the shared Model's linearisability check.
     fn model_check_check(model: &Arc<Model>) -> crate::checker::CheckResult {
        model.check()
        }
}
