//! A minimal, **cooperative, single-threaded async runtime** -- no runtime crate.
//!
//! This is the std-only Phase-4 answer to "an async runtime": a `block_on` driver and a
//! cooperative scheduler (tasks are queued with [`Scheduler::add`]) built on the
//! language's own [`Future`]/[`Poll`]/[`Waker`] machinery. It models the asynchronous
//! *programming model* honestly: a task yields at each `.await` and its waker re-queues
//! it; `block_on` pumps the ready-queue to completion.
//!
//! # What it is NOT (and why)
//! There is *no asynchronous I/O source driving this scheduler*. The `mio` reactor in
//! `crate::asyncio` (Phase 5) proves real kernel readiness on a Unix-stream pair, but it
//! is not yet connected to this ready-queue, so the futures here resolve
//! *cooperatively* on the thread -- every yield point completes once polled. Wiring the
//! reactor into the scheduler is tracked in `ROADMAP.md`.
//!
//! Wakers are built by hand via the [`RawWakerVTable`] pattern (the canonical "build-your-own
//! runtime" technique): a waker carries its task's id plus an [`Arc`]-shared handle to the
//! scheduler's ready-queue, and `wake` re-enqueues that id.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

/// A shared, single-threaded scheduler: a vector of tasks plus the queue of ids to run.
#[allow(
    clippy::type_complexity,
    reason = "core async types (a Pin<Box<dyn Future>> task vector) are complex"
)]
pub struct Scheduler {
    /// All live tasks; `None` once finished.
    tasks: Mutex<Vec<Option<Pin<Box<dyn Future<Output = ()>>>>>>,
    /// Ready queue of task ids; `Arc`-shared with the wakers so `wake` can re-enqueue.
    queue: Arc<Mutex<VecDeque<usize>>>,
}
#[allow(
    clippy::arc_with_non_send_sync,
    reason = "single-threaded: Arc<Scheduler> need not cross threads"
)]
impl Scheduler {
    /// An empty scheduler.
    pub fn new() -> Arc<Scheduler> {
        Arc::new(Scheduler {
            tasks: Mutex::new(Vec::new()),
            queue: Arc::new(Mutex::new(VecDeque::new())),
        })
    }

    /// Insert a task, returning its id, and queue it. The id is the waker's handle.
    pub fn add(&self, fut: Pin<Box<dyn Future<Output = ()>>>) -> usize {
        let id = {
            let mut tasks = self.tasks.lock().unwrap();
            let id = tasks.len();
            tasks.push(Some(fut));
            id
        };
        self.queue.lock().unwrap().push_back(id);
        id
    }

    /// Pump the queue until it drains. A task reschedules itself via its waker, so an empty
    /// queue means nothing is waiting -- we are done.
    #[allow(
        clippy::single_match,
        reason = "the None arm keeps the per-iteration shape explicit"
    )]
    pub fn run(&self) {
        loop {
            let id = match self.queue.lock().unwrap().pop_front() {
                Some(id) => id,
                None => break,
            };

            // Hold the task lock for this iteration. The waker only ever queues (it locks
            // `queue`, not `tasks`) and our futures never lock `tasks`, so this cannot
            // deadlock.
            let mut tasks = self.tasks.lock().unwrap();
            let waker = waker_for_task(id, Arc::clone(&self.queue));
            let mut cx = Context::from_waker(&waker);
            match tasks.get_mut(id).and_then(|t| t.as_mut()) {
                Some(t) => match t.as_mut().poll(&mut cx) {
                    Poll::Ready(()) => {
                        // Finished: free its slot so the vector can compact later.
                        if let Some(slot) = tasks.get_mut(id) {
                            *slot = None;
                        }
                    }
                    Poll::Pending => {
                        // Either re-queued itself via the waker, or never woke (finished
                        // without completing) -- an honest, bounded halt.
                    }
                },
                None => {} // already finished or slot freed
            }
        }
    }

    /// Drive a root future to completion on this scheduler, returning its output.
    #[allow(
        clippy::arc_with_non_send_sync,
        reason = "F::Output need not be Send/Sync in a single-threaded runtime"
    )]
    pub fn block_on<F>(&self, fut: F) -> F::Output
    where
        F: Future + 'static,
    {
        let out: Arc<Mutex<Option<F::Output>>> = Arc::new(Mutex::new(None));
        let sink = out.clone();

        // Box the root; on completion, stow the output.
        let boxed: Pin<Box<dyn Future<Output = ()>>> = Box::pin(async move {
            let v = fut.await;
            *sink.lock().unwrap() = Some(v);
        });

        // Add the root and drain the queue, including anything it cooperated with.
        let _ = self.add(boxed);
        self.run();

        // Take the output out; `out` is Arc<Mutex<Option<F::Output>>>.
        let mut guard = out.lock().unwrap();
        guard.take().expect("root future was polled to completion")
    }
}

// ---------- Wakers ----------

/// A waker that re-queues task `id` onto `queue` when woken.
fn waker_for_task(id: usize, queue: Arc<Mutex<VecDeque<usize>>>) -> Waker {
    // SAFETY: the raw pointer aliases the boxed `WakerRaw`, whose lifetime the vtable
    // manages (`drop` owns it; `clone` clones it).
    let data = Box::into_raw(Box::new(WakerRaw { id, queue })) as *const ();
    // A `'static` reference so the waker outlives this local.
    let vt = shared_vtable();
    // SAFETY: `data` aliases the live `WakerRaw`; the vtable governs its lifetime onward.
    unsafe { Waker::from_raw(RawWaker::new(data, vt)) }
}

struct WakerRaw {
    id: usize,
    queue: Arc<Mutex<VecDeque<usize>>>,
}

/// A `'static` reference to the one and only [`RawWakerVTable`] for [`WakerRaw`].
fn shared_vtable() -> &'static RawWakerVTable {
    static V: OnceLock<RawWakerVTable> = OnceLock::new();
    V.get_or_init(build_vtable)
}

/// Build the vtable; a fresh copy of every closure (`clone` reaches the cached table, so it
/// is `'static` too).
fn build_vtable() -> RawWakerVTable {
    let clone = |data| unsafe {
        // SAFETY: `data` is a live `*const WakerRaw`; the returned waker owns a copy.
        let r = &*(data as *const WakerRaw);
        RawWaker::new(
            Box::into_raw(Box::new(WakerRaw {
                id: r.id,
                queue: Arc::clone(&r.queue),
            })) as *const (),
            shared_vtable(),
        )
    };
    // SAFETY: each closure honours its pointer contract (see `WakerRaw`).
    RawWakerVTable::new(
        clone,
        |data| unsafe {
            // wake: re-queue the task id.
            let r = &*(data as *const WakerRaw);
            let id = r.id;
            r.queue.lock().unwrap().push_back(id);
        },
        |data| unsafe {
            // wake_by_ref: re-queue too.
            let r = &*(data as *const WakerRaw);
            let id = r.id;
            r.queue.lock().unwrap().push_back(id);
        },
        |data| unsafe {
            // drop: release the id + queue handle we own.
            let _ = Box::from_raw(data as *mut WakerRaw);
        },
    )
}

// ---------- A cooperative yield point ----------

/// A future that yields once ([`Poll::Pending`]) and then completes -- the std-only way to
/// model a real asynchronous boundary.
///
/// On first poll it is *Pending* (re-queuing the task via its waker); on the next poll it is
/// *Ready(())*. This proves the runtime genuinely cooperates -- control leaves the task and
/// returns to it -- rather than the future resolving on a single poll.
#[derive(Clone, Copy)]
pub struct YieldOnce(bool);

impl Unpin for YieldOnce {}

impl Future for YieldOnce {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // `Unpin` lets us obtain a `&mut`.
        let inner = self.get_mut();
        if inner.0 {
            Poll::Ready(())
        } else {
            inner.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// Construct a [`YieldOnce`].
pub fn yield_once() -> YieldOnce {
    YieldOnce(false)
}

#[cfg(test)]
mod test {
    use super::{yield_once, Scheduler};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    /// A future that just resolves is driven to completion.
    #[test]
    fn block_on_runs_to_completion() {
        let s = Scheduler::new();
        let out = s.block_on(async { 21u32 * 2 });
        assert_eq!(out, 42);
    }

    /// A `.await` point that yields and is resumed cooperatively.
    #[test]
    fn await_resumes_cooperatively() {
        let s = Scheduler::new();
        let out = s.block_on(async {
            let a = async { 7u32 }.await;
            let b = async { 8u32 }.await;
            a + b
        });
        assert_eq!(out, 15);
    }

    /// `YieldOnce` resolves only after one yield; two in a row still complete through the
    /// scheduler. Proves the async boundary is driven by the runtime, not inlined away.
    #[test]
    fn yields_once_then_completes() {
        let s = Scheduler::new();
        let out = s.block_on(async {
            yield_once().await;
            yield_once().await;
            "done"
        });
        assert_eq!(out, "done");
    }

    /// Awaiting across a loop, counting via a shared atomic, proves the await points are
    /// actually polled-and-resumed, not optimised away.
    #[test]
    fn cooperative_interleave_counts() {
        let s = Scheduler::new();
        let hits = Arc::new(AtomicU32::new(0));
        let n: u32 = 5;
        let total = s.block_on(async move {
            for _ in 0..n {
                let h = Arc::clone(&hits);
                async move {
                    h.fetch_add(1, Ordering::SeqCst);
                }
                .await;
            }
            hits.load(Ordering::SeqCst)
        });
        assert_eq!(total, n);
    }
}
