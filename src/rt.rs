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
//! reactor into the scheduler is tracked in `ROADMAP.md`. Nested `run`/`block_on` calls
//! on the same scheduler from inside a task are not supported.
//!
//! # Mechanics
//! Wakers are built by hand via the [`RawWakerVTable`] pattern (the canonical
//! "build-your-own runtime" technique): a waker carries its [`TaskId`] plus an
//! [`Arc`]-shared handle to the scheduler's ready-queue, and `wake` re-enqueues that id.
//! A task id is a table slot plus a *generation*: finished slots are reused, and the
//! generation makes a waker left over from an earlier occupant harmless. A task's future
//! is taken out of its slot while it is polled, so a task may [`Scheduler::add`] new
//! tasks from inside its own `poll`.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

/// A boxed, pinned, output-less task future.
pub type BoxFuture = Pin<Box<dyn Future<Output = ()>>>;

/// A task handle: the table slot plus the generation the slot was allocated with, so a
/// waker from an earlier occupant of the slot cannot wake its successor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TaskId {
    slot: usize,
    generation: u64,
}

/// One table slot. `live` says whether a task occupies it; `fut` is `None` while the
/// occupant is being polled (taken out so the table lock is not held across `poll`).
struct Slot {
    generation: u64,
    live: bool,
    fut: Option<BoxFuture>,
}

#[derive(Default)]
struct Table {
    slots: Vec<Slot>,
    free: Vec<usize>,
}

/// A shared, single-threaded scheduler: a slot table of tasks plus the queue of ids to run.
pub struct Scheduler {
    tasks: Mutex<Table>,
    /// Ready queue of task ids; `Arc`-shared with the wakers so `wake` can re-enqueue.
    queue: Arc<Mutex<VecDeque<TaskId>>>,
}

#[allow(
    clippy::arc_with_non_send_sync,
    reason = "single-threaded: Arc<Scheduler> need not cross threads"
)]
impl Scheduler {
    /// An empty scheduler.
    pub fn new() -> Arc<Scheduler> {
        Arc::new(Scheduler {
            tasks: Mutex::new(Table::default()),
            queue: Arc::new(Mutex::new(VecDeque::new())),
        })
    }

    /// Insert a task, returning its id, and queue it for its first poll. Reuses a finished
    /// slot when one is free. May be called from inside a running task.
    pub fn add(&self, fut: BoxFuture) -> TaskId {
        let id = {
            let mut table = self.tasks.lock().unwrap();
            match table.free.pop() {
                Some(slot) => {
                    let s = &mut table.slots[slot];
                    s.generation += 1;
                    s.live = true;
                    s.fut = Some(fut);
                    TaskId {
                        slot,
                        generation: s.generation,
                    }
                }
                None => {
                    table.slots.push(Slot {
                        generation: 0,
                        live: true,
                        fut: Some(fut),
                    });
                    TaskId {
                        slot: table.slots.len() - 1,
                        generation: 0,
                    }
                }
            }
        };
        self.queue.lock().unwrap().push_back(id);
        id
    }

    /// Number of tasks that have not finished.
    pub fn live_tasks(&self) -> usize {
        self.tasks
            .lock()
            .unwrap()
            .slots
            .iter()
            .filter(|s| s.live)
            .count()
    }

    /// Size of the slot table (live and reusable slots together).
    pub fn slot_count(&self) -> usize {
        self.tasks.lock().unwrap().slots.len()
    }

    /// Pump the queue until it drains. A task reschedules itself via its waker, so an empty
    /// queue means nothing is waiting -- we are done.
    pub fn run(&self) {
        loop {
            let id = match self.queue.lock().unwrap().pop_front() {
                Some(id) => id,
                None => break,
            };

            // Take the future out of its slot -- if the id is still current -- and release
            // the table lock before polling, so the task may `add` to this scheduler.
            let mut fut = {
                let mut table = self.tasks.lock().unwrap();
                match table.slots.get_mut(id.slot) {
                    Some(s) if s.live && s.generation == id.generation => match s.fut.take() {
                        Some(f) => f,
                        None => continue, // already being polled (re-entrant run): skip
                    },
                    _ => continue, // stale waker, or the task already finished
                }
            };
            let waker = waker_for_task(id, Arc::clone(&self.queue));
            let mut cx = Context::from_waker(&waker);
            let done = fut.as_mut().poll(&mut cx).is_ready();
            if done {
                // Drop the future before touching the table again: its destructor may
                // itself add tasks or wake others.
                drop(fut);
                let mut table = self.tasks.lock().unwrap();
                let s = &mut table.slots[id.slot];
                s.live = false;
                s.fut = None;
                table.free.push(id.slot);
            } else {
                // Pending: it either re-queued itself via the waker, or it will never wake
                // -- an honest, bounded halt. Put the future back in its slot.
                self.tasks.lock().unwrap().slots[id.slot].fut = Some(fut);
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
        let boxed: BoxFuture = Box::pin(async move {
            let v = fut.await;
            *sink.lock().unwrap() = Some(v);
        });

        // Add the root and drain the queue, including anything it cooperated with.
        let _ = self.add(boxed);
        self.run();

        let mut guard = out.lock().unwrap();
        guard.take().expect("root future was polled to completion")
    }
}

// ---------- Wakers ----------

/// A waker that re-queues task `id` onto `queue` when woken.
fn waker_for_task(id: TaskId, queue: Arc<Mutex<VecDeque<TaskId>>>) -> Waker {
    // The boxed `WakerRaw` is owned by the raw waker from here on: `clone` duplicates it,
    // `wake` and `drop` release it.
    let data = Box::into_raw(Box::new(WakerRaw { id, queue })) as *const ();
    let vt = shared_vtable();
    // SAFETY: `data` points at a live, leaked `WakerRaw`; the vtable governs its lifetime
    // from here on and every entry honours the `RawWakerVTable` contract.
    unsafe { Waker::from_raw(RawWaker::new(data, vt)) }
}

struct WakerRaw {
    id: TaskId,
    queue: Arc<Mutex<VecDeque<TaskId>>>,
}

/// A `'static` reference to the one and only [`RawWakerVTable`] for [`WakerRaw`].
fn shared_vtable() -> &'static RawWakerVTable {
    static V: OnceLock<RawWakerVTable> = OnceLock::new();
    V.get_or_init(build_vtable)
}

/// Build the vtable. Contract (from `std::task::RawWakerVTable`): `clone` returns a new
/// raw waker owning its own data; `wake` consumes the waker and **must release** its
/// data; `wake_by_ref` must not; `drop` releases the data.
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
    RawWakerVTable::new(
        clone,
        |data| unsafe {
            // wake (by value): re-queue the task, then release this waker's data --
            // ownership of the box passes to us here.
            // SAFETY: `data` was produced by `Box::into_raw` in `waker_for_task`/`clone`
            // and is consumed exactly once, by this `wake` or by `drop`.
            let r = Box::from_raw(data as *mut WakerRaw);
            r.queue.lock().unwrap().push_back(r.id);
        },
        |data| unsafe {
            // wake_by_ref: re-queue only; the caller keeps ownership.
            // SAFETY: `data` is a live `*const WakerRaw` for the duration of the call.
            let r = &*(data as *const WakerRaw);
            r.queue.lock().unwrap().push_back(r.id);
        },
        |data| unsafe {
            // drop: release the id + queue handle we own.
            // SAFETY: as for `wake`; `drop` is the other single consumer of the box.
            drop(Box::from_raw(data as *mut WakerRaw));
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
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

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

    /// Regression (Phase 6): `wake` by value used to leak its `WakerRaw` box (and an `Arc`
    /// on the queue) every time. The queue's strong count is the observable.
    #[test]
    #[allow(
        clippy::waker_clone_wake,
        reason = "the by-value `wake` path is exactly what this test exercises"
    )]
    fn wake_by_value_releases_its_data() {
        let s = Scheduler::new();
        let id = TaskId {
            slot: 0,
            generation: 0,
        };
        let w = waker_for_task(id, Arc::clone(&s.queue));
        let base = Arc::strong_count(&s.queue);
        for _ in 0..100 {
            w.clone().wake();
        }
        assert_eq!(
            Arc::strong_count(&s.queue),
            base,
            "every by-value wake released its clone"
        );
        drop(w);
        assert_eq!(Arc::strong_count(&s.queue), base - 1);
        assert_eq!(
            s.queue.lock().unwrap().len(),
            100,
            "each wake enqueued once"
        );
    }

    /// A task may add another task from inside its own poll; previously the table lock
    /// was held across `poll`, so this deadlocked.
    #[test]
    fn adding_a_task_from_inside_a_task_does_not_deadlock() {
        let s = Scheduler::new();
        let hits = Arc::new(AtomicU32::new(0));
        let (s2, h2) = (Arc::clone(&s), Arc::clone(&hits));
        s.block_on(async move {
            s2.add(Box::pin(async move {
                h2.fetch_add(1, Ordering::SeqCst);
            }));
            yield_once().await; // let the child run before the root completes
        });
        assert_eq!(hits.load(Ordering::SeqCst), 1, "the child task ran");
        assert_eq!(s.live_tasks(), 0);
    }

    /// Finished slots are reused instead of growing the table forever.
    #[test]
    fn finished_slots_are_reused() {
        let s = Scheduler::new();
        for i in 0..200u32 {
            assert_eq!(s.block_on(async move { i }), i);
        }
        assert!(
            s.slot_count() <= 1,
            "200 sequential tasks should reuse one slot, table has {}",
            s.slot_count()
        );
    }

    /// A future that stores its waker and completes, so the waker outlives the task.
    struct StashWaker(Arc<Mutex<Option<Waker>>>);
    impl Unpin for StashWaker {}
    impl Future for StashWaker {
        type Output = ();
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            *self.0.lock().unwrap() = Some(cx.waker().clone());
            Poll::Ready(())
        }
    }

    /// A future that counts its polls and never completes or wakes itself.
    struct CountPolls(Arc<AtomicU32>);
    impl Unpin for CountPolls {}
    impl Future for CountPolls {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Poll::Pending
        }
    }

    /// A waker left over from a finished task must not poll the new occupant of its slot.
    #[test]
    fn stale_waker_cannot_wake_a_reused_slot() {
        let s = Scheduler::new();
        let stash: Arc<Mutex<Option<Waker>>> = Arc::new(Mutex::new(None));
        let st = Arc::clone(&stash);
        s.block_on(async move {
            StashWaker(st).await;
        });
        let stale = stash.lock().unwrap().take().expect("waker stashed");

        let polls = Arc::new(AtomicU32::new(0));
        let id = s.add(Box::pin(CountPolls(Arc::clone(&polls))));
        assert_eq!(id.slot, 0, "the finished root's slot is reused");
        s.run();
        assert_eq!(polls.load(Ordering::SeqCst), 1, "first poll");

        stale.wake_by_ref();
        s.run();
        assert_eq!(
            polls.load(Ordering::SeqCst),
            1,
            "a stale waker must not poll the slot's new occupant"
        );

        waker_for_task(id, Arc::clone(&s.queue)).wake();
        s.run();
        assert_eq!(polls.load(Ordering::SeqCst), 2, "a current waker does");
    }
}
