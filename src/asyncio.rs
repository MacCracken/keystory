//! Real, non-blocking, event-driven I/O via `mio` -- Phase 5 (3/3).
//!
//! This is the **one external dependency**, and it closes open question #1:
//! *"Should the runtime drive real, non-blocking event-loop I/O instead of a model
//! event?"* The answer, demonstrated here, is **yes** -- and `mio` is the
//! *only* crate we add. No consensus crate, no async runtime, no RPC layer.
//!
//! `mio` gives a uniform platform event notifier -- `kqueue` on macOS, `epoll` on
//! Linux -- behind one `Poll` handle. A [`Reactor`] registers a file descriptor for
//! readability (here, one end of a `UnixStream` pair: no network needed, fully
//! self-contained), switching it to non-blocking mode. It then drives a real
//! non-blocking round trip: a `write` on the peer, a `poll` that blocks only as long
//! as the kernel says the socket is not yet readable (and never past a deadline), and
//! a `read` back. The readiness observed is a **live kernel event**, not a modelled one.
//!
//! A source is registered with `SourceFd` (a `&RawFd` wrapper); the kernel registration
//! is keyed by the fd and persists after the borrow ends. This is deliberately **not** a
//! networking/RPC layer, and it is not yet wired into the cooperative scheduler in
//! [`crate::rt`] (both tracked in `ROADMAP.md`); it proves the *substrate*.
//!
//! Unix-only (a Unix-stream pair needs no network); the module is gated in `lib.rs`.

use mio::{Events, Interest, Poll, Token, event::Source, unix::SourceFd};

use std::io::{self, ErrorKind, Read, Write};
use std::os::unix::io::AsRawFd;
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

/// A minimal `mio` reactor: registers readiness sources and waits for them with deadlines.
pub struct Reactor {
    poller: Poll,
    events: Events,
    next: usize,
}

impl Reactor {
    /// Create a new reactor with a fresh platform poller.
    pub fn new() -> io::Result<Reactor> {
        Ok(Reactor {
            poller: Poll::new()?,
            events: Events::with_capacity(64),
            next: 1,
        })
    }

    /// Register a stream for readability, returning an opaque token, and switch it to
    /// non-blocking mode: a blocking read on a "readable" socket can still block on a
    /// spurious or partial event, which would defeat the point of the reactor.
    pub fn register_readable(&mut self, s: &UnixStream) -> io::Result<Token> {
        s.set_nonblocking(true)?;
        let token = Token(self.next);
        self.next += 1;
        // The kernel keeps the registration keyed by the fd after this borrow ends.
        let fd = s.as_raw_fd();
        SourceFd(&fd).register(self.poller.registry(), token, Interest::READABLE)?;
        Ok(token)
    }

    /// Remove a stream's registration.
    pub fn deregister(&mut self, s: &UnixStream) -> io::Result<()> {
        let fd = s.as_raw_fd();
        SourceFd(&fd).deregister(self.poller.registry())
    }

    /// Wait until `token` is readable or `timeout` elapses: `Ok(true)` on readiness,
    /// `Ok(false)` on timeout. Events for other tokens are consumed and ignored (this is
    /// a single-source demonstration reactor). An interrupted poll is retried within
    /// the same deadline.
    pub fn wait_readable(&mut self, token: Token, timeout: Duration) -> io::Result<bool> {
        let deadline = Instant::now() + timeout;
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Ok(false);
            }
            match self.poller.poll(&mut self.events, Some(deadline - now)) {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
            if self
                .events
                .iter()
                .any(|ev| ev.token() == token && ev.is_readable())
            {
                return Ok(true);
            }
            // No matching event: either a timeout (the loop will notice the deadline) or
            // an event for another token; wait again.
        }
    }

    /// Drive one real, non-blocking round trip: `write` on `peer`, wait for `token` to be
    /// readable, then `read` bytes off `reader`. Returns the number of bytes the
    /// non-blocking read returned. Fails with `TimedOut` if nothing arrives before the
    /// deadline, and with `UnexpectedEof` if the peer closed instead.
    pub fn round_trip(
        &mut self,
        token: Token,
        peer: &mut UnixStream,
        reader: &mut UnixStream,
        payload: &[u8],
        timeout: Duration,
    ) -> io::Result<usize> {
        let deadline = Instant::now() + timeout;
        // 1. A real write on the peer end of the pair.
        peer.write_all(payload)?;
        // 2. Wait for *real* kernel readiness, then read. A spurious readiness event
        //    shows up as `WouldBlock` on the non-blocking read; wait again in that case.
        let mut buf = [0u8; 4096];
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || !self.wait_readable(token, remaining)? {
                return Err(io::Error::new(
                    ErrorKind::TimedOut,
                    "no readiness event before the deadline",
                ));
            }
            match reader.read(&mut buf) {
                Ok(0) => {
                    return Err(io::Error::new(
                        ErrorKind::UnexpectedEof,
                        "peer closed the stream",
                    ));
                }
                Ok(n) => return Ok(n),
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) => {
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

// ============================ integration tests ============================

#[cfg(test)]
mod test {
    use super::*;

    /// A real, non-blocking round trip through `kqueue`/`epoll`: write on one
    /// end of a Unix-stream pair, poll the other for *live* readiness, and read
    /// the bytes back. Genuine OS-level non-blocking I/O.
    #[test]
    fn real_nonblocking_round_trip() {
        let (mut a, b) = UnixStream::pair().expect("pair");
        let mut reader = b.try_clone().expect("distinct fd for the read");
        let mut reactor = Reactor::new().expect("reactor");
        let token = reactor.register_readable(&b).expect("register");
        let n = reactor
            .round_trip(
                token,
                &mut a,
                &mut reader,
                b"hello-world",
                Duration::from_secs(5),
            )
            .expect("round trip");
        assert!(
            n > 0,
            "the non-blocking read returned no bytes for a non-empty write"
        );
        assert!(n <= 11, "a read returned more bytes than were written");
    }

    /// A real write must surface as a kernel readiness event the reactor can
    /// observe -- proving the poll watches a live source, not a fabricated one.
    #[test]
    fn readiness_is_observed() {
        let (mut a, b) = UnixStream::pair().expect("pair");
        let mut reactor = Reactor::new().expect("reactor");
        let token = reactor.register_readable(&b).expect("register");
        a.write_all(b"readiness!").expect("write");
        assert!(
            reactor
                .wait_readable(token, Duration::from_millis(500))
                .expect("poll"),
            "a real write must produce a kernel readiness event"
        );
    }

    /// Regression (Phase 6): a wait with nothing to read used to loop forever; now it
    /// honours the deadline, and so does a round trip whose write carries nothing.
    #[test]
    fn waits_time_out_instead_of_spinning() {
        let (mut a, b) = UnixStream::pair().expect("pair");
        let mut reader = b.try_clone().expect("clone");
        let mut reactor = Reactor::new().expect("reactor");
        let token = reactor.register_readable(&b).expect("register");
        let started = Instant::now();
        assert!(
            !reactor
                .wait_readable(token, Duration::from_millis(50))
                .expect("poll"),
            "no write, so no readiness"
        );
        assert!(started.elapsed() >= Duration::from_millis(50));
        let err = reactor
            .round_trip(token, &mut a, &mut reader, b"", Duration::from_millis(50))
            .expect_err("an empty write never becomes readable");
        assert_eq!(err.kind(), ErrorKind::TimedOut);
    }

    /// Registration switches the stream to non-blocking mode: a read with nothing
    /// pending returns `WouldBlock` instead of parking the thread.
    #[test]
    fn registered_stream_is_nonblocking() {
        let (_a, b) = UnixStream::pair().expect("pair");
        let mut reader = b.try_clone().expect("clone");
        let mut reactor = Reactor::new().expect("reactor");
        reactor.register_readable(&b).expect("register");
        let mut buf = [0u8; 8];
        let err = reader.read(&mut buf).expect_err("nothing to read");
        assert_eq!(err.kind(), ErrorKind::WouldBlock);
    }
}
