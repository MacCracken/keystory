//! # Crash runner (Phase 1 acceptance harness)
//!
//! A tiny executable used by the crash-recovery integration test. Two sub-commands:
//!
//! * `run <dir> [n]`    open the store at `dir`, durably (WAL-fsync'd) commit `n`
//!   keys (default 1000), write a `CHILD_DONE` marker in that directory, then
//!   **block** so the parent may kill it at will.
//! * `verify <dir> <n>` re-open the same store *fresh* and assert that all `n` keys are
//!   present with the exact values written before the crash; print `OK`.
//!
//! SIGKILL (`kill -9`) cannot be caught or handled, so it is the strongest "abrupt
//! crash" simulation available on a single host without a hypervisor. The whole point
//! is that **every write was `fsync`'d to disk before it was observable**, so a SIGKILL
//! mid-run cannot lose a committed write: recovery replays the durable WAL.
//!
//! Std-only: the signal is delivered by shelling out to the OS `kill(1)` utility, so we
//! take no `libc` dependency (honouring the Phase-1 "no dependencies" constraint).

use keystory::Store;
use std::path::{Path, PathBuf};
use std::process::{exit, Command};

/// Sub-command: durably write `n` keys, signal "done", then block until killed.
fn run(dir: &Path, n: u64) {
    let store = match Store::open(dir) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("run: open failed: {e}");
            exit(1);
        }
    };

    // Each `put` appends to the WAL and `fsync`s it, so it is durable before the next
    // `get` could observe it. We commit `n` of them: k00000000 .. k(n-1).
    for i in 0..n {
        let key = format!("k{i:08}");
        match store.put(key.as_bytes(), format!("v{i:08}")) {
            Ok(_) => {}
            Err(e) => {
                eprintln!("run: put {i} failed: {e}");
                exit(1);
            }
        }
    }

    // Announce completion with a marker file so the parent can SIGKILL us *after* all
    // writes are durable. Then block forever so the kill we receive is an abrupt,
    // uncaught crash rather than a clean exit.
    let marker = dir.join("CHILD_DONE");
    if std::fs::write(&marker, b"1").is_err() {
        eprintln!("run: failed to write done marker");
        exit(1);
    }
    // Best-effort global sync on POSIX (ignored on failure / non-POSIX).
    let _ = Command::new("sync").status();
    loop {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// Sub-command: re-open the store fresh and assert all `n` keys recovered.
fn verify(dir: &Path, n: u64) {
    let store = match Store::open(dir) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("verify: open failed: {e}");
            exit(1);
        }
    };

    if store.len() < n as usize {
        eprintln!("verify: only {} of {} keys recovered", store.len(), n);
        exit(1);
    }
    for i in 0..n {
        let key = format!("k{i:08}");
        let got = store.get(key.as_bytes());
        let want = format!("v{i:08}");
        if got.as_deref() != Some(want.as_bytes()) {
            eprintln!("verify: key {key} = {got:?}, expected {want:?}");
            exit(1);
        }
    }
    // Confirm a fresh checkpoint now reflects the recovered state.
    match store.checkpoint() {
        Ok(_) => println!("OK {n}"),
        Err(e) => {
            eprintln!("verify: post-recovery checkpoint failed: {e}");
            exit(1);
        }
    }
}

fn usage() -> ! {
    eprintln!("usage: keystory-crash-runner run <dir> [n]   (n defaults to 1000)");
    eprintln!("       keystory-crash-runner verify <dir> <n>");
    exit(2);
}

/// Parse the optional key count; `default` is used when the argument is absent, and a
/// sub-command with no default requires it.
fn parse_n(arg: Option<&String>, default: Option<u64>) -> u64 {
    match (arg, default) {
        (Some(s), _) => s.parse().unwrap_or_else(|_| {
            eprintln!("invalid key count {s:?}");
            exit(2)
        }),
        (None, Some(d)) => d,
        (None, None) => usage(),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = match args.get(2) {
        Some(d) => PathBuf::from(d),
        None => usage(),
    };
    match args.get(1).map(String::as_str) {
        Some("run") => run(&dir, parse_n(args.get(3), Some(1000))),
        Some("verify") => verify(&dir, parse_n(args.get(3), None)),
        Some(other) => {
            eprintln!("unknown sub-command {other:?}");
            usage();
        }
        None => usage(),
    }
}
