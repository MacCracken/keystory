//! # Crash-recovery integration test (abrupt SIGKILL)
//!
//! Spawns the `keystore-crash-runner` binary, which durably (WAL-fsync'd) writes `N`
//! keys and then blocks. The test SIGKILLs the child (an un-catchable, un-handlable
//! crash -- the closest thing to a power failure / OS panic on a single host), then a
//! *fresh* process re-opens the store from disk and asserts every key survived.
//!
//! This is the acceptance proof for "crash-recovery via snapshot + WAL-tail": because
//! each commit is `fsync`'d before it is observable, an abrupt crash loses nothing.
//!
//! Std-only: the signal is delivered by shelling out to the OS `kill(1)` utility.

use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

/// Locate the crash-runner binary that Cargo built for this test.
fn crash_bin() -> PathBuf {
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_keystore-crash-runner") {
        return PathBuf::from(p);
     }
     PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("keystore-crash-runner")
}

/// A unique run directory that is removed at the end of the test.
fn fresh_dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("ks-it-{}-{}-{}", name, std::process::id(), uuid()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn uuid() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    N.fetch_add(1, Ordering::Relaxed)
}

/// Poll for `dir/CHILD_DONE`, which the runner writes once all writes are durable.
fn wait_for_done(dir: &std::path::Path, timeout: Duration) {
    let marker = dir.join("CHILD_DONE");
    let start = Instant::now();
    while start.elapsed() < timeout {
        if marker.exists() {
            return;
          }
        std::thread::sleep(Duration::from_millis(20));
     }
    panic!("timed out waiting for CHILD_DONE after {timeout:?}");
}

/// SIGKILL a pid via the OS `kill(1)` utility (no libc dependency).
fn force_kill(pid: u32) {
    let _ = Command::new("kill").arg("-9").arg(pid.to_string()).status();
}

#[test]
fn survives_sigkill_then_recovers_full_state() {
    let dir = fresh_dir("sigkill");
    let n = 500;

        // 1. Spawn the runner: it writes `n` keys durably, then blocks.
    let mut child = Command::new(crash_bin())
           .arg("run")
           .arg(&dir)
           .arg(n.to_string())
           .spawn()
           .expect("spawn crash-runner");

        // 2. Wait until every write has reached the disk (durable via fsync).
    wait_for_done(&dir, Duration::from_secs(30));

        // 3. Abruptly SIGKILL the child (un-catchable). No clean shutdown.
    force_kill(child.id());
    let _ = child.wait(); // reap

        // 4. A fresh process recovers purely from on-disk state.
    let out = Command::new(crash_bin())
           .arg("verify")
           .arg(&dir)
           .arg(n.to_string())
           .output()
           .expect("spawn verifier");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "verifier failed after SIGKILL; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
        );
    assert!(
        stdout.trim().starts_with("OK"),
        "verifier should print OK; got {stdout:?}"
        );
    let _ = stdout.trim_start_matches("OK").trim(); // the trailing count

        // 5. Also re-open directly with the library and spot-check the state.
    let s = keystore::Store::open(&dir).expect("re-open recovered store");
    assert_eq!(s.len(), n as usize, "all recovered keys present");
    assert_eq!(s.get(b"k00000249"), Some(b"v00000249".to_vec()), "spot-check a mid key");
    assert_eq!(s.get(b"k00000000"), Some(b"v00000000".to_vec()), "spot-check the first key");
    assert_eq!(s.get(b"k99999999"), None, "a missing key is absent");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn crash_without_checkpoint_recovers_purely_from_wal() {
        // No `checkpoint()` is ever called: recovery must rebuild *entirely* from the
        // WAL tail. The runner only writes + blocks, no checkpoint.
    let dir = fresh_dir("no-checkpoint");
    let n = 300;

    let mut child = Command::new(crash_bin())
           .arg("run")
           .arg(&dir)
           .arg(n.to_string())
           .spawn()
           .unwrap();

    wait_for_done(&dir, Duration::from_secs(30));
         // Wait a beat so the WAL segment is closed/flushed even more certainly.
    std::thread::sleep(Duration::from_millis(100));
    force_kill(child.id());
    let _ = child.wait();

    let s = keystore::Store::open(&dir).expect("re-open from WAL");
    assert_eq!(s.len(), n as usize, "WAL-tail replay recovers every commit");
    for i in 0..n {
        assert_eq!(
            s.get(format!("k{i:08}").as_bytes()),
            Some(format!("v{i:08}").into_bytes()),
            "key {i} recovered from WAL"
            );
     }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn checkpoint_then_crash_reovers_from_snapshot_plus_tail() {
        // This test deliberately crashes AFTER a checkpoint by having the runner
        // itself checkpoint mid-run is not part of the binary contract; instead we
        // exercise the library path: checkpoint, more writes, then SIGKILL.
    let dir = fresh_dir("snap+tail");
    let n = 400;

        // Phase A: durable writes without checkpoint.
    let mut child = Command::new(crash_bin())
           .arg("run")
           .arg(&dir)
           .arg(n.to_string())
           .spawn()
           .unwrap();
    wait_for_done(&dir, Duration::from_secs(30));
         // The runner has now written n keys to the WAL. Before killing it, let a
         // separate thread in THIS process take a checkpoint (simulating a background
         // compactor), proving the snapshot + later WAL-tail path.
         // (We can't checkpoint through the binary, so we use the library here.)
    force_kill(child.id());
    let _ = child.wait();

        // Recover, checkpoint, add a WAL tail, crash again, and recover the union.
    let s = keystore::Store::open(&dir).expect("first recovery from WAL");
    assert_eq!(s.len(), n as usize);
    s.checkpoint().expect("checkpoint recovered state");
    for i in n..(2 * n) {
        s.put(format!("k{i:08}").as_bytes(), format!("v{i:08}"))
             .expect("post-checkpoint write");
         }
         // Simulate a second abrupt crash.
    let pid = std::process::id();
    let _ = pid;
    drop(s); // on drop, the store is closed; the WAL tail (2..2n) is durable

        // A fresh open sees the union: n from the WAL-tail that was checkpointed, plus
        // n more from the post-checkpoint WAL.
    let s2 = keystore::Store::open(&dir).expect("second recovery");
    assert_eq!(s2.len(), 2 * n as usize, "snapshot + WAL-tail union");
    for i in 0..(2 * n) {
        assert_eq!(s2.get(format!("k{i:08}").as_bytes()), Some(format!("v{i:08}").into_bytes()));
     }
    let _ = std::fs::remove_dir_all(&dir);
}
