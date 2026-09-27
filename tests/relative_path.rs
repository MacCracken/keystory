//! # A store stays in the directory it opened
//!
//! Regression (pre-release review, second pass): `Store` kept its directory as given, and
//! resolved it again for every new WAL segment, checkpoint and deletion. Opened through a
//! relative path, it followed the process's working directory: after a `chdir`, new
//! segments landed in another directory, and reopening the original store silently lost
//! the acknowledged writes in them (probe: 3 of 13 recovered). `open` now pins the
//! directory.
//!
//! This file holds a single test because the working directory is process-wide state.

mod common;

use common::TempDir;
use keystory::{Options, Store};
use std::path::PathBuf;

/// Restores the working directory when dropped, even if the test panics.
struct RestoreCwd(PathBuf);

impl Drop for RestoreCwd {
    fn drop(&mut self) {
        let _ = std::env::set_current_dir(&self.0);
    }
}

#[test]
fn a_store_opened_by_a_relative_path_stays_where_it_was_opened() {
    let root = TempDir::new("relative");
    let (a, b) = (root.path().join("a"), root.path().join("b"));
    // Both working directories hold a `store` directory, so a path resolved against the
    // wrong one would succeed silently rather than fail loudly.
    std::fs::create_dir_all(a.join("store")).unwrap();
    std::fs::create_dir_all(b.join("store")).unwrap();
    let _restore = RestoreCwd(std::env::current_dir().unwrap());

    std::env::set_current_dir(&a).unwrap();
    let s = Store::open_with("store", Options::new().wal_segment_size(256)).unwrap();
    s.put("k00", "v").unwrap();
    std::env::set_current_dir(&b).unwrap();
    for i in 1..40 {
        s.put(format!("k{i:02}"), "v").unwrap(); // tiny segments: many rotations
    }
    s.checkpoint().unwrap(); // checkpoint files
    for i in 40..50 {
        s.put(format!("k{i:02}"), "v").unwrap();
    }
    s.checkpoint().unwrap(); // and the deletion of reclaimed segments
    drop(s);

    let s = Store::open(a.join("store")).unwrap();
    assert_eq!(
        (s.len(), s.index()),
        (50, 50),
        "every acknowledged write is in the store that was opened"
    );
    assert_eq!(
        std::fs::read_dir(b.join("store")).unwrap().count(),
        0,
        "nothing landed in the other directory"
    );
}
