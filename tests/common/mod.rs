//! Shared helpers for the integration tests.
#![allow(dead_code)] // each test binary uses a subset of these

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// A unique scratch directory under the system temp dir, removed on drop -- including
/// when the test panics, so failed runs do not leak directories.
pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new(name: &str) -> TempDir {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let d = std::env::temp_dir().join(format!(
            "keystory-{name}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).expect("create temp dir");
        TempDir(d)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
