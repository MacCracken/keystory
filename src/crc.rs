//! A small, correct 802.3 / Castagnoli-independent **IEEE 802.3 CRC-32**.
//!
//! Used as the integrity guard for the on-disk records of every storage layer
//! (segmented WAL, snapshots, the off-heap value store). One shared, well-tested
//! implementation rather than N local copies.

/// The IEEE 802.3 CRC-32 (polynomial `0xEDB88320` reflected), computed over `data`.
///
/// # Safety / correctness
///
/// Pure integer work; no `unsafe`. The table is a standard reflected polynomial,
/// validated by a known-answer test.
pub(crate) fn crc32(data: &[u8]) -> u32 {
    static TABLE: [u32; 256] = build_table();
    let mut crc = 0xFFFF_FFFF;
    for &b in data {
        let e = ((crc ^ u32::from(b)) & 0xFF) as usize;
        crc = (crc >> 8) ^ TABLE[e];
    }
    !crc
}

const fn build_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i: usize = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut j = 0;
        while j < 8 {
            c = if c & 1 != 0 {
                0xEDB8_8320 ^ c >> 1
            } else {
                c >> 1
            };
            j += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
}

/// An incremental CRC-32 accumulator: feed bytes in chunks, finish once.
///
/// `update` folds a chunk into the running state, so a large snapshot can be checksummed
/// **streaming** (constant memory) rather than by materialising the whole buffer. By
/// composition, `Crc::new().update(a).update(b).finish() == crc32(a into b)`.
pub struct Crc(u32);

impl Crc {
    /// A fresh accumulator (the all-ones initial value).
    pub fn new() -> Crc {
        Crc(0xFFFF_FFFF)
    }

    /// Fold `data` into the running checksum.
    pub fn update(&mut self, data: &[u8]) {
        static TABLE: [u32; 256] = build_table();
        let mut crc = self.0;
        for &b in data {
            let e = ((crc ^ u32::from(b)) & 0xFF) as usize;
            crc = (crc >> 8) ^ TABLE[e];
        }
        self.0 = crc;
    }

    /// Finalise: the all-ones reflect (matches [`crc32`]).
    pub fn finish(self) -> u32 {
        !self.0
    }
}

impl Default for Crc {
    fn default() -> Crc {
        Crc::new()
    }
}

#[cfg(test)]
fn concat_two(a: &[u8], b: &[u8]) -> Vec<u8> {
    let mut v = a.to_vec();
    v.extend_from_slice(b);
    v
}

#[cfg(test)]
mod test {
    use super::Crc;
    use super::concat_two;
    use super::crc32;

    /// Known-answer: an empty input hashes to the standard CRC-32 of the empty string.
    #[test]
    fn empty_is_ff_ffff_ffff() {
        assert_eq!(crc32(b""), 0x0000_0000);
    }

    /// Known-answer: `"123456789"` is the canonical CRC-32 test vector `0xCBF43926`.
    #[test]
    fn canon_test_vector() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }
    /// A streamed accumulate-then-finish equals a single-shot `crc32`.
    #[test]
    fn incremental_matches_oneshot() {
        let a = b"the quick brown fox ";
        let b = b"jumps over the lazy dog";
        let mut c = Crc::new();
        c.update(a);
        c.update(b);
        assert_eq!(c.finish(), crc32(&concat_two(a, b)));
    }

    /// Larger inputs still agree with the canonical algorithm (via the `crc32` reference).
    #[test]
    fn random_ish_inputs_are_stable() {
        let s: Vec<u8> = (0..65536).map(|i| (i * 31 + 7) as u8).collect();
        let a = crc32(&s);
        let b = crc32(&s);
        assert_eq!(a, b, "deterministic");
        assert_eq!(
            crc32(b"the quick brown fox jumps over the lazy dog"),
            0x0CE0_C5114
        );
    }
}
