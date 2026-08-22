//! Overlay-address math: proximity order and neighborhood assignment.
//!
//! Semantics match bee's `pkg/swarm` (verified against
//! `../ant/crates/ant-p2p/src/routing.rs`, which carries bee's test
//! vectors): proximity is the number of equal leading bits of two
//! 32-byte overlay addresses, capped at [`MAX_PO`].

/// A 32-byte Swarm overlay address (chunk address or node overlay).
pub type SwarmAddress = [u8; 32];

/// bee caps proximity order at 31 (`swarm.MaxPO`).
pub const MAX_PO: u8 = 31;

/// Proximity order of two addresses: leading equal bits, capped at
/// [`MAX_PO`]. Matches bee's `swarm.Proximity`.
#[must_use]
pub fn proximity(a: &SwarmAddress, b: &SwarmAddress) -> u8 {
    for (i, (x, y)) in (0u32..).zip(a.iter().zip(b.iter())) {
        let diff = x ^ y;
        if diff != 0 {
            let po = (i * 8 + diff.leading_zeros()).min(u32::from(MAX_PO));
            return u8::try_from(po).unwrap_or(MAX_PO);
        }
    }
    MAX_PO
}

/// The neighborhood a chunk address belongs to at network depth
/// `depth`: its leading `depth` bits, returned as a right-aligned
/// integer (so depth 9 yields values 0..512). Two addresses share a
/// storer neighborhood iff `neighborhood(a, d) == neighborhood(b, d)`.
///
/// # Panics
/// `depth` must be ≤ 32 (mainnet is ~9–11 today).
#[must_use]
pub fn neighborhood(addr: &SwarmAddress, depth: u8) -> u32 {
    assert!(depth <= 32, "depth {depth} out of range");
    if depth == 0 {
        return 0;
    }
    let prefix = u32::from_be_bytes([addr[0], addr[1], addr[2], addr[3]]);
    prefix >> (32 - u32::from(depth))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(first_bytes: &[u8]) -> SwarmAddress {
        let mut a = [0u8; 32];
        a[..first_bytes.len()].copy_from_slice(first_bytes);
        a
    }

    #[test]
    fn proximity_of_identical_is_max_po() {
        let a = addr(&[0xab, 0xcd]);
        assert_eq!(proximity(&a, &a), MAX_PO);
    }

    #[test]
    fn proximity_first_bit_differs() {
        assert_eq!(proximity(&addr(&[0x00]), &addr(&[0x80])), 0);
    }

    #[test]
    fn proximity_counts_leading_equal_bits() {
        // 0b0100_0000 vs 0b0110_0000: bits 0,1 equal, bit 2 differs.
        assert_eq!(proximity(&addr(&[0x40]), &addr(&[0x60])), 2);
        // equal first byte, second byte differs in its top bit → 8
        assert_eq!(proximity(&addr(&[0xff, 0x00]), &addr(&[0xff, 0x80])), 8);
    }

    #[test]
    fn proximity_caps_at_max_po() {
        // differ only in the very last bit → 255 leading equal bits
        let mut b = [0u8; 32];
        b[31] = 0x01;
        assert_eq!(proximity(&[0u8; 32], &b), MAX_PO);
    }

    #[test]
    fn neighborhood_depth9() {
        // depth 9 → top 9 bits. 0xff80… → 0b1_1111_1111 = 511.
        assert_eq!(neighborhood(&addr(&[0xff, 0x80]), 9), 511);
        assert_eq!(neighborhood(&addr(&[0x00, 0x00]), 9), 0);
        // 0x80… → 0b1_0000_0000 = 256.
        assert_eq!(neighborhood(&addr(&[0x80]), 9), 256);
    }

    #[test]
    fn neighborhood_agrees_with_proximity() {
        let a = addr(&[0b1010_1010, 0b1100_0000]);
        let b = addr(&[0b1010_1010, 0b1100_1111]);
        // 12 leading equal bits → same neighborhood at any depth ≤ 12.
        assert_eq!(proximity(&a, &b), 12);
        for d in 0..=12 {
            assert_eq!(neighborhood(&a, d), neighborhood(&b, d));
        }
        assert_ne!(neighborhood(&a, 13), neighborhood(&b, 13));
    }
}
