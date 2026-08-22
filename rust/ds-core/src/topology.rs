//! The topology cache: a bin-organized map of overlay → reachable
//! underlays, with freshness stamps and dial-RTT estimates, plus the
//! neighborhood-coverage queries the scheduler needs.
//!
//! Pure and sans-I/O: the crawler (`ds-net`) feeds records in with
//! caller-supplied timestamps (this crate owns no clock), and asks it
//! which cached storers cover a fetch's chunks.

use crate::swarm::{neighborhood, proximity, SwarmAddress, MAX_PO};
use std::collections::HashMap;

/// One known node: its overlay, the underlays it announced, and how
/// fresh/fast we believe it to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRecord {
    pub overlay: SwarmAddress,
    /// Announced underlay multiaddrs (as strings — ds-core doesn't
    /// depend on a multiaddr type).
    pub underlays: Vec<String>,
    /// Round-trip estimate in milliseconds, when measured (dial +
    /// handshake wall-clock, or a later pingpong).
    pub rtt_ms: Option<u32>,
    /// Caller's monotonic tick when this record was last refreshed.
    /// Freshness is the caller's clock; the cache only stores it.
    pub last_seen_tick: u64,
    /// Whether we have successfully dialed+handshaked this node
    /// ourselves (vs. only heard it gossiped).
    pub dialed_ok: bool,
}

/// Bin-organized topology cache, keyed by overlay.
#[derive(Debug, Clone)]
pub struct TopologyCache {
    /// Our own overlay — the reference for proximity-order binning.
    reference: SwarmAddress,
    nodes: HashMap<SwarmAddress, NodeRecord>,
}

impl TopologyCache {
    /// New empty cache binned against our own overlay.
    #[must_use]
    pub fn new(reference: SwarmAddress) -> Self {
        Self {
            reference,
            nodes: HashMap::new(),
        }
    }

    /// Number of distinct nodes known.
    #[must_use]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Insert or refresh a record. A record that already exists keeps
    /// its `dialed_ok` if the incoming one is only gossip
    /// (`dialed_ok == false`), and takes the fresher `last_seen_tick`,
    /// the measured RTT if the incoming has one, and any new underlays.
    pub fn upsert(&mut self, record: NodeRecord) {
        match self.nodes.get_mut(&record.overlay) {
            Some(existing) => {
                existing.last_seen_tick = existing.last_seen_tick.max(record.last_seen_tick);
                existing.dialed_ok |= record.dialed_ok;
                if record.rtt_ms.is_some() {
                    existing.rtt_ms = record.rtt_ms;
                }
                for underlay in record.underlays {
                    if !existing.underlays.contains(&underlay) {
                        existing.underlays.push(underlay);
                    }
                }
            }
            None => {
                self.nodes.insert(record.overlay, record);
            }
        }
    }

    /// Look up a node by overlay.
    #[must_use]
    pub fn get(&self, overlay: &SwarmAddress) -> Option<&NodeRecord> {
        self.nodes.get(overlay)
    }

    /// All records, unordered.
    pub fn records(&self) -> impl Iterator<Item = &NodeRecord> {
        self.nodes.values()
    }

    /// Count of nodes per proximity-order bin against our reference
    /// (index = PO, `0..=MAX_PO`).
    #[must_use]
    pub fn bin_counts(&self) -> [usize; MAX_PO as usize + 1] {
        let mut bins = [0usize; MAX_PO as usize + 1];
        for record in self.nodes.values() {
            bins[proximity(&self.reference, &record.overlay) as usize] += 1;
        }
        bins
    }

    /// Storers whose overlay shares `chunk`'s neighborhood at `depth`,
    /// i.e. proximity(node, chunk) >= depth. Sorted by RTT ascending
    /// (unmeasured RTTs last), so the caller can dial the fastest
    /// 2–3 members (DESIGN: latency-aware source selection).
    #[must_use]
    pub fn storers_for(&self, chunk: &SwarmAddress, depth: u8) -> Vec<&NodeRecord> {
        let mut hits: Vec<&NodeRecord> = self
            .nodes
            .values()
            .filter(|n| proximity(&n.overlay, chunk) >= depth)
            .collect();
        hits.sort_by_key(|n| n.rtt_ms.unwrap_or(u32::MAX));
        hits
    }

    /// For a whole chunk set, how many chunks have at least one covering
    /// storer in the cache at `depth`, and how many distinct
    /// neighborhoods those chunks fall into. Returns
    /// `(chunks_covered, chunks_total, neighborhoods_total,
    /// neighborhoods_covered)`.
    #[must_use]
    pub fn coverage(&self, chunks: &[SwarmAddress], depth: u8) -> Coverage {
        let mut neighborhoods: HashMap<u32, bool> = HashMap::new();
        let mut chunks_covered = 0usize;
        for chunk in chunks {
            let nbhd = neighborhood(chunk, depth);
            let covered = self
                .nodes
                .values()
                .any(|n| proximity(&n.overlay, chunk) >= depth);
            if covered {
                chunks_covered += 1;
            }
            let entry = neighborhoods.entry(nbhd).or_insert(false);
            *entry = *entry || covered;
        }
        let neighborhoods_covered = neighborhoods.values().filter(|c| **c).count();
        Coverage {
            chunks_covered,
            chunks_total: chunks.len(),
            neighborhoods_total: neighborhoods.len(),
            neighborhoods_covered,
        }
    }
}

/// Result of [`TopologyCache::coverage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Coverage {
    pub chunks_covered: usize,
    pub chunks_total: usize,
    pub neighborhoods_total: usize,
    pub neighborhoods_covered: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn overlay(first: &[u8]) -> SwarmAddress {
        let mut a = [0u8; 32];
        a[..first.len()].copy_from_slice(first);
        a
    }

    fn rec(first: &[u8], rtt: Option<u32>, tick: u64, dialed: bool) -> NodeRecord {
        NodeRecord {
            overlay: overlay(first),
            underlays: vec![format!("/ip4/1.2.3.4/tcp/{}", tick)],
            rtt_ms: rtt,
            last_seen_tick: tick,
            dialed_ok: dialed,
        }
    }

    #[test]
    fn upsert_merges_and_keeps_dialed() {
        let mut cache = TopologyCache::new(overlay(&[0x00]));
        cache.upsert(rec(&[0xab], None, 1, true));
        // gossip re-sighting: newer tick, no rtt, not dialed
        cache.upsert(NodeRecord {
            overlay: overlay(&[0xab]),
            underlays: vec!["/ip4/5.6.7.8/tcp/9".into()],
            rtt_ms: Some(42),
            last_seen_tick: 5,
            dialed_ok: false,
        });
        let got = cache.get(&overlay(&[0xab])).unwrap();
        assert!(got.dialed_ok, "dialed_ok must stick");
        assert_eq!(got.last_seen_tick, 5);
        assert_eq!(got.rtt_ms, Some(42));
        assert_eq!(got.underlays.len(), 2);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn storers_for_sorts_by_rtt() {
        let mut cache = TopologyCache::new(overlay(&[0x00]));
        // all share 12+ leading bits with the chunk 0xff_f0…
        let chunk = overlay(&[0xff, 0xf0]);
        cache.upsert(rec(&[0xff, 0xf0, 0x01], Some(300), 1, true));
        cache.upsert(rec(&[0xff, 0xf0, 0x02], Some(100), 1, true));
        cache.upsert(rec(&[0xff, 0xf0, 0x03], None, 1, true));
        cache.upsert(rec(&[0x00, 0x00], Some(10), 1, true)); // far away
        let hits = cache.storers_for(&chunk, 12);
        assert_eq!(hits.len(), 3, "only same-neighborhood nodes");
        assert_eq!(hits[0].rtt_ms, Some(100));
        assert_eq!(hits[1].rtt_ms, Some(300));
        assert_eq!(hits[2].rtt_ms, None, "unmeasured last");
    }

    #[test]
    fn coverage_counts_chunks_and_neighborhoods() {
        let mut cache = TopologyCache::new(overlay(&[0x00]));
        cache.upsert(rec(&[0xff, 0x80], Some(1), 1, true)); // covers 0xff… at depth 9
        let chunks = [
            overlay(&[0xff, 0x80]), // covered
            overlay(&[0xff, 0x81]), // same neighborhood (top 9 bits), covered
            overlay(&[0x00, 0x00]), // different neighborhood, uncovered
        ];
        let cov = cache.coverage(&chunks, 9);
        assert_eq!(cov.chunks_total, 3);
        assert_eq!(cov.chunks_covered, 2);
        assert_eq!(cov.neighborhoods_total, 2);
        assert_eq!(cov.neighborhoods_covered, 1);
    }
}
