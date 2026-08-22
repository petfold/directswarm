//! Scheduler primitives (sans-I/O): chunk→peer assignment over the
//! topology cache, per-peer observed-rate tracking, and AIMD pipeline
//! depth. The tokio engine (`ds-net::fleet`) owns sockets and clocks;
//! everything here is pure state fed with caller-observed events.

use crate::swarm::{proximity, SwarmAddress};
use std::collections::HashMap;

/// Per-peer service statistics: an EWMA of observed chunk latency and
/// an AIMD pipeline-depth controller.
///
/// Discipline (DESIGN "Latency-aware source selection"): RTT is the
/// prior, observed service rate the posterior — AIMD keeps the final
/// say. Additive increase per successful chunk batch, multiplicative
/// decrease on failure/overdraft signals.
#[derive(Debug, Clone)]
pub struct PeerStats {
    /// EWMA of per-chunk wall latency, milliseconds.
    pub latency_ewma_ms: f64,
    /// Current pipeline depth (outstanding requests allowed).
    pub depth: u32,
    pub chunks_ok: u64,
    pub chunks_err: u64,
    successes_since_increase: u32,
}

/// Depth starts low and earns its way up (Phase-0: depth-100 provoked
/// disconnect-limit; cap well under it).
pub const DEPTH_MIN: u32 = 2;
pub const DEPTH_MAX: u32 = 16;
/// Additive increase: +1 depth per this many consecutive successes.
const INCREASE_EVERY: u32 = 8;
const EWMA_ALPHA: f64 = 0.2;

impl Default for PeerStats {
    fn default() -> Self {
        Self {
            latency_ewma_ms: 0.0,
            depth: DEPTH_MIN,
            chunks_ok: 0,
            chunks_err: 0,
            successes_since_increase: 0,
        }
    }
}

impl PeerStats {
    /// Record a successful chunk (latency in ms).
    pub fn on_success(&mut self, latency_ms: u64, prior_rtt_ms: Option<u32>) {
        self.chunks_ok += 1;
        #[allow(clippy::cast_precision_loss)]
        let sample = latency_ms as f64;
        if self.latency_ewma_ms == 0.0 {
            // Seed from the topology prior if we have one.
            self.latency_ewma_ms = prior_rtt_ms.map_or(sample, f64::from);
        }
        self.latency_ewma_ms = EWMA_ALPHA * sample + (1.0 - EWMA_ALPHA) * self.latency_ewma_ms;
        self.successes_since_increase += 1;
        if self.successes_since_increase >= INCREASE_EVERY && self.depth < DEPTH_MAX {
            self.depth += 1;
            self.successes_since_increase = 0;
        }
    }

    /// Record a failure (timeout, reset, overdraft-refusal): halve the
    /// pipeline depth.
    pub fn on_failure(&mut self) {
        self.chunks_err += 1;
        self.successes_since_increase = 0;
        self.depth = (self.depth / 2).max(DEPTH_MIN);
    }
}

/// A connected peer the assigner can route chunks to.
#[derive(Debug, Clone)]
pub struct AssignablePeer<K> {
    /// Engine-side key (e.g. a `PeerId`).
    pub key: K,
    pub overlay: SwarmAddress,
    /// Topology-cache RTT prior, if measured.
    pub rtt_ms: Option<u32>,
}

/// Assign each chunk to the best available peer: among connected peers
/// with `proximity(peer, chunk) >= depth` (they actually store it),
/// pick the lowest-RTT one, spilling to the next-best by round-robin
/// weight so a single fast peer doesn't absorb a whole neighborhood
/// (ε-greedy floor against hot-spotting). Chunks no peer covers land
/// in the returned `unassigned` list (fallback territory).
pub struct Assignment<K> {
    pub per_peer: HashMap<K, Vec<SwarmAddress>>,
    pub unassigned: Vec<SwarmAddress>,
}

/// `spread`: how many of a neighborhood's covering members share its
/// ch