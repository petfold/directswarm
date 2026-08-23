//! ds-net — the native (tokio) adapter for directswarm.
//!
//! Owns everything ds-core must not: sockets, clocks, the libp2p
//! swarm, Gnosis RPC, and the Bee wire protocols (reused from the
//! `ant` crates where they fit). Populated milestone by milestone;
//! see PLAN-phase1.md.
//!
//! M1: the forwarding-fallback path — [`bee_api::BeeApiFetcher`] +
//! [`fetch::fetch_to_file`].
//! M2: one direct, settled storer stream — [`direct::probe_storer`].

pub mod bee_api;
pub mod crawl;
pub mod direct;
pub mod fetch;
pub mod fund;
pub mod growth;
pub mod hive;
pub mod identity;
pub mod keystore;
pub mod ledger;
pub mod peerstate;
pub mod schedule;
pub mod store;

pub use bee_api::BeeApiFetcher;
pub use direct::{probe_storer, ProbeOptions, ProbeReport, ProbeTarget};
pub use growth::{probe_growth, GrowthOptions, GrowthReport, PhaseStats};
pub use fetch::{fetch_to_file, FetchError, FetchOutcome, MAX_FETCH_BYTES};
pub use identity::Identity;

/// Price a storer charges for serving `chunk` (bee's fixed pricer,
/// re-exported for CLI-side estimates like prepay sizing).
#[must_use]
pub fn peer_price_for(storer_overlay: &[u8; 32], chunk: &[u8; 32]) -> u64 {
    ant_retrieval::accounting::Accounting::peer_price(storer_overlay, chunk)
}
