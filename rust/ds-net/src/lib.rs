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
pub mod direct;
pub mod fetch;
pub mod identity;
pub mod keystore;

pub use bee_api::BeeApiFetcher;
pub use direct::{probe_storer, ProbeOptions, ProbeReport, ProbeTarget};
pub use fetch::{fetch_to_file, FetchError, FetchOutcome, MAX_FETCH_BYTES};
pub use identity::Identity;
