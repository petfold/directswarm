//! ds-net — the native (tokio) adapter for directswarm.
//!
//! Owns everything ds-core must not: sockets, clocks, the libp2p
//! swarm, Gnosis RPC, and the Bee wire protocols (reused from the
//! `ant` crates where they fit). Populated milestone by milestone;
//! see PLAN-phase1.md.
//!
//! M1: the forwarding-fallback path — [`bee_api::BeeApiFetcher`] +
//! [`fetch::fetch_to_file`].

pub mod bee_api;
pub mod fetch;

pub use bee_api::BeeApiFetcher;
pub use fetch::{fetch_to_file, FetchError, FetchOutcome, MAX_FETCH_BYTES};
