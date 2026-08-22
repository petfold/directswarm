//! ds-net — the native (tokio) adapter for directswarm.
//!
//! Owns everything ds-core must not: sockets, clocks, the libp2p
//! swarm, Gnosis RPC, and the Bee wire protocols (reused from the
//! `ant` crates where they fit). Populated milestone by milestone;
//! see PLAN-phase1.md.
