//! ds-core — the sans-I/O core of directswarm.
//!
//! Everything here is pure logic over injected inputs: no sockets, no
//! owned clocks, no async runtime. The native adapter (`ds-net`) and a
//! future browser adapter drive it. This crate compiles for
//! `wasm32-unknown-unknown`; CI enforces that from the first commit.

pub mod resume;
pub mod swarm;

pub use resume::{ResumeState, SIDECAR_SUFFIX};
pub use swarm::{neighborhood, proximity, SwarmAddress, MAX_PO};
