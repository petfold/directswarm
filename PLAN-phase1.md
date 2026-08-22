# Phase 1 — fetcher MVP: implementation plan (2026-08-22)

Gate: Phase 0 reviewed **GO** (STATUS.md, 2026-08-22). Scope per
PLAN.md Phase 1: topology cache (RTT-aware) + latency-aware scheduler
(S1 only) + verification + forwarding fallback + resume; CLI
`directswarm fetch <ref> [-o file]` against mainnet. Acceptance: 1 GiB
cold fetch, byte-verified, ≥25 MB/s median over 5 runs (understood as
~110 settled connections at today's light-peer thresholds), graceful
degradation at 50% cache staleness, resume without refetch, settlement
cost per GiB reported.

## Substrate: build on `ant` (surveyed 2026-08-22, ~/projects/ant @ c526a33)

`solardev-xyz/ant` (Rust, MIT/Apache-2.0) already implements the wire
layer we need. Capability survey conclusions (full map in session
notes; verify at the cited files before relying on them):

**Reuse as dependencies (lift as-is):**
- `ant-crypto` — BMT/CAC/SOC validation, overlay derivation, handshake
  signing. No async/no I/O, bee golden vectors; wasm-compatible in
  practice (unverified — CI will tell).
- `ant-chain` — Gnosis RPC, EIP-155 tx, chequebook EIP-712
  (`chequebook.rs` has the correct 3-field bee domain — do not
  rediscover this bug).
- `ant-retrieval`'s manifest layer — `joiner.rs`, `traversal.rs`,
  `mantaray.rs`, `enumerate_chunk_tree` → `ChunkInventory`; needs only
  the `ChunkFetcher` trait (`&self` fan-out friendly). Crate drags
  tokio+rusqlite, so it lives on the native side only.
- `ant-p2p` wire modules — `handshake.rs` (v14+v15, half-close
  semantics), `sinks.rs` (bee-headers framing, hive decode, the
  mount-or-be-ejected sink set), `retrieve_chunk_inner`,
  `pseudosettle.rs`, `swap.rs` (JSON-marshalled `SignedCheque`,
  base64 signature), `routing.rs` (proximity math), `underlay.rs`.
  Where the crate boundary fights us, vendor the module with
  attribution rather than fork.

**Do not reuse:** `behaviour.rs` (8.3k-line daemon swarm loop — we
need a fetch-session lifecycle, not a resident node), `ant-gateway`,
`ant-ffi`/`antd`/`antctl`.

**Must build (does not exist anywhere):**
1. **Retrieval-side SWAP settlement** — ant's read path settles via
   pseudosettle only. directswarm principle 1 forbids that at our
   volume: cheque issuance triggered by retrieval debt crossing
   threshold/2, with the **cached-invariant balance check**
   (balance+totalPaidOut cached; the Phase-0 16× fix) native from day
   one. Candidate upstream contribution to ant (it is on their own
   gap list).
2. **Pricing parse** — `AnnouncePaymentThreshold` protobuf actually
   decoded per peer (ant drains it; we pace by it).
3. **Topology cache + crawler** — bounded polite hive crawl,
   bin-organized overlay→underlay map, freshness stamps, dial-RTT
   records, prefix-targeted mode. Etiquette caps inherited from the
   Phase-0 blessing (rate-limited dials, backoff on refusal, polite
   disconnects).
4. **Scheduler** — chunk→neighborhood assignment, 2–3 storers per
   neighborhood up to ~110 connections, AIMD pipeline depth per peer,
   RTT prior / observed-rate posterior with ε-greedy floor, hedged
   tail requests (bounded, settled, ledgered), retry across members,
   forwarding fallback, resume from verified state.
5. **Forwarding fallback + resume** — fallback via local bee HTTP API
   (chunk endpoint), total not best-effort; resume = persisted set of
   verified chunk addresses + partial file.
6. **CLI + bench output** — `directswarm fetch`; wsbench CSV
   vocabulary (medians/p95, cold/warm labels, settlement snapshot
   around runs).

## Workspace shape (day-one discipline from DESIGN.md)

`rust/` cargo workspace in this repo:

- **`ds-core`** — sans-I/O: scheduler state machine, topology-cache
  data model, accounting/threshold policy, cheque-issuance decision
  logic, resume state, neighborhood math. No sockets, no owned clocks
  (time injected), no tokio. **Compiles for `wasm32-unknown-unknown`
  in CI from the first commit.** Depends on `ant-crypto` only (if the
  wasm build proves it clean; otherwise nothing).
- **`ds-net`** — native tokio adapter: libp2p dial, handshake,
  protocol sinks, retrieval/pseudosettle/swap streams, Gnosis RPC.
  Depends on ant crates (path deps to ~/projects/ant for now; pin to
  git rev before anything ships).
- **`ds-cli`** — `directswarm` binary: fetch command, bench CSVs,
  bee-fallback client.

## Milestones (each ends compiling + tested; measured ones snapshot balances)

- **M0 scaffold**: workspace + CI (fmt, clippy -D warnings, test,
  `cargo check -p ds-core --target wasm32-unknown-unknown`).
- **M1 correct-but-slow**: `directswarm fetch` end-to-end via the
  local-bee fallback path only — manifest walk (ant joiner), BMT
  verification, file sink, resume. Byte-verified against the Phase-0
  1 GiB payload. Establishes the harness before any fast-plane risk.
- **M2 one settled stream**: dial one mainnet storer, handshake,
  mount sinks, retrieve chunks with accounting + pseudosettle +
  **cheque issuance (cached invariant)**; verify a cheque lands and
  is accepted (chequebook: the spike's, 0xE8C7aD…, ~0.99 xBZZ
  available). First measured per-connection rate vs Phase-0 spike.
- **M3 topology cache**: crawler with Phase-0 etiquette caps, RTT
  recording, neighborhood map over the payload's chunk set.
- **M4 scheduler at scale**: multi-connection fetch, AIMD depth,
  latency-aware selection, hedged tails, fallback integration, 50%-
  stale degradation test. Ramp connection count gradually (20 → 50 →
  110) with etiquette review at each step.
- **M5 acceptance**: 5 × 1 GiB cold runs (Ethernet — warn user
  first), medians/p95, cost/GiB, REPORT-phase1.md, STATUS.md, spend
  ledger.

## Risks / notes

- **~110 connections etiquette**: one light slot per storer across
  ~110 distinct nodes; still, ramp with review, cap pipeline depth 32
  (Phase-0 finding: depth-100 provokes disconnect-limit).
- **ant cheque path live-unproven**: ant's `cheque_smoke` hasn't been
  re-run since their EIP-712 fix. M2 validates it for real; if the
  wire shape fights us, our Phase-0 Go spike is the reference
  implementation of a working emit.
- **Settlement funding**: retrieval settlement for repeated 1 GiB runs
  ≈ 0.0065 xBZZ per sweep at Phase-0 prices — cheap; the binding
  constraint is batch TTL (0.28 xBZZ/day) and the low Nook wallet
  (0.47 xBZZ — user warned 2026-08-22).
- **wasm32 for `ant-crypto`**: claimed clean, never tested by ant. If
  it fails, `ds-core` re-exports its own BMT (vendored) instead.
- **License**: directswarm is BSD-3-Clause; ant is MIT/Apache-2.0 —
  compatible for dependency use and for vendoring with attribution.
