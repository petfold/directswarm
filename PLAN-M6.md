# PLAN-M6 — throughput optimization on measured constants

Written 2026-08-24 from the post-Phase-1 diagnosis discussion. Phase-1
correctness stands (byte-exact 1 GiB, fully settled, ~99.8% direct
plane); this plan turns the measured constants into the original 20×
goal. Everything below is stock-protocol: cheques, pseudosettle,
retrieval, and on-chain reads only.

## Measured constants this plan is built on

| constant | value | provenance |
|---|---|---|
| storer service ceiling, one client connection | **~50 chunks/s ≈ 0.2 MB/s**, flat for pipeline ≥ 8 | prepaid depth-scaling probe (8/16/32 → 48.7/41.5/49.5 chunks/s at p50 86/236/563 ms) |
| optimal in-flight per connection | **~8** (saturates; deeper only queues at the peer) | same |
| prepayment effect | 2× vs pay-as-you-go on the same peer; removes the threshold/λ throttle entirely (bee consumes persisted surplus before balance — verified in source + live) | prepay probes |
| cheque-validation latency λ | ~80% of storers ≤1.5 s; ~20% 12–19 s (their RPC) | λ sweep, 30+ peers |
| threshold model | announced threshold RESETS per reconnect; peer retains our settled volume → fast regrowth; irrelevant under prepay | prepay/control probes |
| chunks per depth-9 neighborhood (1 GiB) | Binomial(N, 2⁻⁹): mean 516, σ 22.7 (observed 23.2); exact count computable from the address list | chunks.csv |
| neighborhood roster known today | ~5 members/bucket from crawls (lower bound) | M3 topology cache |
| client-side ceilings removed | ledger fsync-under-mutex (was ~40 cheques/s node-wide) fixed; ant fresh-threshold cap bypassed | M5 diagnosis, A/B |
| end-to-end best today | 1 GiB byte-verified in 10m14s = 1.76 MB/s (110 conns) | acceptance run 7 |

Arithmetic target: 0.2 MB/s × 110 connections ≈ 22 MB/s aggregate
before multi-member gains — the 20×-stock goal (stock ≈ 1.1–1.36 MB/s)
is reachable on measured numbers if the constants compose.

## Design decisions (from the 2026-08-24 discussion)

1. **Prepay-first settlement.** Per storer: one up-front cheque sized
   to its EXACT assigned chunk count (address list known after a
   ~8.5 MB manifest walk, itself prepayable level-by-level or covered
   by daemon-mode standing surplus). Top up at a low-water mark (~20%
   surplus left) so serving never stalls; finish with an exact sweep.
   No repayment exists in SWAP (cheques are cumulative promissory
   notes), so convergence — bulk prepay, then small top-ups near the
   end — is the mechanism; deliberate small residue with recurring
   storers is a feature (next fetch starts throttle-free).
   Sizing when the address list is NOT yet known: newsvendor logic —
   under-provisioning costs ~nothing with proactive top-ups, so prepay
   ≈ the binomial mean, not a high quantile; a fire-and-forget mode
   uses mean + 2.33σ (99% one-sided).
2. **Use ALL good members of a neighborhood, not the top 1–2.** The
   0.2 MB/s ceiling is per client connection, so per-neighborhood
   throughput multiplies with members served in parallel. "Good" =
   dialable and not measured-bad; slow cheque validators are fine
   under prepay (λ only delays the surplus activating once — pipeline
   the prepay with the dial).
3. **Bandwidth-first selection; RTT only as a cold-start prior.** For
   bulk transfer, bandwidth — not RTT — is the ranking signal; the two
   correlate but are not the same. Bandwidth is monitored DIRECTLY and
   for free: at depth 8 a connection is saturated, so its realized
   chunks/s is a live per-peer bandwidth measurement. Persist an EWMA
   service rate per peer in peerstate; rank members by it; unknown
   peers get the RTT prior and an exploration slot (ε-greedy floor per
   DESIGN.md "Latency-aware source selection" — rank, never
   disqualify). Allocation WITHIN a run needs no weights at all: the
   shared per-neighborhood pull queue already gives each member work
   in proportion to its realized rate.
4. **Exact neighborhood rosters from the chain.** Full storers stake
   on-chain; the staking contract's events enumerate registered
   overlays (public RPC read, zero Swarm-network load). Sweep it,
   intersect with dial-liveness, and per-neighborhood membership is
   near-exact instead of crawl-sampled. Refresh with polite crawls.
5. **Daemon mode.** Persistent connections + standing surplus make
   repeat fetches zero-setup and zero-warm-up; the fetch CLI becomes a
   client of a long-lived engine. This also amortizes the manifest
   walk and keeps bandwidth EWMAs fresh.

## Work items, in order

- **M6.1 prepay in the scheduler**: exact per-storer sizing from
  assigned buckets, low-water top-ups, exact final sweep; per-conn
  depth default 8. Re-run 1 GiB. *(Expected: flow ≥ 2×; end-to-end
  limited next by wave count.)*
- **M6.2 two-client probe**: is the ~50 chunks/s ceiling per
  connection or per storer node? (Two identities, same storer,
  simultaneously.) Decides whether multi-member is the only
  multiplier or per-storer parallelism also exists.
- **M6.3 stake-registry roster sweep** + dial-verify; peerstate gains
  service-rate EWMA (recorded whenever a saturated connection closes)
  and real handshake RTT (current crawl RTTs are wall-clock artifacts
  — discard).
- **M6.4 all-good-members scheduling**: neighborhood work queues
  served by every known-good member in parallel, bandwidth-ranked
  admission of new members, ε-greedy exploration. Re-run 1 GiB.
  Concurrency stays within the blessed 110 until the etiquette review
  (below).
- **M6.5 daemon mode**: keep connections + surplus warm across
  fetches; repeat-fetch benchmark (the "hot library" number).
- **M6.6 etiquette review with the user**: raising concurrency toward
  one-connection-per-neighborhood (~512) — needed for single-wave
  1 GiB; present measured per-storer load (one conn, depth 8, fully
  prepaid) as the politeness case.
- **M6.7 (option) RS-coded payload**: upload with stock erasure coding,
  fetch k-of-n per stripe, cancel stragglers — replaces tail
  management with reconstruction; needs a new batch (~1 xBZZ) and
  pairs naturally with a 10 GiB payload if funded.

## Honesty & etiquette rails (unchanged)

Every connection fully settled (prepay is MORE eager than the
protocol requires, never less); depth ≤ 8 per connection; one
connection per storer per identity; no free-tier games; measured
numbers with cold/warm labels; spend ledgered in STATUS.md.
Blocking resource asks before M6 measured runs: chequebook top-up
(headroom ~0.6 xBZZ; a full-GiB rerun costs ~0.45).

## Open questions folded in

- Per-storer vs per-connection service ceiling (M6.2 answers).
- Surplus float policy: residue → 0 vs standing deposit per recurring
  storer (policy knob; default: converge to ~0, daemon keeps a small
  float only with proven peers).
- Whether the oracle exchange rate mid-run drift needs handling beyond
  the final sweep (observed stable so far).
