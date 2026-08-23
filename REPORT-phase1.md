# Phase 1 report — the settled direct fetcher, measured

**Date:** 2026-08-23. **Environment:** Ethernet (enp7s0), Nook bee
2.8.1 as forwarding fallback only; all direct-plane traffic settled
from the spike identity (eth `0xDEdAc9…`, chequebook `0xE8C7aD…`).
**Payload:** the Phase-0 1 GiB deterministic payload (wsbench seed 1,
ref `842efaa9…a759a`, 264,209 chunks, 512 depth-9 neighborhoods).
Raw data: `.phase1/` (event JSONLs, per-run logs, CSVs, peerstate).

## Headline

A stranger light client with no kademlia of its own can fetch a
1 GiB payload **byte-exact, ~99.8% over direct settled storer
connections**, on stock chunks and stock protocols, paying real SWAP
cheques accepted by over a thousand distinct storers:

| acceptance run (full GiB, cold local store) | wall | rate | cost |
|---|---|---|---|
| 1 (network-cold: 832 peers learned) | 51m03s | 0.353 MB/s | 0.4961 xBZZ |
| 2 | 33m02s | 0.546 MB/s | 0.4428 xBZZ |
| 3 | 22m03s | 0.817 MB/s | 0.4280 xBZZ |
| 4 | 18m59s | 0.949 MB/s | 0.4356 xBZZ |
| 5 | 19m07s | 0.943 MB/s | 0.4307 xBZZ |

**Median 0.817 MB/s, median cost 0.436 xBZZ/GiB** (≈ 25–30% cheaper
than the ~0.58 all-cheque price because the pseudosettle free tier
covers the rest). Reassembly SHA-256 matched the independent wsbench
reference exactly on runs 1 and 5; runs 2–4 skipped the end-to-end
reassembly check (harness bug, fixed, disclosed below) but every chunk
of every run was BMT/CAC-validated at fetch time. Warm within-pass
aggregate peaked at **1.36 MB/s** (110 connections).

The runs get faster because the network remembers us: bee grows a
paying peer's payment threshold with settled volume (verified in bee
source and live), and our peer-state cache accumulates each storer's
threshold and cheque-validation latency (1,188 peers after the
battery). This "earned trust" is the phase's central mechanism.

## What was proven, milestone by milestone

- **M1** — end-to-end fetch + byte-verify + resume over the bee
  fallback (correctness harness).
- **M2** — first fully settled direct storer stream on mainnet: SWAP
  cheques from a stranger light client accepted live; found and fixed
  two ant cheque bugs (quoted JSON int; exchange-rate ignored).
- **M3** — polite snowball crawl → topology cache: 2,734 storers,
  511/512 payload neighborhoods (99.81%), 314/315 dials accepted.
- **M4** — multi-connection scheduler: correct and fully settled;
  first version didn't scale (shared swarm poller) — fixed with one
  swarm/poller per connection; then two settlement bugs (in-memory
  cheque ledger vs bee's persistent chequestore; exposure bound).
- **M5 groundwork (probe-growth)** — the decision experiment:
  - bee's per-peer threshold growth confirmed exactly (1.35 M + 450 k
    per 45 M units settled, linear to 9.45 M, exponential after;
    re-announced via pricing; persists across reconnects);
  - per-peer cheque-validation latency λ measured (sweep cheque +
    small pseudosettle probes; ACK hits zero when the cheque lands):
    **~80% of storers validate in ≤1.5 s; ~20% take 12–19 s** (their
    RPC infrastructure — a selection criterion, not a network limit);
  - sustained single-connection rate at grown thresholds: **0.081 MB/s**
    (grown light T ≈ 9.45 M), **0.139 MB/s** on a high-threshold
    operator (3 of 10 sampled announce 13.5 M to strangers);
  - ant's `Accounting` hard-caps debt at the fresh light limit —
    silently re-caps grown connections to free-tier pacing (upstream
    note pending user review); replaced with a threshold-aware mirror.
- **M5 scheduler** — live-threshold pacing, λ-aware exposure control
  (bee's worst-case view ≤ 1.05 × T vs its 1.25 × T disconnect limit),
  per-neighborhood shared work buckets with redundancy-2 work-stealing
  (a single slow storer alone on a bucket had held a run's wall clock
  25 minutes), selection by earned trust. Connection ramp measured:
  aggregate now **linear in connections** (50 → 110 conns =
  0.289 → 0.581 MB/s; the M4 design had flattened by 32).

## Honesty notes

1. **The 25 MB/s acceptance target was NOT met** (median 0.817 MB/s,
   ~30× short). The gap decomposes into measured, addressable parts:
   - *Per-connection ceiling*: warm scheduler connections average
     ~0.012 MB/s vs the probe's 0.08–0.14 MB/s sustained, because at
     1 GiB each neighborhood holds only ~2.1 MB — a connection's
     entire work is ~25 s of flow against ~15–25 s of dial/handshake/
     λ-measure/threshold-rampup overhead. **The payload is too small
     to amortize connection setup at today's per-connection rates**;
     a 10 GiB payload gives each bucket ~21 MB and the same machinery
     projects to ~9–15 MB/s at 110 connections, rising as thresholds
     grow toward the 0.23 MB/s/conn regime (measured reachable on
     fast-λ, high-threshold storers).
   - *Wave structure*: full coverage needs 512 neighborhoods; at 110
     concurrent connections that is ~5 sequential waves plus pass
     restart overhead (~10% of wall).
2. **Cold vs warm**: run 1 is network-cold (first contact with 832
   storers). Runs 2–5 are network-warm (grown thresholds, cached λ) —
   labeled as such; the warm state is the design's intended operating
   point, persists on the storers while their bees run, and is
   re-earned per the growth curve if lost.
3. **Verify coverage**: end-to-end reassembly verified byte-exact on
   runs 1 and 5. Runs 2–4 hit a harness bug (the fallback-drain pass
   errored when the remaining chunks had no covering storer in the
   cache, aborting before verify; stores were cleared by the next
   run). Fixed the same day; per-chunk BMT validation covered all
   runs at fetch time.
4. **Residuals**: peers that hang up before the final sweep leave
   owed-but-unpayable units (94.5 / 20.0 / 35.8 / 51.7 / ~50 M units
   per run ≈ 0.001 xBZZ/run). The persisted outbound ledger repays
   them automatically at next contact (emit-time cumulative runs ahead
   of bee's validated cumulative). 107/110 drain connections ended
   with the PEER confirming zero debt (pseudosettle probe ACK = 0).
5. **Timeout-killed passes** strand debt the same way (SIGKILL, no
   sweep); same auto-repair applies. Rare (slow-λ tail passes).
6. Every run fully settled: cheques at the announced exchange rate,
   free tier consumed only at protocol cadence, no pseudosettle
   multiplication. Battery settlement: 2.233 xBZZ across ~217k
   cheques; lifetime 2.414 xBZZ to 1,159 beneficiaries.

## Cost model (measured)

~0.44 xBZZ per 1 GiB retrieved 1-hop-settled at today's oracle rate
(219,936 units/chunk × 100,000 PLUR/unit, free tier covering ~25%).
Plus one-time "trust warm-up": growing a light connection's threshold
to plateau ≈ 15 MB of paid traffic ≈ 0.008 xBZZ per storer, persisted
on the storer while its bee runs.

## What Phase 2 should carry upstream (user review gates all posting)

1. bee chequebook cached-invariant fix (issue #5570 filed, PR held).
2. ant: cheque JSON quoting + exchange-rate bugs (fixed locally, M2);
   `Accounting` fresh-threshold overdraft cap (M5).
3. Findings worth sharing with data: threshold growth as an earned-
   trust mechanism works and is measurable; cheque-validation latency
   is the dominant per-connection rate variable and is operator
   infrastructure (a slow public RPC), not protocol.

## Next (Phase 1 wrap → human review)

- The fetcher meets its correctness bar and its scaling shape; the
  throughput target needs bigger payloads and/or per-connection gains
  (longer-lived connections, exposure tuning past the λ-resolution
  floor, high-threshold storer preference). Options for the review:
  accept Phase 1 with the measured curve and revisit the target as
  "MB/s at N GiB", or hold for a 10 GiB payload experiment
  (~0.9 xBZZ postage + ~4.4 xBZZ settlement per full fetch).
