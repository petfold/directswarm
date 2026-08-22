# Phase 0 — storer service-rate spike: report (2026-08-22)

**Verdict for the gate: split.** Reachability PASSES decisively (100%
of sampled storers accept a stranger's dial+handshake, gate needed
≥50%). The per-storer service-rate threshold **fails as written**
(median 0.074 MB/s at the best pipeline depth vs the ≥1 MB/s line) —
but the measured cause is **not the storer-throttling failure mode the
no-go clause anticipated**: storers served every single request
(zero refusals in 39 runs) and demonstrated ≥0.35 MB/s instantaneous
capability. The binding ceiling is the **payment layer's light-peer
credit policy**: settlement pacing, not service policy. This is a
third exit the plan didn't enumerate; human review decides the path.

## Environment

- Client: `spike/cmd/svcrate` (Go, bee 2.8.1-era packages via local
  checkout v2.8.1-10-gda3549af; wire-identical to release in hive/bzz),
  light-node identity overlay `b52342…`, own funded chequebook
  `0xE8C7aD1A…dFD41` (1.0 xBZZ deposited), Gnosis RPC
  rpc.gnosischain.com.
- Host: 8-core Linux laptop, **Ethernet** (all measured runs), NAT'd
  (reachability status: Private — all connections outbound).
- Network: Swarm mainnet, depth 9, 2026-08-22 midday CEST. Target
  storers: bee 2.8.0/2.8.1 full nodes.
- Payload: 1 GiB deterministic (wsbench seed 1), ref `842efaa9…a759a`,
  freshly uploaded and tag-verified synced (264,209 chunks incl.
  intermediates; offline BMT enumeration reproduced the mainnet root
  bit-exactly). Batch: depth-21 mutable, 0.47 utilization.
- Raw data: `.phase0/reach.csv` (reachability), `.phase0/svcrate.csv`
  (service rate; per-run rows incl. settlement columns), logs in
  `.phase0/`.

## Method

- **Reachability**: 41 full-node records collected via hive gossip
  from 3 seed full nodes; all 41 dialed once (≤2 dials/s), full bee
  handshake timed, polite disconnect.
- **Service rate**: 10 storers in 10 distinct depth-9 neighborhoods,
  strictly sequential, 10 s pauses. Per storer: pipeline depths
  1/8/32/100, each given an even share (~129) of the storer's ~516
  payload chunks (chunks with proximity ≥9 to its overlay; never
  re-requested), 60 s cap, 100 MB byte cap. Every chunk BMT-verified.
  **Settlement active throughout**: stock accounting (light-node
  defaults), pseudosettle refresh, and real SWAP cheques from the
  spike's own chequebook; amounts labeled separately per run. Our own
  accounting's overdraft signal treated as backpressure (wait 100 ms,
  retry) — the achieved rate is therefore the settlement-limited
  service rate.

## Results

### Reachability (gate: ≥50% → PASS)

41/41 sampled storer-side full nodes accepted dial + handshake from an
unknown, NAT'd light peer: **100%**, across 35 distinct depth-9
neighborhoods. Dial+handshake wall-clock: median 191 ms, p95 559 ms
(2–3 RTTs + crypto). Caveat: hive gossips only full nodes, and records
came from live peers of healthy nodes — the sample is biased toward
reachable nodes; a snowball crawl should firm this up.

### Service rate (gate: median ≥1 MB/s at some depth → FAILS AS WRITTEN)

Medians across 10 storers (settled, verified chunks):

| pipeline depth | median MB/s | median chunks/s | note |
|---|---|---|---|
| 1   | 0.044 | ~11 | RTT-bound (p50 latency 40–141 ms per peer) |
| 8   | 0.057 | ~14 | settlement-bound begins |
| 32  | 0.074 | ~18 | **peak**; settlement-bound |
| 100 | 0.054 | ~13 | settlement-bound + 3 peers disconnected us |

- **Zero refusals in 39 runs.** Every request every storer accepted
  was served and verified. Per-chunk service latency stayed at
  connection RTT (p50 40–141 ms depending on peer geography) at every
  depth — storers never queued us meaningfully.
- **The ceiling is settlement pacing, quantitatively:** 40,481
  overdraft-wait events across the runs. As a light peer we are
  granted the light payment threshold (1.35M accounting units ≈ 6
  chunks of unpaid headroom at ~220k units/chunk); pseudosettle
  refreshes 450k units/s (~2 chunks/s); each SWAP cheque restores only
  a few chunks of headroom and cheque cadence observed was ~0.5–2/s.
  Net sustained ceiling ≈ 10–24 chunks/s ≈ **0.04–0.10 MB/s per
  connection**, saturating by depth 8–32 — the depth curve flattens
  exactly as this model predicts.
- **Storer capability is far higher than the paid rate**: one peer
  served an 84 chunks/s burst (0.35 MB/s) at depth 100 **on unpaid
  credit headroom** before disconnecting us at its debt limit —
  labeled unpaid, not a result; cited only as a lower bound on raw
  service capability. Three depth-100 runs ended in peer disconnects
  (debt-limit enforcement) — future probes should cap depth at ~32 for
  etiquette.

### Economics

- 3,607 chunks fetched, verified, and settled; 901 SWAP cheques
  issued; **0.0065 xBZZ total settled** this run.
- **Direct 1-hop price: 219,936 accounting units/chunk** vs ~310k
  units/chunk that weightstation measured through forwarding — **~29%
  cheaper at 1 hop**, confirming the proximity-pricing prediction
  (like-for-like in protocol units). In cheque terms the run
  extrapolates to ~0.47 xBZZ/GiB — higher than the unit ratio implies;
  oracle exchange-rate timing and cheque-granularity overshoot on
  short runs inflate it; treat the unit comparison as the honest one
  and re-measure ratio on longer runs.

## Protocol findings (from getting the probe accepted at all)

1. **A peer must speak accounting to exist**: stock peers open a
   pricing stream immediately post-handshake; a client without it is
   disconnected in seconds.
2. **A peer must accept hive too**: remotes announce peers to fresh
   light connections at once; a client without the hive protocol gets
   its stream reset and the connection torn down (measured — this cost
   a day of debugging; "stock-shaped light client" is the minimum
   viable protocol surface: handshake, hive, pricing, pseudosettle,
   swap, retrieval).
3. **Bootnodes drip-feed, ordinary full nodes bulk-announce** — seed
   crawls from full nodes; bee dialers refuse to hold connections to
   light peers (`ErrDialLightNode`, relevant to Phase-4 S2 design);
   bee-as-library needs a PickyNotifier and `AllowPrivateCIDRs` for
   loopback; bootnode `/ws` (libp2p.direct) underlays are unsupported
   by bee's own transports (browser-transport data point).

## Gate analysis — for human review

The no-go clause said: *"if storers throttle strangers to ≲0.1 MB/s —
the fix is then peer policy, not a client."* Measured reality: strangers
get ~0.07 MB/s **and it is not the storers doing it** — it is the
protocol's light-peer credit policy pacing our own payments. The
distinction matters for every option on the table:

- **The funnel-inversion still works arithmetically**: aggregate rate
  scales with concurrent connections (runs here were strictly
  sequential, one peer at a time). ~340 connections × 0.074 MB/s ≈ the
  25 MB/s target; ~500 neighborhoods exist. **Unmeasured** — a
  concurrency test (e.g. 5–20 parallel peers) is the single most
  valuable next measurement and needs a fresh etiquette blessing.
- **The ceiling is policy, not physics**: a full-node threshold is 10×
  (≈0.7 MB/s/connection), and the whole ceiling is exactly the shape
  of weightstation's "funding is not incentivised" finding — a paying
  client cannot buy more throughput than a free-rider. One upstream
  change (credit/threshold that scales with demonstrated settlement)
  lifts every number here by an order of magnitude.
- Per-chunk pricing at 1 hop is confirmed ~29% cheaper in protocol
  units, and settlement itself worked flawlessly: 901 cheques accepted
  by 10 strangers with zero disputes.

**Options for review:** (a) run the aggregate-concurrency measurement
next (cheap, decisive for the client's viability at today's policy);
(b) fold these numbers into the Phase-2 upstream write-up now —
"storers are open; the credit policy is the funnel" is a stronger,
better-evidenced claim than either planned exit; (c) both, concurrency
first. Phase 1 (fetcher MVP) should not start before (a), since the
scheduler's whole design assumes concurrency delivers the aggregate.

## Addendum: aggregate concurrency measurement (same day, user-approved)

Depth 32, whole per-peer chunk sets, all peers in parallel:

| concurrent peers | aggregate MB/s | median per-peer MB/s | note |
|---|---|---|---|
| 1 (from sequential run) | 0.074 | 0.074 | baseline |
| 5  | 0.149 | 0.031 | |
| 20 | 0.251 | 0.013 | majority of marginal gain is free-tier refresh |

**Aggregate does not scale with connections today, and the reason is
client-side:** bee's chequebook service serializes cheque issuance
under one mutex held across the entire sign-and-send round trip
(~90 ms), capping the whole client at ~5–6 cheques/s regardless of
peer count (the identical global cheque counters across concurrent
rows are the fingerprint; in concurrent-mode CSVs the cheque columns
are global-window values, not per-peer). Per-peer pseudosettle refresh
DOES stack (each peer grants 450k units/s), which is why aggregate
creeps up with peer count — but that marginal throughput is free-tier
funded: exactly the per-peer credit aggregation weightstation's report
flags as a loophole to report, not a strategy. Labeled accordingly:
**the honest paid-aggregate ceiling of a stock-chequebook client is
~0.1–0.15 MB/s regardless of connection count.**

The fix hierarchy, and why the design survives:

1. **Client-side, protocol-compliant**: cumulative cheques only need
   ordering *per beneficiary*; the global mutex is an implementation
   convenience. Per-beneficiary issuance locking makes each
   connection's cheque cadence its own wire-RTT-bound ~11 cheques/s ≈
   ~0.28 MB/s paid per connection — then ~90 connections reach the
   25 MB/s target at today's thresholds. This is Phase-1 engineering
   in our own client (bee-as-library's lock replaced, wire protocol
   untouched), plus a small upstream patch suggestion to bee.
2. **Upstream policy**: light-peer threshold scaling with demonstrated
   settlement lifts the per-connection ceiling ~10× further (ties
   directly into the funding-is-not-incentivised report).

## Caveats

Sample of 10 storers from a reachability-biased record set; single
client location/identity (light, NAT'd); short per-run chunk budgets
(~129 chunks) bounded by the payload's ~516 chunks per neighborhood;
sequential only — no concurrency measured yet; depth-100 runs pushed 3
peers to disconnect-limit (avoid in future); one unpaid burst labeled
as such. Spend: postage 0.880 + upload settlement 0.286 + retrieval
settlement 0.0065 ≈ **1.17 xBZZ total spent** this phase (plus 2.0 and
1.5+1.0 xBZZ repositioned between own wallets/chequebooks).
