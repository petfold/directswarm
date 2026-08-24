# directswarm — STATUS

## 2026-08-24 (M6.5-lite) — daemon-warm connections measured: full
## payload at 4.28 MB/s over reused connections (best yet); bee
## serving-cost investigated: NO upstream smoking gun (async writes)

**User design confirmations recorded:** replanning is non-blocking
(dispatcher runs on the control task between completion events, ~µs;
transfers never pause); near-critical neighborhoods are served via
repeated argmax-with-feedback (equivalent to the full criticality
ranking); the ABSOLUTE download-time estimate is not needed for
scheduling (only the relative ranking) — kept as telemetry only.

**Bee serving-cost investigation:** bee's per-chunk accounting Puts go
through goleveldb with nil write options = async memtable writes — NOT
fsync-bound. The 20–80 ms per-chunk service time therefore points at
the storers' chunk-read path (multiple index lookups + reserve read;
consistent with our measured cold→warm 1.6×), i.e., node hardware and
storer-internals — no #5570-class upstream bug to propose. Upstream
"wildcard" demoted to speculative.

**M6.5-lite (warm pool):** connections now PARK at clean close (live,
settled, zero-debt, threshold feed + λ retained) and are reused by the
next fetch — no dial, no handshake, no settle-wait, no re-prepay.
`fetch-direct --repeat N` benchmarks the daemon steady state:
- iteration 1 (cold): 256,564 direct in 277.6 s = **3.80 MB/s**
- iteration 2 (warm): 260,395 direct in 250.2 s = **4.28 MB/s**,
  563 connections parked; fewer strandings warm (dead dials vanish).
Warm gain is +13% — smaller than the raw setup share because rolling
admission already overlaps setup with flow at window 256; the residual
wall in both iterations is the slow-member decay tail (network
property). Progression: 3.22 (run 17 full pipeline) → 4.28 MB/s warm
fetch portion ≈ 3.1–3.5× stock bee.

| spend item (this entry) | amount |
|---|---|
| warm benchmark (2 × 1 GiB fetch portions) | 1.1130 xBZZ |

Lifetime issued 11.87 xBZZ; issuable ~15.0 → headroom ~3.1. Batch
47265a62 TTL ~4.5 d (top-up due ~2 days, standing grant).

## 2026-08-24 — critical-path dispatcher (user's replanning design)
## + candidate recycling: 1 GiB byte-exact in 5m36s = 3.22 MB/s
## (new record; 2.4–2.9× stock bee end-to-end)

User proposed: after the manifest, estimate the whole download, then
continuously optimise connections for the CRITICAL neighborhoods —
replanning during the download, all members connected where it
matters. Implemented exactly that: the static LPT queue is replaced by
a dispatcher — every freed slot dials the best unused member of
whichever bucket most threatens the finish time (criticality =
remaining chunks ÷ active service rate; no active member = infinite).
This subsumes LPT at start, avoids run-14's mid-run dilution (a second
member joins only when its bucket IS the critical path), and gives the
endgame all-members-on-stragglers behaviour for free. Plus bounded
candidate RECYCLING (≤3 rounds): a bucket with work left, nothing
active and nothing unused re-tries its members instead of stranding
remnants (run 16 stranded 14,488 chunks that cascaded into mop-up
passes and an 11k-chunk bee drain).

| run (1 GiB, byte-exact all) | wall | rate |
|---|---|---|
| 15: 256 window, static LPT | 7m08s | 2.53 MB/s |
| 16: + dispatcher | 7m17s | 2.47 (exposed the stranding) |
| 17: + recycling | **5m36s** | **3.22 MB/s** |

Run 17: stranded 2,566 (was 14,488), passes 2 (bulk + sliver), spend
0.5463 xBZZ. Flow still peaks >15 MB/s early; remaining wall = decay
tail of genuinely slow single-member buckets + per-conn dead time —
next: daemon-warm connections (M6.5) and the bee serving-cost
investigation (upstream case).

| spend item (this entry) | amount |
|---|---|
| runs 16 + 17 cheques | 1.0567 xBZZ |

## 2026-08-25 — 256-window ramp: flow peaked 16.9 MB/s (135 Mbps,
## 36% CPU) — ISP-safe so far; wall now ~65% TAIL; gap diagnosis:
## storer cold-vs-warm 1.6× + per-conn dead start, not scheduler flow

Standing grant extended by user: chequebook top-ups no longer need
per-instance asks. Topped up +3.0 xBZZ (txs `0x619d7e9d…`,
`0x6105e837…`) → chequebook 11.97 xBZZ.

**Gap diagnosis (controlled single-peer tests):** the "3× probe vs
scheduler" gap decomposes into (a) the probe cycling the same chunks =
STORER-WARM vs the scheduler's distinct chunks = storer-disk-cold —
measured 12 → 19.6 chunks/s cold→warm on the same peer (1.6×; fleet
EWMAs are honest cold numbers, probe rates were warm-biased);
(b) ~8–10 s per-connection dead time (dial+handshake+settle-wait+
prepay-validation window at start, sweep+zero-confirm at end). Fixes
landed: initial prepay emitted AT HANDSHAKE so its validation window
overlaps setup; settle-wait now event-driven (threshold announcement)
instead of a fixed 2 s. Depth ruled out again at the fleet median.

**Run 15 (window 256, RED 2, prepay, enriched roster):** 428 s fetch,
byte-exact, spend 0.6082 xBZZ. Resource monitor (user requirement):
peak net 16.9 MB/s ≈ 135 Mbps of the 664 Mbps line, CPU peak 36%
all-cores — no ISP/NAT symptoms at ~400 total connections (incl.
bee's ~137; bee kept running: negligible idle footprint, needed for
the final drain). **Wall is now tail-dominated (~65%)**: bulk lands in
~2 min at >10 MB/s flow, then slow-bucket stragglers + mop-up passes.
Next levers: tail policy (dynamic member-add when slots idle, no
mop-up reinvocations), then daemon-warm connections.

| spend item (this entry) | amount |
|---|---|
| gap-diagnosis probes + controlled tests | ~0.05 xBZZ |
| run 15 cheques | 0.6082 xBZZ |

## 2026-08-24 (late) — the 10× question: depth is NOT the lever
## (measured), the scheduler-vs-probe efficiency gap IS (~3×), and the
## 10× is a multiplicative stack, not a silver bullet

User asked what on the roadmap can give 10× (256 conns alone ≈ 2×).
Measured tonight on a median peer (in-run EWMA 8 chunks/s):
- depth 8 vs 32 (prepaid probe): 23 vs 24 chunks/s, p50 268 → 1273 ms
  — flat rate, latency ∝ depth ⇒ per-peer SERVER-side ceiling; the
  RTT/bandwidth-delay hypothesis for the fleet median is dead.
- BUT the probe extracted 23 chunks/s from a peer the scheduler only
  gets 8 from ⇒ **~3× in-run efficiency gap** (candidates: EWMA
  measurement including setup/sweep in the denominator, admission
  gaps around prepay/top-up windows, bucket-end idling, tick
  granularity). Diagnosing this is the top client-side item.

**The 10× stack (multiplicative, all measured or bounded):**
1. in-run efficiency (probe-vs-scheduler gap): ~2–3×
2. window 110 → 256 → 512 (one per neighborhood): 2.3–4.7×
   (256 ramp is ISP-safe to try; 512 needs the incremental ramp test)
3. daemon-warm persistent connections (setup ≈ 40% of wall): ~1.5×
4. wildcard, upstream: why do storers serve one client at only
   ~23–50 chunks/s (43 ms/chunk service time — plausibly bee's
   per-chunk accounting write)? A bee-side batching fix (#5570-class
   proposal, needs user gate) could lift EVERY storer 2–5×.
Items 1×2×3 span ≈ 8–20× ⇒ 25 MB/s is inside the stack without the
wildcard. Also found: private-range ip4 underlays slipped into
topology-staked.csv (merge filter gap) — fix queued. Probe spend
~0.03 xBZZ.

## 2026-08-24 (M6.4 refinement) — member-parallelism A/B: SLOTS, not
## member choice, are the binding mid-run resource; 45 s drain target
## restored (run 13 confirmed near-optimal)

User asked whether we load-balance within neighborhoods and whether to
avoid slow nodes. Data answers: (a) yes — shared pull-queues distribute
work demand-proportionally among selected members, and selection is
bandwidth-ranked; run 13's wall (479 s) is within 18% of the
peerstate-predicted single-member-per-bucket optimum (407 s + setup);
(b) "avoid slow" is already policy, but 163/512 neighborhoods (32%)
have NO fast member (best-known <10 chunks/s; only 38 buckets own a
≥20 member) — there, someone slow must serve.

**A/B (run 14):** forcing multi-member parallelism everywhere (drain
target 45→20 s) made things WORSE: 549 s / 0.8636 xBZZ (vs run 13's
479 s / 0.5861), byte-exact both. Lesson: under a fixed 110-slot
budget, aggregate = slots × average ACTIVE-member rate; adding
second-best (slower) members mid-run LOWERS slot productivity and adds
dial/prepay overhead. Member-parallelism pays only at the TAIL (slots
otherwise idle) and on critical-path buckets — which the 45 s target +
late-hedge queue already approximates. Reverted to 45 s.

**Refined bottleneck ranking for the wall (data-backed):** 1) per-slot
setup (dial+handshake+settle ≈ 15–25 s per target vs ~40 s of work) —
the structural fix is daemon-mode persistent connections (M6.5);
2) network member-speed distribution (median best-of-bucket 12
chunks/s) — roster quality, not our code; 3) tail policy.

| spend item (this entry) | amount |
|---|---|
| run 14 cheques (A/B, negative result) | 0.8636 xBZZ |

Lifetime issued 8.395 xBZZ vs issuable ~9.0 → headroom ~0.6 (⚠ next
run needs a top-up; wallet has 12.47 xBZZ).

## 2026-08-24 (M6.3 cont.) — swarmscan cross-check + underlay merge:
## every neighborhood now dialable-staked (fallback no longer
## structurally required); clean run 13 = 7m59s byte-exact

User pointed at swarmscan.io: its census (3,917 tracked nodes)
corroborates our on-chain count (4,409 ever-staked). Better, its API
serves UNDERLAYS: pulled all 3,902 tracked nodes (paginated, polite),
1,352 of them staked with public ip4/tcp addresses → merged with the
stake registry + our cache into `.phase1/topology-staked.csv`:
**1,544 dialable entries (+376 new), dialable-staked per bucket
min 1 / median 3 / max 4, ZERO empty buckets** — including M3's
"uncovered" neighborhood. The forwarding fallback is now optional
(stragglers only), not structural. IPv6 works on this box but adds no
peers (all reachable staked nodes have ip4). The 3,057 staked overlays
still unreachable are invisible to swarmscan too (NAT/churn) — the
underlay-discovery crawl is DEMOTED to low priority (low expected
yield); external-dependency caveat on swarmscan stands (measurement
aid, not product path — the product learns rosters from chain + hive).

**Run 13 (enriched roster, RED=3, prepay):** 479 s fetch = 2.257 MB/s,
byte-exact SHA-256, bee healthy throughout — the first CLEAN wall on
par with run 11, while paying first-contact exploration for 376 new
peers (their bandwidth EWMAs now persisted for future runs). Spend
0.5861 xBZZ.

| spend item (this entry) | amount |
|---|---|
| run 13 cheques | 0.5861 xBZZ |

## 2026-08-24 (M6.3) — STAKE-REGISTRY CENSUS: 4,409 staked storers on
## mainnet, median 8 per depth-9 neighborhood, ZERO empty neighborhoods
## — we only knew 22% of them

Swept the mainnet staking contract's StakeUpdated events
(`0xda2a16EE…518F4`, public Gnosis RPC, ~0.35 s/call politeness, zero
Swarm-network load; `tools/stake-sweep.py` →
`.phase1/stake-registry.csv`). Findings:
- **4,409 staked overlays**, all with active stake; per depth-9 bucket
  min 3 / median 8 / max 21; **no bucket is empty** — even M3's
  "uncovered" neighborhood has ≥3 staked members whose underlays we
  simply never learned.
- Our topology cache knows underlays for only **976 of them (22%)**;
  3,433 staked storers (median 7 per bucket) are undialable until we
  learn their underlays. 1,758 cache entries are NOT staked (gossip
  ghosts / light nodes) — dial-failure fodder to deprioritize.
- Only 369 staked peers have measured bandwidth so far.

Implication: per-bucket member-parallelism is currently starved at ~2
known members while the network offers a true median of 8 — the
capacity ceiling we measured (best-110 ≈ 10.4 MB/s) is a floor of
what fuller rosters allow. **Next needs a user etiquette blessing: a
targeted underlay-discovery crawl** (hive gossip snowball focused on
the 3,433 unknown staked overlays; dials + gossip only, no
settlement; M3-style caps — propose ≤600 dials at ≤2/s across a few
sessions).

Also this entry: 15 stale watcher shells cleaned (self-matching pgrep
loops — harness hygiene); line speed measured: **~83 MB/s down
(664 Mbps), ~34 MB/s up (270 Mbps)** — peak Swarm flow uses ~7% of
downlink; run-12 harness now refuses to hash partial reassemblies.
Spend this entry: 0 (RPC reads only).

## 2026-08-24 (M6.4) — bandwidth-ranked LPT scheduling + a bee outage:
## 98.7% of the GiB in ONE rolling invocation; Nook bee died mid-run
## and was restarted (authorized); run 12 byte-exact after tail drain

**Built:** selection now ranks members by measured bandwidth EWMA
(unknown peers assumed median so exploration continues), orders the
target queue longest-drain-first (LPT — slow buckets start early and
finish inside the bulk), and sizes per-bucket member-parallelism to a
45 s drain target (cap = --redundancy). The user's framing drove this:
slow nodes aren't bad, they just occupy a slot a faster member could
use — raise slot productivity instead of the connection window.

**Run 12 (RED=3):** pass 1 = ONE rolling invocation, 260,676 chunks
(98.7%) direct, 1.07 GB at 2.289 MB/s including ramp. Then the LOCAL
BEE NODE was found dead (Nook's embedded bee had exited; Nook did not
restart it) — the fallback tail (~800 chunks incl. the known uncovered
neighborhood) could not drain, the driver hashed a partial reassembly
(NOT corruption — the joiner failed cleanly on the missing chunks; the
harness now only hashes a completed verify). Bee restarted from
Nook's own binary+config (grant, 2026-08-22; bee-claude-restart.log
pattern), tail drained (698 chunks via forwarding), **SHA-256
byte-exact confirmed**. Clean-wall measurement deferred to the next
milestone run (a re-run for the number alone ≈ 0.5 xBZZ).

**Resource footprint measured (user question):** no ant processes run
on this box; bee idle ≈ negligible bandwidth, ~137 mostly-idle
connections (and it is the REQUIRED fallback plane — keep it up; the
risk is it dying unnoticed, so the driver should health-check it).
During full flow: ~48 Mbps down, CPU ~15% all-cores — far from any
local ceiling, as the user judged. Added to plan: sampler to log CPU%
+ net utilization and throttle admission if either nears its maximum.

**Chequebook topped up** +2.0 xBZZ (txs `0x0d4af59b…`, `0xe753ca8e…`);
chequebook 8.97 xBZZ. Spend this entry (run 12 + drain) ≈ 0.55 xBZZ.
Next: M6.3 stake-registry rosters (staking contract
`0xda2a16EE…518F4` confirmed; eth_getLogs sweep → near-exact
neighborhood membership), then clean benchmark run 13.

## 2026-08-24 (M6 session) — prepay scheduler + rolling admission:
## full byte-verified GiB in 7m08s = 2.53 MB/s (3.1× battery best,
## ~2× stock bee end-to-end; flow bursts 5.8 MB/s)

**Chequebook topped up** (user grant): Nook →3.0→ spike (tx
`0x00a0d285…`) →3.0→ chequebook (tx `0x1983564b…`), 3.981 → 6.981 xBZZ.

**Built (M6.1–M6.4 partial):**
- Prepay-first settlement in the scheduler: per-storer slice prepay +
  low-water top-ups converging to exact consumption; prepaid-aware
  exposure gate, spend projection, final sweep; parked surplus tracked
  ABSOLUTELY per peer in peerstate and re-used on reconnect (no
  double-prepay). Depth default 8 (measured saturation).
- Bandwidth learning: per-peer service-rate EWMA recorded from
  saturated connections (905 peers already have one; median
  14 chunks/s — confirms wide member-speed spread around the pilot's
  48).
- **Rolling admission**: the wave/pass structure is gone — a fixed
  window of 110 connection slots fed from a target queue covering ALL
  512 buckets (largest first, redundancy rounds appended; a redundancy
  dial is skipped when its bucket is nearly drained). Fast connections
  never idle behind slow ones.

**Measured (full 1 GiB, cold local store, byte-exact SHA-256 each):**
| run | config | wall | rate |
|---|---|---|---|
| 7 (M5 best) | pay-as-you-go, waves | 10m14s | 1.761 MB/s |
| 8 | prepay, waves | 14m29s | 1.244 — REGRESSION: pass churn re-prepaid peers (0.68 xBZZ parked; per-peer amounts unrecorded — standing deposit, recovery path = surplus-aware sync, M6.5) |
| 9 | + surplus tracking | 12m17s | 1.467 |
| 10 | + rolling admission | 15m18s | 1.178 — driver's per-invocation spend cap (0.09 xBZZ) chopped the roll every ~38k chunks |
| 11 | + cap fixed (0.7/invocation) | **7m08s** | **2.526 MB/s**, spend 0.5037 |

Flow inside the rolling window peaked **5.8 MB/s** (1,410 chunks/s).
Member-scaling confirmed en route: one bucket from 1 vs 4 members =
43.9 s vs 23.4 s. Residuals zero or peer-confirmed throughout; smoke
run parked only 0.0004 xBZZ across 20 storers (convergence works).

**Honest ledger of the two regressions:** run 8's churn parked
0.68 xBZZ at peers before per-peer tracking existed (recoverable only
by a future surplus-aware mirror sync — the peer's own zero-debt ACK
is the honest signal); run 10 was our own driver cap. Both diagnosed
from logs, fixed same session.

**Next (M6 continues):** stake-registry rosters (more members per
bucket), bandwidth-EWMA-ranked member selection (data now exists),
tail policy on top of rolling (the 428 s wall is ~2× the flow-limited
floor; remainder = slow-member tails + mop-up invocation), daemon mode,
then the >110-window etiquette review.

| spend item (this entry) | amount |
|---|---|
| M6 runs 8–11 + smoke + member-scaling cheques | 2.9280 xBZZ |
| of which tracked parked surplus (reusable) | 0.1257 xBZZ |
| untracked parked (run-8 legacy, standing deposit) | ~0.68 xBZZ |
| wallet→chequebook deposit (moved, not spent) | (3.000 xBZZ) |

Lifetime issued 6.32 xBZZ; issuable ~7.0 → headroom ~0.68 (⚠ next
full-GiB run needs another ~1–2 xBZZ top-up; Nook wallet 14.47 xBZZ).
Batch 47265a62 TTL ~5.5 d — top-up due in ~3 days (~1.1 xBZZ, standing
grant).

## 2026-08-24 (cont.) — prepaid depth-scaling measured: a storer serves
## one connection at ~50 chunks/s (0.20 MB/s) regardless of pipeline
## depth ≥8; optimal in-flight ≈ 8 (p50 86 ms; depth 32 queues 563 ms)

`probe-growth --pipeline N` added. Prepaid runs on the pilot storer:
depth 8 → 48.7 chunks/s @ p50 86 ms; 16 → 41.5 @ 236 ms; 32 → 49.5 @
563 ms. Flat throughput + latency ∝ depth = server-side per-connection
service ceiling. Implications: (a) default depth drops to 8;
(b) 110 conns × 0.2 MB/s = 22 MB/s aggregate potential on prepaid
plane; (c) per-neighborhood throughput multiplies by serving from
SEVERAL members in parallel — load balancing across neighborhood
members is the next lever (pull-based queues already balance
demand-proportionally). Spend ~0.029 xBZZ (two prepaid runs), ledgered.

## 2026-08-24 — PREPAYMENT measured (user's idea): one up-front cheque
## DOUBLES the single-connection rate; protocol-valid in stock bee

User proposed prepaying a larger cheque per storer instead of
pay-as-you-go. Verified in bee source: over-payment parks in a
PERSISTED per-peer surplus and the debit path consumes surplus BEFORE
the balance — prepaid serving never engages the threshold/validation
throttle. Measured (pilot storer, pipeline 16): control 20.7 chunks/s
(0.085 MB/s) vs prepaid **41.5 chunks/s (0.17–0.21 MB/s)**, 0 errors,
clean settle-out, peer-confirmed zero debt. `probe-growth
--prepay-chunks N` added.

Disclosed en route: the first two prepay probes stalled at exactly 15
chunks — OUR bug (the prepay patch to the admission gate silently
no-opped against refactored code, so exposure stayed capped at 1.05T
without the prepaid credit); bee behaved correctly throughout. Also
added a 10 s fetch timeout (stalled pipeline slots must fail loudly).

**Model revision:** the announced threshold RESETS to 1.35M on each
reconnect; what persists (while the storer's bee runs) is our
cumulative settled volume, which makes regrowth fast — one +450k
upgrade per settlement event, not per 45M units. peerstate's
`threshold_last` is a regrowth-speed indicator, not a standing value.
Prepay sidesteps the whole mechanism and is the M6 centerpiece
(scheduler integration: per-storer bucket-sized prepay + top-ups,
residue parked as surplus is reusable on the SAME storers next fetch).

| spend item (this entry) | amount |
|---|---|
| prepay probes (4 runs incl. controls) | 0.0355 xBZZ |

## 2026-08-23 (late night) — improvement VERIFIED end-to-end: full
## byte-verified GiB in 10m14s = 1.76 MB/s (1.9× the battery best)

Two follow-up fixes on top of the ledger repair, then two comparison
runs (same driver, same payload, cold local store, byte-exact SHA-256
verify BOTH):
- selection never spends a redundancy slot on a measured-slow
  validator (it may still carry a bucket alone);
- graceful stall wind-down (direct plane <40 chunks/20 s → actors
  sweep + exit, leftovers to the next pass/fallback) — kills the
  slow-λ tail without stranding debt.

| config (full 1 GiB, 110 conns) | fetch wall | end-to-end |
|---|---|---|
| battery best (run 4, pre-fix, red 2) | 18m59s | 0.949 MB/s |
| run 6: ledger fix + tail fixes, red 2 | 17m40s | 1.019 MB/s |
| run 7: same, redundancy 1 (110 buckets/wave) | **10m14s** | **1.761 MB/s** |

In-pass flow after the ledger fix: 1.4–1.7 MB/s sustained, ~3.9 MB/s
peak (A/B). Run 6 showed the residual limiter is WAVE GEOMETRY (each
of ~10 waves has a setup floor), so run 7 halved the wave count —
redundancy 1 is now safe because slow-λ storers are known and the
stall wind-down bounds their tails. End-to-end vs stock bee
(weightstation: 1.10–1.36 MB/s cold): **1.3–1.6× faster at 1 GiB**,
flow rate ~3× — the first configuration that beats the forwarding
plane end-to-end. Next levers (unchanged): grown-threshold regime
end-to-end, larger payloads (geometry), >110 concurrency (etiquette
review needed).

| spend item (this entry) | amount |
|---|---|
| run 6 cheques | 0.4688 xBZZ |
| run 7 cheques | 0.4355 xBZZ |

Ledger lifetime 3.371 xBZZ; issuable ~4.0 → headroom ~0.63 (next
measured GiB run needs a chequebook top-up or user call). Batch
47265a62 TTL ~6.0 days.

## 2026-08-23 (night) — post-battery diagnosis (user: "that is a
## failure — diagnose"): THE AGGREGATE CEILING WAS OUR OWN LEDGER'S
## FSYNC — fixed and A/B-measured (flow 1.36 → 3.9 MB/s)

Battery cheque rates were FLAT (26–32/s across all five runs, in-pass
peak 54/s) despite wildly different threshold regimes and 220/s of
per-connection headroom → a shared serializer. Found: ant's
`OutboundLedger::record_issued` re-serializes the whole beneficiary
map (1,159 entries) and **fsyncs a temp file under one global mutex on
every cheque**, and the emit is awaited inline in each connection's
drive loop (stalling its fetch admission too) — the Phase-0 bee
chequebook-mutex finding (#5570) reproduced in our own client. This
capped the node-wide aggregate at ~1–1.4 MB/s regardless of connection
count and fully explains the probe-vs-scheduler 10× per-conn gap
(single-conn probe at 1.3 cheques/s never felt it).

**Fix:** `FastLedger` — in-memory authoritative, 200 ms debounced
background persist off the tokio workers, same JSON format
(ant-compatible, roundtrip-tested), flush at run end; crash window
≤200 ms of cheques, bounded + self-healing, documented. **A/B (same
warm 110-conn workload): bulk 116 MB in ~40 s, flow peak 3.88 MB/s
(945 chunks/s) vs 1.8 pre-fix; cheque rate 75/s.** The pass is now
work-starved (buckets drain), the wall tail is slow-λ storers — a
selection/hedging problem, not a ceiling. Third serialized
money-critical section found on this path (bee chequebook mutex, ant
fresh-threshold cap, our ledger fsync) — the settlement path attracts
this bug class; audit rule added to the M6 list. Full diagnosis
narrative → next session's REPORT addendum; spend 0.053 xBZZ (A/B).

| spend item (this entry) | amount |
|---|---|
| A/B warm pass cheques | 0.0529 xBZZ |

## 2026-08-23 (evening) — M5 ACCEPTANCE BATTERY DONE — Phase 1
## measurements complete; REPORT-phase1.md written; human review open

**Chequebook funded (user-approved):** Nook wallet →3.0 xBZZ→ spike
wallet (bee withdraw, tx `0x6f964265…`) →3.0 xBZZ→ chequebook deposit
(new `fund-chequebook` command, signed ERC-20 transfer, tx
`0x349daae0…`); chequebook 0.981 → 3.981 xBZZ.

**Battery: 5 × full-1 GiB cold-store runs, 110 conns, redundancy 2,
multi-pass over all 512 neighborhoods + parallel bee-fallback drain +
reassembly verify.** Results (full table + honesty notes in
REPORT-phase1.md):
- walls 51:03 / 33:02 / 22:03 / 18:59 / 19:07 → **median 0.817 MB/s
  per byte-complete GiB** (max 0.949; warm within-pass peak 1.36);
- **byte-exact SHA-256 vs the wsbench reference on runs 1 and 5**;
  runs 2–4 skipped reassembly-verify (drain-pass bug: errored when no
  cache storer covered the tail — FIXED same day; all chunks in all
  runs BMT-validated at fetch time);
- **median cost 0.436 xBZZ/GiB**; ~99.8% of chunks over the direct
  settled plane; 1,159 lifetime beneficiaries, 1,188 peers in
  peerstate; runs sped up 51→19 min as thresholds grew (earned trust);
- residual owed-on-drop ~0.001 xBZZ/run, auto-repaid at next contact.

**Acceptance verdict: correctness + scaling shape PASS; the 25 MB/s
number NOT met (30×)** — decomposed in the report: at 1 GiB each
neighborhood holds only ~2.1 MB, so connection setup (~15–25 s)
cancels the ~25 s of flow per storer; a 10 GiB payload projects to
~9–15 MB/s on the same machinery. **Phase-1 review gate open**;
options for the human: accept with the measured curve (target restated
as MB/s at N GiB), or fund a 10 GiB experiment (~0.9 xBZZ postage +
~4.4 xBZZ per full settled fetch).

| spend item (this entry) | amount |
|---|---|
| acceptance runs 1–5 cheques | 2.2332 xBZZ |
| wallet→chequebook deposit (moved, not spent) | (3.000 xBZZ) |
| gas (withdraw + ERC-20 transfer) | ~0.0001 xDAI |

Balances after: Nook wallet 17.47 xBZZ / ~1.19 xDAI; spike wallet
0.5 xBZZ / ~0.1 xDAI; chequebook 3.981 xBZZ on-chain, lifetime issued
2.414 (uncashed cheques are liabilities against it — headroom ~1.57).
Batch 47265a62 TTL 6.1 days (top-up ~1.1 xBZZ/4 days when due —
standing grant).

## 2026-08-23 (later) — M5 scheduler rebuilt on measured constants +
## connection ramp 20→50→110 MEASURED — scaling now linear, 0.72 MB/s
## aggregate warm at 110 conns (peak flow ~1.8 MB/s)

**Built (schedule.rs rework + peerstate.rs):**
1. **Live-threshold pacing** — each connection parses pricing
   announcements (M4 drained them) and paces against the live T.
2. **λ-aware exposure control** (the probe's ceiling algorithm): bee's
   worst-case ledger view (mirror debt + reserved + cheques within the
   1.5λ window) ≤ 1.05 × T. λ per peer from the new persisted
   peer-state cache (`.phase1/peerstate.csv`: threshold_last, λ,
   settled volume), measured inline once on first contact (sweep
   cheque + small pseudosettle probes), else a conservative default.
3. **Threshold-aware mirror** replaces ant's `Accounting` here too
   (same fresh-cap bug as the probe found).
4. **Work-stealing**: chunks in shared per-neighborhood buckets; every
   covering connection pulls, `--redundancy N` puts N storers on each
   bucket so fast siblings absorb a slow storer's tail.
5. **Selection by earned trust**: per bucket, measured-fast-λ first,
   then last-known threshold, then RTT; slow validators deprioritized,
   never refused. Overlay verified against the cache on handshake.
6. Per-run zero-debt confirmation FROM THE PEER per connection;
   10 s progress sampling for straggler-honest curves.

**Measured (Ethernet, direct-only, full-payload bucket coverage):**
| tier | conns | red. | direct chunks | MB | wall | aggregate | note |
|---|---|---|---|---|---|---|---|
| 1 | 20 | 1 | 9,282 | 38 | 1746 s | 0.022 MB/s | straggler-dominated: one slow storer alone on its bucket ran 25 min |
| 2 | 50 | 2 | 13,534 | 56 | 192 s | **0.289 MB/s** | redundancy killed the tail |
| 3 | 110 | 2 | 27,598 | 113 | 195 s | **0.581 MB/s** | 50→110 ≈ linear |
| warm | 110 | 2 | 29,079 | 119 | 166 s | **0.718 MB/s** | peak flow ~**1.8 MB/s** (t=20–30 s: 440 chunks/s) |

- **Scaling is linear in connections now** (old M4: flattened by 32
  conns). Cold per-conn is warm-up-dominated (λ probe + fresh 1.35 M
  thresholds); the warm run's early burst shows the grown regime:
  ~0.016 MB/s/conn at only-partly-grown T after ~5 min of lifetime
  paid volume per peer. Rate ∝ T and T grows with volume, so longer/
  bigger runs climb toward the probe-measured 0.08–0.14 MB/s/conn.
- **λ distribution at scale confirms the sweep**: 204 peers now in
  peerstate; tier-1's 20 fresh λs: 16 fast (~1.2–1.4 s), 4 slow
  (12–19 s) — the slow quartile is exactly the straggler set.
- **Etiquette**: ~290 dials this entry (one attempt each), ZERO
  refusals, zero blocklists. Residual owed-on-drop 2.2 + 1.8 + 14.0 +
  13.5 M units (~0.003 xBZZ total) on peers that hung up before the
  final sweep — auto-repaid on next contact via the persisted ledger;
  84/110 warm-run connections got bee-side zero confirmation.

| spend item (this entry) | amount |
|---|---|
| tier 1 (20 conns) cheques | 0.0183 xBZZ |
| tier 2 (50 conns, red 2) cheques | 0.0267 xBZZ |
| tier 3 (110 conns, red 2) cheques | 0.0515 xBZZ |
| warm 110 re-run cheques | 0.0541 xBZZ |
| **entry total** | **0.1506 xBZZ** |

Lifetime chequebook cumulative 0.1801 xBZZ across 227 beneficiaries
(issuable ~0.99 → ~0.81 headroom). Nook wallet 20.47 xBZZ / 1.19 xDAI.

**Next: M5 acceptance battery** (PLAN: 5 × 1 GiB cold runs, medians/
p95, cost/GiB, REPORT-phase1.md). ⚠ NEEDS A DECISION + FUNDING: full
cheque settlement ≈ 0.58 xBZZ/GiB → ~2.9 xBZZ for the battery; the
spike chequebook holds ~0.81 headroom, so a ~3 xBZZ wallet→spike-
chequebook top-up path is required (Nook wallet → spike wallet →
chequebook deposit; deposits are pre-authorized by standing grant, but
the ~3 xBZZ spend scale deserves explicit user sign-off first). A
single full-GiB run (~0.58) fits current headroom if a cheaper
first-look is preferred. Also queued for the battery: connections stay
at ~110 with redundancy 2 (≈ 512 buckets need coverage for a FULL
payload → the full-GiB run wants ~256–512+ conns or multiple passes —
propose: full-payload runs at 110 conns × repeated passes with the bee
fallback covering the uncovered tail, labeled accordingly).

## 2026-08-23 — M5 groundwork: decision experiment MEASURED — threshold
## growth + validation latency; 25 MB/s path confirmed, NO design rethink

**Context:** post-M4 review question ("did the phase fail? rethink the
design?"). Answer: M4 did not fail — every milestone's results stand;
the open risk was whether the 25 MB/s target's arithmetic (per-conn
rate × ~110 conns) survives contact with bee's real settlement
behavior. Ran the decisive experiment before building M5.

**Built:** `directswarm probe-growth` (ds-net/src/growth.rs + CLI) —
one long-lived, fully settled storer connection with three phases:
(A) *growth*: continuous paced fetch cycling the storer's neighborhood
chunk set, threshold-adaptive pacing, every pricing announcement
logged; (B) *λ sampling*: quiesce, sweep debt with one cheque, then
tiny (50k-unit) pseudosettle probes — bee ACKs
`min(attempted, allowance, its-debt-view)`, so the ACK drops to zero
exactly when the cheque credits → per-peer cheque-validation latency
at ±1.15 s resolution; (C) *ceiling*: λ-aware exposure pacing (mirror
debt + reserved + unvalidated-cheques-in-λ-window ≤ 1.05 × T) measuring
sustained per-conn rate at the grown threshold. Every run ends with a
sweep + a bee-side zero-debt confirmation probe. Events per run in
`.phase1/growth/*.jsonl`, summaries in `.phase1/m5-growth.csv`.

**Verified in bee source first** (pkg/accounting): threshold growth is
real, VOLUME-driven, and announced — a light peer starts at 1.35M
units and gains +450k (lightRefreshRate) each time its cumulative
settled debt crosses checkpoints of 45M units, linear until T≈9.45M
(~15 MB paid), then exponentially spaced checkpoints (near-plateau);
each upgrade is announced via the pricing stream. Persists in the
storer's memory across our reconnects (while their bee runs).

**Measured (13 live runs, 11 distinct storers, Ethernet):**
1. **Growth curve confirmed exactly**: pilot (M2's storer 1e9d7cc9…)
   walked 1.35M → 9.45M in the predicted 18 linear steps (~60 s/step at
   our settle rate), then entered the exponential phase (→9.9M at the
   predicted 1.62B cumulative) — 3,592 fetches, 0 errors, 211 cheques
   all accepted. Grown threshold re-announced on reconnect.
2. **Validation latency λ (the swing variable): 10/11 storers are
   FAST** — λ ≤ 1.2–1.5 s, all probe-resolution-limited (true λ likely
   well under 1 s); 1/11 (157.180.102.154) is slow at ~18 s
   (their RPC). λ is per-operator infrastructure → a storer-selection
   criterion alongside RTT.
3. **Sustained settled per-connection rate** (phase C, 3-min runs,
   zero errors, zero residual — bee-side confirmed): **0.081 MB/s** at
   grown light threshold (T≈9.45M); **0.139 MB/s** on a high-threshold
   operator (T=13.5M fresh → 27M by run end). NOTE: 3/10 sweep storers
   announce 13.5M (full-node default) to fresh strangers — a
   no-warm-up fast lane that grows at full-rate steps (+4.5M/checkpoint).
4. **Two implementation bugs found by the pilot, fixed**: (a) ant's
   `Accounting::try_reserve` hard-caps balance+reserved at the FRESH
   light disconnect limit, silently re-capping any grown-threshold
   connection to free-tier pacing (~0.008 MB/s) — replaced with a
   threshold-aware single-peer mirror in growth.rs (ant kept stock;
   candidate upstream note, user-gated); (b) probe launch script needed
   its output dir (trivial).

**Verdict: NO design rethink.** The M4 slowness decomposes fully into
(i) fresh-threshold pacing — cured by volume-driven growth we can now
drive deliberately, (ii) λ-safe pacing conservatism — cured by exposure
control and λ-aware storer selection, (iii) the fixed mirror cap bug.
Path to 25 MB/s: 0.139 MB/s/conn × ~180 conns TODAY on high-threshold
fast-λ storers; ~110 conns needs ~0.23 MB/s/conn, plausible via M5
(exposure AIMD instead of resolution-capped λ̂ pacing, longer-lived
connections growing T past 27M, storer selection by threshold + λ).
Warm-up economics: growing a light connection to plateau costs ~15 MB
of paid traffic ≈ 0.008 xBZZ per storer, persists across reconnects →
argues for the design's long-lived daemon/connection-reuse posture.

**M5 plan sharpened by this data:** per-peer state (threshold, λ,
cumulative-settled) persisted in the topology cache; exposure-based
pacing; storer selection = coverage × RTT × threshold × λ; slow-λ
storers deprioritized, never blacklisted (etiquette).

**Etiquette ledger:** 13 dials (11 distinct peers, one attempt each,
sequential, ≤1 conn at a time); long-lived connections held 3–25 min
each, all politely disconnected after zero-debt confirmation; no
refusals, no blocklists tripped this session. Chunk set cycled
(re-fetches paid full price — settlement-pacing measurement, labeled).

| spend item (this entry) | amount |
|---|---|
| probe cheques, 13 runs (1.9B units settled) | 0.0219 xBZZ |
| pseudosettle free tier consumed | ~0.8B units (protocol allowance) |

Spike chequebook lifetime cumulative now 0.0295 xBZZ of ~0.99
available. Wallet after user top-up: 20.47 xBZZ / 1.19 xDAI (batch
47265a62 TTL 6.66 d at entry — next top-up funded).

## 2026-08-22 — PHASE 0 GATE: **GO** (human review held) — Phase 1 starts

**Human review outcome (user, 2026-08-22):** Go — start Phase 1
(fetcher MVP, S1 storer-direct, Rust/ant substrate per design rev 2.4),
with the 25 MB/s acceptance target kept as written and understood to
require ~110 concurrent connections at today's light-peer thresholds
(one light slot per storer, hundreds of neighborhoods available —
within etiquette). Rationale accepted: the service-rate gate failed
as written but for a client-side reason (light-peer credit pacing +
bee chequebook serialization), not storer policy — storers gave zero
refusals; patched-chequebook aggregate scales ~linearly (4.56 MB/s at
20 peers, fully settled). Our own Rust SWAP implementation issues
cheques with the cached invariant natively, so Phase 1 does not block
on upstream. Exits A/B rejected as not matching the measured facts.

**Bee PR decision (user):** keep the PR unsubmitted; the issue
(ethersphere/bee#5570, still zero comments) already offers it on
request — submit only if a maintainer asks. Keep watching the issue.

**Payload logistics:** batch `47265a62…` topped up +4 days
(amount +5,329,082,880/chunk, tx `0x69a5b024…`) → TTL **6.94 days**;
cost **1.118 xBZZ** (standing top-up grant). Keeping the payload alive
costs ~0.28 xBZZ/day at today's price (77,099 PLUR/chunk/block,
depth 21).

**⚠ BALANCES LOW — user top-up needed:** Nook wallet now
**0.47 xBZZ** / 0.57 xDAI. The next batch top-up (~1.1 xBZZ/4 days)
exceeds the wallet; Phase-1 measured runs will also need settled
retrieval (Phase-0 scale suggests ~0.01–0.1 xBZZ per measured GiB
sweep, plus chequebook headroom). Suggest topping the wallet to
~5 xBZZ to cover the phase.

| spend item (this entry) | amount |
|---|---|
| batch 47265a62 top-up, +4 days TTL | 1.118 xBZZ |

**M3 addendum — full-coverage crawl (user-blessed 240 dials,
2026-08-22):** two further runs (128 + 107 dials, both wall-capped at
their politeness limits, every dial accepted) bring the **grand-union
topology cache to 2,734 storers (296 dialed+verified, 314/315 dials
accepted = 99.7% reachability at n=315) covering 511/512 payload
neighborhoods = 99.81% of the 1 GiB chunk set**. Merged canonical
cache: `.phase1/topology.csv` (M4's input). The one uncovered
neighborhood (~500 chunks, 0.19%) is forwarding-fallback territory —
or a lazy targeted lookup at fetch time. Etiquette ledger: dial
blessings used — 80 under the Phase-0 grant (2×40), 235 under the new
240-dial grant (user, 2026-08-22); ≤2 dials/s, one attempt per peer,
no retries, polite disconnects throughout; spend 0.

**Phase 1 M4 diagnosis (same day, user-prompted "thousands of times
slower — something is seriously wrong"): TWO real bugs found and
fixed; the remainder is a quantified protocol ceiling, not a defect.**
Instrumented per-chunk/per-settlement timing and ran seven live
single/16-conn diagnostics:
1. **Wire is fast**: retrieve p50 60–80 ms; with headroom a single
   connection burst **16 chunks in 0.55 s ≈ 0.12 MB/s** — the target
   regime exists on the wire.
2. **BUG (the big one): in-memory cheque ledgers.** The scheduler
   opened a fresh outbound ledger per run/connection, but bee's
   chequestore permanently keeps the highest validated cumulative per
   chequebook — so re-runs sent non-increasing cheques, bee rejected
   them ("ChequeNotIncreasing"), debt built unsettled, peer blocklisted
   us ~10 s in. Explained ALL the mysterious mid-run deaths. Fixed:
   one persisted ledger (`.phase1/identity/outbound-cheques.json`)
   shared across connections and runs; as a bonus its emit-time
   cumulative runs ahead of bee's validation-time store, so owed-on-drop
   residue is automatically repaid by the next accepted cheque
   (measured: the first synced run paid 0.0012 xBZZ of prior residuals).
3. **Calibrated the true pacing constant**: bee credits a cheque only
   after ~4 on-chain RPC calls ≈ **2.5–3 s validation latency**. Safety
   invariant `cap + one_unvalidated_cheque ≤ 1.6875M` → cap 800k,
   cheque ≤600k, instant self-credit, ≥3 s between cheques. (The old
   2.5 s credit delay had the right magnitude for the wrong reason.)
4. **Result: fully stable and honest.** Single conn: 427 chunks, 40
   cheques, **0 residual, 0 drops**, 0.013 MB/s (4× before). 16 conns:
   **1751/1769 chunks direct (99%), 0 residual, 0 warnings**, steady
   ~12.7 chunks/s ≈ 0.05 MB/s aggregate during flow; wall time is
   straggler-dominated (static queues — work-stealing is M5).
5. **The remaining gap to 0.23 MB/s/conn is protocol, not code**: a
   FRESH light connection's settled inflow is capped at ~450k units/s
   free tier + (337k margin ÷ ~3 s validation) cheque channel ≈
   0.6–1M units/s ≈ 3–5 chunks/s. Bee **grows** a well-behaved peer's
   threshold over minutes (announced via pricing, which we parse) and
   every margin scales with T — Phase-0's 0.23 MB/s/conn was measured
   in that grown regime. M5: per-peer threshold tracking, per-peer
   validation-latency probes, work-stealing, long-lived connections.
Diag spend ≈ 0.003 xBZZ. Cheque acceptance now proven at 231
cheques/run with zero residual.

**Phase 1 M4-cont (same day): transport reworked — SCALING FIXED.**
Replaced the single shared swarm with **one libp2p swarm + poller per
connection**, actors dialed in PARALLEL (churned-storer timeouts now
overlap instead of summing). Re-measured on Ethernet, direct-only:

| conns | direct MB/s | wall | per-conn | vs old |
|---|---|---|---|---|
| 4 | 0.021 | 24 s | 0.0054 | (old 4: 0.020, 79 s) |
| 16 | 0.055 | 41 s | 0.0035 | **old 16: flat, never finished** |
| 32 | 0.085 | 92 s | 0.0027 | (old couldn't reach) |

**Aggregate now rises monotonically and every run completes** — the
single-poller ceiling is gone; the funnel works structurally, as
Phase 0 predicted. Two honest caveats remain:
1. **Sublinear** (4→32 = 8× conns → ~4× throughput; per-conn declines
   0.0054→0.0027). A secondary shared-resource bottleneck — most
   likely the global `ChunkStore` mutex (every chunk = lock + file
   seek/write) and/or public-RPC contention. M5: shard/blocking-offload
   the store, parse per-peer threshold, AIMD depth, hedged tails.
2. **Per-connection rate is still settlement-paced** (~0.003–0.005
   MB/s): reserve cap = ½ threshold (~4 chunks in flight) × bee's
   ~2.5 s cheque-credit lag. At this rate 25 MB/s is not reachable by
   connection count alone; it needs the per-connection rate up ~40×
   (the Phase-0 *patched-bee* regime showed 0.23 MB/s/conn → ~110
   conns = 25 MB/s). The credit-lag pacing is the M5 lever.
3. **Residual debt on peer-drop** (2–7 M units/run ≈ 0.0002–0.0007
   xBZZ): when bee drops a slow/idle connection before our final sweep
   cheque lands, that debt is owed-but-unpayable (we *attempted* to
   settle — not free-riding; logged). M5: settle more eagerly (lower
   trigger) and track owed-on-drop for repay-on-reconnect.
Rework spend ≈ 0.013 xBZZ (three tiers, all live cheques). Scaling
curve in `.phase1/m4-scaling-reworked.csv`. **The "doesn't scale"
blocker is resolved; remaining work is per-connection throughput
(M5).**

**Phase 1 M4 (same day): multi-connection settled scheduler built +
measured — CORRECT, but does NOT scale as built.** `directswarm
fetch-direct` stands up one libp2p swarm with N settled storer
connections (shared Accounting, shared OutboundLedger, shared
cached-invariant, atomic global cheque spend cap), routes each chunk
to the shortest-queue covering connection, settles per-connection
(pseudosettle cadence + cheque at threshold/4 + final sweep), falls
back to local bee for uncovered chunks, lands chunks in an on-disk
`ChunkStore`, and reassembles+byte-verifies via the M1 joiner over a
`StoreFetcher`. On-Ethernet measurement (route via enp7s0):

| conns | direct chunks | wall | note |
|---|---|---|---|
| 3 | 182 | (capped) | diag |
| 4 | 389 (+210 fallback) | 79 s **completed** | 105 cheques, **0 residual debt**, 0.0006 xBZZ |
| 8 | 1100 | 340 s cap | did not finish |
| 16 | 1622 | 400 s cap | did not finish |

**Correctness proven** (4-conn: fully settled, zero residual, cheques
accepted multi-peer). **Scaling FAILED**: more connections did not
raise throughput (~4–8 chunks/s aggregate regardless of N) — opposite
of Phase-0's arithmetic, so the limiter is this implementation, not
the network. Root causes, in order:
1. **Single swarm poller** — all N connections' retrieval + settlement
   streams funnel through one `libp2p::Swarm` driven by one task,
   serializing stream negotiation. This is the throughput ceiling and
   the reason aggregate is flat in N. Fix (M4-cont / M5): independent
   transport per connection (own swarm/poller per storer, as the M2
   probe had) or shard connections across several swarms/pollers.
2. **Sequential dial, 20 s timeout each** — storers churned out since
   the M3 crawl each burn the full timeout, so a high-N run spends most
   of its wall clock dialing, not fetching. Fix: parallel dial + drop
   the cache's stale/gossip-only entries first (prefer dialed_ok=1).
3. **Settlement pacing** — reserve cap (½ threshold ≈ 4 chunks in
   flight) × bee's ~2.5 s cheque-credit lag paces each connection to a
   few chunks/s. Improvable by raising in-flight headroom carefully or
   pipelining cheques ahead of credit.
Not a phase-gate failure — the fetcher is correct and settled; the
throughput target (≥25 MB/s) is a performance-engineering problem with
a clear cause. **Recommended next: rework the transport to one poller
per connection (restores the M2 per-connection rate, ~0.2 MB/s
patched / lower unpatched, ×N) + parallel dial, then re-run the ramp.**
Spend this milestone: ~0.0006 xBZZ (one completed 4-conn tier; the
capped runs settled their own partial debt to zero, ledgered).
`m4-scaling.csv` holds the one completed row. AIMD depth, hedged tails,
and per-peer threshold parse remain deferred to M5.

**Phase 1 M3 done (same day): topology cache + polite crawler,
measured.** `directswarm crawl` — bounded snowball: seed dials (from
Phase-0 reach.csv), BZZ handshake with RTT recorded, ~4 s gossip
harvest per peer, strict etiquette (≤40 dials/run at ≤2/s, one
attempt, no retries, polite disconnects, 10-min cap — inside the
Phase-0 blessing). Two runs, 80/80 dials accepted, ~910 hive hints
each; **union cache 1,142 distinct storers covering 399/512 payload
neighborhoods = 77.9% of the 1 GiB chunk set**, in ~3 min/run, spend
0 (no retrieval, no cheques). Sans-I/O `TopologyCache` in ds-core
(wasm-clean, RTT-sorted `storers_for()` per the latency-aware
selection design); hive protobuf + bee-2.8 multi-underlay decode in
ds-net. Full coverage path: either a larger crawl (needs a fresh
etiquette blessing, ~200+ dials) or M4's lazy prefix-targeted lookups
with forwarding fallback for the tail — the design prefers lazy.
Next: **M4** — the multi-connection scheduler (cache + M2's settled
stream engine, AIMD depth, hedged tails, fallback, 20→50→110
connection ramp with etiquette review at each step).

**Phase 1 M2 done (same day): first fully-settled DIRECT storer
stream on mainnet.** Run 6 against stranger storer `1e9d7cc9…`:
**200/200 chunks, 0 errors, p50 59 ms / p95 81 ms, 17 SWAP cheques
(9.63M units = 9.63e11 PLUR ≈ 0.0001 xBZZ) accepted** — acceptance
proven live by the deduction header dropping 100 → 0 after cheque #1 —
plus 76 pseudosettle refreshes (34.15M units), connection healthy end
to end, polite disconnect. Throughput 0.007 MB/s **by design** (probe
runs safety-paced: reserve cap = announced-threshold/2, 2.5 s cheque
credit delay; adaptive pacing is M4's job — Phase-0's patched-bee
client showed ~0.23 MB/s/connection is reachable). Getting here took
six live runs whose failures are findings:
- **ant bug (to report upstream): every ant-emitted cheque is rejected
  by bee** — `encode_signed_cheque_json` quotes `CumulativePayout`,
  bee unmarshals into Go `math/big.Int` which accepts only an unquoted
  JSON number ("unmarshal cheque" → cheque discarded). Also ant's
  `emit_cheque` ignores the swap stream's settlement headers, so even
  with fixed JSON its amounts are off by the exchange rate (100,000×).
  Both fixed in ds-net (`emit_cheque_at_rate`, `encode_cheque_json_bee`,
  unit-tested); ant issue/PR to propose (user review first — Phase-2
  style gate applies to any upstream posting).
- **Cheque-credit lag is a real protocol hazard**: bee credits a
  cheque only after ~4 on-chain validation calls; a payer that runs at
  the threshold (or ant's disconnect-limit cap) overruns bee's ledger
  during that lag and gets blocklisted (runs 3–5: bee peaked +22k
  units over its 1.6875M limit). Mirroring bee's own early-payment
  posture (cap = T/2) makes the worst-case refill burst peak at ~T,
  safely under 1.25T.
- Pricing threshold parse (M2 deliverable) works: peers announce
  1,350,000 units and rate 100,000 PLUR/unit + one-time deduction 100.
- Residual 450k units on run 6 = spend guard (1e12 PLUR cap) rightly
  refusing to exceed budget but thereby blocking the final settlement
  sweep — design fix queued (guard must stop fetching, not settling);
  default raised to 5e12.
- Cost reality check for Phase 2 economics: full cheque settlement at
  today's rate ≈ 2.2e10 PLUR per 1-hop chunk → **~0.58 xBZZ/GiB when
  cheque-settled** (the pseudosettle free tier covers ~4% at target
  speeds). Phase-0's tiny settlement spends reflect free-tier subsidy
  at low rates, not the real price.
- Open M2 items → M3/M4: 10 s handshake (V15 open likely timing out
  before V14 fallback — log/measure), adaptive credit-lag pacing,
  threshold-growth tracking, spend-guard fetch-gating.
Spike identity reused (eth 0xDEdAc9…, chequebook 0xE8C7aD…, fresh
persisted overlay nonce); cheque spend this session ≈ **0.00014 xBZZ
ledgered** (runs 1–5 cheques were rejected by peers and cannot be
cashed; run 6's 0.0001 xBZZ is live). Blocklist etiquette: runs 1–5
each tripped one storer's accounting blocklist (short, escalating
timer, self-healing) — all on distinct peers, stopped immediately on
diagnosis, no retries against blocked peers.

**Phase 1 M1 done (same day): `directswarm fetch` works, byte-verified.**
End-to-end fetch of the 1 GiB payload over the forwarding-fallback path
(ant streaming joiner + our `BeeApiFetcher` over bee `/chunks`, every
chunk CAC/SOC-validated in our code): SHA-256 of the output **exactly
matches** the independently generated wsbench seed-1 reference
(`aa9e07db…5c0f`), across a deliberately interrupted run + resume —
sidecar committed at byte 176,160,768, unverified tail truncated,
resumed with a range-join, sidecar removed on completion. Honest
labels: transport was the LOCAL bee node, **cache-warm** (payload
still in its localstore from upload) — 14.5 MB/s average is a
correctness figure, NOT a retrieval benchmark; cold `/chunks` is very
slow (~2 chunks in 25 s observed — bee does a full network retrieval
per request, serialized behind the joiner's fanout of 16), which is
fine for a residual-chunk fallback but confirms the fast plane is the
throughput story. Curiosity for later: a full-file `/bytes` GET on
bee 2.8 stalled (43 KB delivered in 13 min) while range requests
served briskly — not investigated, noted only. Settlement: run was
almost entirely cache-served; whatever residual network retrieval bee
did, it settled itself (dust, unmeasured, non-benchmark run).
Toolchain note: rustup stable updated 1.86 → 1.98 (reqwest transitive
deps require ≥1.88). User informed the host disk is ~100% full
(2.7 GB free); the 1 GiB verify artifact was deleted after hashing.
Next: **M2** — one settled direct storer stream (dial, handshake,
retrieval + accounting + pseudosettle + cached-invariant cheque
issuance via the spike chequebook 0xE8C7aD…).

**Phase 1 M0 done (same session):** `solardev-xyz/ant` cloned to
`~/projects/ant` (@ c526a33) and surveyed crate-by-crate;
**PLAN-phase1.md** written (build-vs-reuse map, milestones M0–M5,
risks). Rust workspace scaffolded under `rust/` — `ds-core` (sans-I/O,
first module: overlay proximity/neighborhood math, 6 tests),
`ds-net` (native adapter, empty shell), `ds-cli` (`directswarm`
binary stub) — with CI (fmt, clippy -D warnings, tests, and a
`wasm32-unknown-unknown` check of ds-core) green locally from the
first commit. Key survey facts driving the plan: ant's retrieval path
settles via pseudosettle only (no cheques — collides with principle 1;
we build the retrieval-side SWAP trigger with the cached-invariant
balance check, candidate upstream contribution), pricing thresholds
are drained unparsed (we parse them), and ant has no sans-I/O/wasm
story (our workspace shape supplies it). Next: **M1** — end-to-end
`directswarm fetch` over the local-bee fallback path, byte-verified
against the Phase-0 payload.

## 2026-08-22 — PHASE 0 MEASUREMENTS COMPLETE — human review gate open

Full report: **REPORT-phase0.md** (raw CSVs in `.phase0/`). Headline:

- **Reachability gate: PASS** — 41/41 sampled storers accept a
  stranger's dial+handshake (gate ≥50%).
- **Service-rate gate: FAILS AS WRITTEN, for a reason the plan didn't
  enumerate** — median peak 0.074 MB/s per storer (gate ≥1 MB/s), but
  with ZERO refusals in 39 settled runs and storer bursts ≥0.35 MB/s:
  the ceiling is the light-peer payment threshold pacing our own
  settlement (40,481 overdraft waits), not storer policy. Aggregate
  still scales with concurrent connections (unmeasured — next ask).
- 1-hop price confirmed ~29% cheaper than forwarding in protocol units
  (219,936 vs ~310k units/chunk); 901 real cheques accepted by 10
  strangers, 0.0065 xBZZ settled.
- Milestone-2 debugging findings folded into the report: a client must
  mount pricing AND hive to be tolerated at all; depth-100 pipelines
  push peers to disconnect-limit (cap 32 in future).

**Aggregate concurrency measured (user-approved option 1):** 1/5/20
parallel peers → 0.074/0.149/0.251 MB/s aggregate — sublinear because
**bee's chequebook re-verifies covering balance via two on-chain RPC
calls under one global mutex on every cheque (~5–6 cheques/s per
client; verified in source, `reserveTotalIssued`→`AvailableBalance`)**;
the marginal gain at high peer counts is stacked pseudosettle free
tier, labeled as such, never a strategy. Fix built and MEASURED: ~50-line cached-invariant patch (bee branch
`chequebook-cached-balance`, worktree `.phase0/bee-patched`) →
5-peer 0.155→1.08 MB/s (7×), 20-peer 0.277→**4.56 MB/s (16×)**,
cheques 5/s→62/s node-wide, aggregate now ~linear in connections;
25 MB/s target needs ~110 connections at today's thresholds, fully
settled. Bee issue **FILED (user-approved) as ethersphere/bee#5570** with
before/after numbers, self-contained (no directswarm disclosure), PR
offered on request — watch for maintainer response; patch ready on
branch `chequebook-cached-balance`. Retrieval settlement spend now ~0.01 xBZZ total.
**Phase-0 measurement set complete; human review gate open.**

**Spend total this phase: ~1.17 xBZZ** (0.880 postage + 0.286 upload
settlement + 0.0065 retrieval settlement + gas dust). Balances:
Nook wallet ~2.28 xBZZ / 0.55 xDAI, Nook chequebook ~2.28 xBZZ
available; spike wallet 0.5 xBZZ / ~0.1 xDAI, spike chequebook
~0.99 xBZZ available.

## 2026-08-22 — Phase 0 in progress (live session)

**Environment:** Nook's bee 2.8.1 light node (localhost:1633), 137
peers, depth 9. Laptop moved from wifi to Ethernet mid-session (before
any measured retrieval runs; payload upload started on wifi, finished
on Ethernet — logistics, not a benchmark claim).

**Done so far:**
- Test payload live on mainnet: 1 GiB deterministic (wsbench seed 1),
  ref `842efaa92f86fe67dd7bd244a7c7935cade4da1eee41ea558f49c00da90a759a`,
  all 264,209 chunks tag-verified synced; local accept 2.80 MB/s.
  Batch `47265a62…` depth-21 mutable, ~3.1 days TTL left, utilization
  0.47 after upload.
- Spike milestone 1 (`spike/cmd/reach`, bee-as-library, compiles
  clean): reachability probe. Run 1+2 collected zero hive records —
  mainnet bootnodes never announced to us (finding to investigate:
  that's how stock light nodes are meant to bootstrap), and bee's
  dialer silently drops private/loopback underlays without
  `AllowPrivateCIDRs` (fixed). Also: bootnode `libp2p.direct` /ws
  underlays are rejected as unsupported transport — data point for the
  browser-transport story. Run 3 (seeded from Nook's node) in flight.
- Etiquette caps blessed by user for the probe: ≤3 bootnode connects,
  90 s passive listen, ≤50 dials at ≤2/s, one attempt per node, no
  retries, polite disconnects, 15-min hard cap.

**Reachability probe — first results (run 7, 2026-08-22, Ethernet):**
41 full-node records collected from 3 seed announces (35 distinct
depth-9 neighborhoods); **41/41 sampled nodes accepted a dial + full
bee handshake from an unknown light peer (100%)**. Dial+handshake
wall-clock: median 191 ms, p25 189, p75 346, p95 559, max 891 (2–3
round trips + crypto — network RTT is roughly a third). Caveats,
honestly: the sample is *biased toward reachable nodes* (records are
peers currently connected to three healthy full nodes, and hive only
gossips full nodes), and n=41 came from one announce burst. Gate-grade
reachability needs a snowball crawl with a larger cap (needs a new
etiquette blessing). Raw rows: `.phase0/reach.csv`.

**Protocol findings from the debugging path (runs 1–6):**
1. **Mounting hive alone gets you ejected**: stock peers open a
   pricing stream right after the handshake to announce their payment
   threshold; a client that can't accept it is disconnected within
   seconds. Any directswarm client must mount pricing/accounting from
   the first dial. (This is also a nice fact upstream: the network
   already refuses accounting-incapable peers.)
2. **Bootnodes don't bulk-announce**: their own bins are empty
   (bootnode mode kicks everyone), so a hanging light peer gets only a
   drip of records as new full nodes connect. Ordinary full nodes
   announce their whole connected set immediately — seed crawls from
   full nodes, not bootnodes.
3. **bee dialers refuse to keep light peers** (`ErrDialLightNode`) —
   S2 client↔client connections (Phase 4) cannot ride stock bee's
   Connect as-is; relevant to OPEN-QUESTIONS 11.
4. bee's libp2p service panics without a topology notifier
   (reachability worker); a kademlia-less client must install a no-op
   PickyNotifier. Also: `AllowPrivateCIDRs` needed for loopback/LAN
   dials; bootnode `libp2p.direct` /ws underlays are unsupported by
   bee's own transport set (browser-transport data point).
Seeding note: probe bootstraps from swarmscan (public explorer) — fine
for a spike; the real crawler should snowball from bootnode drip alone
to avoid any external dependency.

**Spike settlement identity (milestone 2, live):** eth
0xDEdAc9Ac6BaDD4B4C9ff99e4B54f3E8835892E8A, overlay b52342…, own
chequebook **0xE8C7aD1Af8CAb91E2695EfD1a12dBfCc186dFD41** (deploy tx
0xd61c96…), 1.0 xBZZ deposited/available, 0.5 xBZZ + ~0.1 xDAI in its
wallet. Chunk enumeration verified offline: 264,209 chunks, computed
root == mainnet ref (ROOT CHECK PASS). Nook config gained
`withdrawal-addresses-whitelist` (spike address only).

**Spend ledger (this session):**
| item | amount |
|---|---|
| postage batch 47265a62 (depth 21, mutable) | 0.880 xBZZ |
| upload sync settlement (1 GiB pushsync)    | 0.286 xBZZ |
| wallet→chequebook deposit (tx 0x4b2bda…)   | (2.000 xBZZ moved, not spent) |
| Nook wallet → spike wallet (txs 0x9ce874…, 0xbf5f60…) | (1.5 xBZZ + 0.1 xDAI moved) |
| spike chequebook deploy+deposit gas        | ~0.000005 xDAI |
| spike chequebook deposit                   | (1.0 xBZZ moved, spends as settlement during measurement) |
| **total spent** | **1.166 xBZZ (+gas dust)** |

Standing grants added to CLAUDE.md: batch purchases/top-ups AND
wallet→chequebook deposits when necessary (user, 2026-08-22).
Chequebook available after deposit: ~2.28 xBZZ.

Balances after upload: wallet ~5.78 xBZZ + 0.667 xDAI; chequebook
available **0.281 xBZZ** (total 3.026). Retrieval runs need ~1–1.7
xBZZ settled → wallet→chequebook deposit of 2 xBZZ proposed, awaiting
user approval.

## 2026-08-22 — pre-flight approvals; design stage closed

Human approvals, defaults accepted: **Q5 posture confirmed** (spike
quietly — protocol-compliant, paid, polite — propose with data at
Phase 2, whole direction disclosed); **Q20a** test payload = fresh
1 GiB deterministic payload, mutable batch, ~1 week TTL (~0.5–1 xBZZ
postage + ~2.5 h upload); **Q20b** fresh dedicated spike chequebook
(~1–2 xBZZ + xDAI gas to deploy/fund); **Q20c** etiquette caps
proposed and blessed at spike start; **Q20d** license =
**BSD-3-Clause** (LICENSE file added).

New operational permissions from the user (CLAUDE.md updated):
starting/stopping nodes is authorized; warn in advance when a speed
test needs the laptop on Ethernet (wifi not fully reliable); warn when
wallet balances run low so the user can top up.

**Design stage is closed.** Next session starts Phase 0: Bee node up +
funded, payload upload, chequebook deployment — each with a spend
estimate first, balances snapshotted around measured runs.

**Spend this session:** 0 xBZZ.

## 2026-08-22 — design revision 2.4: substrate resolved (Rust); browser reality

**Built:** nothing — design stage.

**Done:** resolved the substrate posture (Q4) and recorded the
browser/Wasm analysis:

- **Product in Rust (extend ant)**, decided by the browser endgame:
  rust-libp2p ships browser transports (websocket/webtransport/
  webrtc-websys), wasm32 is first-class, PyO3 covers Python bindings
  for swarmfs/weightstation. Go compiles to Wasm but go-libp2p has no
  browser-transport story and bee-as-library cannot run in a page —
  bee's value is the fast Phase-0 spike (throwaway) and wire-semantics
  reference. Spike stays substrate-agnostic.
- **Day-one discipline, not a later deliverable**: sans-I/O core crate
  behind a transport trait (native tokio adapter now, websys later);
  wasm32 kept compiling in CI from the first commit.
- **Browser transport reality recorded** (DESIGN.md "Form factor"):
  pages can't dial raw TCP/QUIC, storers listen on nothing else today
  (ws flag ~unused; wss needs certs), so the in-page client becomes
  realistic at Phase 4 where we control both endpoints (seeds/peers on
  WebTransport/WebRTC). New Phase-2 upstream ask: browser-dialable
  listeners on full nodes (WebTransport certhash — config, not
  protocol).
- **New Q20 (H), "Pre-flight"**: what must be settled before Phase-0
  code touches mainnet — test payload (weightstation batches
  lapsed/lapsing; fresh upload, mutable batch, ~0.5–1 xBZZ + ~2.5 h),
  dedicated spike chequebook (+~1–2 xBZZ + gas to approve), etiquette
  caps sign-off, license pick before first code push.

**Measured:** nothing. **Spend:** 0 xBZZ. **Deviations:** none.

**Open items:** blocking before development: Q20 (a–d) and Q5 posture
confirmation. Everything else (fallback semantics, resume format, CLI
shape, Phase-4 questions) decides during or after Phase 0/1.

## 2026-08-22 — design revision 2.3: latency-aware source selection

**Built:** nothing — design stage.

**Done:** folded in latency-aware source selection (DESIGN.md new
section "Latency-aware source selection"; crawler and scheduler
components updated; PLAN Phase 0 now records dial RTTs and Phase 1's
scheduler is latency-aware; Q6 extended with the probe budget and the
prediction-method choice). Key points:

- With pipeline depth capped by the accounting threshold, per-storer
  rate ∝ 1/RTT — choosing which 2–3 of a neighborhood's members to
  dial is the main per-connection throughput lever.
- Discipline: latency ranks the healthy but never disqualifies; RTT
  is the prior, observed service rate the posterior (AIMD keeps the
  final say); ε-greedy floor against hot-spotting and earnings skew.
- Measurement in etiquette order: passive handshake timing → stock
  pingpong probes within the dial budget → GeoIP/ASN or Vivaldi-style
  coordinate prediction for the unprobed.
- Hedged duplicate requests for tail chunks, with the honesty caveat
  that a hedged chunk arriving second is still settled — surplus
  bounded and ledgered.

**Measured:** nothing. **Spend:** 0 xBZZ. **Deviations:** none.

**Open items:** (H) agenda unchanged; rev 2.3 uncommitted pending
review.

## 2026-08-21 — design revision 2.2: discussion points folded into docs

**Built:** nothing — still design stage.

**Done:** recorded the remaining points from the 2026-08-21 design
discussion (form factor, systemic adoption, anonymity), the parts not
already in the docs:

- **Form factor** (DESIGN.md "Form factor and deployment"; README
  "How it ships"): standalone client — core library + thin CLI, daemon
  mode at Phase 4 for seeding; not a bee add-on (no plugin mechanism,
  no fork); own overlay identity and own funded chequebook (no
  chequebook sharing — concurrent cheque issuance conflicts); a local
  bee node is an optional companion for the forwarding fallback only,
  never a bundle; endgame inverts the packaging if upstream adopts the
  strategy natively.
- **Systemic-adoption analysis** (DESIGN.md "Systemic effects"):
  cannot overtake the network structurally — competes only on the
  retrieval forwarding path (storage/sync/incentives untouched by
  invariant 1); self-limiting where it competes (light-slot
  contention, storer policy lever, no benefit for small fetches);
  expected equilibrium is segmentation; externalities disclosed
  (forwarder income shift, cover-traffic thinning, colder forwarding
  caches); counterweight: newly feasible use cases bring settlement
  revenue and postage demand. Phase-2 write-up now asks upstream to
  pick a posture — embrace / constrain / segment (new Q15, (H)).
- **Anonymity reassessed** (DESIGN.md subsection; README "The honest
  trade" updated): the stock baseline is weaker than advertised —
  light clients provably originate every request (they forward
  nothing; the first hop sees requester + content linked), SWAP
  cheques tie fetches to a chequebook identity on any path, and no
  adversarial analysis of "ambient anonymity" exists (community doubts
  noted). Layered anonymity (Tor/Nym in front) is the principled fix
  and composes with direct transfer at least as well as with
  forwarding — measurement parked as new Q14 (C). "Never claim
  privacy" and the opt-in posture are unchanged.
- OPEN-QUESTIONS: new "Ecosystem" section (Q14–15); meta renumbered
  16–19. PLAN Phase-2 write-up scope extended accordingly.

**Measured:** nothing; no network operations.

**Spend:** 0 xBZZ.

**Deviations:** none.

**Open items / next:** revs 2.1 and 2.2 are both uncommitted, pending
the user's review. (H) agenda now: Q5, Q7, Q9, Q13, Q15.

## 2026-08-21 — design revision 2.1: streaming demoted; bee facts verified

**Built:** nothing — still design stage.

**Done:**

- **Live streaming demoted from design goal to deferred note** (user
  decision after discussion; it must not influence the design at this
  stage). Rationale recorded in DESIGN.md "Deferred: live streams":
  streaming largely works on today's Swarm at common bitrates
  (~1.36 MB/s measured ≥ HD), live fan-out is the workload forwarding
  caching handles best, and the one genuine gap the fast plane fixes
  (publish headroom) is already in the plan for bulk reasons. Removed:
  PLAN Phase 5, relay trees, per-stream session meshes, stream/
  component, all streaming references in README/CLAUDE.md.
  OPEN-QUESTIONS 14–16 (relay mechanics, feed latency, streaming
  economics) dropped; meta renumbered 14–17.
- **Verified against `../bee` source** (answers folded into DESIGN.md
  and OPEN-QUESTIONS.md): retrieval handler serves any connected peer,
  no role gating (Q1 mechanism, Q11 partly answered — light nodes
  mount the handler and serve from cache); **pricing is a fixed
  proximity formula, not peer-announced** (`pkg/pricer` FixedPricer) —
  Q12 answered: 1-hop storer fetch is cheapest by construction, S2
  audience serving is mispriced under stock semantics (follow-on
  decision recorded in Q12); full nodes admit ~100 light peers
  (`defaultLightNodeLimit`); retrieval streams carry blocklisting on
  misbehavior. Conclusion recorded in DESIGN.md: **no bee changes are
  required for any phase** — bee changes appear only as optional
  upstream proposals.

**Measured:** nothing on the network; code inspection only.

**Spend:** 0 xBZZ.

**Deviations:** none.

**Open items / next:** discussion agenda unchanged in kind —
OPEN-QUESTIONS (H) items 5 (upstream posture), 7 (settlement identity),
9 (peer-assist scope, now streaming-free), 13 (audience-serving
default). Q12's follow-on (interim non-storer pricing convention vs
defer S2) needs a proposal before Phase 4. Phase 0 remains the first
executable step.

## 2026-08-21 — design revision 2 (session end)

**Built:** nothing — design stage; no code, per brief.

**Done:** revised all handoff files to widen scope from "bulk fetcher
dialing storer neighborhoods" to a two-plane design: Swarm as the
anchor plane (identity, integrity, persistence, discovery, private
fallback) and a libp2p fast plane for mass data with three source
classes — S1 storer neighborhoods, S2 audience peers, S3 publisher/
mirror seeds — plus a live-streaming sketch (feed-anchored segments +
paid relay trees; gossip for control only, never content). The core of
the revision is the value-preservation analysis (DESIGN.md, "what
forwarding kademlia buys"): four invariants — anchored on Swarm, stock
chunks, always settled, stock fallback intact — with requester
anonymity named as the one knowing trade (opt-in mode, private path
stays first-class, paid 1-hop proxy sketched but deferred).

Files touched: README.md, DESIGN.md (rewritten), PLAN.md (adds
Phase 0 exit B, Phase 3 upload spike, Phases 4–5 with acceptance
criteria and a Phase-5 review gate), OPEN-QUESTIONS.md (adds Q9–16),
CLAUDE.md (context, principle 1, phase gates, non-goals aligned).

**Measured:** nothing; no network operations this session.

**Spend:** 0 xBZZ — no settlement, no stamps, no chain calls.

**Deviations:** none — no code written, no nodes touched.

**Open items / next:** the design needs discussion before anything is
built. Agenda = OPEN-QUESTIONS.md, especially the (H) items: 5
(upstream posture), 7 (settlement identity, now also receiving), 9
(scope appetite for phases 4–5), 13 (audience-serving default), 16
(streaming economics / who-pays split). Phase 0 (storer service-rate
spike) remains the first executable step and still gates everything;
requires the user's funded Bee node when it runs.
