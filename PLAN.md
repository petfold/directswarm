# directswarm — phased plan

Work strictly in order. Every phase ends with a STATUS.md update;
phases 0, 2, and 5 end with a human review gate. Benchmark honesty
rules are inherited from weightstation (medians/p95, cold/warm labeled,
environment disclosed, no cherry-picking). Rev 2 adds phases 4–5
(peer-assist, live streams); phases 0–2 are unchanged in substance, and
nothing downstream starts before its gate.

## Phase 0 — Storer service-rate spike (GATE)

Validate the two load-bearing assumptions with minimal code:

1. **Reachability**: crawl a sample of neighborhoods (hive gossip);
   measure what fraction of storer nodes accept an inbound dial +
   handshake from an unknown light peer.
2. **Service rate**: against ~10 consenting-by-protocol storers across
   distinct neighborhoods, measure sustained chunk service per
   connection vs. pipeline depth (1, 8, 32, 100 outstanding), with
   settlement active, for ≥60 s each. Also record: price per chunk at
   1 hop vs. the ~310k PLUR-units/chunk weightstation measured through
   forwarding.

**Go:** ≥50% of sampled storers reachable AND median sustained
per-storer rate ≥1 MB/s at some pipeline depth (→ ≥25 MB/s aggregate
needs only ~25 effective storers of the hundreds available).

**No-go:** if storers throttle strangers to ≲0.1 MB/s — hard stop,
human review, two exits: **(A)** write the findings up for upstream
(the fix is then peer policy, not a client) and end here; **(B)** pivot
the fast plane to seed/audience sources only (S2/S3: publisher-seeded
distribution anchored on Swarm, storers used solely as fallback). Exit
B is a materially different value proposition and is the human's call,
never a default.

**Deliverables:** spike code (throwaway allowed), REPORT-phase0.md with
raw CSVs, substrate recommendation (ant vs bee-as-library vs custom)
with measured per-chunk CPU cost per substrate candidate.

## Phase 1 — Fetcher MVP (S1 only)

Topology cache + source-class-aware scheduler (storer sources only) +
verification + forwarding fallback + resume.
CLI: `directswarm fetch <ref> [-o file]` against mainnet.

**Acceptance:** 1 GiB cold fetch, byte-verified, ≥25 MB/s median over
5 runs from a well-connected host; graceful degradation with the cache
50% stale; interrupted fetch resumes without refetching verified
chunks; total settlement cost per GiB measured and reported.

## Phase 2 — Economics + upstream write-up (REVIEW GATE)

Settlement correctness (SWAP, no free-tier reliance — audit the
accounting the way weightstation audited Bee's), cost per GiB vs the
forwarding path, connection-etiquette limits. Write the upstream
proposal: Bee "direct retrieval strategy" issue/SWIP draft with our
numbers — and include the rendezvous/provider-record and
audience-serving sketches so the community sees the whole direction,
not a drip-feed. **Human review before publishing anything.**

## Phase 3 — Integration + upload spike

core/ backend for weightstation (its Phase-0 gate re-runs against
directswarm numbers); swarmfs fast-read-path adapter. Direct pushsync
(upload) spike, gated exactly like Phase 0: measure stranger push
acceptance and receipt latency per neighborhood. Live streaming
(Phase 5) depends on this gate passing — segment pushes must beat
real time.

## Phase 4 — Peer-assist plane (S2/S3)

Serve-while-fetching and seed-after; rendezvous MVP (metadata hints +
presence feed + session gossip); client↔client SWAP settlement.

**Acceptance:** two cooperating clients on distinct hosts, one seeded —
the second fetches at ≥ its Phase-1 storer-direct rate with settlement
between the clients audited down to the cheque; a small flash-crowd
simulation (N clients, 1 seed; testnet or lab) shows aggregate delivery
scaling with peer count, every connection settled; serving is capped,
visible, and off by default pending the OPEN-QUESTIONS 13 decision.

## Phase 5 — Live-stream spike (REVIEW GATE)

Feed-anchored segmented stream + paid relay tree, publisher and viewer
sides (design: DESIGN.md "Live streams").

**Acceptance:** a ≥30-minute live stream in which fast-plane viewers
hold end-to-end latency ≤ segment length + a few seconds through
induced relay churn; a stock Bee client plays the same feed (lagging is
fine — that it plays at all is the point); the finished stream replays
as a plain Swarm object; per-viewer-hour and per-published-hour cost
(postage + settlement) measured. **Human review before publishing any
streaming write-up or SWIP.**

## Explicit deferrals

Anonymity-preserving variants (including the paid 1-hop privacy proxy
sketched in DESIGN.md); incentivized forwarder-compensation schemes;
DRM/access-control (ACT) integration; transcoding; hosted/gateway
deployments; Windows support before Phase 3.
