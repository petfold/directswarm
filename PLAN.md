# directswarm — phased plan

Work strictly in order. Every phase ends with a STATUS.md update;
phases 0 and 2 end with a human review gate. Benchmark honesty rules
are inherited from weightstation (medians/p95, cold/warm labeled,
environment disclosed, no cherry-picking). Rev 2.1: the live-streaming
phase was removed (demoted to a deferred note, DESIGN.md); phases 0–2
are unchanged in substance, and nothing downstream starts before its
gate.

## Phase 0 — Storer service-rate spike (GATE)

Validate the two load-bearing assumptions with minimal code:

1. **Reachability**: crawl a sample of neighborhoods (hive gossip);
   measure what fraction of storer nodes accept an inbound dial +
   handshake from an unknown light peer (each full node admits ~100
   light peers — measure slot availability in practice).
2. **Service rate**: against ~10 consenting-by-protocol storers across
   distinct neighborhoods, measure sustained chunk service per
   connection vs. pipeline depth (1, 8, 32, 100 outstanding), with
   settlement active, for ≥60 s each. Also record: price per chunk at
   1 hop vs. the ~310k PLUR-units/chunk weightstation measured through
   forwarding (bee's fixed proximity pricing says 1 hop is cheapest by
   construction — confirm empirically).

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
numbers — and include the rendezvous/provider-record,
audience-serving, and non-storer-pricing sketches so the community
sees the whole direction, not a drip-feed. The write-up also carries
the systemic-adoption analysis and posture question (embrace /
constrain / segment — OPEN-QUESTIONS 15), the anonymity reassessment
(DESIGN.md), and the strengthening argument: use cases currently
infeasible on Swarm bring traffic, settlement revenue, and postage
demand. **Human review before publishing anything.**

## Phase 3 — Integration + upload spike

core/ backend for weightstation (its Phase-0 gate re-runs against
directswarm numbers); swarmfs fast-read-path adapter. Direct pushsync
(upload) spike, gated exactly like Phase 0: measure stranger push
acceptance and receipt latency per neighborhood. In passing, record
whether sustained direct push could carry a live encode — a data point
for the deferred streaming note (DESIGN.md), not a goal.

## Phase 4 — Peer-assist plane (S2/S3)

Serve-while-fetching and seed-after; rendezvous MVP (metadata hints +
presence feed + session gossip); client↔client SWAP settlement, with
the non-storer pricing decision (OPEN-QUESTIONS 12) resolved first.

**Acceptance:** two cooperating clients on distinct hosts, one seeded —
the second fetches at ≥ its Phase-1 storer-direct rate with settlement
between the clients audited down to the cheque; a small flash-crowd
simulation (N clients, 1 seed; testnet or lab) shows aggregate delivery
scaling with peer count, every connection settled; serving is capped,
visible, and off by default pending the OPEN-QUESTIONS 13 decision.

## Explicit deferrals

Live-stream machinery (paid relay trees, per-stream session meshes,
segmenter — see DESIGN.md "Deferred: live streams");
anonymity-preserving variants (including the paid 1-hop privacy proxy
sketched in DESIGN.md); incentivized forwarder-compensation schemes;
DRM/access-control (ACT) integration; hosted/gateway deployments;
Windows support before Phase 3.
