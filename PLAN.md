# directswarm — phased plan

Work strictly in order. Every phase ends with a STATUS.md update;
phases 0 and 2 end with a human review gate. Benchmark honesty rules
are inherited from weightstation (medians/p95, cold/warm labeled,
environment disclosed, no cherry-picking).

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

**Go/no-go:** ≥50% of sampled storers reachable AND median sustained
per-storer rate ≥1 MB/s at some pipeline depth (→ ≥25 MB/s aggregate
needs only ~25 effective storers of the ~hundreds available). If
storers throttle strangers to ≲0.1 MB/s: stop, write up, and take the
findings upstream instead — the fix would then be peer policy, not a
client.

**Deliverables:** spike code (throwaway allowed), REPORT-phase0.md with
raw CSVs, substrate recommendation (ant vs bee-as-library vs custom)
with measured per-chunk CPU cost per substrate candidate.

## Phase 1 — Fetcher MVP

Topology cache + scheduler + verification + forwarding fallback + resume.
CLI: `directswarm fetch <ref> [-o file]` against mainnet.

**Acceptance:** 1 GiB cold fetch, byte-verified, ≥25 MB/s median over
5 runs from a well-connected host; graceful degradation with the cache
50% stale; interrupted fetch resumes without refetching verified chunks;
total settlement cost per GiB measured and reported.

## Phase 2 — Economics + upstream write-up (REVIEW GATE)

Settlement correctness (SWAP, no free-tier reliance — audit the
accounting the way weightstation audited Bee's), cost per GiB vs
forwarding path, connection-etiquette limits. Write the upstream
proposal: Bee "direct retrieval strategy" issue/SWIP draft with our
numbers. **Human review before publishing the proposal.**

## Phase 3 — Integration

core/ backend for weightstation (its Phase-0 gate re-runs against
directswarm numbers); swarmfs fast-read path adapter; optional direct
pushsync (upload) spike, gated the same way as Phase 0.

## Explicit deferrals

Anonymity-preserving variants; incentivized forwarder compensation
schemes; upload path beyond a spike; hosted/gateway deployments;
Windows support before Phase 3.
