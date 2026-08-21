# directswarm

**Kademlia for discovery, direct connections for mass data.**

directswarm is a bulk-transfer client for [Ethereum Swarm](https://www.ethswarm.org/):
it computes where a file's chunks live (chunk addresses are derivable
client-side; each chunk's storer neighborhood is an address-prefix
match), dials those storer neighborhoods **directly** over libp2p, and
streams chunks at one hop with deep request pipelining — instead of
funneling every request through the client's few local peers via
multi-hop forwarding kademlia. The same shape IPFS (Bitswap sessions)
and BitTorrent converged on: the DHT finds, direct connections move.

## Why

weightstation's Phase-0 benchmark (2026-08, Bee 2.8.1 light node, funded
chequebook, fast link) measured Swarm's stock bulk path at:

- **~1.4 MB/s cold retrieval, best case** (single stream); client-side
  parallelism and larger server lookahead both made it *slower*
- ~1.1–1.2 MB/s upload (pushsync), independent of payment state
- per-chunk retrieval latency ~90 ms (healthy) — the ceiling is the
  request *funnel*: all traffic crosses the client's ~84 peers, each
  serving roughly serially, over ~3–4 forwarding hops
- transport never the limit: libp2p connections sat ~90% idle, CPU unpegged

A 1 GiB file's ~260k chunks spread over ~all ≈512 neighborhoods
(~500 chunks each) of today's network. Fetching each neighborhood
directly turns one narrow funnel into hundreds of independent 1-hop
sources. Even ~10 pipelined chunks per storer at ~30 ms clears
25 MB/s aggregate with a wide margin; per-connection libp2p capacity is
~2–3 orders of magnitude above what's needed.

## Principles

1. **Protocol-compliant, not a fork.** Stock chunks, stock manifests,
   stock retrieval + handshake + settlement protocols. Storer nodes
   already answer retrieval requests from any connected peer — that is
   how forwarding terminates today. Any generic Swarm tool can fetch
   the same content.
2. **Pay for what you fetch.** SWAP settlement per connection, to the
   nodes actually storing and serving. Never engineered around payment:
   free-tier (pseudosettle) multiplication is a loophole to be reported,
   not exploited — see weightstation's Phase-0 report. 1-hop proximity
   pricing should also make direct retrieval *cheaper* per chunk than a
   paid forwarding chain (verify in Phase 1).
3. **Opt-in mode with honest tradeoffs.** Direct dialing reveals the
   requester and what they fetch, and bypasses forwarder earnings and
   path caching. Fine for public bulk artifacts (the target workload:
   ML model weights); wrong as a silent default. Forwarding remains the
   fallback path.

## What it is not

Not a new storage network, not a protocol change, not an anonymity
system, not a gateway service. If the approach proves out, the endgame
is an upstream proposal (a Bee retrieval *strategy* / SWIP), with
directswarm as the reference implementation and measurement harness.

## Relationship to sister projects

- **weightstation** — the customer: its Phase-0 gate failed on stock
  bulk throughput (report: `weightstation/bench/REPORT.md`); directswarm
  is optimization proposal #6 made concrete. Its `wsbench` harness and
  deterministic payloads are the benchmark vocabulary here too.
- **ant** (solardev-xyz/ant) — a Rust Swarm light node speaking the Bee
  p2p stack; the most likely implementation substrate (see DESIGN.md).
- **swarmfs** — fsspec access layer; a working directswarm becomes its
  fast read path.
- **bee** (ethersphere/bee) — Go reference implementation; importable
  as a library (alternate substrate), and the venue for upstreaming.

## Status

Handoff / design stage. Nothing implemented. Read DESIGN.md, PLAN.md,
OPEN-QUESTIONS.md — Phase 0 (the storer-service-rate spike) gates
everything, same discipline that served weightstation well.
