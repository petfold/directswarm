# directswarm

**Kademlia for discovery and persistence; direct libp2p connections
for mass data.**

directswarm is a fast data plane for [Ethereum Swarm](https://www.ethswarm.org/).
Content is published, discovered, and persisted on Swarm exactly as
today — stock chunks, stock manifests and feeds, postage-stamped,
replicated in storer neighborhoods. What changes is how the mass bytes
move: instead of funneling every chunk request through the client's few
local peers over a 3–4 hop forwarding chain, directswarm dials the
nodes that hold or want the data — storer neighborhoods, the publisher,
other downloaders — **directly** over libp2p (the stack Bee itself is
built on) and streams chunks at one hop with deep pipelining, settling
SWAP on every connection. The same shape IPFS (Bitswap sessions) and
BitTorrent converged on: the DHT finds, direct connections move.

The unit of handoff between the planes is a **Swarm reference** (root
hash or feed). Swarm answers *what* the content is, that it is intact,
and that it persists; directswarm answers only *how fast it moves*.
Everything directswarm transfers can also be fetched — slower, and
privately — by any stock Bee client from the same reference.

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

## Invariants

Bypassing forwarding kademlia on the data path costs real Swarm
properties. Four invariants bound what may ever be traded away; the
per-value accounting is DESIGN.md's "what forwarding kademlia buys".

1. **Anchored.** Nothing moves on the fast plane that is not anchored
   on Swarm: a valid reference, valid postage, chunks stored (or
   syncing) in their neighborhoods. The fast plane is an accelerator
   and a cache — never the system of record, never a shadow network.
2. **Stock chunks.** The transfer unit is the 4 KiB BMT chunk; every
   chunk self-verifies against its address on receipt, whoever served
   it. Untrusted sources can waste time, never corrupt content — and
   any generic Swarm tool can fetch the same reference.
3. **Always settled.** SWAP payment on every connection that moves
   real data — storer, publisher seed, or audience peer. Gossip
   carries coordination messages only, never content. Free-tier
   (pseudosettle) multiplication is a loophole to report upstream, not
   a feature — the user's explicit policy.
4. **Fallback intact.** Stock forwarding retrieval remains first-class
   for every reference: the fast plane can degrade to zero and content
   stays reachable — and privately fetchable — through plain Swarm.

Plus etiquette throughout: rate-limited crawls and dials, back off from
refusals; a fetch must never be distinguishable from abuse.

## The honest trade

Direct dialing reveals the requester to the source: **requester
anonymity is reduced on the fast plane**, and this design does not
pretend otherwise. It is an opt-in mode — right for public bulk
artifacts (ML weights, published media), wrong as a silent default;
privacy-sensitive fetches use the stock path (invariant 4).

That said, the baseline being traded is weaker than advertised
(DESIGN.md, "Anonymity, reassessed"): Swarm's ambient anonymity has
never had an adversarial analysis, light clients provably originate
every request they make (they forward no one else's, and the first hop
sees requester and content linked), and SWAP cheques tie fetches to a
chequebook identity on *any* path. Rigorous requester anonymity was
always going to need a dedicated layer (Tor, Nym) in front of Swarm —
and such a layer composes with direct transfer at least as well as
with forwarding. directswarm claims no privacy either way.

Forwarder earnings are bypassed on direct fetches, but the serving
role stays paid and widens: any peer holding wanted chunks can sell
them (audience serving), which restores relay economics in a
different — arguably stronger — form. All of it is raised upstream as
a proposal, never presented as a fait accompli — including the
systemic question of what happens if everyone adopts this (DESIGN.md,
"Systemic effects"; the short answer: it can only ever displace the
retrieval forwarding path for public bulk bytes, it is self-limiting
there, and the use cases it opens may strengthen the network more than
they cost it).

## How it ships

A standalone client — a core library plus a thin CLI (later a daemon
mode for seeding) — **not an add-on to bee**, which has no plugin
mechanism, and never a fork of it. A local Bee node is an optional
companion (the forwarding fallback), not a requirement or a bundle:
directswarm has its own overlay identity and its own funded
chequebook. If upstream adopts the direct retrieval strategy, the
capability lands inside stock bee for everyone, and directswarm's
remaining role is reference implementation and measurement harness.

## What it is not

Not a new storage network, not a protocol change, not an anonymity
system, not a gateway service, not a CDN business. If the approach
proves out, the endgame is an upstream proposal (a Bee retrieval
*strategy*, SWIPs for session rendezvous), with directswarm as the
reference implementation and measurement harness.

## Relationship to sister projects

- **weightstation** — the customer: its Phase-0 gate failed on stock
  bulk throughput (report: `weightstation/bench/REPORT.md`); directswarm
  is optimization proposal #6 made concrete. Its `wsbench` harness and
  deterministic payloads are the benchmark vocabulary here too.
- **ant** (solardev-xyz/ant) — a Rust Swarm light node speaking the Bee
  p2p stack; the most likely implementation substrate (see DESIGN.md).
- **swarmfs** — fsspec access layer; a working directswarm becomes its
  fast read path.
- **bee** (ethersphere/bee, checked out at `../bee`) — Go reference
  implementation; importable as a library (alternate substrate), the
  ground truth for protocol semantics, and the venue for upstreaming.

## Status

Handoff / design stage, revision 2.3 (2026-08-22): the two-plane
design (bulk fetcher first, peer-assist later); live streaming was
considered and deliberately demoted to a deferred note so it cannot
shape the design (DESIGN.md, "Deferred: live streams"); rev 2.2 adds
form factor, the systemic-adoption analysis, and the anonymity
reassessment; rev 2.3 (2026-08-22) adds latency-aware source
selection. Nothing
implemented. Read DESIGN.md, PLAN.md, OPEN-QUESTIONS.md — Phase 0 (the
storer service-rate spike) still gates everything, and several
fast-plane decisions are explicitly parked for discussion
(OPEN-QUESTIONS.md is the agenda).
