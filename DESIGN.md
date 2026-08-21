# directswarm — design (draft, rev 2)

Rev 2 (2026-08-21) widens scope from "bulk fetcher dialing storer
neighborhoods" to a two-plane architecture that can also carry
peer-assisted distribution and live streams. The bulk fetcher is
unchanged as the first deliverable; everything new here is design only,
gated behind the same Phase 0 (PLAN.md).

## The problem, restated

Swarm's forwarding kademlia routes every chunk request through the
client's local peer set over ~3–4 hops. That architecture buys real
values — requester anonymity, relay compensation, opportunistic path
caching, O(log n) client state — and costs bulk throughput:
weightstation measured a ~1.1–1.5 MB/s structural ceiling in both
directions that parallelism makes *worse*, with transport ~90% idle.
The design goal is to move mass data at wire speed while giving up as
little of what forwarding buys as possible, and recovering what must be
given up in another form where we can.

## Architecture: two planes, one handoff

```
ANCHOR PLANE — Swarm, stock and unchanged
  identity      references, manifests, feeds
  integrity     BMT chunk addressing (self-verifying 4 KiB chunks)
  persistence   postage stamps, neighborhood replication, sync/repair
  discovery     kademlia overlay (hive gossip, iterative lookups)
  fallback      forwarding retrieval — always works, and is private
        │
        │   handoff unit: a Swarm reference (root hash or feed)
        ▼
FAST PLANE — directswarm client overlay, libp2p
  sources       S1 storer neighborhoods · S2 audience peers · S3 seeds
  transport     direct dials, stock retrieval protocol, deep pipelining
  settlement    SWAP on every data connection (invariant 3)
  coordination  rendezvous + gossip — control messages only, no content
```

The planes meet only at the reference. Swarm decides *what* exists,
that it is intact, and that it persists; the fast plane decides only
*how fast it moves this session*. A reference acquired anywhere (ENS,
a registry, a feed) is valid on both planes, and the fast plane may
never carry content the anchor plane doesn't hold (invariant 1) — that
rule is what stops "fast Swarm" from quietly becoming a separate,
unaccountable network.

## Source classes

All three source classes speak the same wire protocols — stock Bee
handshake, retrieval (request/response per chunk), pricing, SWAP. What
varies is who they are and how they are found. S2/S3 are novel *roles*,
not novel protocols: a peer answering retrieval requests for chunks it
holds is exactly how forwarding chains terminate today.

- **S1 — storer neighborhoods.** The nodes whose overlay addresses
  prefix-match the chunk. Ground truth for cold content; found via the
  topology cache (below). The Phase-0 gate measures what service rate
  a stranger is actually granted here.
- **S2 — audience peers.** Other directswarm clients holding verified
  chunks of the same content (still fetching, or seeding after). They
  sell chunks onward via stock retrieval + SWAP. This is the BitTorrent
  effect with settlement: popular content creates its own paid supply —
  this design's replacement for forwarding-path caching. Serving is
  capped, visible, and opt-in (default under discussion,
  OPEN-QUESTIONS 13).
- **S3 — publisher / mirror seeds.** Nodes run by whoever wants the
  content to move fast: the publisher during a release or a live
  stream, paid mirrors for a flash crowd. Operationally just S2 peers
  that are provisioned and always-on; advertised as session hints in
  the content's metadata or rendezvous records.

## Bulk retrieval pipeline (first deliverable)

```
manifest/ref ──► chunk address set          (client-side: BMT split is
                                             deterministic; swarmfs
                                             already does it offline)
chunk addrs ──► neighborhood map            (address prefix → storer
                                             neighborhood at network
                                             depth d; d ≈ 9–11 today)
overlay crawl ─► topology cache             (hive gossip: overlay →
                                             underlay/IP for reachable
                                             full nodes, per bin)
scheduler ────► per-source streams          (dial 2–3 storers per
                                             neighborhood + any known
                                             S2/S3 peers; pipeline
                                             50–100 outstanding chunk
                                             requests per stream)
chunks ───────► verify (BMT) ► reassemble ► sink (file / pipe / fsspec)
```

Throughput model: per-source rate ≈ outstanding × 4 KiB ÷ RTT
(100 × 4 KiB ÷ 30 ms ≈ 13 MB/s); aggregate ≈ min(Σ source rates, own
downlink, disk). The **existential unknown is the per-storer service
rate a stranger peer is actually granted** — measured first (PLAN
Phase 0), designed around second.

The scheduler is source-class-aware: it assigns chunk addresses to
queues, balances across sources by observed rate and quoted price
(AIMD pipeline depth per connection; user-settable spend budget),
retries across neighborhood members and source classes, and falls back
to forwarding retrieval (via a local Bee node) for anything
unreachable — invariant 4 makes that fallback total, not best-effort.

## Discovery

Two different problems, kept separate:

**Finding storers (S1): the topology cache.** Today a bounded, polite
overlay crawl (~10k nodes) yields a complete bin-organized map in
minutes; cached, freshness-stamped, NAT-reachability flagged, refreshed
lazily on dial failure. At larger network sizes a full crawl stops
being acceptable; the replacement is prefix-targeted iterative kademlia
lookups — resolve only the neighborhoods the current fetch needs,
O(log n) each. Either way kademlia remains the discovery root; we never
build a parallel routing infrastructure. Whether a signed topology
snapshot can be shared (published on Swarm itself) without becoming a
centralization point is an open question.

**Finding each other (S2/S3): rendezvous.** Downloaders and seeds of
the same reference need to find live sessions. Candidates, weakest
dependency first:

a. **Session hints in content metadata** — the publisher lists seed
   underlays alongside the reference (manifest metadata). Zero new
   machinery; covers the publisher-seeded case, including live streams
   (where the publisher is by definition online).
b. **Feed-as-rendezvous** — serving peers post short-lived signed
   presence records to a deterministic feed topic derived from the
   reference. Fully on-Swarm, stock single-owner chunks,
   censorship-resistant; costs postage dust and feed-lookup latency.
c. **Gossipsub session mesh** — a libp2p gossipsub topic per reference
   among directswarm clients, used once connected for low-latency
   presence and (for streams) segment announcements. Stock libp2p,
   touches no Swarm protocol; needs (a) or (b) to bootstrap into.
d. **Upstream: provider/session records in Swarm's kademlia proper**
   (the IPFS provider-record shape) — the clean long-term answer, and a
   protocol addition, so it is a SWIP proposal carrying our data, not a
   local hack.

MVP posture: (a) + (b) to bootstrap, (c) within a session, (d) proposed
upstream at Phase 2 with measurements. No trackers, no well-known
servers — bootstrap must stay decentralized (Tradeoffs, below).

## Live streams (design sketch — later phase, gated)

A stream is a **stock Swarm feed**; the fast plane accelerates its
head. Nothing about the stream's identity, integrity, or persistence
depends on directswarm existing.

Publisher side:

1. Encode segments (1–4 s of media, LL-HLS-shaped).
2. BMT-split each segment into stock chunks; push to Swarm (stamped
   pushsync) — persistence, late-join, and VOD in one step.
3. Append the segment reference to the stream's feed (stock feed
   update) — the anchor any client can follow.
4. Simultaneously offer the segment's chunks on the fast plane as the
   root of a **paid relay tree**.

Viewer side: resolve the feed (anchor) → join the session (rendezvous)
→ buy chunks from a parent in the tree — the publisher, a mirror, or
another viewer — over a stock retrieval + SWAP connection, optionally
selling onward to children. Gossip carries segment announcements and
tree-repair control only; every content byte moves on a settled
connection (invariant 3).

The paid relay tree deliberately re-creates forwarding economics:
every relay hop earns SWAP — exactly the role direct dialing takes from
forwarders — but relays are chosen for capacity and proximity (network
RTT) rather than overlay address, and each hop adds ~1 RTT instead of
a kademlia routing hop.

Latency budget (targets, to verify in the spike): fast-plane viewers
≈ segment length + tree depth × RTT + jitter buffer — LL-HLS-class,
seconds not sub-second. Stock Bee viewers play the same feed tens of
seconds behind (pushsync + feed lookup + forwarding retrieval) — which
is a feature: the stream is watchable with zero directswarm installed.
When the stream ends, the feed history is already an ordinary seekable
Swarm object: VOD for free.

Out of scope for the sketch: transcoding, DRM/access control (Swarm's
ACT exists; integration is not this project), sub-second latency
claims.

## What forwarding kademlia buys — and this design's answer

| Swarm value | stock mechanism | fast-plane treatment |
|---|---|---|
| Content integrity | self-verifying BMT chunks | **kept** — invariant 2; source honesty never assumed |
| Persistence, censorship resistance | postage + neighborhood replication + repair | **kept** — invariant 1: fast plane never the system of record |
| Permissionless access | any client, any content, no gatekeepers | **kept** — invariant 4: stock path always works; the fast plane needs no one's permission either |
| Bandwidth incentives | SWAP settled hop-by-hop along the path | **kept and widened** — invariant 3: every serving peer settles; audience/relay serving adds earners |
| Auto-scaling of popular content | opportunistic caching along forwarding paths | **recovered differently** — S2 audience serving: popularity creates paid supply |
| Relay compensation | forwarders earn on every path | **bypassed on direct fetches; recovered in relay trees** — raised upstream, not fait accompli |
| Requester anonymity | per-hop plausible deniability | **traded, knowingly** — opt-in mode; stock path for sensitive fetches; paid 1-hop proxy variant sketched, deferred |
| O(log n) client state | routed lookups, no global view | **partly traded** — topology cache today; prefix-targeted lookups restore O(log n)/neighborhood at scale |
| Zero-cash entry | pseudosettle free tier | **not offered on the fast plane, by policy** (always settle); the stock path keeps it |
| Spam/DoS protection | postage gates uploads; accounting gates bandwidth | **kept** — invariants 1+3, plus crawl/dial etiquette |

The one outright loss is requester anonymity, and the position is
honesty: never claim privacy, keep the mode opt-in, keep the private
path first-class. A "paid privacy hop" — route a fetch through one
randomly chosen relay that re-sells at 1-hop prices, restoring
requester/source unlinkability for roughly 2× cost — is a natural
extension, sketched here and deliberately deferred (PLAN, deferrals).

## Settlement design

- Every fast-plane data connection runs stock pricing + SWAP; the
  client has its own funded chequebook (custody/UX: OPEN-QUESTIONS 7).
- Client↔client settlement (S2) is stock SWAP on paper — two peers with
  chequebooks — but a light-role peer *serving* retrieval is a novel
  configuration; verify handshake/role handling against `../bee`
  before building on it (OPEN-QUESTIONS 11).
- Pricing semantics for non-storer serving must be verified in bee's
  pricer: proximity-based pricing assumes the server is near the chunk;
  an audience peer is not. If price tables are peer-announced, S2
  serving prices sanely; if price is hard-derived from proximity, S2
  economics need an upstream conversation first (OPEN-QUESTIONS 12).
- 1-hop proximity pricing should make S1 direct retrieval *cheaper* per
  chunk than a paid forwarding chain (weightstation measured ~310k
  PLUR-units/chunk, ~0.33 xBZZ/GiB through forwarding) — verify in
  Phase 0/1.
- Budgets: per-fetch and per-stream spend caps surfaced to the user;
  spend-ledger discipline inherited from weightstation.

## Protocol surface

- **Stock, used as-is:** handshake, hive, retrieval, pricing,
  pseudosettle (as a protocol, never as a lever), SWAP, pushsync
  (upload phase), feeds, manifests, postage.
- **Novel roles on stock wire protocols:** a stranger dialing storers
  at 1 hop (S1); clients serving retrieval to clients (S2/S3, relay
  trees). Compliant on the wire, unusual in role — both go into the
  Phase-2 upstream write-up explicitly.
- **Genuinely new, client-overlay only:** rendezvous presence records,
  session gossip, relay-tree control. These touch no Swarm protocol;
  where they create behavior Swarm should have natively (provider
  records, a direct retrieval strategy), the answer is a SWIP carrying
  our measurements, not a local hack.

## Components

1. **crawler/** — bounded overlay walk building the topology cache:
   bin-organized, freshness-stamped, NAT-reachability flagged;
   rate-limited and polite; refreshed lazily on dial failures;
   prefix-targeted lookup mode for scale.
2. **rendezvous/** — session discovery: metadata hints, presence feed,
   per-reference gossip topic; presence records signed and short-lived.
3. **scheduler/** — chunk→queue assignment across source classes,
   price- and rate-aware; AIMD pipeline depth per peer; retry across
   sources; forwarding fallback; resume from verified state.
4. **transport/** — libp2p dial + Bee handshake + retrieval +
   settlement. Substrate decision (OPEN-QUESTIONS 4): extend **ant**
   (Rust; stack exists, Solar Punk codebase; needs multi-peer
   scheduler) vs. import **bee as a Go library** (protocols come free;
   carries bee's ~10 CPU-ms/chunk measured overhead) vs. clean-room
   libp2p client (most control, most work). The Phase-0 spike may be
   quickest via bee-as-library; the product likely wants ant/Rust.
5. **verify/** — BMT per chunk (microseconds); whole-object integrity
   via the manifest; identical guarantees to stock retrieval.
6. **stream/** (later) — segmenter, feed writer, relay-tree logic,
   jitter buffer. Nothing in it may weaken invariants 1–4.
7. **bench/** — wsbench-compatible output (same CSV vocabulary,
   medians/p95, cold/warm honesty rules) so results compare directly
   against weightstation's Phase-0 numbers.

## Failure and degradation model

- Fast plane fully unavailable → stock Swarm retrieval: slower,
  private, correct (invariant 4).
- Topology cache stale → dial failures trigger targeted re-crawl;
  meanwhile forwarding fallback per chunk.
- S2/S3 peers vanish mid-session → scheduler re-sources from S1/Swarm;
  stream viewers re-parent in the tree, the jitter buffer absorbs the
  gap; worst case a viewer drops to anchor-plane latency, not to
  nothing.
- Settlement failure (chequebook empty, cheque refused) → stop pulling
  on that connection; never degrade into free-tier multiplication.

## Tradeoffs (candid)

- **Anonymity**: sources learn who fetches what. Opt-in, documented,
  never claimed otherwise; the private path remains first-class.
- **Forwarder economics**: bypassed for direct fetches. Mitigations:
  payment still lands on storers (arguably better aligned); serving
  economics widen to any peer; raised upstream early with data.
- **Churn/NAT**: topology-cache staleness and unreachable storers are
  the operational risk; the forwarding fallback bounds the damage.
- **Connection budget**: hundreds of short-lived dials per fetch;
  connection reuse, backoff, never hammering neighborhoods that refuse
  — etiquette is a design requirement, not a nicety.
- **Mesh bootstrap**: rendezvous options (a)/(b) keep bootstrap
  decentralized; resist any drift toward a well-known tracker or
  hosted rendezvous service.
- **Audience serving**: users' machines re-serving third-party public
  content is the same posture as running a Bee node, but it must be
  visible, capped, and deliberate (OPEN-QUESTIONS 13).

## Upload path (later phase)

The same inversion applies to publish: push chunks directly to their
storer neighborhoods (stock pushsync at 1 hop) instead of forwarding —
attacks weightstation's measured ~1.1 MB/s publish ceiling, and live
streaming depends on it (segment pushes must beat real time). Deferred
until retrieval proves out; uploads carry stamp semantics (batch owner
key) and a stricter correctness burden (receipts).
