# directswarm — design (draft, rev 2.3)

Rev 2 (2026-08-21) widened scope from "bulk fetcher dialing storer
neighborhoods" to a two-plane architecture with peer-assisted
distribution. Rev 2.1 (same day, after discussion) **demotes live
streaming from a design goal to a deferred note** — it must not
influence the design at this stage (rationale at the end) — and folds
in protocol facts verified against the bee source. Rev 2.2 (same day)
records the deployment form factor, the systemic-adoption analysis
("would it overtake the network?"), and a reassessment of the
anonymity trade. Rev 2.3 (2026-08-22) adds latency-aware source
selection. Rev 2.4 (same day) resolves the substrate posture (Rust
product, sans-I/O core, Wasm kept buildable) and records the
browser-transport reality.

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
  a stranger is actually granted here. Verified in `../bee`
  (2026-08-21): the retrieval handler (`pkg/retrieval`) serves **any
  connected peer** with no role gating — it even forwards on the
  requester's behalf on a local miss; admission is connection
  acceptance plus accounting, and misbehavior on retrieval streams
  triggers blocklisting (etiquette is functional, not just polite).
- **S2 — audience peers.** Other directswarm clients holding verified
  chunks of the same content (still fetching, or seeding after). They
  sell chunks onward via stock retrieval + SWAP. This is the BitTorrent
  effect with settlement: popular content creates its own paid supply —
  this design's replacement for forwarding-path caching. Serving is
  capped, visible, and opt-in (default under discussion,
  OPEN-QUESTIONS 13). Stock bee light nodes also mount the retrieval
  handler and serve from cache, so the role is not foreign to the
  protocol; pricing for it is the open issue (Settlement, below).
- **S3 — publisher / mirror seeds.** Nodes run by whoever wants the
  content to move fast: the publisher during a release, paid mirrors
  for a flash crowd. Operationally just S2 peers that are provisioned
  and always-on; advertised as session hints in the content's metadata
  or rendezvous records.

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
queues, balances across sources by observed rate and price (AIMD
pipeline depth per connection; user-settable spend budget), retries
across neighborhood members and source classes, and falls back to
forwarding retrieval (via a local Bee node) for anything unreachable —
invariant 4 makes that fallback total, not best-effort.

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

One admission fact to design around (verified in `../bee`,
`pkg/p2p/libp2p`): a bee full node admits at most ~100 light peers
(`defaultLightNodeLimit`). A fetch occupies one slot per storer dialed —
fine for a single client, a contention point if many directswarm
clients converge on the same neighborhoods. Phase 0's reachability
measurement covers practice; the knob itself is a peer-policy
conversation for the upstream write-up.

**Finding each other (S2/S3): rendezvous.** Downloaders and seeds of
the same reference need to find live sessions. Candidates, weakest
dependency first:

a. **Session hints in content metadata** — the publisher lists seed
   underlays alongside the reference (manifest metadata). Zero new
   machinery; covers the publisher-seeded case.
b. **Feed-as-rendezvous** — serving peers post short-lived signed
   presence records to a deterministic feed topic derived from the
   reference. Fully on-Swarm, stock single-owner chunks,
   censorship-resistant; costs postage dust and feed-lookup latency.
c. **Gossipsub session mesh** — a libp2p gossipsub topic per reference
   among directswarm clients, used once connected for low-latency
   presence. Stock libp2p, touches no Swarm protocol; needs (a) or (b)
   to bootstrap into.
d. **Upstream: provider/session records in Swarm's kademlia proper**
   (the IPFS provider-record shape) — the clean long-term answer, and a
   protocol addition, so it is a SWIP proposal carrying our data, not a
   local hack.

MVP posture: (a) + (b) to bootstrap, (c) within a session, (d) proposed
upstream at Phase 2 with measurements. No trackers, no well-known
servers — bootstrap must stay decentralized (Tradeoffs, below).

## Latency-aware source selection (rev 2.3)

Because per-connection pipeline depth is capped by the accounting
threshold (~50 chunks outstanding; Settlement design), per-storer rate
≈ depth × 4 KiB ÷ RTT — **throughput is inversely proportional to
RTT**. The scheduler dials only 2–3 of a neighborhood's ~4–10 members,
so *which* members is the main per-connection lever: a 30 ms member
over a 150 ms one is a 5× difference on the same accounting budget.
The same logic applies even more freely to S2/S3 peers, which are not
neighborhood-constrained at all.

Selection discipline:

- **Latency ranks the healthy; it never disqualifies.** All
  neighborhood members remain valid sources — a high-RTT member may be
  the only reachable one. Removing the latency signal degrades speed
  only, never availability.
- **RTT is the prior; observed service rate is the posterior.** A
  nearby storer on a struggling disk loses to a farther one on NVMe;
  the existing AIMD-on-observed-rate keeps the final say.
- **Bounded preference (ε-greedy)**: a minimum probability mass on
  non-preferred members keeps estimates fresh and avoids hot-spotting
  the best-connected storer in every neighborhood — which is also
  fairer to SWAP earnings across members.

Measurement sources, in etiquette order:

1. **Passive** — every dial already yields a handshake RTT; record it
   in the topology cache, freshness-stamped. Zero extra traffic; every
   fetch enriches the map.
2. **Active** — bee's stock `pingpong` protocol; rate-limited probes
   for never-dialed candidates, drawn from the same dial budget as the
   crawl (OPEN-QUESTIONS 6).
3. **Predicted** — for the unprobed majority of ~10k nodes: coarse
   priors from underlay IP (GeoIP/ASN buckets), or a
   network-coordinate embedding (Vivaldi-style) fitted over the few
   hundred measured RTTs, so unvisited nodes get an estimate without
   being probed.

**Hedged tail**: for the last straggling chunks, issue the same request to two
neighborhood members and take the first delivery. Settlement caveat
the upstream version doesn't face: under invariant 3, a hedged chunk
that arrives second was still served and is still paid for — hedging
costs real xBZZ, so the surplus is bounded (a few % of chunks, tail
only) and reported in the spend ledger, never hidden.

A privacy note: latency bias leaks coarse requester geography — moot
here, since the fast plane is already the opt-in mode where sources
see the requester ("Anonymity, reassessed"); the marginal leakage is
nil.

## What forwarding kademlia buys — and this design's answer

| Swarm value | stock mechanism | fast-plane treatment |
|---|---|---|
| Content integrity | self-verifying BMT chunks | **kept** — invariant 2; source honesty never assumed |
| Persistence, censorship resistance | postage + neighborhood replication + repair | **kept** — invariant 1: fast plane never the system of record |
| Permissionless access | any client, any content, no gatekeepers | **kept** — invariant 4: stock path always works; the fast plane needs no one's permission either |
| Bandwidth incentives | SWAP settled hop-by-hop along the path | **kept and widened** — invariant 3: every serving peer settles; audience serving adds earners |
| Auto-scaling of popular content | opportunistic caching along forwarding paths | **recovered differently** — S2 audience serving: popularity creates paid supply |
| Relay compensation | forwarders earn on every path | **bypassed on direct fetches; partly recovered via paid audience serving** — raised upstream, not fait accompli |
| Requester anonymity | per-hop plausible deniability (unstudied; weak for light, settling clients — see below) | **traded — from a weaker baseline than advertised**; opt-in; rigorous anonymity composes on top (Tor/Nym); no privacy claims either way |
| O(log n) client state | routed lookups, no global view | **partly traded** — topology cache today; prefix-targeted lookups restore O(log n)/neighborhood at scale |
| Zero-cash entry | pseudosettle free tier | **not offered on the fast plane, by policy** (always settle); the stock path keeps it |
| Spam/DoS protection | postage gates uploads; accounting gates bandwidth | **kept** — invariants 1+3, plus crawl/dial etiquette |

### Anonymity, reassessed (2026-08-21 discussion)

The trade is real, but the baseline is weaker than the "ambient
anonymity" story suggests, on three grounds:

1. **Light clients never had requester anonymity.** Deniability comes
   from mixing your own requests among traffic you forward for others.
   Light nodes forward nothing — bee keeps them out of the routing
   bins (the `lightnodes` container) and the handshake reveals the
   role — so every request a light node makes is provably its own, and
   the request carries the chunk address: the first-hop peer sees
   requester and content linked, today, on the stock path. Forwarding
   hides the requester only from the *storer*, by exposing them fully
   to a few randomly-assigned intermediaries.
2. **Settlement deanonymizes on every path.** SWAP cheques are signed
   against the client's chequebook contract — an on-chain identity
   handed to the first hop with every settled fetch. Any paying client
   (the only sustainable kind, per invariant 3) has already linked its
   requests to a funded identity. Anonymous settlement does not exist;
   that gap is upstream research, not this project (OPEN-QUESTIONS 14).
3. **No adversarial analysis exists.** The property is asserted
   without a threat model or metrics, and has been doubted in the
   community; an unstudied anonymity property should be presumed weak.

The principled fix — discussed in the Swarm community as putting Nym
or Tor in front of Swarm — makes anonymity a *transport option* rather
than a topology property, and it composes with direct transfer at
least as well as with forwarding: the throughput model
(outstanding × 4 KiB ÷ RTT) degrades gracefully under onion-routing
RTTs by deepening pipelines and adding circuits, where the stock
forwarding funnel just gets slower. Requester-hidden-from-everyone is
categorically stronger than plausible deniability. This stays a design
note until measured (OPEN-QUESTIONS 14).

Position unchanged in substance: never claim privacy in either
direction, keep the mode opt-in, keep the stock path first-class. The
"paid privacy hop" — one randomly chosen relay re-selling at 1-hop
prices, restoring requester/source unlinkability for roughly 2× cost —
remains sketched and deferred (PLAN, deferrals).

## Systemic effects: if everyone adopted this (2026-08-21 discussion)

Could the fast plane gradually overtake the network? **Structurally it
cannot**: it competes only on the retrieval forwarding path. Storage,
persistence, postage, push/pull sync, the redistribution game, and
kademlia discovery are untouched by construction (invariant 1). In a
maximal-adoption world, "overtaking" means "most public bulk bytes
travel the last hop directly" — which is the IPFS/BitTorrent
architecture (the DHT finds, direct connections move), not a coup.

Where it does compete, adoption is self-limiting:

- a direct client holds connections to every neighborhood it fetches
  from instead of kademlia's O(log n) peers, each taking one of a
  storer's ~100 light-peer slots — mass adoption creates slot
  contention that erodes the advantage;
- storer operators hold the policy lever (throttling strangers is the
  Phase-0 no-go case);
- small, interactive, and private fetches gain nothing — crawl and
  dial overhead makes direct *worse* for browsing-shaped traffic.

The plausible equilibrium is segmentation: direct for public bulk,
forwarding for everything small, interactive, or privacy-sensitive.

Externalities to disclose upstream honestly: forwarders lose
bulk-relay income (revenue shifts to whoever stores and serves —
arguably better aligned, but a real redistribution); cover traffic
thins for full-node users who still rely on forwarding deniability
(softened by "Anonymity, reassessed" — light clients had no cover to
lose); and forwarding caches run colder with less traffic through
them.

The counterweight: use cases currently infeasible on Swarm — bulk
distribution of models and media — bring traffic, settlement revenue
to storers, and postage demand. Utility that pays node operators may
strengthen the network more than an unstudied deniability property
protected it.

Governance conclusion: the capability is latent in the protocol —
storers answer any connected peer, chunk addresses are
client-computable, and weightstation's report has already published
the idea — so someone will build it regardless. The containment here
(opt-in, always settled, polite, fallback first-class, upstream-first)
exists so the network's evolution happens inside Swarm's governance:
the Phase-2 write-up explicitly asks upstream to pick a posture —
embrace, constrain, or segment (OPEN-QUESTIONS 15).

## Settlement design

- Every fast-plane data connection runs stock pricing + SWAP; the
  client has its own funded chequebook (custody/UX: OPEN-QUESTIONS 7).
- **Pricing is a fixed formula, not negotiated** (verified in `../bee`,
  `pkg/pricer`): `PeerPrice = (MaxPO − proximity(peer, chunk) + 1) ×
  poPrice`, computed identically by both sides — there are no
  peer-announced price tables. Two consequences:
  - **S1 direct-from-storer is the cheapest price on the network by
    construction** (maximum proximity → minimum price). The empirical
    check against weightstation's ~310k PLUR-units/chunk and
    ~0.33 xBZZ/GiB through forwarding remains Phase 0/1 work.
  - **S2 audience serving is mispriced under stock semantics**: an
    audience peer is far from most chunks it caches, so the formula
    prices its serving near maximum. Interim answer: a mutually-agreed
    accounting convention between consenting directswarm clients (bee
    is not a party on those connections); principled answer: an
    upstream SWIP. Decision parked as OPEN-QUESTIONS 12.
- Client↔client settlement (S2) is stock SWAP — two peers with
  chequebooks; the serving role is already exercised by stock light
  nodes (handler mounted unconditionally). Remaining verification:
  handshake/accounting symmetry in practice (OPEN-QUESTIONS 11).
- Budgets: per-fetch spend caps surfaced to the user; spend-ledger
  discipline inherited from weightstation.

## Protocol surface

- **Stock, used as-is:** handshake, hive, retrieval, pricing,
  pseudosettle (as a protocol, never as a lever), SWAP, pushsync
  (upload phase), feeds, manifests, postage.
- **Novel roles on stock wire protocols:** a stranger dialing storers
  at 1 hop (S1); clients serving retrieval to clients (S2/S3).
  Compliant on the wire, unusual in role — both go into the Phase-2
  upstream write-up explicitly.
- **Genuinely new, client-overlay only:** rendezvous presence records,
  session gossip. These touch no Swarm protocol; where they create
  behavior Swarm should have natively (provider records, a direct
  retrieval strategy, sane pricing for non-storer serving), the answer
  is a SWIP carrying our measurements, not a local hack.

**No bee changes are required for any phase.** The client runs outside
bee (ant/Rust, bee-as-a-Go-library, or clean-room — all leave bee
untouched); bee changes appear only in the endgame as optional upstream
proposals.

## Form factor and deployment

A **standalone client**, not a bee add-on: bee has no plugin
mechanism, and living inside it would mean patching it (excluded by
the no-fork principle). directswarm speaks the wire protocols itself,
with its own overlay identity and its own funded chequebook — two
processes must not share a chequebook (concurrent cheque issuance
against one contract conflicts on cumulative amounts per beneficiary;
custody is OPEN-QUESTIONS 7).

Deliverable shape: a **core library plus a thin CLI**. The library
form is load-bearing — both consumers are libraries (weightstation's
core/ backend, swarmfs's fsspec adapter; both Python, so the Rust
substrate implies PyO3 bindings). Phase 4 adds a daemon mode: fetching
is a one-shot run, but seeding requires a long-lived process.

**Browser (Wasm) endgame.** A user who needs one or two large files
won't install a client; a no-install, in-page fetch is a stated goal —
and it constrains the substrate, not the schedule. Rust is the
decisive choice here: rust-libp2p ships real browser transports
(websocket/webtransport/webrtc-websys), wasm32 is first-class, and the
sans-I/O core compiles to both targets; Go compiles to Wasm but
go-libp2p has no supported browser-transport story, and bee-as-library
can never run in a page. The hard constraint is transports, not
language: a browser cannot dial raw TCP/QUIC, and today's storers
listen on nothing else (bee's `p2p-ws-enable` is ~unused, and wss from
an https page needs TLS certificates storers don't have). So the
browser client becomes realistic with **Phase 4**, where we control
both endpoints — seeds and audience peers listen on WebTransport/
WebRTC — while storer-direct fetching stays native. Getting full nodes
browser-dialable (WebTransport with certhash — configuration/adoption
of an existing go-libp2p capability, not a protocol change) joins the
Phase-2 upstream asks. Large-file sinks in a page are workable via
OPFS / File System Access streaming writes.

A local Bee node is an **optional companion, never a bundle**: it is
used for exactly one thing — the forwarding fallback (required vs
optional is OPEN-QUESTIONS 8; a substrate that implements client-side
forwarding retrieval itself needs no bee at all). Not installed with
bee: bee is upstream's product, and the fast plane's anonymity trade
means installing directswarm must be a deliberate, opt-in act — never
something that arrives silently with a node. The endgame inverts the
packaging: if upstream adopts the direct retrieval strategy, the
capability lands inside stock bee for everyone, and directswarm's
remaining role is reference implementation and measurement harness.

## Components

1. **crawler/** — bounded overlay walk building the topology cache:
   bin-organized, freshness-stamped, NAT-reachability flagged; carries
   per-node RTT estimates (measured on dial, probed via stock
   pingpong, or predicted — see "Latency-aware source selection");
   rate-limited and polite; refreshed lazily on dial failures;
   prefix-targeted lookup mode for scale.
2. **rendezvous/** — session discovery: metadata hints, presence feed,
   per-reference gossip topic; presence records signed and short-lived.
3. **scheduler/** — chunk→queue assignment across source classes;
   price-, rate-, and latency-aware (RTT prior, observed service rate
   posterior, ε-greedy floor); AIMD pipeline depth per peer; hedged
   duplicate requests for tail chunks (bounded, settled, ledgered);
   retry across sources; forwarding fallback; resume from verified
   state.
4. **transport/** — libp2p dial + Bee handshake + retrieval +
   settlement. Substrate posture (resolved 2026-08-22, OPEN-QUESTIONS
   4): the **product is Rust** (extend ant), decided by the
   browser/Wasm endgame; the **Phase-0 spike uses whatever measures
   fastest** (likely bee-as-Go-library; throwaway allowed). Structural
   rule from day one: a **sans-I/O core** (scheduler, BMT, manifest
   walking, accounting/cheque logic — no sockets, no owned clocks)
   behind a transport trait, with a native (tokio/TCP) adapter now and
   a browser (websys) adapter later; `wasm32` kept compiling in CI
   even before anything browser-facing ships.
5. **verify/** — BMT per chunk (microseconds); whole-object integrity
   via the manifest; identical guarantees to stock retrieval.
6. **bench/** — wsbench-compatible output (same CSV vocabulary,
   medians/p95, cold/warm honesty rules) so results compare directly
   against weightstation's Phase-0 numbers.

## Failure and degradation model

- Fast plane fully unavailable → stock Swarm retrieval: slower,
  private, correct (invariant 4).
- Topology cache stale → dial failures trigger targeted re-crawl;
  meanwhile forwarding fallback per chunk.
- S2/S3 peers vanish mid-session → scheduler re-sources from S1/Swarm.
- Settlement failure (chequebook empty, cheque refused) → stop pulling
  on that connection; never degrade into free-tier multiplication.

## Tradeoffs (candid)

- **Anonymity**: sources learn who fetches what. Opt-in, documented,
  never claimed otherwise; the private path remains first-class — and
  the stock baseline is itself weak for light, settling clients (see
  "Anonymity, reassessed").
- **Forwarder economics**: bypassed for direct fetches. Mitigations:
  payment still lands on storers (arguably better aligned); serving
  economics widen to any peer; raised upstream early with data.
- **Churn/NAT**: topology-cache staleness and unreachable storers are
  the operational risk; the forwarding fallback bounds the damage.
- **Connection budget**: hundreds of short-lived dials per fetch, each
  taking one of a storer's ~100 light-peer slots; connection reuse,
  backoff, never hammering neighborhoods that refuse — bee blocklists
  misbehaving retrieval streams, so etiquette is a design requirement,
  not a nicety.
- **Mesh bootstrap**: rendezvous options (a)/(b) keep bootstrap
  decentralized; resist any drift toward a well-known tracker or
  hosted rendezvous service.
- **Audience serving**: users' machines re-serving third-party public
  content is the same posture as running a Bee node, but it must be
  visible, capped, and deliberate (OPEN-QUESTIONS 13).

## Upload path (later phase)

The same inversion applies to publish: push chunks directly to their
storer neighborhoods (stock pushsync at 1 hop) instead of forwarding —
attacks weightstation's measured ~1.1 MB/s publish ceiling. Deferred
until retrieval proves out; uploads carry stamp semantics (batch owner
key) and a stricter correctness burden (receipts).

## Deferred: live streams (demoted 2026-08-21 — not a design input)

Considered in rev 2 and deliberately demoted after discussion. The
reasoning, recorded so it isn't relitigated by accident:

- **Streaming largely works on today's Swarm.** Measured cold retrieval
  (~1.36 MB/s) covers common video bitrates behind a few seconds of
  buffer, and live fan-out is the workload forwarding kademlia handles
  *best*: every viewer wants the same fresh chunks, so path caching
  acts as a natural CDN and viewers don't contend the way one bulk
  client's parallel requests do.
- **The genuine gaps are narrow** — publish-side headroom (~1.1 MB/s
  measured upload has no margin over an HD live encode; 4K doesn't
  fit), tens-of-seconds latency, unmeasured flash-crowd behavior — and
  the one the fast plane clearly fixes (publish headroom via direct
  pushsync) is already in the plan for bulk reasons (Phase 3).
- **The streaming-specific machinery** sketched in rev 2 (paid relay
  trees, per-stream session meshes, segmenter/jitter components) was
  the most speculative part of the design, with no customer and no
  measurements behind it. It is removed from the architecture; nothing
  in the components, phases, or invariants may assume it.

If streaming returns, it returns as a workload with a customer and
numbers: a stock feed of segment references, accelerated by the same
bulk fetcher — and the Phase-3 pushsync spike should record in passing
whether sustained direct push could carry a live encode, as a data
point only.
