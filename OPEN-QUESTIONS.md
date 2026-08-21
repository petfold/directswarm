# directswarm — open questions

(H) needs the human; (C) may be proposed by Claude Code with rationale
in STATUS.md. Rev 2 (2026-08-21) adds the fast-plane and streaming
questions (9–16) — together they are the agenda for the design
discussions that must precede any Phase-4/5 commitment.

## Existential (Phase 0 answers these)

1. **(C) Storer service policy toward strangers.** What sustained
   chunk rate does a Bee full node grant one unknown inbound peer, and
   what limits it — accounting credit, stream limits, disk, or explicit
   rate limiting? The S1 path stands or falls here.
2. **(C) Reachability share.** What fraction of storers accept inbound
   dials (NAT, connection-slot policy, bin gating for unknown peers)?
   Bee full nodes must be publicly reachable in principle; measure
   practice.
3. **(C) 1-hop pricing.** Is direct retrieval actually cheaper per
   chunk than the paid forwarding chain (proximity-based pricing says
   yes; verify against weightstation's ~310k PLUR-units/chunk and
   ~0.33 xBZZ/GiB numbers).

## Bulk design

4. **(C) Substrate**: extend ant (Rust) vs bee-as-Go-library vs
   clean-room libp2p. Spike may differ from product; measure per-chunk
   CPU cost on each candidate (Bee spends ~10 CPU-ms/chunk; BMT itself
   is microseconds — how lean can the client be?).
5. **(H) Upstream-first or client-first?** Building first produces
   evidence but risks community friction (forwarder-earnings bypass);
   proposing first risks debating without data. Current plan: spike
   quietly (protocol-compliant, paid, polite), propose with data at
   Phase 2 — with the *whole* direction (rendezvous, audience serving,
   streams) disclosed in that write-up, not drip-fed. Confirm this
   posture.
6. **(C) Crawl etiquette**: rate limits, cache TTLs, dial budgets that
   keep a fetch from looking like an attack. Also: can the topology
   cache be shared/published (a signed snapshot on Swarm itself?)
   without becoming a centralization point?
7. **(H) Settlement identity**: directswarm needs its own funded
   chequebook (invariant 3). One per client install? Shared with a
   local Bee node's wallet? Custody + funding UX — and with S2 serving,
   the same chequebook *receives*: does earning change the custody
   answer?
8. **(C) Fallback semantics**: when a neighborhood is unreachable,
   fall back to forwarding via a local Bee node — required dependency
   or optional?

## Fast plane & streams (new in rev 2)

9. **(H) Scope appetite.** Phases 4–5 (audience serving, live streams)
   roughly double the project. Confirm they are goals, not
   nice-to-haves, before Phase 2's write-up frames them publicly.
10. **(C) Rendezvous mechanism.** Metadata hints / presence feed /
    gossipsub / upstream provider records (DESIGN.md "Discovery"):
    which combination for the MVP? And what prevents presence-record
    spam — are signed records plus their postage cost enough?
11. **(C) Client-serving legitimacy.** Verify against `../bee`: can a
    light-role peer mount and serve the retrieval protocol without
    tripping handshake, topology, or accounting assumptions? Is a
    client serving cached chunks acceptable to upstream as-is, or does
    it need the SWIP first?
12. **(C) Pricing for non-storer serving.** Are price tables
    peer-announced (then S2 can price sanely) or hard-derived from
    chunk↔peer proximity (then audience serving looks absurdly
    expensive on paper)? Read bee's pricer; confirm empirically in
    Phase 4.
13. **(H) Audience-serving default.** Opt-in only, or opt-out after a
    stabilization period? A user's machine re-serving third-party
    public content for payment is the same posture as running a Bee
    node, but it must be a visible, capped, deliberate choice — and any
    jurisdiction/abuse considerations the human wants recorded belong
    here.
14. **(C) Relay-tree mechanics.** Parent selection (capacity, RTT,
    quoted price), churn-repair time vs jitter-buffer size, cycle
    prevention, and what a fair resale price is at each tier. Simulate
    before spiking.
15. **(C) Feed latency for live anchors.** Measure feed update + lookup
    latency on mainnet: is the anchor path fast enough that a stock Bee
    viewer runs seconds-to-tens-of-seconds behind, as DESIGN.md
    assumes — or worse?
16. **(H) Streaming economics.** Publisher pays persistence (postage),
    viewers pay delivery (SWAP) — confirm that split is the intended
    model, and set the per-viewer-hour / per-published-hour cost
    targets at which the human would call this viable against a
    conventional CDN.

## Meta

17. **(H) Name.** `directswarm` (descriptive, no collisions) chosen at
    handoff; alternatives considered: `beeline` (collides with Apache
    Hive's CLI), `waggle` (bee dance that communicates locations —
    apt but cute). Rename cheap until first release.
18. **(H) License** — sisters vary (freedom-browser MPL-2.0); pick
    before first push of implementation code.
19. **(H) Repo home** — starts under `petfold/` like weightstation and
    swarmfs; move to an org if/when it becomes a Solar Punk deliverable.
20. **(H) Relationship to the incentive findings**: weightstation's
    "funding is not incentivised" report may lead to protocol changes
    that alter this design's economics — track ethersphere/bee once
    that issue is filed. (Rev-2 note: audience serving *adds* a reason
    to fix it — paid serving only competes if paying is actually how
    you get served.)
