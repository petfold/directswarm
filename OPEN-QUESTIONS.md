# directswarm — open questions

(H) needs the human; (C) may be proposed by Claude Code with rationale
in STATUS.md. Rev 2 (2026-08-21) added the fast-plane questions; rev
2.1 (same day) removed the streaming-specific ones (streaming demoted —
DESIGN.md "Deferred: live streams") and recorded answers verified
against the bee source; rev 2.2 (same day) added the ecosystem
questions 14–15 (meta renumbered 16–19).

## Existential (Phase 0 answers these)

1. **(C) Storer service policy toward strangers.** What sustained
   chunk rate does a Bee full node grant one unknown inbound peer, and
   what limits it — accounting credit, stream limits, disk, or explicit
   rate limiting? The S1 path stands or falls here. (Code-verified
   2026-08-21: the retrieval handler serves any connected peer with no
   role gating; the limits are accounting thresholds, the ~100
   light-peer admission cap, and blocklisting on stream misbehavior —
   what remains is measuring the *rates* these produce in practice.)
2. **(C) Reachability share.** What fraction of storers accept inbound
   dials (NAT, connection-slot policy — including the ~100 light-peer
   cap per full node — bin gating for unknown peers)? Bee full nodes
   must be publicly reachable in principle; measure practice.
3. **(C) 1-hop pricing.** Bee's fixed proximity pricing
   (`PeerPrice = (MaxPO − proximity + 1) × poPrice`, verified in
   `pkg/pricer`) makes direct-from-storer the cheapest price by
   construction. Confirm empirically against weightstation's ~310k
   PLUR-units/chunk and ~0.33 xBZZ/GiB through forwarding.

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
   non-storer pricing) disclosed in that write-up, not drip-fed.
   Confirm this posture.
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

## Fast plane (peer-assist)

9. **(H) Scope appetite.** Phase 4 (audience serving) extends the
   project beyond the bulk fetcher weightstation needs. Streaming was
   considered and demoted (2026-08-21, must not influence the design);
   confirm peer-assist stays a goal — it can be judged on the bulk
   flash-crowd case alone.
10. **(C) Rendezvous mechanism.** Metadata hints / presence feed /
    gossipsub / upstream provider records (DESIGN.md "Discovery"):
    which combination for the MVP? And what prevents presence-record
    spam — are signed records plus their postage cost enough?
11. **(C) Client-serving legitimacy.** Partly answered 2026-08-21: the
    retrieval handler is mounted unconditionally in bee, and stock
    light nodes serve from cache, so the role exists on the wire.
    Remaining: handshake/accounting symmetry between two light-role
    peers in practice, and whether upstream considers client serving
    acceptable as-is or wants the SWIP first.
12. **(C) Pricing for non-storer serving.** Answered 2026-08-21:
    prices are hard-derived from chunk↔peer proximity (`pkg/pricer`
    FixedPricer), not peer-announced — stock semantics price an
    audience peer's serving near maximum. Follow-on decision: interim
    mutually-agreed convention between consenting directswarm clients
    (bee is not a party on those connections) vs. accepting stock
    prices (buyers overpay S2) vs. deferring S2 until an upstream SWIP
    lands. Propose with rationale before Phase 4.
13. **(H) Audience-serving default.** Opt-in only, or opt-out after a
    stabilization period? A user's machine re-serving third-party
    public content for payment is the same posture as running a Bee
    node, but it must be a visible, capped, deliberate choice — and any
    jurisdiction/abuse considerations the human wants recorded belong
    here.

## Ecosystem (new in rev 2.2)

14. **(C) Anonymity-layer composability.** Verify that a Bee handshake
    + retrieval session works over libp2p dialed through Tor (SOCKS5)
    or Nym: what RTT results, what pipeline depth bulk needs under it,
    and whether any protocol step leaks the underlay address. Also
    record the settlement linkage plainly: SWAP cheques tie fetches to
    a chequebook identity regardless of transport — anonymous
    settlement is an upstream research gap that applies to stock Swarm
    too. A design note until measured; never a privacy claim.
15. **(H) Upstream adoption posture.** The Phase-2 write-up should ask
    upstream to choose a posture toward direct retrieval — *embrace*
    (native strategy, provider records, pricing/priority for
    settlers), *constrain* (slot/rate policy for strangers), or
    *segment* (bulk tier vs private tier) — since the capability is
    latent in the protocol and will be built by someone regardless.
    The human decides the framing and tone of that ask.

## Meta

16. **(H) Name.** `directswarm` (descriptive, no collisions) chosen at
    handoff; alternatives considered: `beeline` (collides with Apache
    Hive's CLI), `waggle` (bee dance that communicates locations —
    apt but cute). Rename cheap until first release.
17. **(H) License** — sisters vary (freedom-browser MPL-2.0); pick
    before first push of implementation code.
18. **(H) Repo home** — starts under `petfold/` like weightstation and
    swarmfs; move to an org if/when it becomes a Solar Punk deliverable.
19. **(H) Relationship to the incentive findings**: weightstation's
    "funding is not incentivised" report may lead to protocol changes
    that alter this design's economics — track ethersphere/bee once
    that issue is filed. (Rev-2 note: audience serving *adds* a reason
    to fix it — paid serving only competes if paying is actually how
    you get served.)
