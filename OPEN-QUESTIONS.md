# directswarm — open questions

(H) needs the human; (C) may be proposed by Claude Code with rationale
in STATUS.md.

## Existential (Phase 0 answers these)

1. **(C) Storer service policy toward strangers.** What sustained
   chunk rate does a Bee full node grant one unknown inbound peer, and
   what limits it — accounting credit, stream limits, disk, or explicit
   rate limiting? The whole design stands or falls here.
2. **(C) Reachability share.** What fraction of storers accept inbound
   dials (NAT, connection-slot policy, bin gating for unknown peers)?
   Bee full nodes must be publicly reachable in principle; measure
   practice.
3. **(C) 1-hop pricing.** Is direct retrieval actually cheaper per
   chunk than the paid forwarding chain (proximity-based pricing says
   yes; verify against weightstation's ~310k PLUR-units/chunk and
   ~0.33 xBZZ/GiB numbers).

## Design

4. **(C) Substrate**: extend ant (Rust) vs bee-as-Go-library vs
   clean-room libp2p. Spike may differ from product; measure per-chunk
   CPU cost on each candidate (Bee spends ~10 CPU-ms/chunk; BMT itself
   is microseconds — how lean can the client be?).
5. **(H) Upstream-first or client-first?** Building first produces
   evidence but risks community friction (forwarder-earnings bypass);
   proposing first risks debating without data. Current plan: spike
   quietly (protocol-compliant, paid, polite), propose with data at
   Phase 2 — confirm this posture.
6. **(C) Crawl etiquette**: rate limits, cache TTLs, dial budgets that
   keep a fetch from looking like an attack. Also: can the topology
   cache be shared/published (a signed snapshot on Swarm itself?)
   without becoming a centralization point?
7. **(H) Settlement identity**: directswarm needs its own funded
   chequebook (per README principle 2). One per client install? Shared
   with a local Bee node's wallet? Custody + funding UX.
8. **(C) Fallback semantics**: when a neighborhood is unreachable,
   fall back to forwarding via a local Bee node — required dependency
   or optional?

## Meta

9. **(H) Name.** `directswarm` (descriptive, no collisions) chosen at
   handoff; alternatives considered: `beeline` (collides with Apache
   Hive's CLI), `waggle` (bee dance that communicates locations —
   apt but cute). Rename cheap until first release.
10. **(H) License** — sisters vary (freedom-browser MPL-2.0); pick
    before first push of implementation code.
11. **(H) Repo home** — starts under `petfold/` like weightstation and
    swarmfs; move to an org if/when it becomes a Solar Punk deliverable.
12. **(H) Relationship to the incentive findings**: weightstation's
    "funding is not incentivised" report may lead to protocol changes
    that alter this design's economics — track ethersphere/bee once
    that issue is filed.
