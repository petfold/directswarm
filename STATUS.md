# directswarm — STATUS

## 2026-08-21 — design revision 2 (session end)

**Built:** nothing — design stage; no code, per brief.

**Done:** revised all handoff files to widen scope from "bulk fetcher
dialing storer neighborhoods" to a two-plane design: Swarm as the
anchor plane (identity, integrity, persistence, discovery, private
fallback) and a libp2p fast plane for mass data with three source
classes — S1 storer neighborhoods, S2 audience peers, S3 publisher/
mirror seeds — plus a live-streaming sketch (feed-anchored segments +
paid relay trees; gossip for control only, never content). The core of
the revision is the value-preservation analysis (DESIGN.md, "what
forwarding kademlia buys"): four invariants — anchored on Swarm, stock
chunks, always settled, stock fallback intact — with requester
anonymity named as the one knowing trade (opt-in mode, private path
stays first-class, paid 1-hop proxy sketched but deferred).

Files touched: README.md, DESIGN.md (rewritten), PLAN.md (adds
Phase 0 exit B, Phase 3 upload spike, Phases 4–5 with acceptance
criteria and a Phase-5 review gate), OPEN-QUESTIONS.md (adds Q9–16),
CLAUDE.md (context, principle 1, phase gates, non-goals aligned).

**Measured:** nothing; no network operations this session.

**Spend:** 0 xBZZ — no settlement, no stamps, no chain calls.

**Deviations:** none — no code written, no nodes touched.

**Open items / next:** the design needs discussion before anything is
built. Agenda = OPEN-QUESTIONS.md, especially the (H) items: 5
(upstream posture), 7 (settlement identity, now also receiving), 9
(scope appetite for phases 4–5), 13 (audience-serving default), 16
(streaming economics / who-pays split). Phase 0 (storer service-rate
spike) remains the first executable step and still gates everything;
requires the user's funded Bee node when it runs.
