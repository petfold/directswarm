# directswarm — STATUS

## 2026-08-22 — design revision 2.3: latency-aware source selection

**Built:** nothing — design stage.

**Done:** folded in latency-aware source selection (DESIGN.md new
section "Latency-aware source selection"; crawler and scheduler
components updated; PLAN Phase 0 now records dial RTTs and Phase 1's
scheduler is latency-aware; Q6 extended with the probe budget and the
prediction-method choice). Key points:

- With pipeline depth capped by the accounting threshold, per-storer
  rate ∝ 1/RTT — choosing which 2–3 of a neighborhood's members to
  dial is the main per-connection throughput lever.
- Discipline: latency ranks the healthy but never disqualifies; RTT
  is the prior, observed service rate the posterior (AIMD keeps the
  final say); ε-greedy floor against hot-spotting and earnings skew.
- Measurement in etiquette order: passive handshake timing → stock
  pingpong probes within the dial budget → GeoIP/ASN or Vivaldi-style
  coordinate prediction for the unprobed.
- Hedged duplicate requests for tail chunks, with the honesty caveat
  that a hedged chunk arriving second is still settled — surplus
  bounded and ledgered.

**Measured:** nothing. **Spend:** 0 xBZZ. **Deviations:** none.

**Open items:** (H) agenda unchanged; rev 2.3 uncommitted pending
review.

## 2026-08-21 — design revision 2.2: discussion points folded into docs

**Built:** nothing — still design stage.

**Done:** recorded the remaining points from the 2026-08-21 design
discussion (form factor, systemic adoption, anonymity), the parts not
already in the docs:

- **Form factor** (DESIGN.md "Form factor and deployment"; README
  "How it ships"): standalone client — core library + thin CLI, daemon
  mode at Phase 4 for seeding; not a bee add-on (no plugin mechanism,
  no fork); own overlay identity and own funded chequebook (no
  chequebook sharing — concurrent cheque issuance conflicts); a local
  bee node is an optional companion for the forwarding fallback only,
  never a bundle; endgame inverts the packaging if upstream adopts the
  strategy natively.
- **Systemic-adoption analysis** (DESIGN.md "Systemic effects"):
  cannot overtake the network structurally — competes only on the
  retrieval forwarding path (storage/sync/incentives untouched by
  invariant 1); self-limiting where it competes (light-slot
  contention, storer policy lever, no benefit for small fetches);
  expected equilibrium is segmentation; externalities disclosed
  (forwarder income shift, cover-traffic thinning, colder forwarding
  caches); counterweight: newly feasible use cases bring settlement
  revenue and postage demand. Phase-2 write-up now asks upstream to
  pick a posture — embrace / constrain / segment (new Q15, (H)).
- **Anonymity reassessed** (DESIGN.md subsection; README "The honest
  trade" updated): the stock baseline is weaker than advertised —
  light clients provably originate every request (they forward
  nothing; the first hop sees requester + content linked), SWAP
  cheques tie fetches to a chequebook identity on any path, and no
  adversarial analysis of "ambient anonymity" exists (community doubts
  noted). Layered anonymity (Tor/Nym in front) is the principled fix
  and composes with direct transfer at least as well as with
  forwarding — measurement parked as new Q14 (C). "Never claim
  privacy" and the opt-in posture are unchanged.
- OPEN-QUESTIONS: new "Ecosystem" section (Q14–15); meta renumbered
  16–19. PLAN Phase-2 write-up scope extended accordingly.

**Measured:** nothing; no network operations.

**Spend:** 0 xBZZ.

**Deviations:** none.

**Open items / next:** revs 2.1 and 2.2 are both uncommitted, pending
the user's review. (H) agenda now: Q5, Q7, Q9, Q13, Q15.

## 2026-08-21 — design revision 2.1: streaming demoted; bee facts verified

**Built:** nothing — still design stage.

**Done:**

- **Live streaming demoted from design goal to deferred note** (user
  decision after discussion; it must not influence the design at this
  stage). Rationale recorded in DESIGN.md "Deferred: live streams":
  streaming largely works on today's Swarm at common bitrates
  (~1.36 MB/s measured ≥ HD), live fan-out is the workload forwarding
  caching handles best, and the one genuine gap the fast plane fixes
  (publish headroom) is already in the plan for bulk reasons. Removed:
  PLAN Phase 5, relay trees, per-stream session meshes, stream/
  component, all streaming references in README/CLAUDE.md.
  OPEN-QUESTIONS 14–16 (relay mechanics, feed latency, streaming
  economics) dropped; meta renumbered 14–17.
- **Verified against `../bee` source** (answers folded into DESIGN.md
  and OPEN-QUESTIONS.md): retrieval handler serves any connected peer,
  no role gating (Q1 mechanism, Q11 partly answered — light nodes
  mount the handler and serve from cache); **pricing is a fixed
  proximity formula, not peer-announced** (`pkg/pricer` FixedPricer) —
  Q12 answered: 1-hop storer fetch is cheapest by construction, S2
  audience serving is mispriced under stock semantics (follow-on
  decision recorded in Q12); full nodes admit ~100 light peers
  (`defaultLightNodeLimit`); retrieval streams carry blocklisting on
  misbehavior. Conclusion recorded in DESIGN.md: **no bee changes are
  required for any phase** — bee changes appear only as optional
  upstream proposals.

**Measured:** nothing on the network; code inspection only.

**Spend:** 0 xBZZ.

**Deviations:** none.

**Open items / next:** discussion agenda unchanged in kind —
OPEN-QUESTIONS (H) items 5 (upstream posture), 7 (settlement identity),
9 (peer-assist scope, now streaming-free), 13 (audience-serving
default). Q12's follow-on (interim non-storer pricing convention vs
defer S2) needs a proposal before Phase 4. Phase 0 remains the first
executable step.

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
