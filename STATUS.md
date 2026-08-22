# directswarm — STATUS

## 2026-08-22 — PHASE 0 MEASUREMENTS COMPLETE — human review gate open

Full report: **REPORT-phase0.md** (raw CSVs in `.phase0/`). Headline:

- **Reachability gate: PASS** — 41/41 sampled storers accept a
  stranger's dial+handshake (gate ≥50%).
- **Service-rate gate: FAILS AS WRITTEN, for a reason the plan didn't
  enumerate** — median peak 0.074 MB/s per storer (gate ≥1 MB/s), but
  with ZERO refusals in 39 settled runs and storer bursts ≥0.35 MB/s:
  the ceiling is the light-peer payment threshold pacing our own
  settlement (40,481 overdraft waits), not storer policy. Aggregate
  still scales with concurrent connections (unmeasured — next ask).
- 1-hop price confirmed ~29% cheaper than forwarding in protocol units
  (219,936 vs ~310k units/chunk); 901 real cheques accepted by 10
  strangers, 0.0065 xBZZ settled.
- Milestone-2 debugging findings folded into the report: a client must
  mount pricing AND hive to be tolerated at all; depth-100 pipelines
  push peers to disconnect-limit (cap 32 in future).

**Aggregate concurrency measured (user-approved option 1):** 1/5/20
parallel peers → 0.074/0.149/0.251 MB/s aggregate — sublinear because
**bee's chequebook serializes cheque issuance under one global mutex
held across the sign-and-send round trip (~5–6 cheques/s per client)**;
the marginal gain at high peer counts is stacked pseudosettle free
tier, labeled as such, never a strategy. Fix hierarchy in
REPORT-phase0.md: per-beneficiary issuance locking (client-side,
protocol-compliant → ~0.28 MB/s paid per connection → ~90 connections
reach 25 MB/s at today's thresholds) + upstream threshold policy (10×
further). **Phase-0 measurement set complete; human review gate open
on the exit decision** (upstream write-up scope + whether Phase 1
starts with per-beneficiary settlement as its first work item).

**Spend total this phase: ~1.17 xBZZ** (0.880 postage + 0.286 upload
settlement + 0.0065 retrieval settlement + gas dust). Balances:
Nook wallet ~2.28 xBZZ / 0.55 xDAI, Nook chequebook ~2.28 xBZZ
available; spike wallet 0.5 xBZZ / ~0.1 xDAI, spike chequebook
~0.99 xBZZ available.

## 2026-08-22 — Phase 0 in progress (live session)

**Environment:** Nook's bee 2.8.1 light node (localhost:1633), 137
peers, depth 9. Laptop moved from wifi to Ethernet mid-session (before
any measured retrieval runs; payload upload started on wifi, finished
on Ethernet — logistics, not a benchmark claim).

**Done so far:**
- Test payload live on mainnet: 1 GiB deterministic (wsbench seed 1),
  ref `842efaa92f86fe67dd7bd244a7c7935cade4da1eee41ea558f49c00da90a759a`,
  all 264,209 chunks tag-verified synced; local accept 2.80 MB/s.
  Batch `47265a62…` depth-21 mutable, ~3.1 days TTL left, utilization
  0.47 after upload.
- Spike milestone 1 (`spike/cmd/reach`, bee-as-library, compiles
  clean): reachability probe. Run 1+2 collected zero hive records —
  mainnet bootnodes never announced to us (finding to investigate:
  that's how stock light nodes are meant to bootstrap), and bee's
  dialer silently drops private/loopback underlays without
  `AllowPrivateCIDRs` (fixed). Also: bootnode `libp2p.direct` /ws
  underlays are rejected as unsupported transport — data point for the
  browser-transport story. Run 3 (seeded from Nook's node) in flight.
- Etiquette caps blessed by user for the probe: ≤3 bootnode connects,
  90 s passive listen, ≤50 dials at ≤2/s, one attempt per node, no
  retries, polite disconnects, 15-min hard cap.

**Reachability probe — first results (run 7, 2026-08-22, Ethernet):**
41 full-node records collected from 3 seed announces (35 distinct
depth-9 neighborhoods); **41/41 sampled nodes accepted a dial + full
bee handshake from an unknown light peer (100%)**. Dial+handshake
wall-clock: median 191 ms, p25 189, p75 346, p95 559, max 891 (2–3
round trips + crypto — network RTT is roughly a third). Caveats,
honestly: the sample is *biased toward reachable nodes* (records are
peers currently connected to three healthy full nodes, and hive only
gossips full nodes), and n=41 came from one announce burst. Gate-grade
reachability needs a snowball crawl with a larger cap (needs a new
etiquette blessing). Raw rows: `.phase0/reach.csv`.

**Protocol findings from the debugging path (runs 1–6):**
1. **Mounting hive alone gets you ejected**: stock peers open a
   pricing stream right after the handshake to announce their payment
   threshold; a client that can't accept it is disconnected within
   seconds. Any directswarm client must mount pricing/accounting from
   the first dial. (This is also a nice fact upstream: the network
   already refuses accounting-incapable peers.)
2. **Bootnodes don't bulk-announce**: their own bins are empty
   (bootnode mode kicks everyone), so a hanging light peer gets only a
   drip of records as new full nodes connect. Ordinary full nodes
   announce their whole connected set immediately — seed crawls from
   full nodes, not bootnodes.
3. **bee dialers refuse to keep light peers** (`ErrDialLightNode`) —
   S2 client↔client connections (Phase 4) cannot ride stock bee's
   Connect as-is; relevant to OPEN-QUESTIONS 11.
4. bee's libp2p service panics without a topology notifier
   (reachability worker); a kademlia-less client must install a no-op
   PickyNotifier. Also: `AllowPrivateCIDRs` needed for loopback/LAN
   dials; bootnode `libp2p.direct` /ws underlays are unsupported by
   bee's own transport set (browser-transport data point).
Seeding note: probe bootstraps from swarmscan (public explorer) — fine
for a spike; the real crawler should snowball from bootnode drip alone
to avoid any external dependency.

**Spike settlement identity (milestone 2, live):** eth
0xDEdAc9Ac6BaDD4B4C9ff99e4B54f3E8835892E8A, overlay b52342…, own
chequebook **0xE8C7aD1Af8CAb91E2695EfD1a12dBfCc186dFD41** (deploy tx
0xd61c96…), 1.0 xBZZ deposited/available, 0.5 xBZZ + ~0.1 xDAI in its
wallet. Chunk enumeration verified offline: 264,209 chunks, computed
root == mainnet ref (ROOT CHECK PASS). Nook config gained
`withdrawal-addresses-whitelist` (spike address only).

**Spend ledger (this session):**
| item | amount |
|---|---|
| postage batch 47265a62 (depth 21, mutable) | 0.880 xBZZ |
| upload sync settlement (1 GiB pushsync)    | 0.286 xBZZ |
| wallet→chequebook deposit (tx 0x4b2bda…)   | (2.000 xBZZ moved, not spent) |
| Nook wallet → spike wallet (txs 0x9ce874…, 0xbf5f60…) | (1.5 xBZZ + 0.1 xDAI moved) |
| spike chequebook deploy+deposit gas        | ~0.000005 xDAI |
| spike chequebook deposit                   | (1.0 xBZZ moved, spends as settlement during measurement) |
| **total spent** | **1.166 xBZZ (+gas dust)** |

Standing grants added to CLAUDE.md: batch purchases/top-ups AND
wallet→chequebook deposits when necessary (user, 2026-08-22).
Chequebook available after deposit: ~2.28 xBZZ.

Balances after upload: wallet ~5.78 xBZZ + 0.667 xDAI; chequebook
available **0.281 xBZZ** (total 3.026). Retrieval runs need ~1–1.7
xBZZ settled → wallet→chequebook deposit of 2 xBZZ proposed, awaiting
user approval.

## 2026-08-22 — pre-flight approvals; design stage closed

Human approvals, defaults accepted: **Q5 posture confirmed** (spike
quietly — protocol-compliant, paid, polite — propose with data at
Phase 2, whole direction disclosed); **Q20a** test payload = fresh
1 GiB deterministic payload, mutable batch, ~1 week TTL (~0.5–1 xBZZ
postage + ~2.5 h upload); **Q20b** fresh dedicated spike chequebook
(~1–2 xBZZ + xDAI gas to deploy/fund); **Q20c** etiquette caps
proposed and blessed at spike start; **Q20d** license =
**BSD-3-Clause** (LICENSE file added).

New operational permissions from the user (CLAUDE.md updated):
starting/stopping nodes is authorized; warn in advance when a speed
test needs the laptop on Ethernet (wifi not fully reliable); warn when
wallet balances run low so the user can top up.

**Design stage is closed.** Next session starts Phase 0: Bee node up +
funded, payload upload, chequebook deployment — each with a spend
estimate first, balances snapshotted around measured runs.

**Spend this session:** 0 xBZZ.

## 2026-08-22 — design revision 2.4: substrate resolved (Rust); browser reality

**Built:** nothing — design stage.

**Done:** resolved the substrate posture (Q4) and recorded the
browser/Wasm analysis:

- **Product in Rust (extend ant)**, decided by the browser endgame:
  rust-libp2p ships browser transports (websocket/webtransport/
  webrtc-websys), wasm32 is first-class, PyO3 covers Python bindings
  for swarmfs/weightstation. Go compiles to Wasm but go-libp2p has no
  browser-transport story and bee-as-library cannot run in a page —
  bee's value is the fast Phase-0 spike (throwaway) and wire-semantics
  reference. Spike stays substrate-agnostic.
- **Day-one discipline, not a later deliverable**: sans-I/O core crate
  behind a transport trait (native tokio adapter now, websys later);
  wasm32 kept compiling in CI from the first commit.
- **Browser transport reality recorded** (DESIGN.md "Form factor"):
  pages can't dial raw TCP/QUIC, storers listen on nothing else today
  (ws flag ~unused; wss needs certs), so the in-page client becomes
  realistic at Phase 4 where we control both endpoints (seeds/peers on
  WebTransport/WebRTC). New Phase-2 upstream ask: browser-dialable
  listeners on full nodes (WebTransport certhash — config, not
  protocol).
- **New Q20 (H), "Pre-flight"**: what must be settled before Phase-0
  code touches mainnet — test payload (weightstation batches
  lapsed/lapsing; fresh upload, mutable batch, ~0.5–1 xBZZ + ~2.5 h),
  dedicated spike chequebook (+~1–2 xBZZ + gas to approve), etiquette
  caps sign-off, license pick before first code push.

**Measured:** nothing. **Spend:** 0 xBZZ. **Deviations:** none.

**Open items:** blocking before development: Q20 (a–d) and Q5 posture
confirmation. Everything else (fallback semantics, resume format, CLI
shape, Phase-4 questions) decides during or after Phase 0/1.

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
