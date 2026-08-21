# CLAUDE.md — working instructions for directswarm

## Context

You are building directswarm: a fast data plane for Ethereum Swarm —
kademlia for discovery and persistence, direct libp2p connections for
mass data (bulk files first, peer-assisted distribution later; live
streaming deliberately deferred and not a design input), everything
anchored on Swarm and settled via SWAP.
Read README.md, DESIGN.md, PLAN.md,
and OPEN-QUESTIONS.md before writing code. The project exists because of
measured findings in the sister repo — read
`../weightstation/bench/REPORT.md` for the numbers this design answers.

## Environment

- Live Swarm mainnet work requires the user's funded Bee node
  (http://localhost:1633) for bootstrap/fallback and a funded
  chequebook for settlement — **warn the user to start it when needed;
  never install or start nodes unprompted.**
- Gnosis chain RPC: https://rpc.gnosischain.com (public, rate-limited).
- Sister codebases to reuse, not reinvent: `../weightstation/bench`
  (wsbench harness, deterministic payloads, honesty rules),
  `../swarmfs` (offline BMT split, stamps math, measured Bee facts),
  `../freedom-browser` + solardev-xyz/ant (Rust Bee-protocol stack).

## Non-negotiable principles

1. **Always settle.** SWAP payment on every connection that moves real
   data — storer, publisher seed, or audience peer; coordination
   gossip is the only unpaid traffic. Never exploit or
   recommend pseudosettle free-tier
   multiplication — the user's explicit policy (it's a loophole to
   report upstream, not a feature). Benchmarks that accidentally ran
   unpaid are labeled as such, never presented as the result.
2. **Protocol-compliant.** Stock chunks, stock protocols, no forks. If
   a needed behavior doesn't exist in the protocol, that's an upstream
   proposal, not a local hack.
3. **Network etiquette.** Rate-limit crawls and dials; back off from
   peers that refuse; a benchmark must never be distinguishable from
   abuse. When in doubt about load, ask the user.
4. **Honest benchmarking** (inherited from weightstation): medians and
   p95s, cold vs warm explicitly labeled and actually true (verify the
   cache is bypassed — antd taught us headers can be silently ignored),
   environment and payment state disclosed, raw CSVs kept.

## Phase gates

Work strictly in PLAN.md order. **Phase 0 (storer service-rate spike)
gates everything** — if strangers get ≲0.1 MB/s per storer, hard stop:
no fetcher; bring the findings and the seed-pivot option (PLAN Phase 0,
exit B) to human review. Human review gates: end of Phase 0, and
before publishing anything upstream (Phase 2 proposal, issues, SWIP
drafts).

## Spending rules

Settlement and any stamp/chequebook operations spend the user's real
xBZZ. Estimate before running, confirm anything beyond trivial amounts,
snapshot chequebook balances around measured runs (the weightstation
burn-accounting pattern), and keep a spend ledger in STATUS.md.

## Non-goals (do not drift)

No anonymity claims, no gateway/hosted service, no protocol forks, no
custom chunk formats, no upload path before retrieval proves out, no
shadow network (nothing moves on the fast plane that isn't anchored on
Swarm with valid postage), no trackers or hosted rendezvous, no
incentive-scheme design (report incentive findings upstream instead).

## Status discipline

Update STATUS.md (create it) at every phase end and session end: built,
measured numbers, spend, deviations, open items. It is read by the
human between sessions.
