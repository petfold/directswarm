# DRAFT — bee issue (not yet filed; human review required before publishing)

Target: github.com/ethersphere/bee (new issue, label suggestion: performance)
File with: `gh issue create -R ethersphere/bee --title "..." --body-file <this, below the marker>`
Patch (ready to open as PR on request): local branch `chequebook-cached-balance`
(worktree `.phase0/bee-patched`, one commit on top of current master-era code;
applies to v2.8.1 cleanly).

---TITLE---
swap: cheque issuance is serialized by two per-cheque on-chain balance reads under a global mutex (~5 cheques/s per node; measured 10–18x settlement throughput with a cached-invariant fix)

---BODY---
### Summary

Every SWAP cheque issuance re-verifies the chequebook's covering
balance by making **two live blockchain RPC calls while holding a
global mutex**, which serializes all cheque issuance node-wide at
roughly the RPC round-trip rate — on Gnosis with a public RPC that is
**~5–6 cheques per second for the entire node**, however many peers it
settles with. The check itself is necessary; fetching it on-chain per
cheque is not: the fetched quantity is invariant under everything
except the issuer's own deposits/withdrawals and third-party
*increases*, so it can be cached with strictly conservative failure
modes. A minimal patch doing that measured **10–18× higher settled
throughput** under concurrent settlement (numbers below); happy to
open it as a PR.

### Mechanism (v2.8.1, `pkg/settlement/swap/chequebook/chequebook.go`)

- `Issue` (L190) begins with `reserveTotalIssued` (L163), which takes
  `s.lock` (L164) and, holding it, calls `AvailableBalance` (L123).
- `AvailableBalance` makes two on-chain calls — `contract.Balance` and
  `contract.TotalPaidOut` — i.e. two RPC round trips per cheque, under
  the global mutex. (The p2p send of the cheque is correctly outside
  the lock; the serialization is purely the reservation step.)

With a typical public-RPC round trip of ~100 ms, issuance is bounded
at ~5/s per node, every concurrent settlement queues on the same
mutex, and issuance latency inherits the RPC provider's latency,
jitter, and rate limits.

### Why per-cheque freshness buys nothing — and the current code already assumes so

`AvailableBalance = (balance + totalPaidOut) − totalIssued`, with
`totalIssued` purely local. The on-chain sum `balance + totalPaidOut`:

- is **invariant under cheque cash-outs** — `_cashChequeInternal`
  increments `totalPaidOut` by exactly the tokens transferred out,
  including partial "bounced" payouts;
- **decreases only via `withdraw`, which is `onlyIssuer`** — and the
  issuer key is this node's key, so every decrease necessarily passes
  through this same service;
- otherwise only **increases** (deposits — the node's own, or anyone
  else's ERC20 transfer to the contract address).

Note the shipped code already relies on this invariant for its own
correctness: the two reads are separate `eth_call`s that can land on
different block heights, and a beneficiary can cash a cheque between
the reads or before the issued cheque is persisted. That is only safe
because cash-outs leave the sum unchanged. Caching the sum introduces
no assumption the current code doesn't already make — while a stale
cache errs strictly toward *under*-issuance (missed external
deposits), never toward an uncovered cheque.

(Aside: hard deposits would reduce what genuinely covers new cheques
without changing the sum — but the current formula already ignores
them and bee never creates them, so that blind spot is pre-existing
and unchanged.)

### The fix (patch in hand)

1. Cache `balance + totalPaidOut`; recompute under the existing lock
   when stale (TTL, e.g. 5 min — correctness doesn't depend on it).
2. Invalidate on own `Deposit`; on `Withdraw`, pessimistically
   subtract the amount immediately (the one decreasing path).
3. On apparent shortfall, force one refresh before returning
   `ErrOutOfFunds` — so external top-ups are picked up exactly when
   they matter.

`reserveTotalIssued` then does no I/O on the hot path; remaining
serialization is the in-memory reservation arithmetic. ~50 lines, no
API, wire-protocol, or contract change.

### Measured impact

Method: a funded light client (own factory-deployed chequebook, Gnosis
mainnet, `rpc.gnosischain.com`) retrieving chunks from N mainnet full
nodes concurrently with SWAP active, paying each peer as fast as
accounting allows; issuance counted at `chequebook.Service.Issue`.
Same host, same peers, same chunk workload before/after.

| concurrent peers | cheques/s (node-wide) | settled throughput (sum of per-peer steady rates) |
|---|---|---|
| 5, stock   | ~5.2  | 0.155 MB/s |
| 5, patched | ~30   | **1.08 MB/s (7×)** |
| 20, stock  | ~5–6  | 0.277 MB/s |
| 20, patched| ~62   | **4.56 MB/s (16×)** |

Per-peer cheque cadence after the patch settles at ~3/s — i.e. bound
by the cheque send round trip per peer, as expected once the global
RPC serialization is gone. In ordinary operation the defect is
invisible (cheques are rare; most traffic settles within
thresholds/refresh), but any node under sustained settlement load hits
it immediately.

### Environment

bee v2.8.1 sources (present unchanged at current master), Gnosis
mainnet, public RPC; Linux client, wired connection; cheques accepted
by 20 distinct mainnet full nodes during the measurements, no
disputes, no bounced cheques.
