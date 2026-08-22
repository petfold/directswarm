# DRAFT — bee PR (prepared, NOT submitted; user approval required)

Closes/relates: https://github.com/ethersphere/bee/issues/5570

**Code**: single commit `297e17d3` on branch `swap-cached-chequebook-balance`
in the local bee repo, based cleanly on upstream `origin/master` (6ab12c63) —
deliberately NOT based on `feat/manifest-listing`. Worktree:
`directswarm/.phase0/bee-pr`. Diff: chequebook.go +33/−14, chequebook_test.go
+62 (new `TestChequebookIssueCachedBalance`); full
`go test ./pkg/settlement/...` green, `go vet` clean, no existing test
modified.

**To submit when approved:**
```sh
cd /home/test/projects/directswarm/.phase0/bee-pr
git push fork swap-cached-chequebook-balance
gh pr create -R ethersphere/bee --head petfold:swap-cached-chequebook-balance \
  --title "<below>" --body-file <body below>
```

---TITLE---
swap: cache chequebook balance+totalPaidOut instead of two chain reads per cheque issuance

---BODY---
### Checklist

- [x] I have read the coding guide.
- [x] My change requires a documentation update, and I have done it. *(no user-facing docs affected)*
- [x] I have added tests to cover my changes.

### Description

Fixes #5570.

Every cheque issuance re-verifies the chequebook's covering balance via
two `eth_call`s (`balance`, `totalPaidOut`) inside
`reserveTotalIssued`, under the service mutex — serializing all
issuance node-wide at the RPC round-trip rate (~5 cheques/s against a
public Gnosis endpoint) and coupling settlement latency to the RPC
provider. Details and measurements in #5570.

This PR caches the sum `balance + totalPaidOut`, which is safe because
the sum is invariant under everything except the issuer's own actions
and third-party increases:

- **cheque cash-outs leave it unchanged** — `_cashChequeInternal`
  moves exactly the paid tokens from `balance` to `totalPaidOut`,
  including partial "bounced" payouts (note the current code already
  relies on this: its two reads are separate `eth_call`s that are not
  atomic across block heights);
- **it decreases only via `withdraw`, which is `onlyIssuer`** — every
  decrease necessarily passes through this same service;
- **all other changes are increases** (deposits from any party), so a
  stale cache can only under-issue, never overcommit.

Behavior:

- recompute on TTL expiry (5 min; correctness does not depend on it);
- drop the cache on own `Deposit`;
- pessimistically subtract on own `Withdraw` (the one decreasing path)
  as soon as the tx is sent, rather than waiting for it to mine;
- on an apparent shortfall, refresh once before returning
  `ErrOutOfFunds` — unless the failing computation was already served
  fresh from the backend — so external top-ups are seen exactly when
  they matter.

`AvailableBalance` (the API-facing method) still reads live, unchanged.

Measured effect (light client settling concurrently with 20 mainnet
full nodes, methodology in #5570): cheque issuance 5/s → 62/s
node-wide, settled throughput ×16; the residual per-peer cadence is
bound by the cheque send round trip, as expected.

### Open questions for reviewers

- Should the pessimistic `Withdraw` subtraction instead drop the cache
  and force a live read (simpler, one extra RPC per withdrawal)?
- Hard deposits would reduce genuine coverage without changing the
  cached sum; the current formula already ignores them (bee never
  creates them), so this PR preserves that pre-existing behavior — flag
  if you'd rather it be addressed here.
