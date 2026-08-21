# directswarm — design (draft)

## Retrieval pipeline

```
manifest/ref ──► chunk address set          (client-side: BMT split is
                                             deterministic; swarmfs
                                             already does it offline)
chunk addrs ──► neighborhood map            (address prefix → storer
                                             neighborhood at network
                                             depth d; d ≈ 9–11 today)
overlay crawl ─► topology cache             (hive gossip: overlay →
                                             underlay/IP for reachable
                                             full nodes, per bin; crawl
                                             once, refresh incrementally)
scheduler ────► per-storer streams          (dial 2–3 storers per
                                             neighborhood; pipeline
                                             50–100 outstanding chunk
                                             requests per stream)
chunks ───────► verify (BMT) ► reassemble ► sink (file / pipe / fsspec)
```

Throughput model: per-storer rate ≈ outstanding × 4 KiB ÷ RTT
(100 × 4 KiB ÷ 30 ms ≈ 13 MB/s); aggregate ≈ min(Σ storer rates, own
downlink, disk). The **existential unknown is the per-storer service
rate a stranger peer is actually granted** — measured first (PLAN
Phase 0), designed around second.

## Protocol surface used (all stock Bee protocols)

- handshake + hive (peer records / topology gossip)
- retrieval (request/response per chunk) — storers answer any connected
  peer; that is how forwarding chains terminate today
- pricing/pseudosettle/SWAP — settlement per connection; chequebook
  required for sustained rates. directswarm always settles (README
  principle 2); at 1-hop proximity pricing this should also be the
  cheapest way to pay for a chunk (verify empirically).

## Components

1. **crawler/** — bounded overlay walk (~10k nodes today) building the
   topology cache: bin-organized, freshness-stamped, NAT-reachability
   flagged. Rate-limited and polite; cache shared across fetches and
   refreshed lazily on dial failures.
2. **scheduler/** — assigns chunk addresses to neighborhood queues,
   balances across 2–3 storers per neighborhood, adapts pipeline depth
   per peer (AIMD on observed service rate), retries across
   neighborhood members, falls back to forwarding retrieval (via a
   local Bee node) for chunks whose neighborhood is unreachable.
3. **transport/** — libp2p dial + Bee handshake + retrieval + settlement.
   Substrate decision (OPEN-QUESTIONS): extend **ant** (Rust; stack
   exists, Solar Punk codebase; needs multi-peer scheduler) vs. import
   **bee as a Go library** (protocols come free; carries bee's
   per-chunk overhead, ~10 CPU-ms measured) vs. clean-room libp2p
   client (most control, most work). Phase-0 spike may be quickest via
   bee-as-library; the product likely wants ant/Rust.
4. **verify/** — BMT per chunk (microseconds), whole-file integrity via
   the manifest; identical guarantees to stock retrieval.
5. **bench/** — wsbench-compatible output (same CSV vocabulary,
   medians/p95, cold/warm honesty rules) so results compare directly
   against weightstation's Phase-0 numbers.

## Upload path (later phase)

Same inversion applies to publish: push chunks directly to their storer
neighborhoods (stock pushsync at 1 hop) instead of forwarding — attacks
weightstation's measured ~1.1 MB/s publish ceiling. Deferred until
retrieval proves out; uploads carry stamp semantics (batch owner key)
and stricter correctness burden (receipts).

## Tradeoffs (candid)

- **Anonymity**: storers learn who fetches what. Acceptable for public
  artifacts; must remain opt-in; never claim privacy properties.
- **Forwarder economics**: bypasses forwarding earnings and path
  caching. Mitigation: payment still lands on storers (arguably better
  aligned); raise the design upstream early rather than presenting a
  fait accompli.
- **Churn/NAT**: topology cache staleness and unreachable storers are
  the operational risk; the forwarding fallback bounds the damage.
- **Peer-set / connection budget**: hundreds of short-lived dials per
  fetch; be a good citizen (connection reuse, backoff, no hammering
  neighborhoods that refuse).
