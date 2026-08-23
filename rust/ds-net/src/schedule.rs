//! M5 — the multi-connection settled scheduler, rebuilt around the
//! constants the probe-growth experiment measured (2026-08-23).
//!
//! N storer connections run as independent actors, each owning its own
//! libp2p swarm and poller (the M4-cont fix — a shared swarm serialized
//! all stream I/O and did not scale), dialed in parallel. What M5
//! changes:
//!
//! 1. **Live-threshold pacing.** Each connection PARSES pricing
//!    announcements (M4 drained them) and paces against the live
//!    threshold T. Bee grows T with settled volume (+lightRefreshRate
//!    per checkpoint, verified in source and live) and re-announces
//!    upgrades mid-connection, so throughput rises as we pay.
//! 2. **λ-aware exposure control** (the probe's ceiling algorithm):
//!    bee's worst-case ledger view — our mirror debt + reserved +
//!    cheques emitted within the validation window — is kept
//!    ≤ 1.05 × T, safely under bee's 1.25 × T disconnect limit.
//!    λ (per-peer cheque-validation latency) comes from the persisted
//!    peer-state cache, or is measured inline once on first contact
//!    (sweep cheque + small pseudosettle probes), or defaults
//!    conservatively.
//! 3. **Threshold-aware mirror.** ant's `Accounting` hard-caps debt at
//!    the FRESH light disconnect limit, silently re-capping grown
//!    connections to free-tier pacing (found live in the pilot);
//!    replaced by [`crate::growth::Mirror`] bounded by the live gates.
//! 4. **Work-stealing.** Chunks live in shared per-neighborhood buckets
//!    (first `depth` bits); every connection covering a bucket pulls
//!    from it, so a slow or dead connection's work is picked up by its
//!    neighbors instead of straggling (M4's static queues were
//!    straggler-dominated).
//! 5. **Selection by earned trust.** Storers are ranked per bucket by
//!    measured λ class, last-known threshold, then RTT — slow
//!    validators are deprioritized, never refused.
//!
//! Chunks no connection covers fall back to the local bee's forwarding
//! retrieval (invariant 4). Fetched chunks land in the on-disk
//! [`ChunkStore`]; the caller reassembles + byte-verifies with the M1
//! joiner over [`crate::store::StoreFetcher`].

use ant_retrieval::accounting::Accounting;
use ant_retrieval::retrieve_chunk;
use anyhow::{anyhow, Result};
use ds_core::TopologyCache;
use libp2p::swarm::SwarmEvent;
use libp2p::{dns, identify, noise, ping, tcp, yamux};
use libp2p::{Multiaddr, PeerId, SwarmBuilder};
use libp2p_stream::{Behaviour as StreamBehaviour, Control};
use primitive_types::U256;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};

use crate::bee_api::BeeApiFetcher;
use crate::direct::{
    emit_settlement_cheque, handshake_with_fallback_raw, mount_sinks,
    read_chequebook_issuable_raw, ChequeEmit,
};
use crate::growth::{refresh_probe, Mirror};
use crate::identity::Identity;
use crate::peerstate::PeerStateStore;
use crate::store::ChunkStore;

const REFRESH_MIN_INTERVAL: Duration = Duration::from_millis(1100);
const TICK: Duration = Duration::from_millis(50);
const DIAL_TIMEOUT: Duration = Duration::from_secs(20);
const POST_HANDSHAKE_SETTLE: Duration = Duration::from_secs(2);
/// Fresh light-peer threshold — the floor before any announcement.
const DEFAULT_THRESHOLD: u64 = 1_350_000;
/// λ for peers never measured when inline measurement is off: above
/// the fast cohort (≤1.5 s measured on 10/11 storers) with margin.
const DEFAULT_LAMBDA_MS: u64 = 3_000;
/// λ clamp range; a probe timeout maps to the slow end.
const LAMBDA_MIN_MS: u64 = 800;
const LAMBDA_MAX_MS: u64 = 25_000;
/// Cheque cadence floor (stream setup + signature are not free).
const CHEQUE_MIN_INTERVAL: Duration = Duration::from_millis(500);
/// λ-probe parameters (see `growth.rs` for the mechanism).
const LAMBDA_PROBE_INTERVAL: Duration = Duration::from_millis(1150);
const LAMBDA_PROBE_UNITS: u64 = 50_000;
const LAMBDA_PROBE_TIMEOUT: Duration = Duration::from_secs(25);
/// Exchange-rate fallback for spend projection before the first cheque.
const DEFAULT_RATE: u64 = 100_000;

#[derive(Debug, Clone)]
pub struct ScheduleOptions {
    pub network_id: u64,
    pub chain_id: u64,
    pub chequebook: [u8; 20],
    pub rpc_url: String,
    pub ledger_path: std::path::PathBuf,
    pub bee_url: String,
    pub store_base: std::path::PathBuf,
    /// Persisted per-peer settlement state (threshold, λ, volume).
    pub peerstate_path: std::path::PathBuf,
    /// Target number of concurrent storer connections.
    pub connections: usize,
    /// Per-connection pipeline depth (etiquette cap 32).
    pub start_depth: usize,
    pub max_depth: usize,
    /// Neighborhood depth for storer↔chunk matching and work buckets.
    pub depth: u8,
    /// Global cheque spend cap in PLUR (safety budget). Enforced at
    /// emit AND projected at the fetch gate so the final sweep always
    /// has room (the M2 lesson).
    pub max_issue_plur: u64,
    /// Measure λ inline on first contact with an unknown peer (one
    /// sweep cheque + small probes, ~5–30 s once per peer, persisted).
    pub measure_lambda: bool,
    /// Storers per bucket (≥1). At 1 a slow storer finishes its bucket
    /// alone (tier-1 measured: one fresh-threshold straggler held a
    /// 20-conn run's wall clock for 25 min); at 2+ the shared bucket
    /// lets fast siblings steal the tail.
    pub redundancy: usize,
    /// Measurement mode: drop chunks no connection covers instead of
    /// sending them to the bee fallback, so the reported throughput is
    /// the direct plane's alone.
    pub direct_only: bool,
}

#[derive(Debug, Default)]
pub struct ScheduleReport {
    pub connections_opened: usize,
    pub chunks_total: usize,
    pub chunks_from_direct: u64,
    pub chunks_from_fallback: u64,
    pub chunks_dropped_uncovered: u64,
    pub chunks_failed: u64,
    pub direct_bytes: u64,
    pub fallback_bytes: u64,
    pub wall: Duration,
    pub cheques_issued: u64,
    pub cheque_plur: u128,
    pub refresh_units: u64,
    pub residual_debt_units: u64,
    /// Connections whose end-of-run zero-debt was confirmed by the
    /// PEER (pseudosettle probe ACK == 0).
    pub zero_confirmed_conns: u64,
    /// λ measured inline this run (new peers learned).
    pub lambdas_measured: u64,
    pub errors: Vec<String>,
}

impl ScheduleReport {
    /// Direct-plane aggregate throughput — the scaling metric.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn direct_mbps(&self) -> f64 {
        let s = self.wall.as_secs_f64();
        if s > 0.0 {
            self.direct_bytes as f64 / 1e6 / s
        } else {
            0.0
        }
    }

    /// Total (direct + fallback) throughput.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn total_mbps(&self) -> f64 {
        let s = self.wall.as_secs_f64();
        if s > 0.0 {
            (self.direct_bytes + self.fallback_bytes) as f64 / 1e6 / s
        } else {
            0.0
        }
    }
}

#[derive(libp2p::swarm::NetworkBehaviour)]
struct Behaviour {
    stream: StreamBehaviour,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
}

/// First `depth` bits of an address as a bucket key (PO ≥ depth ⟺
/// equal first `depth` bits, so a storer's bucket is exactly the
/// chunks it is duty-bound to hold).
fn bucket_prefix(addr: &[u8; 32], depth: u8) -> u64 {
    let mut first = [0u8; 8];
    first.copy_from_slice(&addr[..8]);
    let v = u64::from_be_bytes(first);
    if depth == 0 {
        0
    } else {
        v >> (64 - u32::from(depth.min(63)))
    }
}

/// Shared per-neighborhood work buckets — the work-stealing pool.
struct WorkPool {
    buckets: Mutex<HashMap<u64, Vec<[u8; 32]>>>,
}

impl WorkPool {
    fn pop(&self, prefix: u64) -> Option<[u8; 32]> {
        self.buckets.lock().ok()?.get_mut(&prefix)?.pop()
    }

    fn drain_all(&self) -> Vec<[u8; 32]> {
        let Ok(mut b) = self.buckets.lock() else {
            return Vec::new();
        };
        b.drain().flat_map(|(_, v)| v).collect()
    }
}

/// Fetch `chunk_addrs` across `opts.connections` settled storer
/// connections (from `cache`) plus a bee fallback, landing chunks in
/// the on-disk store.
///
/// # Errors
/// Fails on unrecoverable setup (swarm build, chain read). Per-chunk
/// and per-peer failures are recorded, not fatal.
///
/// # Panics
/// Only on a poisoned internal mutex.
#[allow(clippy::too_many_lines)]
pub async fn fetch_scheduled(
    id: &Identity,
    cache: &TopologyCache,
    chunk_addrs: Vec<[u8; 32]>,
    opts: &ScheduleOptions,
) -> Result<ScheduleReport> {
    let mut report = ScheduleReport {
        chunks_total: chunk_addrs.len(),
        ..Default::default()
    };
    let store = Arc::new(ChunkStore::open(&opts.store_base)?);
    let peerstate = Arc::new(PeerStateStore::open(&opts.peerstate_path));

    // Resume: drop chunks already stored.
    let needed: Vec<[u8; 32]> = chunk_addrs
        .into_iter()
        .filter(|a| !store.contains(a))
        .collect();
    if needed.is_empty() {
        report.wall = Duration::ZERO;
        return Ok(report);
    }

    // Cached invariant read (once) + shared global cheque spend cap +
    // ONE PERSISTED outbound ledger shared by every connection and
    // every run (bee's chequestore keeps the highest validated
    // cumulative forever — the M4 diagnosis).
    let issuable = read_chequebook_issuable_raw(&opts.rpc_url, opts.chequebook).await?;
    let issued_plur = Arc::new(AtomicU64::new(0));
    let ledger = Arc::new(ant_p2p::swap::OutboundLedger::open(Some(
        opts.ledger_path.clone(),
    )));
    tracing::info!(%issuable, "chequebook cached invariant read once (shared spend cap across connections)");

    // Select storers per bucket: breadth first (one per non-empty
    // bucket, largest buckets first), then redundancy rounds.
    let selected = select_storers(
        cache,
        &needed,
        opts.depth,
        opts.connections,
        opts.redundancy.max(1),
        &peerstate,
    );
    if selected.is_empty() {
        return Err(anyhow!("no storer in the cache covers any needed chunk"));
    }
    let covered: std::collections::HashSet<u64> = selected.iter().map(|s| s.prefix).collect();

    // Route chunks: covered → shared work buckets (any covering
    // connection pulls); uncovered → fallback now (or dropped).
    let (fb_tx, fb_rx) = mpsc::unbounded_channel::<[u8; 32]>();
    let mut buckets: HashMap<u64, Vec<[u8; 32]>> = HashMap::new();
    for addr in &needed {
        let p = bucket_prefix(addr, opts.depth);
        if covered.contains(&p) {
            buckets.entry(p).or_default().push(*addr);
        } else if opts.direct_only {
            report.chunks_dropped_uncovered += 1;
        } else {
            let _ = fb_tx.send(*addr);
        }
    }
    let pool = Arc::new(WorkPool {
        buckets: Mutex::new(buckets),
    });

    // Shared result counters.
    let direct_bytes = Arc::new(AtomicU64::new(0));
    let direct_ok = Arc::new(AtomicU64::new(0));
    let cheques = Arc::new(AtomicU64::new(0));
    let cheque_plur = Arc::new(AtomicU64::new(0));
    let refresh_units = Arc::new(AtomicU64::new(0));
    let opened = Arc::new(AtomicU64::new(0));
    let zero_confirmed = Arc::new(AtomicU64::new(0));
    let lambdas_measured = Arc::new(AtomicU64::new(0));

    // Fallback worker (bee forwarding) drains uncovered + failed chunks.
    let fallback = BeeApiFetcher::new(&opts.bee_url)?;
    let fb_store = store.clone();
    let fb_bytes = Arc::new(AtomicU64::new(0));
    let fb_bytes2 = fb_bytes.clone();
    let fb_count = Arc::new(AtomicU64::new(0));
    let fb_count2 = fb_count.clone();
    let fb_handle = tokio::spawn(async move {
        use ant_retrieval::ChunkFetcher;
        let mut rx = fb_rx;
        let fallback = Arc::new(fallback);
        let mut fails: Vec<String> = Vec::new();
        // Modest concurrency: bee serializes cold forwarding retrievals
        // per request, so a few in flight cut the tail-drain wall time
        // without leaning on the local node.
        let mut tasks: tokio::task::JoinSet<Option<String>> = tokio::task::JoinSet::new();
        loop {
            while tasks.len() >= 8 {
                if let Some(Ok(Some(err))) = tasks.join_next().await {
                    fails.push(err);
                }
            }
            let Some(addr) = rx.recv().await else { break };
            if fb_store.contains(&addr) {
                continue;
            }
            let f = fallback.clone();
            let st = fb_store.clone();
            let b = fb_bytes2.clone();
            let c = fb_count2.clone();
            tasks.spawn(async move {
                match f.fetch(addr).await {
                    Ok(wire) => {
                        let _ = st.put(addr, &wire);
                        b.fetch_add(wire.len() as u64, Ordering::Relaxed);
                        c.fetch_add(1, Ordering::Relaxed);
                        None
                    }
                    Err(err) => Some(format!("fallback {}: {err}", hex::encode(addr))),
                }
            });
        }
        while let Some(joined) = tasks.join_next().await {
            if let Ok(Some(err)) = joined {
                fails.push(err);
            }
        }
        fails
    });

    // One actor per storer, all in parallel, each with its own swarm.
    let started = Instant::now();
    let mut actors = Vec::new();
    for storer in selected {
        actors.push(tokio::spawn(run_connection(ConnArgs {
            id_secret: id.secret,
            id_nonce: id.nonce,
            keypair: id.keypair.clone(),
            storer,
            pool: pool.clone(),
            network_id: opts.network_id,
            chain_id: opts.chain_id,
            chequebook: opts.chequebook,
            issuable,
            issued_plur: issued_plur.clone(),
            ledger: ledger.clone(),
            max_issue_plur: opts.max_issue_plur,
            pipeline: opts.start_depth.clamp(1, 32),
            store: store.clone(),
            fallback: fb_tx.clone(),
            peerstate: peerstate.clone(),
            measure_lambda: opts.measure_lambda,
            direct_bytes: direct_bytes.clone(),
            direct_ok: direct_ok.clone(),
            cheques: cheques.clone(),
            cheque_plur: cheque_plur.clone(),
            refresh_units: refresh_units.clone(),
            opened: opened.clone(),
            zero_confirmed: zero_confirmed.clone(),
            lambdas_measured: lambdas_measured.clone(),
        })));
    }

    // Progress sampler: one INFO line per 10 s so the log yields a
    // completion curve (time-to-90% is the straggler-honest metric).
    let sampler_stop = Arc::new(AtomicBool::new(false));
    {
        let stop = sampler_stop.clone();
        let ok = direct_ok.clone();
        let bytes = direct_bytes.clone();
        let t0 = started;
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(10)).await;
                tracing::info!(
                    target: "m5progress",
                    t_s = t0.elapsed().as_secs(),
                    chunks = ok.load(Ordering::Relaxed),
                    bytes = bytes.load(Ordering::Relaxed),
                    "progress"
                );
            }
        });
    }

    let mut residual = 0u64;
    for a in actors {
        if let Ok(r) = a.await {
            residual += r;
        }
    }
    sampler_stop.store(true, Ordering::Relaxed);
    // Chunks stranded in buckets whose every covering connection died.
    let stranded = pool.drain_all();
    for addr in stranded {
        if opts.direct_only {
            report.chunks_dropped_uncovered += 1;
        } else {
            let _ = fb_tx.send(addr);
        }
    }
    drop(fb_tx);
    let fb_fails = fb_handle.await.unwrap_or_default();
    store.flush()?;

    report.wall = started.elapsed();
    report.connections_opened =
        usize::try_from(opened.load(Ordering::Relaxed)).unwrap_or(usize::MAX);
    report.direct_bytes = direct_bytes.load(Ordering::Relaxed);
    report.fallback_bytes = fb_bytes.load(Ordering::Relaxed);
    report.chunks_from_direct = direct_ok.load(Ordering::Relaxed);
    report.chunks_from_fallback = fb_count.load(Ordering::Relaxed);
    report.cheques_issued = cheques.load(Ordering::Relaxed);
    report.cheque_plur = u128::from(cheque_plur.load(Ordering::Relaxed));
    report.refresh_units = refresh_units.load(Ordering::Relaxed);
    report.residual_debt_units = residual;
    report.zero_confirmed_conns = zero_confirmed.load(Ordering::Relaxed);
    report.lambdas_measured = lambdas_measured.load(Ordering::Relaxed);
    report.chunks_failed = (needed.len() as u64)
        .saturating_sub(report.chunks_from_direct + report.chunks_from_fallback)
        .saturating_sub(report.chunks_dropped_uncovered);
    report.errors.extend(fb_fails);
    if report.connections_opened == 0 {
        return Err(anyhow!("no storer connection established"));
    }
    Ok(report)
}

/// A selected storer: where to dial, who it is, which bucket it serves.
struct Selected {
    underlay: Multiaddr,
    overlay: [u8; 32],
    prefix: u64,
}

/// Candidate tuple: (λ class, inverted threshold, RTT, underlay,
/// overlay) — ordered so a plain tuple sort ranks best first.
type Candidate = (u8, u64, u32, Multiaddr, [u8; 32]);

/// Rank storers per bucket by (λ class, last-known threshold desc,
/// RTT asc); allocate breadth-first across buckets (largest chunk
/// count first), then redundancy rounds until `connections` actors.
fn select_storers(
    cache: &TopologyCache,
    needed: &[[u8; 32]],
    depth: u8,
    connections: usize,
    redundancy: usize,
    peerstate: &PeerStateStore,
) -> Vec<Selected> {
    let mut bucket_count: HashMap<u64, usize> = HashMap::new();
    for a in needed {
        *bucket_count.entry(bucket_prefix(a, depth)).or_default() += 1;
    }
    // Candidates per bucket, quality-sorted.
    let mut per_bucket: HashMap<u64, Vec<Candidate>> = HashMap::new();
    for rec in cache.records() {
        let p = bucket_prefix(&rec.overlay, depth);
        if !bucket_count.contains_key(&p) {
            continue;
        }
        let Some(underlay) = rec.underlays.iter().find_map(|u| public_dialable(u)) else {
            continue;
        };
        let ps = peerstate.get(&rec.overlay).unwrap_or_default();
        // λ class: 0 = measured fast, 1 = unknown, 2 = measured slow.
        let class = match ps.lambda_ms {
            Some(l) if l <= 2_500 => 0u8,
            None => 1,
            Some(_) => 2,
        };
        per_bucket.entry(p).or_default().push((
            class,
            u64::MAX - ps.threshold_last, // ascending sort ⇒ higher T first
            rec.rtt_ms.unwrap_or(u32::MAX),
            underlay,
            rec.overlay,
        ));
    }
    for v in per_bucket.values_mut() {
        v.sort_by_key(|c| (c.0, c.1, c.2));
    }
    // Buckets by needed-chunk count, biggest first; with redundancy R,
    // cover connections/R buckets with up to R storers each.
    let mut order: Vec<u64> = per_bucket.keys().copied().collect();
    order.sort_by_key(|p| std::cmp::Reverse(bucket_count.get(p).copied().unwrap_or(0)));
    if redundancy > 1 {
        order.truncate(connections.div_ceil(redundancy).max(1));
    }

    let mut out = Vec::new();
    let mut round = 0usize;
    while out.len() < connections {
        let mut any = false;
        for p in &order {
            if out.len() >= connections {
                break;
            }
            if let Some(c) = per_bucket.get(p).and_then(|v| v.get(round)) {
                out.push(Selected {
                    underlay: c.3.clone(),
                    overlay: c.4,
                    prefix: *p,
                });
                any = true;
            }
        }
        if !any {
            break;
        }
        round += 1;
    }
    out
}

fn public_dialable(u: &str) -> Option<Multiaddr> {
    if u.starts_with("/ip4/")
        && !u.starts_with("/ip4/10.")
        && !u.starts_with("/ip4/192.168")
        && !u.starts_with("/ip4/172.")
        && !u.contains("/ws")
    {
        u.parse().ok()
    } else {
        None
    }
}

fn build_swarm(keypair: &libp2p::identity::Keypair) -> Result<libp2p::Swarm<Behaviour>> {
    let behaviour = Behaviour {
        stream: StreamBehaviour::default(),
        identify: identify::Behaviour::new(
            identify::Config::new("bee/2.8.0".into(), keypair.public())
                .with_agent_version("directswarm/0.1.0".into()),
        ),
        ping: ping::Behaviour::new(ping::Config::new()),
    };
    Ok(SwarmBuilder::with_existing_identity(keypair.clone())
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_dns_config(
            dns::ResolverConfig::cloudflare(),
            dns::ResolverOpts::default(),
        )
        .with_behaviour(|_| behaviour)
        .expect("infallible behaviour")
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(600)))
        .build())
}

struct ConnArgs {
    id_secret: [u8; 32],
    id_nonce: [u8; 32],
    keypair: libp2p::identity::Keypair,
    storer: Selected,
    pool: Arc<WorkPool>,
    network_id: u64,
    chain_id: u64,
    chequebook: [u8; 20],
    issuable: U256,
    issued_plur: Arc<AtomicU64>,
    ledger: Arc<ant_p2p::swap::OutboundLedger>,
    max_issue_plur: u64,
    pipeline: usize,
    store: Arc<ChunkStore>,
    fallback: mpsc::UnboundedSender<[u8; 32]>,
    peerstate: Arc<PeerStateStore>,
    measure_lambda: bool,
    direct_bytes: Arc<AtomicU64>,
    direct_ok: Arc<AtomicU64>,
    cheques: Arc<AtomicU64>,
    cheque_plur: Arc<AtomicU64>,
    refresh_units: Arc<AtomicU64>,
    opened: Arc<AtomicU64>,
    zero_confirmed: Arc<AtomicU64>,
    lambdas_measured: Arc<AtomicU64>,
}

/// Everything the settlement/pacing side of one connection shares.
struct Pacer {
    mirror: Arc<Mirror>,
    threshold_rx: watch::Receiver<Option<U256>>,
    /// Validation-latency estimate for the exposure window.
    lambda_ms: u64,
    /// (emit time, units) of cheques possibly still validating.
    unvalidated: VecDeque<(Instant, u64)>,
    /// Announced exchange rate once known (spend projection).
    rate: u64,
    /// Units settled on this connection (cheques + refreshes).
    settled_units: u64,
    last_refresh: Option<Instant>,
    last_cheque: Option<Instant>,
}

impl Pacer {
    fn threshold(&self) -> u64 {
        self.threshold_rx
            .borrow()
            .as_ref()
            .map_or(DEFAULT_THRESHOLD, |t| {
                u64::try_from(*t).unwrap_or(DEFAULT_THRESHOLD)
            })
    }

    fn window(&self) -> Duration {
        Duration::from_millis(self.lambda_ms.clamp(LAMBDA_MIN_MS, LAMBDA_MAX_MS) * 3 / 2)
    }

    fn unvalidated_sum(&mut self) -> u64 {
        let w = self.window();
        while let Some(&(at, _)) = self.unvalidated.front() {
            if at.elapsed() > w {
                self.unvalidated.pop_front();
            } else {
                break;
            }
        }
        self.unvalidated.iter().map(|&(_, u)| u).sum()
    }

    /// Exposure gate for one more reservation of `price` units:
    /// mirror debt + reserved + unvalidated + price ≤ 1.05 × T.
    fn admit(&mut self, price: u64) -> bool {
        let t = self.threshold();
        let unval = self.unvalidated_sum();
        let limit = (t.saturating_mul(105) / 100).saturating_sub(unval);
        self.mirror.try_reserve(price, limit)
    }
}

/// One connection actor. Returns residual unsettled units (0 clean).
#[allow(clippy::too_many_lines)]
async fn run_connection(a: ConnArgs) -> u64 {
    let Some(peer_id) = crate::direct::extract_peer_id(&a.storer.underlay) else {
        return 0;
    };
    let mut swarm = match build_swarm(&a.keypair) {
        Ok(s) => s,
        Err(err) => {
            tracing::debug!(%peer_id, "swarm build failed: {err}");
            return 0;
        }
    };
    let mut control = swarm.behaviour().stream.new_control();
    let (threshold_tx, threshold_rx) = watch::channel::<Option<U256>>(None);
    if mount_sinks(&mut control, threshold_tx).is_err() {
        return 0;
    }

    if swarm.dial(a.storer.underlay.clone()).is_err() {
        return 0;
    }
    let alive = Arc::new(AtomicBool::new(true));
    let (est_tx, est_rx) = tokio::sync::oneshot::channel::<()>();
    let (bye_tx, mut bye_rx) = tokio::sync::oneshot::channel::<()>();
    {
        let alive = alive.clone();
        tokio::spawn(async move {
            let mut est_tx = Some(est_tx);
            loop {
                tokio::select! {
                    evt = futures::StreamExt::next(&mut swarm) => match evt {
                        None => break,
                        Some(SwarmEvent::ConnectionEstablished { peer_id: p, .. }) if p == peer_id => {
                            if let Some(tx) = est_tx.take() { let _ = tx.send(()); }
                        }
                        Some(SwarmEvent::ConnectionClosed { peer_id: p, .. }) if p == peer_id => {
                            alive.store(false, Ordering::Relaxed);
                        }
                        Some(_) => {}
                    },
                    _ = &mut bye_rx => {
                        let _ = swarm.disconnect_peer_id(peer_id);
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        alive.store(false, Ordering::Relaxed);
                        break;
                    }
                }
            }
        });
    }

    if tokio::time::timeout(DIAL_TIMEOUT, est_rx).await.is_err() {
        let _ = bye_tx.send(());
        return 0;
    }
    let id_for_hs = crate::identity::Identity {
        secret: a.id_secret,
        eth: [0u8; 20],
        nonce: a.id_nonce,
        overlay: [0u8; 32],
        keypair: a.keypair.clone(),
    };
    let handshake = tokio::time::timeout(
        DIAL_TIMEOUT,
        handshake_with_fallback_raw(
            &mut control,
            &id_for_hs,
            peer_id,
            &a.storer.underlay,
            a.network_id,
        ),
    )
    .await;
    let Ok(Ok(info)) = handshake else {
        let _ = bye_tx.send(());
        return 0;
    };
    if info.remote_overlay != a.storer.overlay {
        // Different node at this underlay than the cache promised —
        // its bucket assignment would be wrong; bail politely.
        tracing::debug!(%peer_id, "overlay mismatch vs topology cache");
        let _ = bye_tx.send(());
        return 0;
    }
    a.opened.fetch_add(1, Ordering::Relaxed);
    tokio::time::sleep(POST_HANDSHAKE_SETTLE).await;

    let known = a.peerstate.get(&info.remote_overlay);
    let mut pacer = Pacer {
        mirror: Arc::new(Mirror::default()),
        threshold_rx,
        lambda_ms: known
            .and_then(|k| k.lambda_ms)
            .unwrap_or(DEFAULT_LAMBDA_MS),
        unvalidated: VecDeque::new(),
        rate: DEFAULT_RATE,
        settled_units: 0,
        last_refresh: None,
        last_cheque: None,
    };
    let mut ctx = ConnCtx {
        control,
        peer_id,
        overlay: info.remote_overlay,
        beneficiary: info.remote_eth_address,
        a: &a,
        alive: alive.clone(),
    };

    // First contact with an unknown peer: measure λ once, persist it.
    let mut measured_lambda: Option<u64> = None;
    if known.and_then(|k| k.lambda_ms).is_none() && a.measure_lambda {
        if let Some(l) = measure_lambda(&mut ctx, &mut pacer).await {
            pacer.lambda_ms = l;
            measured_lambda = Some(l);
            a.lambdas_measured.fetch_add(1, Ordering::Relaxed);
            tracing::info!(peer=%peer_id, lambda_ms=l, "validation latency measured");
        }
    }

    // Main fetch + settle loop.
    let residual = drive(&mut ctx, &mut pacer).await;

    // Bee-side zero confirmation (only meaningful on a live conn).
    let mut confirmed = false;
    if residual == 0 && alive.load(Ordering::Relaxed) {
        let wait = pacer.lambda_ms.clamp(2_500, LAMBDA_MAX_MS) + 1_500;
        tokio::time::sleep(Duration::from_millis(wait)).await;
        if let Ok(0) = refresh_probe(&mut ctx.control, peer_id, LAMBDA_PROBE_UNITS).await {
            confirmed = true;
            a.zero_confirmed.fetch_add(1, Ordering::Relaxed);
        }
    }
    a.peerstate.record(
        &info.remote_overlay,
        pacer.threshold(),
        measured_lambda,
        pacer.settled_units,
        residual == 0 && confirmed,
    );
    let _ = bye_tx.send(());
    residual
}

struct ConnCtx<'a> {
    control: Control,
    peer_id: PeerId,
    overlay: [u8; 32],
    beneficiary: [u8; 20],
    a: &'a ConnArgs,
    alive: Arc<AtomicBool>,
}

/// Emit one cheque for `units` under the global spend cap; instant
/// mirror credit (bee's `NotifyPaymentSent` semantics) + unvalidated
/// tracking.
async fn emit(ctx: &mut ConnCtx<'_>, pacer: &mut Pacer, units: u64) -> Result<()> {
    let outcome = emit_settlement_cheque(ChequeEmit {
        control: &mut ctx.control,
        peer_id: ctx.peer_id,
        secret: &ctx.a.id_secret,
        chequebook: ctx.a.chequebook,
        beneficiary: ctx.beneficiary,
        chain_id: ctx.a.chain_id,
        debt_units: units,
        ledger: &ctx.a.ledger,
        issuable: ctx.a.issuable,
        issued_plur: &ctx.a.issued_plur,
        max_issue_plur: ctx.a.max_issue_plur,
    })
    .await?;
    pacer.mirror.credit(units);
    pacer.settled_units += units;
    pacer.unvalidated.push_back((Instant::now(), units));
    pacer.last_cheque = Some(Instant::now());
    pacer.rate = u64::try_from(outcome.rate).unwrap_or(DEFAULT_RATE);
    ctx.a.cheques.fetch_add(1, Ordering::Relaxed);
    ctx.a
        .cheque_plur
        .fetch_add(u64::try_from(outcome.plur).unwrap_or(u64::MAX), Ordering::Relaxed);
    Ok(())
}

async fn refresh(ctx: &mut ConnCtx<'_>, pacer: &mut Pacer) {
    let (debt, _) = pacer.mirror.snapshot();
    if debt > 0 {
        if let Ok(ok) = ant_p2p::pseudosettle::refresh_peer(&mut ctx.control, ctx.peer_id).await {
            if ok.accepted > 0 {
                pacer.mirror.credit(ok.accepted);
                pacer.settled_units += ok.accepted;
                ctx.a
                    .refresh_units
                    .fetch_add(ok.accepted, Ordering::Relaxed);
            }
        }
    }
    pacer.last_refresh = Some(Instant::now());
}

/// Measure this peer's cheque-validation latency once: build a little
/// debt (real work — the chunks land in the store), sweep it with one
/// cheque, then small-probe until the peer's ACK hits zero twice.
async fn measure_lambda(ctx: &mut ConnCtx<'_>, pacer: &mut Pacer) -> Option<u64> {
    let t = pacer.threshold();
    let target = (t * 4 / 10).max(LAMBDA_PROBE_UNITS * 6);
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline && ctx.alive.load(Ordering::Relaxed) {
        let (debt, _) = pacer.mirror.snapshot();
        if debt >= target {
            break;
        }
        let Some(addr) = ctx.a.pool.pop(ctx.a.storer.prefix) else {
            break;
        };
        if ctx.a.store.contains(&addr) {
            continue;
        }
        let price = Accounting::peer_price(&ctx.overlay, &addr);
        if !pacer.mirror.try_reserve(price, t / 2) {
            break;
        }
        if let Ok(chunk) = retrieve_chunk(&mut ctx.control, ctx.peer_id, addr).await {
            pacer.mirror.apply(price);
            let _ = ctx.a.store.put(addr, &chunk.data);
            ctx.a
                .direct_bytes
                .fetch_add(chunk.data.len() as u64, Ordering::Relaxed);
            ctx.a.direct_ok.fetch_add(1, Ordering::Relaxed);
        } else {
            pacer.mirror.release(price);
            let _ = ctx.a.fallback.send(addr);
            break;
        }
    }
    let (debt, _) = pacer.mirror.snapshot();
    if debt < LAMBDA_PROBE_UNITS * 4 {
        return None;
    }
    // Let the peer's per-second refresh timestamp tick over.
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    emit(ctx, pacer, debt).await.ok()?;
    let t0 = Instant::now();
    let mut zero_at: Option<u64> = None;
    while t0.elapsed() < LAMBDA_PROBE_TIMEOUT {
        tokio::time::sleep(LAMBDA_PROBE_INTERVAL).await;
        let Ok(accepted) = refresh_probe(&mut ctx.control, ctx.peer_id, LAMBDA_PROBE_UNITS).await
        else {
            continue;
        };
        if accepted > 0 {
            pacer.settled_units += accepted;
            ctx.a.refresh_units.fetch_add(accepted, Ordering::Relaxed);
        }
        let ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX);
        match (accepted, zero_at) {
            (0, Some(first)) => {
                pacer.last_refresh = Some(Instant::now());
                return Some(first);
            }
            (0, None) => zero_at = Some(ms),
            _ => zero_at = None,
        }
    }
    pacer.last_refresh = Some(Instant::now());
    Some(LAMBDA_MAX_MS) // timeout ⇒ treat as slow, pace accordingly
}

/// The per-connection fetch + settle loop. Pulls work from the shared
/// bucket, admits fetches under the exposure gate, refreshes on
/// cadence, sweeps debt with full-size cheques, and finishes with a
/// zero-debt sweep. Returns residual unsettled units.
#[allow(clippy::too_many_lines)]
async fn drive(ctx: &mut ConnCtx<'_>, pacer: &mut Pacer) -> u64 {
    let mut tasks: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
    let mut pending: Option<[u8; 32]> = None;
    let mut spend_stop = false;
    let mut bucket_drained = false;
    let mut finishing = false;
    let mut sweep_attempts = 0u32;
    loop {
        if !ctx.alive.load(Ordering::Relaxed) {
            // Peer hung up: in-flight tasks fail over on their own;
            // whatever debt remains is unpayable now.
            while tasks.join_next().await.is_some() {}
            if let Some(addr) = pending.take() {
                let _ = ctx.a.fallback.send(addr);
            }
            let (debt, _) = pacer.mirror.snapshot();
            return debt;
        }

        // Global spend-cap projection: stop FETCHING with sweep room
        // left (a guard that blocks the final settlement strands debt).
        if !spend_stop {
            let (debt, reserved) = pacer.mirror.snapshot();
            let projected = u128::from(ctx.a.issued_plur.load(Ordering::SeqCst))
                + u128::from(debt + reserved) * u128::from(pacer.rate);
            if projected > u128::from(ctx.a.max_issue_plur) * 92 / 100 {
                spend_stop = true;
                tracing::info!(peer=%ctx.peer_id, "spend cap near: fetching stopped, sweeping");
            }
        }
        if spend_stop {
            if let Some(addr) = pending.take() {
                let _ = ctx.a.fallback.send(addr);
            }
        }

        // Admit new fetches.
        while !finishing && !spend_stop && tasks.len() < ctx.a.pipeline {
            let popped = pending.take().or_else(|| ctx.a.pool.pop(ctx.a.storer.prefix));
            let Some(addr) = popped else {
                // Buckets only ever drain; emptiness is final.
                bucket_drained = true;
                break;
            };
            if ctx.a.store.contains(&addr) {
                continue;
            }
            let price = Accounting::peer_price(&ctx.overlay, &addr);
            if !pacer.admit(price) {
                pending = Some(addr);
                break;
            }
            let mut c = ctx.control.clone();
            let peer_id = ctx.peer_id;
            let mirror = pacer.mirror.clone();
            let store = ctx.a.store.clone();
            let bytes = ctx.a.direct_bytes.clone();
            let ok_ctr = ctx.a.direct_ok.clone();
            let fb = ctx.a.fallback.clone();
            tasks.spawn(async move {
                if let Ok(chunk) = retrieve_chunk(&mut c, peer_id, addr).await {
                    mirror.apply(price);
                    let _ = store.put(addr, &chunk.data);
                    bytes.fetch_add(chunk.data.len() as u64, Ordering::Relaxed);
                    ok_ctr.fetch_add(1, Ordering::Relaxed);
                } else {
                    mirror.release(price);
                    let _ = fb.send(addr);
                }
            });
        }
        while tasks.try_join_next().is_some() {}

        // Settle: refresh cadence, then cheque policy.
        if pacer
            .last_refresh
            .is_none_or(|at| at.elapsed() >= REFRESH_MIN_INTERVAL)
        {
            refresh(ctx, pacer).await;
        }
        let (debt, _) = pacer.mirror.snapshot();
        let t = pacer.threshold();
        let cheque_due = pacer
            .last_cheque
            .is_none_or(|at| at.elapsed() >= CHEQUE_MIN_INTERVAL);
        let want = if finishing || spend_stop {
            debt > 0 && cheque_due
        } else {
            debt >= t / 5 && cheque_due
        };
        if want {
            if let Err(err) = emit(ctx, pacer, debt).await {
                tracing::debug!(peer=%ctx.peer_id, "cheque emit failed: {err}");
                if finishing {
                    sweep_attempts += 1;
                    if sweep_attempts >= 3 {
                        let (residual, _) = pacer.mirror.snapshot();
                        tracing::warn!(peer=%ctx.peer_id, residual, "final sweep failed");
                        return residual;
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }

        // Phase transitions.
        if !finishing
            && pending.is_none()
            && tasks.is_empty()
            && (bucket_drained || spend_stop)
        {
            finishing = true;
        }
        if finishing && tasks.is_empty() {
            let (debt, _) = pacer.mirror.snapshot();
            if debt == 0 {
                return 0;
            }
        }
        tokio::time::sleep(TICK).await;
    }
}
