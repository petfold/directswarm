//! M4 — the multi-connection settled scheduler.
//!
//! N storer connections run as independent actors, **each owning its
//! own libp2p swarm and poller** — the original shared-swarm design
//! serialized all connections' stream I/O through one poll loop and
//! did not scale (measured flat in N); per-connection pollers remove
//! that ceiling. Actors are dialed in PARALLEL so a churned storer's
//! timeout overlaps its peers' instead of summing. Each chunk is
//! routed to a covering storer (proximity ≥ depth), pipelined at a
//! fixed per-connection depth, every connection fully SWAP-settled
//! through a shared global spend cap.
//! Chunks no connection covers fall back to the local bee's forwarding
//! retrieval (invariant 4). Fetched chunks land in an on-disk
//! [`ChunkStore`]; the caller reassembles + byte-verifies with the M1
//! joiner over [`crate::store::StoreFetcher`].
//!
//! This is where Phase-0's arithmetic gets tested: does aggregate
//! throughput scale ~linearly with connection count?

use ant_retrieval::accounting::Accounting;
use ant_retrieval::retrieve_chunk;
use anyhow::{anyhow, Result};
use ds_core::{proximity, TopologyCache};
use libp2p::swarm::SwarmEvent;
use libp2p::{dns, identify, noise, ping, tcp, yamux};
use libp2p::{Multiaddr, PeerId, SwarmBuilder};
use libp2p_stream::{Behaviour as StreamBehaviour, Control};
use primitive_types::U256;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};

use crate::bee_api::BeeApiFetcher;
use crate::direct::{
    emit_settlement_cheque, handshake_with_fallback_raw, read_chequebook_issuable_raw,
    run_drain_sink, ChequeEmit,
};
use crate::identity::Identity;
use crate::store::ChunkStore;

/// Uniform mainnet light-peer payment threshold (units). Every storer
/// we have measured announces this; M5 will parse it per-peer.
const ASSUMED_THRESHOLD: u64 = 1_350_000;
const REFRESH_MIN_INTERVAL: Duration = Duration::from_millis(1100);
const CHEQUE_CREDIT_DELAY: Duration = Duration::from_millis(2500);
const DIAL_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Clone)]
pub struct ScheduleOptions {
    pub network_id: u64,
    pub chain_id: u64,
    pub chequebook: [u8; 20],
    pub rpc_url: String,
    pub ledger_path: std::path::PathBuf,
    pub bee_url: String,
    pub store_base: std::path::PathBuf,
    /// Target number of concurrent storer connections.
    pub connections: usize,
    /// Starting per-connection pipeline depth (AIMD grows toward a cap).
    pub start_depth: usize,
    pub max_depth: usize,
    /// Neighborhood depth for storer↔chunk matching.
    pub depth: u8,
    /// Global cheque spend cap in PLUR (safety budget).
    pub max_issue_plur: u64,
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
    /// Bytes delivered by the direct (settled storer) plane.
    pub direct_bytes: u64,
    /// Bytes delivered by the bee forwarding fallback.
    pub fallback_bytes: u64,
    pub wall: Duration,
    pub cheques_issued: u64,
    pub cheque_plur: u128,
    pub refresh_units: u64,
    pub residual_debt_units: u64,
    pub errors: Vec<String>,
}

impl ScheduleReport {
    /// Direct-plane aggregate throughput — the M4 scaling metric.
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

/// One live settled connection to a storer.
struct Conn {
    peer_id: PeerId,
    overlay: [u8; 32],
    beneficiary: [u8; 20],
    /// Chunk addresses routed to this connection.
    queue: Mutex<Vec<[u8; 32]>>,
    alive: Arc<std::sync::atomic::AtomicBool>,
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

    // Resume: drop chunks already stored.
    let needed: Vec<[u8; 32]> = chunk_addrs
        .into_iter()
        .filter(|a| !store.contains(a))
        .collect();
    if needed.is_empty() {
        report.wall = Duration::ZERO;
        return Ok(report);
    }

    // Cached invariant read (once) + shared global cheque spend cap.
    // Each connection keeps its own in-memory outbound cheque ledger
    // (distinct beneficiary per peer, so no cross-connection sharing is
    // needed); the ledger_path is reserved for future persistence.
    let _ = &opts.ledger_path;
    let issuable = read_chequebook_issuable_raw(&opts.rpc_url, opts.chequebook).await?;
    let issued_plur = Arc::new(AtomicU64::new(0));
    tracing::info!(%issuable, "chequebook cached invariant read once (shared spend cap across connections)");

    // Select storers that cover the needed chunks, most-coverage first.
    let selected = select_storers(cache, &needed, opts.depth, opts.connections);
    if selected.is_empty() {
        return Err(anyhow!("no storer in the cache covers any needed chunk"));
    }

    // Route each chunk to the covering storer with the shortest queue;
    // uncovered → fallback (or dropped in direct-only mode).
    let (fb_tx, fb_rx) = mpsc::unbounded_channel::<[u8; 32]>();
    let mut queues: Vec<Vec<[u8; 32]>> = vec![Vec::new(); selected.len()];
    for addr in &needed {
        let mut best: Option<usize> = None;
        for (i, s) in selected.iter().enumerate() {
            if proximity(&s.overlay, addr) >= opts.depth
                && best.is_none_or(|b| queues[i].len() < queues[b].len())
            {
                best = Some(i);
            }
        }
        match best {
            Some(i) => queues[i].push(*addr),
            None => {
                if opts.direct_only {
                    report.chunks_dropped_uncovered += 1;
                } else {
                    let _ = fb_tx.send(*addr);
                }
            }
        }
    }

    // Shared result counters (each connection has its OWN swarm/poller
    // — no shared transport, so no single-poller contention).
    let direct_bytes = Arc::new(AtomicU64::new(0));
    let direct_ok = Arc::new(AtomicU64::new(0));
    let cheques = Arc::new(AtomicU64::new(0));
    let cheque_plur = Arc::new(AtomicU64::new(0));
    let refresh_units = Arc::new(AtomicU64::new(0));
    let opened = Arc::new(AtomicU64::new(0));

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
        let mut fails = Vec::new();
        while let Some(addr) = rx.recv().await {
            if fb_store.contains(&addr) {
                continue;
            }
            match fallback.fetch(addr).await {
                Ok(wire) => {
                    let _ = fb_store.put(addr, &wire);
                    fb_bytes2.fetch_add(wire.len() as u64, Ordering::Relaxed);
                    fb_count2.fetch_add(1, Ordering::Relaxed);
                }
                Err(err) => fails.push(format!("fallback {}: {err}", hex::encode(addr))),
            }
        }
        fails
    });

    // Spawn one connection actor PER storer, all in parallel — each
    // owns its libp2p swarm + poller. Parallel dial tolerates churned
    // storers (their timeouts overlap instead of summing).
    let started = Instant::now();
    let mut actors = Vec::new();
    for (storer, queue) in selected.into_iter().zip(queues) {
        actors.push(tokio::spawn(run_connection(ConnArgs {
            id_secret: id.secret,
            id_nonce: id.nonce,
            keypair: id.keypair.clone(),
            storer,
            queue,
            network_id: opts.network_id,
            chain_id: opts.chain_id,
            chequebook: opts.chequebook,
            issuable,
            issued_plur: issued_plur.clone(),
            max_issue_plur: opts.max_issue_plur,
            depth: opts.start_depth,
            store: store.clone(),
            fallback: fb_tx.clone(),
            direct_bytes: direct_bytes.clone(),
            direct_ok: direct_ok.clone(),
            cheques: cheques.clone(),
            cheque_plur: cheque_plur.clone(),
            refresh_units: refresh_units.clone(),
            opened: opened.clone(),
        })));
    }
    // Only actor-held fallback senders remain; when they finish, the
    // fallback receiver closes.
    drop(fb_tx);

    let mut residual = 0u64;
    for a in actors {
        if let Ok(r) = a.await {
            residual += r;
        }
    }
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
    report.chunks_failed = (needed.len() as u64)
        .saturating_sub(report.chunks_from_direct + report.chunks_from_fallback)
        .saturating_sub(report.chunks_dropped_uncovered);
    report.errors.extend(fb_fails);
    if report.connections_opened == 0 {
        return Err(anyhow!("no storer connection established"));
    }
    Ok(report)
}

/// Everything one connection actor needs. Each actor owns its own
/// libp2p swarm, so the shared items are only the store, the settlement
/// spend cap/ledger, and the result counters.
struct ConnArgs {
    id_secret: [u8; 32],
    id_nonce: [u8; 32],
    keypair: libp2p::identity::Keypair,
    storer: Selected,
    queue: Vec<[u8; 32]>,
    network_id: u64,
    chain_id: u64,
    chequebook: [u8; 20],
    issuable: U256,
    issued_plur: Arc<AtomicU64>,
    max_issue_plur: u64,
    depth: usize,
    store: Arc<ChunkStore>,
    fallback: mpsc::UnboundedSender<[u8; 32]>,
    direct_bytes: Arc<AtomicU64>,
    direct_ok: Arc<AtomicU64>,
    cheques: Arc<AtomicU64>,
    cheque_plur: Arc<AtomicU64>,
    refresh_units: Arc<AtomicU64>,
    opened: Arc<AtomicU64>,
}

/// One connection actor: own swarm + poller, dial + handshake its
/// storer, settle + fetch its assigned chunks, polite disconnect.
/// Returns residual unsettled units (0 on a clean close). On any
/// dial/handshake failure, its assigned chunks go to the fallback.
#[allow(clippy::too_many_lines)]
async fn run_connection(a: ConnArgs) -> u64 {
    let Some(peer_id) = crate::direct::extract_peer_id(&a.storer.underlay) else {
        for addr in a.queue {
            let _ = a.fallback.send(addr);
        }
        return 0;
    };

    // Build this connection's own swarm.
    let mut swarm = match build_swarm(&a.keypair) {
        Ok(s) => s,
        Err(err) => {
            tracing::debug!(%peer_id, "swarm build failed: {err}");
            for addr in a.queue {
                let _ = a.fallback.send(addr);
            }
            return 0;
        }
    };
    let mut control = swarm.behaviour().stream.new_control();
    mount_drain_sinks(&mut control);

    // Dial before handing the swarm to its poller.
    if swarm.dial(a.storer.underlay.clone()).is_err() {
        for addr in a.queue {
            let _ = a.fallback.send(addr);
        }
        return 0;
    }
    let alive = Arc::new(std::sync::atomic::AtomicBool::new(true));
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
                        break;
                    }
                }
            }
        });
    }

    // Wait for the connection, then handshake (both time-bounded so a
    // churned storer fails fast without holding up its peers).
    let connected = tokio::time::timeout(DIAL_TIMEOUT, est_rx).await;
    if connected.is_err() {
        for addr in a.queue {
            let _ = a.fallback.send(addr);
        }
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
        for addr in a.queue {
            let _ = a.fallback.send(addr);
        }
        let _ = bye_tx.send(());
        return 0;
    };
    a.opened.fetch_add(1, Ordering::Relaxed);
    // Let pricing/swap registration land before retrieval.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let conn = Arc::new(Conn {
        peer_id,
        overlay: info.remote_overlay,
        beneficiary: info.remote_eth_address,
        queue: Mutex::new(a.queue),
        alive: alive.clone(),
    });
    let acct = Arc::new(Accounting::new());

    let settle = tokio::spawn(settlement_driver(SettleArgs {
        control: control.clone(),
        conn: conn.clone(),
        acct: acct.clone(),
        secret: a.id_secret,
        chequebook: a.chequebook,
        chain_id: a.chain_id,
        ledger: Arc::new(ant_p2p::swap::OutboundLedger::open(None)),
        issuable: a.issuable,
        issued_plur: a.issued_plur.clone(),
        max_issue_plur: a.max_issue_plur,
        cheques: a.cheques.clone(),
        cheque_plur: a.cheque_plur.clone(),
        refresh_units: a.refresh_units.clone(),
    }));

    fetch_worker(FetchArgs {
        control: control.clone(),
        conn: conn.clone(),
        acct: acct.clone(),
        store: a.store.clone(),
        bytes: a.direct_bytes.clone(),
        direct_ok: a.direct_ok.clone(),
        fallback: a.fallback.clone(),
        depth: a.depth,
    })
    .await;

    conn.alive.store(false, Ordering::Relaxed); // signal settlement to sweep + exit
    let residual = settle.await.unwrap_or(0);
    let _ = bye_tx.send(());
    residual
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
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(300)))
        .build())
}

/// A selected storer to connect to.
struct Selected {
    underlay: Multiaddr,
    overlay: [u8; 32],
}

fn select_storers(
    cache: &TopologyCache,
    needed: &[[u8; 32]],
    depth: u8,
    connections: usize,
) -> Vec<Selected> {
    // Rank candidate storers by how many needed chunks they cover,
    // tie-broken by RTT; greedily take the top `connections`.
    let mut scored: Vec<(usize, u32, Multiaddr, [u8; 32])> = Vec::new();
    for rec in cache.records() {
        let Some(underlay) = rec.underlays.iter().find_map(|u| public_dialable(u)) else {
            continue;
        };
        let covers = needed
            .iter()
            .filter(|a| proximity(&rec.overlay, a) >= depth)
            .count();
        if covers > 0 {
            scored.push((
                covers,
                rec.rtt_ms.unwrap_or(u32::MAX),
                underlay,
                rec.overlay,
            ));
        }
    }
    // Most coverage first, then lowest RTT.
    scored.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    scored
        .into_iter()
        .take(connections)
        .map(|(_, _, underlay, overlay)| Selected { underlay, overlay })
        .collect()
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

fn mount_drain_sinks(control: &mut Control) {
    let mut mount = |proto: &'static str| {
        control
            .accept(libp2p::StreamProtocol::new(proto))
            .expect("registered once")
    };
    tokio::spawn(run_drain_sink(mount("/swarm/pricing/1.0.0/pricing"), 1024));
    tokio::spawn(run_drain_sink(mount("/swarm/hive/2.0.0/peers"), 64 * 1024));
    tokio::spawn(run_drain_sink(mount("/swarm/hive/1.1.0/peers"), 64 * 1024));
    tokio::spawn(ant_p2p::pseudosettle::run_inbound(mount(
        ant_p2p::pseudosettle::PROTOCOL_PSEUDOSETTLE,
    )));
    tokio::spawn(ant_p2p::swap::drain_inbound_unconfigured(mount(
        ant_p2p::swap::PROTOCOL_SWAP,
    )));
}

struct FetchArgs {
    control: Control,
    conn: Arc<Conn>,
    acct: Arc<Accounting>,
    store: Arc<ChunkStore>,
    bytes: Arc<AtomicU64>,
    direct_ok: Arc<AtomicU64>,
    fallback: mpsc::UnboundedSender<[u8; 32]>,
    /// Fixed per-connection pipeline depth (AIMD deferred to M5).
    depth: usize,
}

async fn fetch_worker(args: FetchArgs) {
    let FetchArgs {
        control,
        conn,
        acct,
        store,
        bytes,
        direct_ok,
        fallback,
        depth,
    } = args;
    let _ = &control;
    let reserve_cap = ASSUMED_THRESHOLD / 2;
    let inflight = Arc::new(tokio::sync::Semaphore::new(depth.max(1)));
    let mut tasks = tokio::task::JoinSet::new();
    loop {
        let addr = {
            let mut q = conn.queue.lock().await;
            q.pop()
        };
        let Some(addr) = addr else { break };
        if store.contains(&addr) {
            continue;
        }
        if !conn.alive.load(Ordering::Relaxed) {
            let _ = fallback.send(addr);
            continue;
        }
        let permit = inflight.clone().acquire_owned().await.expect("sem");
        let mut task_control = control.clone();
        let task_acct = acct.clone();
        let task_store = store.clone();
        let task_bytes = bytes.clone();
        let task_ok = direct_ok.clone();
        let task_fb = fallback.clone();
        let peer_id = conn.peer_id;
        let overlay = conn.overlay;
        let alive = conn.alive.clone();
        tasks.spawn(async move {
            let _permit = permit;
            let price = Accounting::peer_price(&overlay, &addr);
            // Reserve under half-threshold cap (bee credit-lag safe).
            let mut waited = 0u32;
            let guard = loop {
                if !alive.load(Ordering::Relaxed) {
                    break None;
                }
                let (bal, res) = task_acct.debug_snapshot(&peer_id).unwrap_or((0, 0));
                if bal.saturating_add(res).saturating_add(price) <= reserve_cap {
                    if let Some(g) = task_acct.try_reserve(peer_id, price) {
                        break Some(g);
                    }
                }
                if waited > 80 {
                    break None; // ~12s stuck → give to fallback
                }
                waited += 1;
                tokio::time::sleep(Duration::from_millis(150)).await;
            };
            let Some(guard) = guard else {
                let _ = task_fb.send(addr);
                return;
            };
            if let Ok(chunk) = retrieve_chunk(&mut task_control, peer_id, addr).await {
                guard.apply();
                let _ = task_store.put(addr, &chunk.data);
                task_bytes.fetch_add(chunk.data.len() as u64, Ordering::Relaxed);
                task_ok.fetch_add(1, Ordering::Relaxed);
            } else {
                drop(guard);
                let _ = task_fb.send(addr);
            }
        });
    }
    while tasks.join_next().await.is_some() {}
}

struct SettleArgs {
    control: Control,
    conn: Arc<Conn>,
    acct: Arc<Accounting>,
    secret: [u8; 32],
    chequebook: [u8; 20],
    chain_id: u64,
    ledger: Arc<ant_p2p::swap::OutboundLedger>,
    issuable: U256,
    issued_plur: Arc<AtomicU64>,
    max_issue_plur: u64,
    cheques: Arc<AtomicU64>,
    cheque_plur: Arc<AtomicU64>,
    refresh_units: Arc<AtomicU64>,
}

/// Per-connection settlement: pseudosettle refresh on cadence, cheque
/// when debt crosses trigger, final sweep to zero. Returns residual
/// unsettled units. Mirrors M2's proven loop with a shared spend cap.
async fn settlement_driver(mut a: SettleArgs) -> u64 {
    let trigger = ASSUMED_THRESHOLD / 4; // half of the reserve cap
    let pending = Arc::new(AtomicU64::new(0));
    let mut last_refresh: Option<Instant> = None;
    let mut finishing = false;
    loop {
        if finishing {
            tokio::time::sleep(Duration::from_millis(100)).await;
        } else if !a.conn.alive.load(Ordering::Relaxed) {
            finishing = true;
        } else {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let debt = a.acct.debug_snapshot(&a.conn.peer_id).map_or(0, |(b, _)| b);

        let refresh_due = last_refresh.is_none_or(|t| t.elapsed() >= REFRESH_MIN_INTERVAL);
        if debt > 0 && refresh_due {
            if let Ok(ok) =
                ant_p2p::pseudosettle::refresh_peer(&mut a.control, a.conn.peer_id).await
            {
                if ok.accepted > 0 {
                    a.acct.credit(a.conn.peer_id, ok.accepted);
                    a.refresh_units.fetch_add(ok.accepted, Ordering::Relaxed);
                }
            }
            last_refresh = Some(Instant::now());
        }

        let debt = a.acct.debug_snapshot(&a.conn.peer_id).map_or(0, |(b, _)| b);
        let effective = debt.saturating_sub(pending.load(Ordering::Relaxed));
        let should = if finishing {
            effective > 0
        } else {
            effective >= trigger
        };
        if should {
            match emit_settlement_cheque(ChequeEmit {
                control: &mut a.control,
                peer_id: a.conn.peer_id,
                secret: &a.secret,
                chequebook: a.chequebook,
                beneficiary: a.conn.beneficiary,
                chain_id: a.chain_id,
                debt_units: effective,
                ledger: &a.ledger,
                issuable: a.issuable,
                issued_plur: &a.issued_plur,
                max_issue_plur: a.max_issue_plur,
            })
            .await
            {
                Ok(outcome) => {
                    a.cheques.fetch_add(1, Ordering::Relaxed);
                    a.cheque_plur.fetch_add(
                        u64::try_from(outcome.plur).unwrap_or(u64::MAX),
                        Ordering::Relaxed,
                    );
                    if finishing {
                        a.acct.credit(a.conn.peer_id, effective);
                    } else {
                        pending.fetch_add(effective, Ordering::Relaxed);
                        let acct = a.acct.clone();
                        let pending = pending.clone();
                        let peer = a.conn.peer_id;
                        tokio::spawn(async move {
                            tokio::time::sleep(CHEQUE_CREDIT_DELAY).await;
                            acct.credit(peer, effective);
                            pending.fetch_sub(effective, Ordering::Relaxed);
                        });
                    }
                }
                Err(err) => {
                    if finishing {
                        tracing::warn!(peer=%a.conn.peer_id, "final cheque failed: {err}");
                        return effective;
                    }
                    tracing::debug!(peer=%a.conn.peer_id, "cheque failed: {err}");
                }
            }
        }
        if finishing {
            let bal = a.acct.debug_snapshot(&a.conn.peer_id).map_or(0, |(b, _)| b);
            let residual = bal.saturating_sub(pending.load(Ordering::Relaxed));
            if residual == 0 {
                return 0;
            }
            // one more sweep pass
        }
    }
}
