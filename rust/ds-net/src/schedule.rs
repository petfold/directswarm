//! M4 — the multi-connection settled scheduler.
//!
//! One libp2p swarm holds N direct storer connections at once. Each
//! chunk is routed to a connected storer whose overlay covers its
//! neighborhood (lowest observed RTT first — the latency-aware
//! selection of DESIGN.md), pipelined with per-connection AIMD depth,
//! every connection fully SWAP-settled through a shared spend cap.
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
use std::collections::HashMap;
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
const DIAL_INTERVAL: Duration = Duration::from_millis(500);
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
}

#[derive(Debug, Default)]
pub struct ScheduleReport {
    pub connections_opened: usize,
    pub chunks_total: usize,
    pub chunks_from_direct: u64,
    pub chunks_from_fallback: u64,
    pub chunks_failed: u64,
    pub bytes: u64,
    pub wall: Duration,
    pub cheques_issued: u64,
    pub cheque_plur: u128,
    pub refresh_units: u64,
    pub residual_debt_units: u64,
    pub errors: Vec<String>,
}

impl ScheduleReport {
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn aggregate_mbps(&self) -> f64 {
        let s = self.wall.as_secs_f64();
        if s > 0.0 {
            self.bytes as f64 / 1e6 / s
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

    // Cached invariant read (once), shared spend counter, shared ledger.
    let issuable = read_chequebook_issuable_raw(&opts.rpc_url, opts.chequebook).await?;
    let issued_plur = Arc::new(AtomicU64::new(0));
    let ledger = Arc::new(ant_p2p::swap::OutboundLedger::open(Some(
        opts.ledger_path.clone(),
    )));
    tracing::info!(%issuable, "chequebook cached invariant read once (shared across connections)");

    // Swarm + shared accounting + sinks.
    let behaviour = Behaviour {
        stream: StreamBehaviour::default(),
        identify: identify::Behaviour::new(
            identify::Config::new("bee/2.8.0".into(), id.keypair.public())
                .with_agent_version("directswarm/0.1.0".into()),
        ),
        ping: ping::Behaviour::new(ping::Config::new()),
    };
    let mut swarm = SwarmBuilder::with_existing_identity(id.keypair.clone())
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
        .build();
    let mut control = swarm.behaviour().stream.new_control();
    mount_drain_sinks(&mut control);

    let acct = Arc::new(Accounting::new());

    // Select storers that cover the needed chunks, RTT-first.
    let selected = select_storers(cache, &needed, opts.depth, opts.connections);
    if selected.is_empty() {
        return Err(anyhow!("no storer in the cache covers any needed chunk"));
    }

    // Drive the swarm; track liveness per peer.
    let live: Arc<Mutex<HashMap<PeerId, Arc<std::sync::atomic::AtomicBool>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let (dial_tx, mut dial_rx) = mpsc::channel::<Multiaddr>(64);
    let (conn_tx, mut conn_rx) = mpsc::channel::<PeerId>(64);
    {
        let live = live.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    evt = futures::StreamExt::next(&mut swarm) => match evt {
                        None => break,
                        Some(SwarmEvent::ConnectionEstablished { peer_id, .. }) => {
                            let _ = conn_tx.send(peer_id).await;
                        }
                        Some(SwarmEvent::ConnectionClosed { peer_id, .. }) => {
                            if let Some(flag) = live.lock().await.get(&peer_id) {
                                flag.store(false, Ordering::Relaxed);
                            }
                        }
                        Some(_) => {}
                    },
                    addr = dial_rx.recv() => match addr {
                        None => break,
                        Some(addr) => { let _ = swarm.dial(addr); }
                    },
                }
            }
        });
    }

    // Dial + handshake each selected storer (rate-limited).
    let mut conns: Vec<Arc<Conn>> = Vec::new();
    for storer in &selected {
        let Some(peer_id) = crate::direct::extract_peer_id(&storer.underlay) else {
            continue;
        };
        if dial_tx.send(storer.underlay.clone()).await.is_err() {
            break;
        }
        // Wait for connection establishment (best effort).
        let _ = tokio::time::timeout(DIAL_TIMEOUT, wait_for(&mut conn_rx, peer_id)).await;
        match tokio::time::timeout(
            DIAL_TIMEOUT,
            handshake_with_fallback_raw(
                &mut control,
                id,
                peer_id,
                &storer.underlay,
                opts.network_id,
            ),
        )
        .await
        {
            Ok(Ok(info)) => {
                let flag = Arc::new(std::sync::atomic::AtomicBool::new(true));
                live.lock().await.insert(peer_id, flag.clone());
                conns.push(Arc::new(Conn {
                    peer_id,
                    overlay: info.remote_overlay,
                    beneficiary: info.remote_eth_address,
                    queue: Mutex::new(Vec::new()),
                    alive: flag,
                }));
            }
            Ok(Err(err)) => report.errors.push(format!("handshake {peer_id}: {err}")),
            Err(_) => report.errors.push(format!("handshake {peer_id}: timeout")),
        }
        tokio::time::sleep(DIAL_INTERVAL).await;
    }
    report.connections_opened = conns.len();
    if conns.is_empty() {
        return Err(anyhow!("no storer connection established"));
    }
    // Let pricing/swap registration settle before retrieval.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Route chunks: each to the covering connection with the shortest
    // queue (spreads load); uncovered → fallback.
    let (fb_tx, fb_rx) = mpsc::unbounded_channel::<[u8; 32]>();
    {
        let mut queues: Vec<Vec<[u8; 32]>> = vec![Vec::new(); conns.len()];
        for addr in &needed {
            let mut best: Option<usize> = None;
            for (i, c) in conns.iter().enumerate() {
                if proximity(&c.overlay, addr) >= opts.depth
                    && best.is_none_or(|b| queues[i].len() < queues[b].len())
                {
                    best = Some(i);
                }
            }
            match best {
                Some(i) => queues[i].push(*addr),
                None => {
                    let _ = fb_tx.send(*addr);
                }
            }
        }
        for (c, q) in conns.iter().zip(queues) {
            *c.queue.lock().await = q;
        }
    }

    // Per-connection settlement drivers + fetch workers.
    let bytes = Arc::new(AtomicU64::new(0));
    let direct_ok = Arc::new(AtomicU64::new(0));
    let cheques = Arc::new(AtomicU64::new(0));
    let cheque_plur = Arc::new(AtomicU64::new(0));
    let refresh_units = Arc::new(AtomicU64::new(0));

    let started = Instant::now();
    let mut settle_handles = Vec::new();
    let mut fetch_handles = Vec::new();
    for c in &conns {
        settle_handles.push(tokio::spawn(settlement_driver(SettleArgs {
            control: control.clone(),
            conn: c.clone(),
            acct: acct.clone(),
            secret: id.secret,
            chequebook: opts.chequebook,
            chain_id: opts.chain_id,
            ledger: ledger.clone(),
            issuable,
            issued_plur: issued_plur.clone(),
            max_issue_plur: opts.max_issue_plur,
            cheques: cheques.clone(),
            cheque_plur: cheque_plur.clone(),
            refresh_units: refresh_units.clone(),
        })));
        fetch_handles.push(tokio::spawn(fetch_worker(FetchArgs {
            control: control.clone(),
            conn: c.clone(),
            acct: acct.clone(),
            store: store.clone(),
            bytes: bytes.clone(),
            direct_ok: direct_ok.clone(),
            fallback: fb_tx.clone(),
            depth: opts.start_depth,
        })));
    }
    // Only worker-held clones remain; when they finish, the fallback
    // receiver closes.
    drop(fb_tx);

    // Fallback worker (bee forwarding) drains uncovered + failed chunks.
    let fallback = BeeApiFetcher::new(&opts.bee_url)?;
    let fb_store = store.clone();
    let fb_bytes = bytes.clone();
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
                    fb_bytes.fetch_add(wire.len() as u64, Ordering::Relaxed);
                    fb_count2.fetch_add(1, Ordering::Relaxed);
                }
                Err(err) => fails.push(format!("fallback {}: {err}", hex::encode(addr))),
            }
        }
        fails
    });

    // Wait for fetch workers, then signal settlement drivers to finish.
    for h in fetch_handles {
        let _ = h.await;
    }
    for c in &conns {
        c.alive.store(false, Ordering::Relaxed); // signal settle drivers to sweep+exit
    }
    let mut residual = 0u64;
    for h in settle_handles {
        if let Ok(r) = h.await {
            residual += r;
        }
    }
    // fb_tx senders inside workers are dropped now; close fallback.
    let fb_fails = fb_handle.await.unwrap_or_default();
    store.flush()?;

    report.wall = started.elapsed();
    report.bytes = bytes.load(Ordering::Relaxed);
    report.chunks_from_direct = direct_ok.load(Ordering::Relaxed);
    report.chunks_from_fallback = fb_count.load(Ordering::Relaxed);
    report.cheques_issued = cheques.load(Ordering::Relaxed);
    report.cheque_plur = u128::from(cheque_plur.load(Ordering::Relaxed));
    report.refresh_units = refresh_units.load(Ordering::Relaxed);
    report.residual_debt_units = residual;
    let stored = store.len() as u64;
    report.chunks_failed = (needed.len() as u64)
        .saturating_sub(report.chunks_from_direct + report.chunks_from_fallback);
    report.errors.extend(fb_fails);
    let _ = stored;
    Ok(report)
}

/// A selected storer to connect to.
struct Selected {
    underlay: Multiaddr,
}

fn select_storers(
    cache: &TopologyCache,
    needed: &[[u8; 32]],
    depth: u8,
    connections: usize,
) -> Vec<Selected> {
    // Rank candidate storers by how many needed chunks they cover,
    // tie-broken by RTT; greedily take the top `connections`.
    let mut scored: Vec<(usize, u32, Multiaddr)> = Vec::new();
    for rec in cache.records() {
        let Some(underlay) = rec.underlays.iter().find_map(|u| public_dialable(u)) else {
            continue;
        };
        let covers = needed
            .iter()
            .filter(|a| proximity(&rec.overlay, a) >= depth)
            .count();
        if covers > 0 {
            scored.push((covers, rec.rtt_ms.unwrap_or(u32::MAX), underlay));
        }
    }
    // Most coverage first, then lowest RTT.
    scored.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    scored
        .into_iter()
        .take(connections)
        .map(|(_, _, underlay)| Selected { underlay })
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

async fn wait_for(rx: &mut mpsc::Receiver<PeerId>, want: PeerId) {
    while let Some(p) = rx.recv().await {
        if p == want {
            return;
        }
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
