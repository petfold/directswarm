//! M2 — one direct, settled storer stream.
//!
//! Dial a mainnet storer over libp2p, run the BZZ handshake as an
//! honest light peer, mount the sink protocols bee requires
//! (Phase-0 finding: a peer that can't accept pricing/accounting
//! streams is ejected within seconds), then retrieve chunks over
//! `/swarm/retrieval/1.4.0` with bee-mirrored accounting, pseudosettle
//! refreshes, and — the part stock clients skip — **SWAP cheques for
//! the residual debt**, issued under the cached-invariant balance
//! check (the Phase-0 16× fix, native here): `balance + totalPaidOut`
//! is read on-chain once and cached; it is invariant under cash-outs
//! and only the issuer's own deposits/withdrawals move it.

use ant_retrieval::accounting::{Accounting, HotHint};
use ant_retrieval::retrieve_chunk;
use anyhow::{anyhow, bail, Context, Result};
use futures::{AsyncReadExt, AsyncWriteExt, StreamExt};
use libp2p::core::ConnectedPoint;
use libp2p::swarm::SwarmEvent;
use libp2p::{dns, identify, noise, ping, tcp, yamux};
use libp2p::{Multiaddr, PeerId, StreamProtocol, SwarmBuilder};
use libp2p_stream::{Behaviour as StreamBehaviour, Control, IncomingStreams};
use primitive_types::U256;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, watch, Semaphore};

use crate::identity::Identity;

/// Minimum spacing between pseudosettle refresh attempts (bee rejects
/// faster refreshes; ant's driver uses 1100 ms).
const REFRESH_MIN_INTERVAL: Duration = Duration::from_millis(1100);
/// Settlement loop tick.
const SETTLE_TICK: Duration = Duration::from_millis(250);
/// Wait after the BZZ handshake so bee's swap/pricing registration and
/// threshold announcement land before we open retrieval streams.
const POST_HANDSHAKE_SETTLE: Duration = Duration::from_secs(2);
/// How long an emitted cheque stays "in flight" before we credit our
/// own mirror: bee validates it with ~3 on-chain RPC calls (issuer,
/// balance, paidOut against a public endpoint) before crediting its
/// ledger, and until then bee's view of our debt is higher than ours.
const CHEQUE_CREDIT_DELAY: Duration = Duration::from_millis(2500);
const DIAL_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Clone)]
pub struct ProbeTarget {
    /// Full multiaddr including `/p2p/<peer-id>`.
    pub underlay: Multiaddr,
    /// The storer's overlay (for pricing and PO bookkeeping).
    pub overlay: [u8; 32],
}

#[derive(Debug, Clone)]
pub struct ProbeOptions {
    pub network_id: u64,
    pub chain_id: u64,
    /// Our chequebook contract.
    pub chequebook: [u8; 20],
    /// Gnosis RPC endpoint for the one-time cached-invariant read.
    pub rpc_url: String,
    /// Outbound cheque ledger (persisted cumulative per beneficiary).
    pub ledger_path: PathBuf,
    /// Concurrent outstanding chunk requests.
    pub pipeline_depth: usize,
    /// Hard cap on PLUR issued as cheques this run (spend guard).
    pub max_issue_plur: u64,
}

#[derive(Debug, Default, Clone)]
pub struct SettlementSummary {
    pub cheques_issued: u64,
    /// Accounting units settled by cheques.
    pub cheque_units: u64,
    /// Real PLUR moved by cheques (units × announced rate + deduction).
    pub cheque_plur: u128,
    /// Peer-announced exchange rate (PLUR per accounting unit) at the
    /// first cheque.
    pub exchange_rate: Option<u128>,
    pub refreshes_accepted: u64,
    /// Accounting units settled by pseudosettle (time-based free tier).
    pub refresh_units: u64,
    /// Unsettled accounting units at disconnect (should be 0).
    pub residual_debt_units: u64,
    /// Peer-announced payment threshold (parsed from the pricing
    /// stream), if it arrived.
    pub announced_threshold: Option<U256>,
}

#[derive(Debug)]
pub struct ProbeReport {
    pub peer_id: PeerId,
    pub remote_overlay: [u8; 32],
    pub remote_eth: [u8; 20],
    pub remote_full_node: bool,
    pub handshake_ms: u128,
    pub chunks_ok: u64,
    pub chunks_err: u64,
    pub bytes: u64,
    pub wall: Duration,
    /// Per-chunk wall latencies (ms), successful fetches only.
    pub latencies_ms: Vec<u64>,
    pub settlement: SettlementSummary,
    pub errors: Vec<String>,
}

/// Dial one storer and retrieve `chunks` from it, fully settled.
///
/// # Errors
/// Fails on dial/handshake/chain-RPC failure; individual chunk
/// failures are recorded in the report instead.
///
/// # Panics
/// Only on poisoned internal state (infallible behaviour construction,
/// a closed process-local semaphore).
#[allow(clippy::too_many_lines)]
pub async fn probe_storer(
    id: &Identity,
    target: &ProbeTarget,
    chunks: Vec<[u8; 32]>,
    opts: &ProbeOptions,
) -> Result<ProbeReport> {
    let peer_id = extract_peer_id(&target.underlay)
        .ok_or_else(|| anyhow!("underlay must end in /p2p/<peer-id>"))?;

    // --- cached invariant: one on-chain read, then no per-cheque RPC ---
    let issuable = read_chequebook_issuable(opts).await?;
    let ledger = ant_p2p::swap::OutboundLedger::open(Some(opts.ledger_path.clone()));
    tracing::info!(
        issuable_plur = %issuable,
        "chequebook cached invariant (balance + totalPaidOut) read once"
    );

    // --- swarm ---
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

    // Mount every sink bee expects BEFORE the handshake can complete —
    // a peer that refuses these streams is disconnected (Phase 0).
    let (threshold_tx, threshold_rx) = watch::channel::<Option<U256>>(None);
    mount_sinks(&mut control, threshold_tx)?;

    // --- dial + identify ---
    swarm.dial(target.underlay.clone())?;
    let dialed_addr = wait_connected(&mut swarm, peer_id).await?;

    // Hand the swarm to a drive task; keep a handle to disconnect
    // politely at the end, and a liveness flag so fetch/settlement
    // loops abort instead of spinning when the peer hangs up on us.
    let alive = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let (bye_tx, mut bye_rx) = oneshot::channel::<oneshot::Sender<()>>();
    {
        let alive = alive.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    evt = swarm.next() => {
                        match evt {
                            None => break,
                            Some(SwarmEvent::ConnectionClosed { peer_id: closed, .. })
                                if closed == peer_id =>
                            {
                                alive.store(false, Ordering::Relaxed);
                            }
                            Some(_) => {}
                        }
                    }
                    cmd = &mut bye_rx => {
                        let _ = swarm.disconnect_peer_id(peer_id);
                        // Give the FIN a moment to flush.
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        alive.store(false, Ordering::Relaxed);
                        if let Ok(done) = cmd { let _ = done.send(()); }
                        break;
                    }
                }
            }
        });
    }

    // --- BZZ handshake, honest light role, V15 then V14 ---
    let hs_started = Instant::now();
    let info = handshake_with_fallback(&mut control, id, peer_id, &dialed_addr, opts).await?;
    let handshake_ms = hs_started.elapsed().as_millis();
    tracing::info!(
        remote_overlay = %hex::encode(info.remote_overlay),
        remote_eth = %hex::encode(info.remote_eth_address),
        full_node = info.remote_full_node,
        "handshake complete"
    );
    if info.remote_overlay != target.overlay {
        bail!(
            "peer overlay mismatch: expected {} got {}",
            hex::encode(target.overlay),
            hex::encode(info.remote_overlay)
        );
    }
    tokio::time::sleep(POST_HANDSHAKE_SETTLE).await;

    // Reserve cap: HALF the announced payment threshold — bee's own
    // early-payment posture. Bee blocks a peer whose ledger reaches
    // disconnectLimit = 1.25 × threshold, and its credit for a cheque
    // lands only after several on-chain validation calls of unknown
    // latency. With outstanding debt capped at T/2, even a full refill
    // burst on top of a still-unvalidated cheque peaks bee's ledger at
    // ~T < 1.25T (runs 3–5 died at caps of 1.25T and T), and if a
    // cheque is ever rejected outright, pseudosettle refreshes alone
    // (450k units/s) outpace our refill rate so debt still shrinks.
    let announced = threshold_rx
        .borrow()
        .as_ref()
        .map_or(1_350_000, |t| u64::try_from(*t).unwrap_or(1_350_000));
    let reserve_cap = announced / 2;
    let cheque_trigger = reserve_cap / 2;

    // --- accounting + settlement task ---
    let (hot_tx, hot_rx) = mpsc::channel::<HotHint>(16);
    let acct = Arc::new(Accounting::new().with_hot_hint(hot_tx));
    let fetch_done = Arc::new(tokio::sync::Notify::new());
    let settle = tokio::spawn(settlement_loop(SettleCtx {
        control: control.clone(),
        peer_id,
        acct: acct.clone(),
        secret: id.secret,
        chequebook: opts.chequebook,
        beneficiary: info.remote_eth_address,
        chain_id: opts.chain_id,
        ledger,
        issuable,
        max_issue_plur: opts.max_issue_plur,
        issued_plur: 0,
        pending_cheque_units: Arc::new(AtomicU64::new(0)),
        cheque_trigger,
        alive: alive.clone(),
        hot_rx,
        done: fetch_done.clone(),
    }));

    // --- pipelined retrieval ---
    let reserve_lock = Arc::new(tokio::sync::Mutex::new(()));
    let started = Instant::now();
    let sem = Arc::new(Semaphore::new(opts.pipeline_depth));
    let bytes = Arc::new(AtomicU64::new(0));
    let mut tasks = tokio::task::JoinSet::new();
    for addr in chunks {
        let permit = sem.clone().acquire_owned().await.expect("semaphore open");
        let mut task_control = control.clone();
        let task_acct = acct.clone();
        let task_bytes = bytes.clone();
        let task_lock = reserve_lock.clone();
        let task_alive = alive.clone();
        let storer_overlay = target.overlay;
        tasks.spawn(async move {
            let _permit = permit;
            let price = Accounting::peer_price(&storer_overlay, &addr);
            // Reserve under the cap (checked and reserved under one
            // lock so concurrent tasks can't stack past it); on
            // overdraft wait for settlement to knock debt down (bee's
            // 600 ms skip). Abort if the connection died.
            let guard = loop {
                if !task_alive.load(Ordering::Relaxed) {
                    return Err(format!(
                        "chunk {}: connection closed before reserve",
                        hex::encode(addr)
                    ));
                }
                {
                    let _serialized = task_lock.lock().await;
                    let (balance, reserved) = task_acct.debug_snapshot(&peer_id).unwrap_or((0, 0));
                    if balance.saturating_add(reserved).saturating_add(price) <= reserve_cap {
                        if let Some(guard) = task_acct.try_reserve(peer_id, price) {
                            break guard;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
            };
            let fetch_started = Instant::now();
            match retrieve_chunk(&mut task_control, peer_id, addr).await {
                Ok(chunk) => {
                    guard.apply();
                    task_bytes.fetch_add(chunk.data.len() as u64, Ordering::Relaxed);
                    let ms = u64::try_from(fetch_started.elapsed().as_millis()).unwrap_or(u64::MAX);
                    Ok(ms)
                }
                Err(err) => Err(format!("chunk {}: {err}", hex::encode(addr))),
            }
        });
    }
    let mut latencies_ms = Vec::new();
    let mut errors = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(ms)) => latencies_ms.push(ms),
            Ok(Err(err)) => errors.push(err),
            Err(join_err) => errors.push(format!("task: {join_err}")),
        }
    }
    let wall = started.elapsed();

    // --- final sweep: leave zero unsettled debt, then hang up ---
    fetch_done.notify_one();
    let mut settlement = settle.await.context("settlement task")??;
    settlement.announced_threshold = *threshold_rx.borrow();

    let (done_tx, done_rx) = oneshot::channel();
    let _ = bye_tx.send(done_tx);
    let _ = tokio::time::timeout(Duration::from_secs(3), done_rx).await;

    let chunks_ok = latencies_ms.len() as u64;
    let chunks_err = errors.len() as u64;
    Ok(ProbeReport {
        peer_id,
        remote_overlay: info.remote_overlay,
        remote_eth: info.remote_eth_address,
        remote_full_node: info.remote_full_node,
        handshake_ms,
        chunks_ok,
        chunks_err,
        bytes: bytes.load(Ordering::Relaxed),
        wall,
        latencies_ms,
        settlement,
        errors,
    })
}

#[derive(libp2p::swarm::NetworkBehaviour)]
pub(crate) struct Behaviour {
    pub(crate) stream: StreamBehaviour,
    pub(crate) identify: identify::Behaviour,
    pub(crate) ping: ping::Behaviour,
}

pub(crate) fn extract_peer_id(addr: &Multiaddr) -> Option<PeerId> {
    addr.iter().find_map(|p| {
        if let libp2p::multiaddr::Protocol::P2p(peer) = p {
            Some(peer)
        } else {
            None
        }
    })
}

async fn read_chequebook_issuable(opts: &ProbeOptions) -> Result<U256> {
    read_chequebook_issuable_raw(&opts.rpc_url, opts.chequebook).await
}

/// Cached-invariant read: `balance + totalPaidOut` of our chequebook,
/// the ceiling on cumulative payout across all beneficiaries. One
/// on-chain read; invariant under cash-outs (see module docs).
pub(crate) async fn read_chequebook_issuable_raw(
    rpc_url: &str,
    chequebook: [u8; 20],
) -> Result<U256> {
    let client = ant_chain::ChainClient::new(rpc_url.to_owned());
    let to = format!("0x{}", hex::encode(chequebook));
    let mut total = U256::zero();
    for selector in [
        ant_chain::chequebook::chequebook_balance_selector(),
        ant_chain::chequebook::chequebook_total_paid_out_selector(),
    ] {
        let data = format!("0x{}", hex::encode(selector));
        let out = client
            .eth_call(&to, &data)
            .await
            .map_err(|e| anyhow!("chequebook read: {e}"))?;
        if out.len() != 32 {
            bail!("chequebook read returned {} bytes (want 32)", out.len());
        }
        total = total
            .checked_add(U256::from_big_endian(&out))
            .ok_or_else(|| anyhow!("chequebook invariant overflow"))?;
    }
    Ok(total)
}

pub(crate) async fn wait_connected(
    swarm: &mut libp2p::Swarm<Behaviour>,
    want: PeerId,
) -> Result<Multiaddr> {
    let deadline = Instant::now() + DIAL_TIMEOUT;
    let mut connected_addr: Option<Multiaddr> = None;
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| anyhow!("dial timeout"))?;
        let evt = tokio::time::timeout(remaining, swarm.next())
            .await
            .map_err(|_| anyhow!("dial timeout"))?
            .ok_or_else(|| anyhow!("swarm ended"))?;
        match evt {
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } if peer_id == want => {
                let addr = match endpoint {
                    ConnectedPoint::Dialer { address, .. } => address,
                    ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr,
                };
                connected_addr = Some(addr);
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } if peer_id == Some(want) => {
                bail!("dial failed: {error}");
            }
            SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
                peer_id,
                ..
            })) if peer_id == want => {
                if let Some(addr) = connected_addr {
                    return Ok(addr);
                }
            }
            _ => {}
        }
    }
}

async fn handshake_with_fallback(
    control: &mut Control,
    id: &Identity,
    peer_id: PeerId,
    dialed_addr: &Multiaddr,
    opts: &ProbeOptions,
) -> Result<ant_p2p::HandshakeInfo> {
    handshake_with_fallback_raw(control, id, peer_id, dialed_addr, opts.network_id).await
}

/// BZZ handshake as an honest light node, V15 then V14 on failure.
pub(crate) async fn handshake_with_fallback_raw(
    control: &mut Control,
    id: &Identity,
    peer_id: PeerId,
    dialed_addr: &Multiaddr,
    network_id: u64,
) -> Result<ant_p2p::HandshakeInfo> {
    use ant_crypto::HandshakeWireVersion;
    for (proto, version) in [
        (ant_p2p::PROTOCOL_HANDSHAKE_V15, HandshakeWireVersion::V15),
        (ant_p2p::PROTOCOL_HANDSHAKE_V14, HandshakeWireVersion::V14),
    ] {
        let stream = match control
            .open_stream(peer_id, StreamProtocol::new(proto))
            .await
        {
            Ok(stream) => stream,
            Err(err) => {
                tracing::debug!("open {proto}: {err}; trying older version");
                continue;
            }
        };
        match ant_p2p::handshake_outbound_with_role(
            stream,
            id.keypair.public().to_peer_id(),
            peer_id,
            &id.secret,
            &id.nonce,
            network_id,
            std::slice::from_ref(dialed_addr),
            Vec::new(),
            false, // honest light node — Phase 0 proved cheques land from light strangers
            version,
        )
        .await
        {
            Ok(info) => return Ok(info),
            Err(err) => tracing::debug!("handshake {proto}: {err}; trying older version"),
        }
    }
    bail!("handshake failed on both wire versions")
}

// --- sinks -----------------------------------------------------------

// Wire ids matching ant's private `sinks` module (bee 2.7/2.8).
const PROTOCOL_PRICING: &str = "/swarm/pricing/1.0.0/pricing";
const PROTOCOL_HIVE_V2: &str = "/swarm/hive/2.0.0/peers";
const PROTOCOL_HIVE_V1: &str = "/swarm/hive/1.1.0/peers";

/// Claim every protocol bee opens toward us. Pricing is PARSED (ant
/// drains it): the `AnnouncePaymentThreshold` big-endian big.Int is the
/// number that paces every connection — Phase 0's whole story.
pub(crate) fn mount_sinks(
    control: &mut Control,
    threshold_tx: watch::Sender<Option<U256>>,
) -> Result<()> {
    let accept = |control: &mut Control, proto: &'static str| -> Result<IncomingStreams> {
        control
            .accept(StreamProtocol::new(proto))
            .map_err(|e| anyhow!("register {proto}: {e}"))
    };
    let pricing = accept(control, PROTOCOL_PRICING)?;
    let hive_v2 = accept(control, PROTOCOL_HIVE_V2)?;
    let hive_v1 = accept(control, PROTOCOL_HIVE_V1)?;
    let pseudosettle = accept(control, ant_p2p::pseudosettle::PROTOCOL_PSEUDOSETTLE)?;
    let swap = accept(control, ant_p2p::swap::PROTOCOL_SWAP)?;

    tokio::spawn(run_pricing_sink(pricing, threshold_tx));
    tokio::spawn(run_drain_sink(hive_v2, 64 * 1024));
    tokio::spawn(run_drain_sink(hive_v1, 64 * 1024));
    tokio::spawn(ant_p2p::pseudosettle::run_inbound(pseudosettle));
    tokio::spawn(ant_p2p::swap::drain_inbound_unconfigured(swap));
    Ok(())
}

/// Bee-headers framing (mirrors ant's private `sinks::drain_stream`):
/// read the dialer's Headers, reply with empty Headers (varint 0), read
/// the body, half-close, drain to EOF.
pub(crate) async fn read_sink_body(
    stream: &mut libp2p::Stream,
    max: usize,
) -> std::io::Result<Vec<u8>> {
    let _their_headers = read_delimited(stream, 8 * 1024).await?;
    stream.write_all(&[0u8]).await?;
    stream.flush().await?;
    let body = read_delimited(stream, max).await?;
    let _ = stream.close().await;
    let mut tail = [0u8; 64];
    loop {
        match stream.read(&mut tail).await {
            Ok(0) => return Ok(body),
            Ok(_) => {}
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
                ) =>
            {
                return Ok(body)
            }
            Err(e) => return Err(e),
        }
    }
}

pub(crate) async fn run_pricing_sink(
    mut incoming: IncomingStreams,
    tx: watch::Sender<Option<U256>>,
) {
    while let Some((peer_id, mut stream)) = incoming.next().await {
        let tx = tx.clone();
        tokio::spawn(async move {
            match tokio::time::timeout(Duration::from_secs(10), read_sink_body(&mut stream, 1024))
                .await
            {
                Ok(Ok(body)) => {
                    if let Some(threshold) = parse_announce_threshold(&body) {
                        tracing::info!(%peer_id, %threshold, "payment threshold announced");
                        let _ = tx.send(Some(threshold));
                    } else {
                        tracing::debug!(%peer_id, "unparseable pricing announcement");
                    }
                }
                Ok(Err(err)) => tracing::debug!(%peer_id, "pricing sink: {err}"),
                Err(_) => tracing::debug!(%peer_id, "pricing sink timeout"),
            }
        });
    }
}

pub(crate) async fn run_drain_sink(mut incoming: IncomingStreams, max: usize) {
    while let Some((peer_id, mut stream)) = incoming.next().await {
        tokio::spawn(async move {
            match tokio::time::timeout(Duration::from_secs(30), read_sink_body(&mut stream, max))
                .await
            {
                Ok(Ok(_)) | Err(_) => {}
                Ok(Err(err)) => tracing::trace!(%peer_id, "drain sink: {err}"),
            }
        });
    }
}

/// Parse bee's `AnnouncePaymentThreshold { bytes PaymentThreshold = 1 }`
/// protobuf: field 1, wire type 2 (length-delimited), payload is a
/// big-endian big.Int.
pub(crate) fn parse_announce_threshold(body: &[u8]) -> Option<U256> {
    let mut rest = body;
    while !rest.is_empty() {
        let (tag, after_tag) = decode_varint(rest)?;
        rest = after_tag;
        let field = tag >> 3;
        let wire = tag & 0x7;
        match wire {
            2 => {
                let (len, after_len) = decode_varint(rest)?;
                let len = usize::try_from(len).ok()?;
                let value = after_len.get(..len)?;
                rest = after_len.get(len..)?;
                if field == 1 {
                    if value.len() > 32 {
                        return None;
                    }
                    return Some(U256::from_big_endian(value));
                }
            }
            0 => {
                let (_, after) = decode_varint(rest)?;
                rest = after;
            }
            _ => return None,
        }
    }
    None
}

pub(crate) fn decode_varint(buf: &[u8]) -> Option<(u64, &[u8])> {
    let mut value: u64 = 0;
    for (i, byte) in buf.iter().enumerate().take(10) {
        value |= u64::from(byte & 0x7f) << (7 * u32::try_from(i).ok()?);
        if byte & 0x80 == 0 {
            return Some((value, &buf[i + 1..]));
        }
    }
    None
}

// --- settlement -------------------------------------------------------

struct SettleCtx {
    control: Control,
    peer_id: PeerId,
    acct: Arc<Accounting>,
    secret: [u8; 32],
    chequebook: [u8; 20],
    beneficiary: [u8; 20],
    chain_id: u64,
    ledger: ant_p2p::swap::OutboundLedger,
    /// Cached `balance + totalPaidOut` — the invariant ceiling on the
    /// beneficiary's cumulative payout. Single-beneficiary guard is
    /// exact for M2; the multi-peer scheduler sums the ledger.
    issuable: U256,
    max_issue_plur: u64,
    /// PLUR issued as cheques so far this run (spend-guard state).
    issued_plur: u128,
    /// Units covered by emitted-but-not-yet-credited cheques.
    pending_cheque_units: Arc<AtomicU64>,
    /// Debt (units) at which to emit a cheque = `reserve_cap` / 2.
    cheque_trigger: u64,
    /// Connection liveness; settlement stops when the peer hangs up.
    alive: Arc<std::sync::atomic::AtomicBool>,
    hot_rx: mpsc::Receiver<HotHint>,
    done: Arc<tokio::sync::Notify>,
}

/// Keep the connection settled: pseudosettle refresh on cadence, SWAP
/// cheque whenever residual debt crosses the trigger, and a final
/// sweep that cheques the remainder down to zero before hanging up.
#[allow(clippy::too_many_lines)]
async fn settlement_loop(mut ctx: SettleCtx) -> Result<SettlementSummary> {
    let mut summary = SettlementSummary::default();
    let mut last_refresh: Option<Instant> = None;
    let mut finishing = false;
    loop {
        if finishing {
            tokio::time::sleep(Duration::from_millis(100)).await;
        } else {
            tokio::select! {
                () = tokio::time::sleep(SETTLE_TICK) => {}
                _ = ctx.hot_rx.recv() => {}
                () = ctx.done.notified() => { finishing = true; }
            }
        }
        // If the peer disconnected, further settlement is impossible;
        // record whatever debt is outstanding and stop.
        if !ctx.alive.load(Ordering::Relaxed) {
            let balance = ctx
                .acct
                .debug_snapshot(&ctx.peer_id)
                .map_or(0, |(balance, _reserved)| balance);
            summary.residual_debt_units =
                balance.saturating_sub(ctx.pending_cheque_units.load(Ordering::Relaxed));
            break;
        }
        let debt = ctx
            .acct
            .debug_snapshot(&ctx.peer_id)
            .map_or(0, |(balance, _reserved)| balance);

        // Refresh first — the free-tier allowance is part of the
        // protocol and bee expects the cadence.
        let refresh_due = last_refresh.is_none_or(|at| at.elapsed() >= REFRESH_MIN_INTERVAL);
        if debt > 0 && refresh_due {
            match ant_p2p::pseudosettle::refresh_peer(&mut ctx.control, ctx.peer_id).await {
                Ok(ok) if ok.accepted > 0 => {
                    ctx.acct.credit(ctx.peer_id, ok.accepted);
                    summary.refreshes_accepted += 1;
                    summary.refresh_units += ok.accepted;
                }
                Ok(_) => {}
                Err(err) => tracing::debug!("refresh: {err}"),
            }
            last_refresh = Some(Instant::now());
        }

        // Effective debt excludes cheques already emitted but not yet
        // credited to the mirror: bee validates a cheque with ~3
        // on-chain RPC calls before applying the credit, so for that
        // window bee's ledger is HIGHER than ours. Crediting instantly
        // let the pipeline consume the freed headroom and push bee past
        // its disconnect limit — measured in probe run 2 (disconnect
        // ~200 ms after emit). We therefore credit after
        // CHEQUE_CREDIT_DELAY and never double-count in-flight cheques.
        let debt = ctx
            .acct
            .debug_snapshot(&ctx.peer_id)
            .map_or(0, |(balance, _reserved)| balance);
        let pending = ctx.pending_cheque_units.load(Ordering::Relaxed);
        let effective_debt = debt.saturating_sub(pending);
        let should_cheque = if finishing {
            effective_debt > 0
        } else {
            effective_debt >= ctx.cheque_trigger
        };
        if should_cheque {
            let debt = effective_debt;
            match emit_cheque_at_rate(&mut ctx, debt).await {
                Ok(outcome) => {
                    if finishing {
                        // Nothing reserves any more; apply directly so
                        // the residual check below sees the truth.
                        ctx.acct.credit(ctx.peer_id, debt);
                    } else {
                        ctx.pending_cheque_units.fetch_add(debt, Ordering::Relaxed);
                        let acct = ctx.acct.clone();
                        let pending = ctx.pending_cheque_units.clone();
                        let peer = ctx.peer_id;
                        tokio::spawn(async move {
                            tokio::time::sleep(CHEQUE_CREDIT_DELAY).await;
                            acct.credit(peer, debt);
                            pending.fetch_sub(debt, Ordering::Relaxed);
                        });
                    }
                    summary.cheques_issued += 1;
                    summary.cheque_units += debt;
                    summary.cheque_plur += outcome.plur;
                    if summary.exchange_rate.is_none() {
                        summary.exchange_rate = Some(outcome.rate);
                    }
                    tracing::info!(
                        units = debt,
                        plur = %outcome.plur_u256,
                        cumulative = %outcome.cumulative,
                        "cheque emitted at announced rate"
                    );
                }
                Err(err) => {
                    if finishing {
                        summary.residual_debt_units = debt;
                        tracing::warn!("final cheque failed, residual debt {debt} units: {err}");
                        break;
                    }
                    tracing::warn!("cheque emit failed: {err}");
                }
            }
        }

        if finishing {
            let balance = ctx
                .acct
                .debug_snapshot(&ctx.peer_id)
                .map_or(0, |(balance, _reserved)| balance);
            let residual = balance.saturating_sub(ctx.pending_cheque_units.load(Ordering::Relaxed));
            if residual == 0 || summary.residual_debt_units > 0 {
                summary.residual_debt_units = residual;
                break;
            }
            // Loop once more to sweep what's left.
        }
    }
    Ok(summary)
}

pub(crate) struct RateEmitOutcome {
    /// PLUR moved by this cheque (units × rate + deduction).
    pub plur: u128,
    pub plur_u256: U256,
    pub rate: u128,
    pub cumulative: U256,
}

/// Inputs to [`emit_settlement_cheque`] — the money-critical wire path,
/// shared by the M2 probe and the M4 scheduler.
pub(crate) struct ChequeEmit<'a> {
    pub control: &'a mut Control,
    pub peer_id: PeerId,
    pub secret: &'a [u8; 32],
    pub chequebook: [u8; 20],
    pub beneficiary: [u8; 20],
    pub chain_id: u64,
    pub debt_units: u64,
    pub ledger: &'a ant_p2p::swap::OutboundLedger,
    /// Cached `balance + totalPaidOut` ceiling on cumulative payout.
    pub issuable: U256,
    /// PLUR issued so far (shared across peers for a global spend cap).
    pub issued_plur: &'a std::sync::atomic::AtomicU64,
    pub max_issue_plur: u64,
}

/// Bee's swap wire done in full (ant's `emit_cheque` skips the header
/// exchange): the receiver's headler announces the current oracle
/// exchange rate (PLUR per accounting unit) and a one-time deduction;
/// the cheque must move `units × rate + deduction` PLUR or the
/// receiver credits the wrong amount — the first probe run proved bee
/// then sees unsettled debt and disconnects. Guards: the cached
/// invariant (cumulative ≤ issuable) and a global PLUR spend cap
/// reserved before emit and released on failure.
pub(crate) async fn emit_settlement_cheque(e: ChequeEmit<'_>) -> Result<RateEmitOutcome> {
    use std::sync::atomic::Ordering;
    let mut stream = e
        .control
        .open_stream(e.peer_id, StreamProtocol::new(ant_p2p::swap::PROTOCOL_SWAP))
        .await
        .map_err(|err| anyhow!("open swap stream: {err}"))?;

    // Dialer headers (empty), then the headler's response headers.
    stream.write_all(&[0u8]).await?;
    stream.flush().await?;
    let response_headers = read_delimited(&mut stream, 8 * 1024).await?;
    let (rate, deduction) = parse_settlement_headers(&response_headers)
        .ok_or_else(|| anyhow!("peer sent no exchange-rate header"))?;
    if rate.is_zero() {
        bail!("peer announced a zero exchange rate");
    }
    // deduction goes to zero once the peer has recorded a cheque from
    // us — seeing it stay non-zero on a later cheque means the earlier
    // one was never accepted (how the quoted-JSON bug was caught).
    tracing::debug!(%rate, %deduction, "swap headers received");

    let plur = rate
        .checked_mul(U256::from(e.debt_units))
        .and_then(|v| v.checked_add(deduction))
        .ok_or_else(|| anyhow!("cheque amount overflow"))?;
    let plur_u128 = u128::try_from(plur).map_err(|_| anyhow!("cheque amount exceeds u128"))?;
    let plur_u64 = u64::try_from(plur_u128).unwrap_or(u64::MAX);

    // Reserve against the global spend cap before doing anything
    // irreversible; release on any failure below.
    let before = e.issued_plur.fetch_add(plur_u64, Ordering::SeqCst);
    if before.saturating_add(plur_u64) > e.max_issue_plur {
        e.issued_plur.fetch_sub(plur_u64, Ordering::SeqCst);
        bail!(
            "spend guard: cheque of {plur} PLUR would exceed max_issue_plur={}",
            e.max_issue_plur
        );
    }

    let result = async {
        let prev = e.ledger.cumulative_for(&e.beneficiary);
        let cumulative = prev
            .checked_add(plur)
            .ok_or_else(|| anyhow!("cumulative overflow"))?;
        if cumulative > e.issuable {
            bail!(
                "cached invariant: cumulative {cumulative} would exceed issuable {} — chequebook needs a deposit",
                e.issuable
            );
        }
        let signed = ant_p2p::swap::issue_cheque(
            e.secret,
            e.chequebook,
            e.beneficiary,
            cumulative,
            e.chain_id,
        )?;
        let cheque_json = encode_cheque_json_bee(&signed);
        // EmitCheque { bytes cheque = 1 }, length-delimited on the wire.
        let mut msg = Vec::with_capacity(cheque_json.len() + 8);
        msg.push(0x0a);
        encode_varint(cheque_json.len() as u64, &mut msg);
        msg.extend_from_slice(&cheque_json);
        let mut frame = Vec::with_capacity(msg.len() + 8);
        encode_varint(msg.len() as u64, &mut frame);
        frame.extend_from_slice(&msg);
        stream.write_all(&frame).await?;
        stream.flush().await?;
        let _ = stream.close().await;
        if let Err(err) = e.ledger.record_issued(&e.beneficiary, cumulative) {
            tracing::warn!("outbound ledger persist failed after emit: {err}");
        }
        Ok::<U256, anyhow::Error>(cumulative)
    }
    .await;

    let cumulative = match result {
        Ok(c) => c,
        Err(err) => {
            e.issued_plur.fetch_sub(plur_u64, Ordering::SeqCst);
            return Err(err);
        }
    };
    Ok(RateEmitOutcome {
        plur: plur_u128,
        plur_u256: plur,
        rate: u128::try_from(rate).unwrap_or(u128::MAX),
        cumulative,
    })
}

/// M2 probe wrapper: adapt `SettleCtx` to [`emit_settlement_cheque`]
/// with a per-run (single-peer) spend counter.
async fn emit_cheque_at_rate(ctx: &mut SettleCtx, debt_units: u64) -> Result<RateEmitOutcome> {
    use std::sync::atomic::Ordering;
    let issued =
        std::sync::atomic::AtomicU64::new(u64::try_from(ctx.issued_plur).unwrap_or(u64::MAX));
    let outcome = emit_settlement_cheque(ChequeEmit {
        control: &mut ctx.control,
        peer_id: ctx.peer_id,
        secret: &ctx.secret,
        chequebook: ctx.chequebook,
        beneficiary: ctx.beneficiary,
        chain_id: ctx.chain_id,
        debt_units,
        ledger: &ctx.ledger,
        issuable: ctx.issuable,
        issued_plur: &issued,
        max_issue_plur: ctx.max_issue_plur,
    })
    .await?;
    ctx.issued_plur = u128::from(issued.load(Ordering::SeqCst));
    Ok(RateEmitOutcome {
        plur: outcome.plur,
        plur_u256: outcome.plur_u256,
        rate: outcome.rate,
        cumulative: outcome.cumulative,
    })
}

/// Encode a `SignedCheque` in the exact JSON shape bee's
/// `chequebook.SignedCheque` unmarshals. Found live in M2 run 4:
/// `CumulativePayout` is Go's `math/big.Int`, whose `UnmarshalJSON`
/// accepts only an UNQUOTED number — ant's
/// `encode_signed_cheque_json` quotes it, so bee rejects every cheque
/// with "unmarshal cheque" and never credits the payment (upstream
/// ant bug to report). Addresses are 0x-hex, `Signature` is
/// standard-padding base64 (geth `[]byte`).
pub(crate) fn encode_cheque_json_bee(signed: &ant_chain::chequebook::SignedCheque) -> Vec<u8> {
    use base64::Engine as _;
    format!(
        "{{\"Chequebook\":\"0x{}\",\"Beneficiary\":\"0x{}\",\"CumulativePayout\":{},\"Signature\":\"{}\"}}",
        hex::encode(signed.cheque.chequebook),
        hex::encode(signed.cheque.beneficiary),
        signed.cheque.cumulative_payout,
        base64::engine::general_purpose::STANDARD.encode(signed.signature),
    )
    .into_bytes()
}

/// Parse bee's headers protobuf: `Headers { repeated Header { string
/// key = 1; bytes value = 2 } }`, returning the `exchange` rate and
/// `deduction` (zero when absent, matching bee's payer side).
pub(crate) fn parse_settlement_headers(body: &[u8]) -> Option<(U256, U256)> {
    let mut exchange: Option<U256> = None;
    let mut deduction = U256::zero();
    let mut rest = body;
    while !rest.is_empty() {
        let (tag, after_tag) = decode_varint(rest)?;
        rest = after_tag;
        if tag >> 3 != 1 || tag & 0x7 != 2 {
            return None;
        }
        let (len, after_len) = decode_varint(rest)?;
        let len = usize::try_from(len).ok()?;
        let header = after_len.get(..len)?;
        rest = after_len.get(len..)?;

        let mut key: &[u8] = &[];
        let mut value: &[u8] = &[];
        let mut inner = header;
        while !inner.is_empty() {
            let (itag, after) = decode_varint(inner)?;
            inner = after;
            if itag & 0x7 != 2 {
                return None;
            }
            let (field_size, past_size) = decode_varint(inner)?;
            let field_size = usize::try_from(field_size).ok()?;
            let field = past_size.get(..field_size)?;
            inner = past_size.get(field_size..)?;
            match itag >> 3 {
                1 => key = field,
                2 => value = field,
                _ => {}
            }
        }
        if value.len() > 32 {
            return None;
        }
        match key {
            b"exchange" => exchange = Some(U256::from_big_endian(value)),
            b"deduction" => deduction = U256::from_big_endian(value),
            _ => {}
        }
    }
    exchange.map(|rate| (rate, deduction))
}

pub(crate) fn encode_varint(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let byte = u8::try_from(value & 0x7f).expect("masked to 7 bits");
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

pub(crate) async fn read_delimited(
    stream: &mut (impl AsyncReadExt + Unpin),
    max: usize,
) -> std::io::Result<Vec<u8>> {
    let mut len: u64 = 0;
    let mut shift = 0u32;
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).await?;
        len |= u64::from(byte[0] & 0x7f) << shift;
        if byte[0] & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 63 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "varint too long",
            ));
        }
    }
    let len = usize::try_from(len)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "varint overflow"))?;
    if len > max {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("message too large: {len} bytes (cap {max})"),
        ));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_announce_threshold() {
        // field 1, wire 2, len 3, big-endian 0x0d2f00 = 864000
        let body = [0x0a, 0x03, 0x0d, 0x2f, 0x00];
        assert_eq!(parse_announce_threshold(&body), Some(U256::from(864_000)));
    }

    #[test]
    fn rejects_garbage_threshold() {
        assert_eq!(parse_announce_threshold(&[0xff]), None);
        assert_eq!(parse_announce_threshold(&[]), None);
    }

    #[test]
    fn cheque_json_matches_bee_shape() {
        let signed = ant_chain::chequebook::SignedCheque {
            cheque: ant_chain::chequebook::Cheque {
                chequebook: [0x11; 20],
                beneficiary: [0x22; 20],
                cumulative_payout: U256::from(89_000_000_100u64),
            },
            signature: [0x33; 65],
        };
        let json = String::from_utf8(encode_cheque_json_bee(&signed)).unwrap();
        // CumulativePayout MUST be an unquoted JSON number for Go's
        // big.Int; addresses 0x-hex; signature base64.
        assert!(json.contains("\"CumulativePayout\":89000000100,"), "{json}");
        assert!(json.contains("\"Chequebook\":\"0x1111111111111111111111111111111111111111\""));
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed["CumulativePayout"].is_u64());
        assert_eq!(parsed["Signature"].as_str().unwrap().len(), 88);
    }

    #[test]
    fn varint_roundtrip() {
        let (v, rest) = decode_varint(&[0xac, 0x02, 0x99]).unwrap();
        assert_eq!(v, 300);
        assert_eq!(rest, &[0x99]);
    }
}
