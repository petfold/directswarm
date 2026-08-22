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

/// Bee's light-peer early-payment point (threshold / 2): issue a cheque
/// once debt crosses this. Mirrors `ant_p2p::LIGHT_PAYMENT_THRESHOLD`'s
/// `DEFAULT_CHEQUE_TRIGGER` on the upload side.
const CHEQUE_TRIGGER: u64 = 675_000;
/// Minimum spacing between pseudosettle refresh attempts (bee rejects
/// faster refreshes; ant's driver uses 1100 ms).
const REFRESH_MIN_INTERVAL: Duration = Duration::from_millis(1100);
/// Settlement loop tick.
const SETTLE_TICK: Duration = Duration::from_millis(250);
/// Wait after the BZZ handshake so bee's swap/pricing registration and
/// threshold announcement land before we open retrieval streams.
const POST_HANDSHAKE_SETTLE: Duration = Duration::from_secs(2);
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
    pub cheque_plur: u64,
    pub refreshes_accepted: u64,
    pub refresh_plur: u64,
    pub residual_debt_plur: u64,
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
    // politely at the end.
    let (bye_tx, mut bye_rx) = oneshot::channel::<oneshot::Sender<()>>();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                evt = swarm.next() => {
                    if evt.is_none() { break; }
                }
                cmd = &mut bye_rx => {
                    let _ = swarm.disconnect_peer_id(peer_id);
                    // Give the FIN a moment to flush.
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    if let Ok(done) = cmd { let _ = done.send(()); }
                    break;
                }
            }
        }
    });

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
        hot_rx,
        done: fetch_done.clone(),
    }));

    // --- pipelined retrieval ---
    let started = Instant::now();
    let sem = Arc::new(Semaphore::new(opts.pipeline_depth));
    let bytes = Arc::new(AtomicU64::new(0));
    let mut tasks = tokio::task::JoinSet::new();
    for addr in chunks {
        let permit = sem.clone().acquire_owned().await.expect("semaphore open");
        let mut task_control = control.clone();
        let task_acct = acct.clone();
        let task_bytes = bytes.clone();
        let storer_overlay = target.overlay;
        tasks.spawn(async move {
            let _permit = permit;
            let price = Accounting::peer_price(&storer_overlay, &addr);
            // Reserve against the mirrored threshold; on overdraft wait
            // for settlement to knock debt down (bee's 600 ms skip).
            let guard = loop {
                if let Some(guard) = task_acct.try_reserve(peer_id, price) {
                    break guard;
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
struct Behaviour {
    stream: StreamBehaviour,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
}

fn extract_peer_id(addr: &Multiaddr) -> Option<PeerId> {
    addr.iter().find_map(|p| {
        if let libp2p::multiaddr::Protocol::P2p(peer) = p {
            Some(peer)
        } else {
            None
        }
    })
}

async fn read_chequebook_issuable(opts: &ProbeOptions) -> Result<U256> {
    let client = ant_chain::ChainClient::new(opts.rpc_url.clone());
    let to = format!("0x{}", hex::encode(opts.chequebook));
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

async fn wait_connected(swarm: &mut libp2p::Swarm<Behaviour>, want: PeerId) -> Result<Multiaddr> {
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
            opts.network_id,
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
fn mount_sinks(control: &mut Control, threshold_tx: watch::Sender<Option<U256>>) -> Result<()> {
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
async fn read_sink_body(stream: &mut libp2p::Stream, max: usize) -> std::io::Result<Vec<u8>> {
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

async fn run_pricing_sink(mut incoming: IncomingStreams, tx: watch::Sender<Option<U256>>) {
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

async fn run_drain_sink(mut incoming: IncomingStreams, max: usize) {
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
fn parse_announce_threshold(body: &[u8]) -> Option<U256> {
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

fn decode_varint(buf: &[u8]) -> Option<(u64, &[u8])> {
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
    hot_rx: mpsc::Receiver<HotHint>,
    done: Arc<tokio::sync::Notify>,
}

/// Keep the connection settled: pseudosettle refresh on cadence, SWAP
/// cheque whenever residual debt crosses the trigger, and a final
/// sweep that cheques the remainder down to zero before hanging up.
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
                    summary.refresh_plur += ok.accepted;
                }
                Ok(_) => {}
                Err(err) => tracing::debug!("refresh: {err}"),
            }
            last_refresh = Some(Instant::now());
        }

        let debt = ctx
            .acct
            .debug_snapshot(&ctx.peer_id)
            .map_or(0, |(balance, _reserved)| balance);
        let should_cheque = if finishing {
            debt > 0
        } else {
            debt >= CHEQUE_TRIGGER
        };
        if should_cheque {
            if summary.cheque_plur.saturating_add(debt) > ctx.max_issue_plur {
                bail!(
                    "spend guard: cheque total would exceed max_issue_plur={} — aborting settlement",
                    ctx.max_issue_plur
                );
            }
            let prev = ctx.ledger.cumulative_for(&ctx.beneficiary);
            let next = prev
                .checked_add(U256::from(debt))
                .ok_or_else(|| anyhow!("cumulative overflow"))?;
            if next > ctx.issuable {
                bail!(
                    "cached invariant: cumulative {next} would exceed issuable {} — chequebook needs a deposit",
                    ctx.issuable
                );
            }
            match ant_p2p::swap::issue_and_emit(
                &mut ctx.control,
                ctx.peer_id,
                &ctx.secret,
                ctx.chequebook,
                ctx.beneficiary,
                U256::from(debt),
                ctx.chain_id,
                &ctx.ledger,
            )
            .await
            {
                Ok(cumulative) => {
                    ctx.acct.credit(ctx.peer_id, debt);
                    summary.cheques_issued += 1;
                    summary.cheque_plur += debt;
                    tracing::info!(amount = debt, %cumulative, "cheque emitted");
                }
                Err(err) => {
                    if finishing {
                        summary.residual_debt_plur = debt;
                        tracing::warn!("final cheque failed, residual debt {debt}: {err}");
                        break;
                    }
                    tracing::warn!("cheque emit failed: {err}");
                }
            }
        }

        if finishing {
            let residual = ctx
                .acct
                .debug_snapshot(&ctx.peer_id)
                .map_or(0, |(balance, _reserved)| balance);
            if residual == 0 || summary.residual_debt_plur > 0 {
                summary.residual_debt_plur = residual;
                break;
            }
            // Loop once more to sweep what's left.
        }
    }
    Ok(summary)
}

async fn read_delimited(
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
    fn varint_roundtrip() {
        let (v, rest) = decode_varint(&[0xac, 0x02, 0x99]).unwrap();
        assert_eq!(v, 300);
        assert_eq!(rest, &[0x99]);
    }
}
