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
    let ledger = crate::ledger::FastLedger::open(opts.ledger_path.clone(), &opts.chequebook)?;
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
        ledger: ledger.clone(),
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
    // Persist the final sweep's cumulative NOW: the background persister
    // runs every 200 ms and dies with the process, so a probe that exits
    // right after its sweep left that cheque off disk (found 2026-11-09:
    // the ledger trailed bee by one 260k-unit sweep), and the next
    // cheque to this peer would have been refused or come up short.
    // The scheduler and the growth probe already flush at run end.
    ledger.flush().context("ledger flush")?;

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
                info,
                ..
            })) if peer_id == want => {
                if adopt_observed_addr(swarm, &info.observed_addr) {
                    swarm.behaviour_mut().identify.push([want]);
                }
                if let Some(addr) = connected_addr {
                    return Ok(addr);
                }
            }
            _ => {}
        }
    }
}

/// Make bee's inbound handshake answer at once instead of after 10 s.
///
/// Bee's `handleIncoming` (pkg/p2p/libp2p) first waits up to
/// `peerstoreWaitAddrsTimeout` = 10 s for its peerstore to hold an
/// address of the dialer, then falls back to the connection's remote
/// address ("typically ... behind NAT"). We never listen, so our identify
/// carried no addresses and every handshake sat out the whole window —
/// the ~10.1 s `handshake_ms` measured since M2. Upstream ant hit the same
/// wall (ant-p2p `behaviour.rs`): it promotes a remote's identify
/// `observed_addr` to an external address.
///
/// Callers that see bee's identify call this with its `observed_addr`
/// and, when it returns `true`, identify-push to that peer
/// (libp2p-identify 0.47 doesn't push on an external-address change by
/// itself). The push needn't land before the handshake: bee subscribes to
/// the peerstore's address stream before it starts waiting, so the push
/// releases it whenever it arrives. Nothing new is claimed — it is the
/// address bee itself falls back to after the wait.
///
/// Adopts only the FIRST observation per swarm (a shared swarm, as in the
/// crawler, would otherwise collect one public-IP:port per connection; any
/// one address releases bee's wait) and only a globally routable one:
/// go-libp2p's identify keeps only public addresses from a public peer.
pub(crate) fn adopt_observed_addr<B>(swarm: &mut libp2p::Swarm<B>, observed: &Multiaddr) -> bool
where
    B: libp2p::swarm::NetworkBehaviour,
{
    if swarm.external_addresses().next().is_some() || !publicly_routable(observed) {
        return false;
    }
    swarm.add_external_address(observed.clone());
    true
}

/// Every IP component globally routable (and at least one present); DNS
/// names don't count. Conservative: rejects RFC 1918, CGNAT
/// (100.64.0.0/10), loopback, link-local, multicast, reserved,
/// documentation and benchmarking space, and IPv6 outside `2000::/3`.
pub(crate) fn publicly_routable(addr: &Multiaddr) -> bool {
    use libp2p::multiaddr::Protocol;
    let v4 = |ip: std::net::Ipv4Addr| {
        let o = ip.octets();
        !(ip.is_unspecified()
            || ip.is_loopback()
            || ip.is_private()
            || ip.is_link_local()
            || ip.is_multicast()
            || ip.is_broadcast()
            || ip.is_documentation()
            || o[0] == 0
            || (o[0] == 100 && (o[1] & 0xc0) == 64)
            || (o[0] == 192 && o[1] == 0 && o[2] == 0)
            || (o[0] == 198 && (o[1] & 0xfe) == 18)
            || o[0] >= 240)
    };
    let v6 = |ip: std::net::Ipv6Addr| {
        let s = ip.segments();
        (s[0] & 0xe000) == 0x2000 && !(s[0] == 0x2001 && s[1] == 0x0db8)
    };
    let mut ip_seen = false;
    for p in addr {
        let ok = match p {
            Protocol::Ip4(ip) => {
                ip_seen = true;
                v4(ip)
            }
            Protocol::Ip6(ip) => {
                ip_seen = true;
                v6(ip)
            }
            Protocol::Dns(_) | Protocol::Dns4(_) | Protocol::Dns6(_) | Protocol::Dnsaddr(_) => {
                false
            }
            _ => true,
        };
        if !ok {
            return false;
        }
    }
    ip_seen
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
    ledger: std::sync::Arc<crate::ledger::FastLedger>,
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
    pub ledger: &'a crate::ledger::FastLedger,
    /// Cached `balance + totalPaidOut` ceiling on cumulative payout.
    pub issuable: U256,
    /// PLUR issued so far (shared across peers for a global spend cap).
    pub issued_plur: &'a std::sync::atomic::AtomicU64,
    pub max_issue_plur: u64,
}

/// Bee's swap wire done in full, on upstream ant's settlement
/// primitives: [`ant_p2p::swap::open_settlement`] reads the receiver's
/// announced oracle exchange rate (PLUR per accounting unit) and
/// one-time deduction; the cheque must move `units × rate + deduction`
/// PLUR or the receiver credits the wrong amount — the first probe run
/// proved bee then sees unsettled debt and disconnects.
/// [`ant_p2p::swap::write_cheque`] sends it in bee's JSON shape (the
/// unquoted `CumulativePayout` we found missing in M2 is fixed upstream,
/// ant #121). Guards: the cached invariant (cumulative ≤ issuable) and
/// a global PLUR spend cap, reserved before signing.
///
/// Ordering follows `write_cheque`'s contract: the cumulative is
/// recorded as issued BEFORE the write, because the receiver may hold
/// the cheque even if the write fails halfway. Reusing that cumulative
/// for a smaller later cheque would be refused (not increasing). Now a
/// failed write costs at worst one cheque's amount of overpayment (if
/// the peer did get it, the debt it settled is paid again by the next
/// cheque; if not, the next cheque carries it), never a refused cheque.
/// Only failures before the record release the spend reservation.
///
/// We still close our half straight after the write, as before the
/// migration; upstream's `await_processed` (refused-cheque verdicts,
/// read off bee's FIN-vs-RST ending) is not adopted here yet.
pub(crate) async fn emit_settlement_cheque(e: ChequeEmit<'_>) -> Result<RateEmitOutcome> {
    use std::sync::atomic::Ordering;
    let (mut stream, rates) = ant_p2p::swap::open_settlement(e.control, e.peer_id)
        .await
        .map_err(|err| anyhow!("open settlement: {err}"))?;
    let (rate, deduction) = (rates.exchange_rate, rates.deduction);
    if rate.is_zero() {
        bail!("peer announced a zero exchange rate");
    }
    // deduction goes to zero once the peer has recorded a cheque from
    // us — seeing it stay non-zero on a later cheque means the earlier
    // one was never accepted (how the quoted-JSON bug was caught).
    tracing::debug!(%rate, %deduction, "swap headers received");

    let plur = rates
        .cheque_amount(e.debt_units)
        .ok_or_else(|| anyhow!("cheque amount overflow"))?;
    let plur_u128 = u128::try_from(plur).map_err(|_| anyhow!("cheque amount exceeds u128"))?;
    let plur_u64 = u64::try_from(plur_u128).unwrap_or(u64::MAX);

    // Reserve against the global spend cap before doing anything
    // irreversible; release on any failure before the record below.
    let before = e.issued_plur.fetch_add(plur_u64, Ordering::SeqCst);
    if before.saturating_add(plur_u64) > e.max_issue_plur {
        e.issued_plur.fetch_sub(plur_u64, Ordering::SeqCst);
        bail!(
            "spend guard: cheque of {plur} PLUR would exceed max_issue_plur={}",
            e.max_issue_plur
        );
    }

    let prepared = (|| {
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
        Ok::<_, anyhow::Error>((cumulative, signed))
    })();
    let (cumulative, signed) = match prepared {
        Ok(p) => p,
        Err(err) => {
            e.issued_plur.fetch_sub(plur_u64, Ordering::SeqCst);
            return Err(err);
        }
    };

    // Counted as issued from here on (see the ordering note above).
    e.ledger.record_issued(&e.beneficiary, cumulative);
    ant_p2p::swap::write_cheque(&mut stream, &signed)
        .await
        .map_err(|err| {
            anyhow!(
                "write cheque (recorded as issued: the peer may hold it; if not, \
                 the next cheque carries its amount): {err}"
            )
        })?;
    let _ = stream.close().await;
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

    /// Byte-exact guard on upstream's encoder (now used on the wire):
    /// the JSON bee's `chequebook.SignedCheque` unmarshals, with
    /// `CumulativePayout` an UNQUOTED number (Go's `big.Int` refuses a
    /// quoted one; found live in M2 run 4, fixed upstream in ant #121).
    /// The expected bytes are what our own encoder sent through all of
    /// Phase 1 and M6.
    #[test]
    fn upstream_cheque_json_is_bee_shape_byte_for_byte() {
        let signed = ant_chain::chequebook::SignedCheque {
            cheque: ant_chain::chequebook::Cheque {
                chequebook: [0x11; 20],
                beneficiary: [0x22; 20],
                cumulative_payout: U256::from(89_000_000_100u64),
            },
            signature: [0x33; 65],
        };
        let json = ant_p2p::swap::encode_signed_cheque_json(&signed);
        let expected = format!(
            "{{\"Chequebook\":\"0x{}\",\"Beneficiary\":\"0x{}\",\"CumulativePayout\":89000000100,\"Signature\":\"{}\"}}",
            "11".repeat(20),
            "22".repeat(20),
            {
                use base64::Engine as _;
                base64::engine::general_purpose::STANDARD.encode([0x33u8; 65])
            },
        );
        assert_eq!(String::from_utf8(json).unwrap(), expected);
    }

    #[test]
    fn upstream_parses_bee_settlement_headers() {
        // Headers { Header { key "exchange", value 0x0186a0 = 100000 },
        //           Header { key "deduction", value 0x64 = 100 } }
        let mut body = Vec::new();
        for (key, value) in [
            (&b"exchange"[..], &[0x01u8, 0x86, 0xa0][..]),
            (b"deduction", &[0x64]),
        ] {
            let mut header = vec![0x0a, u8::try_from(key.len()).unwrap()];
            header.extend_from_slice(key);
            header.extend_from_slice(&[0x12, u8::try_from(value.len()).unwrap()]);
            header.extend_from_slice(value);
            body.push(0x0a);
            body.push(u8::try_from(header.len()).unwrap());
            body.extend_from_slice(&header);
        }
        let rates = ant_p2p::swap::parse_settlement_headers(&body).unwrap();
        assert_eq!(rates.exchange_rate, U256::from(100_000u64));
        assert_eq!(rates.deduction, U256::from(100u64));
        // units × rate + deduction, as bee's payer computes it.
        assert_eq!(
            rates.cheque_amount(220_000),
            Some(U256::from(22_000_000_100u64))
        );
    }

    #[test]
    fn only_globally_routable_observations_are_adopted() {
        let ok = |s: &str| publicly_routable(&s.parse::<Multiaddr>().unwrap());
        assert!(ok("/ip4/84.2.33.17/tcp/40512"));
        assert!(ok("/ip6/2a01:4f8:1c1b:abcd::1/tcp/1634"));
        for bad in [
            "/ip4/192.168.1.224/tcp/40512",
            "/ip4/10.0.0.7/tcp/1",
            "/ip4/172.20.0.2/tcp/1",
            "/ip4/100.71.3.9/tcp/1", // CGNAT: go-libp2p drops it too
            "/ip4/127.0.0.1/tcp/1",
            "/ip4/169.254.3.3/tcp/1",
            "/ip4/0.0.0.0/tcp/1",
            "/ip4/198.18.0.5/tcp/1",
            "/ip4/203.0.113.9/tcp/1",
            "/ip4/255.255.255.255/tcp/1",
            "/ip6/fd00::1/tcp/1",
            "/ip6/fe80::1/tcp/1",
            "/ip6/::1/tcp/1",
            "/ip6/2001:db8::1/tcp/1",
            "/dns4/example.org/tcp/1",
            "/tcp/1",
        ] {
            assert!(!ok(bad), "{bad} must not be adopted");
        }
    }

    #[test]
    fn varint_roundtrip() {
        let (v, rest) = decode_varint(&[0xac, 0x02, 0x99]).unwrap();
        assert_eq!(v, 300);
        assert_eq!(rest, &[0x99]);
    }
}
