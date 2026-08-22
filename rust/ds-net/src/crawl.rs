//! M3 — the topology crawler.
//!
//! A bounded, polite snowball crawl that builds a [`TopologyCache`]:
//! dial a seed, handshake (recording dial+handshake RTT), mount the
//! sinks bee requires, and harvest the hive gossip the peer pushes —
//! full nodes announce their whole connected set on connect (Phase-0
//! finding). Each fresh overlay becomes a dial candidate; we widen
//! outward under strict etiquette caps until the budget is spent.
//!
//! Etiquette (inherited from the Phase-0 blessing, tightened): a hard
//! dial cap, a max dial rate, one attempt per peer, no retries, polite
//! disconnect after harvesting, and an overall wall-clock cap.

use ant_crypto::HandshakeWireVersion;
use anyhow::{anyhow, Result};
use ds_core::{NodeRecord, TopologyCache};
use futures::{AsyncReadExt, AsyncWriteExt, StreamExt};
use libp2p::swarm::SwarmEvent;
use libp2p::{dns, identify, noise, ping, tcp, yamux};
use libp2p::{Multiaddr, PeerId, StreamProtocol, SwarmBuilder};
use libp2p_stream::{Behaviour as StreamBehaviour, Control, IncomingStreams};
use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use crate::hive::{decode_peers, PeerHint};
use crate::identity::Identity;

const PROTOCOL_PRICING: &str = "/swarm/pricing/1.0.0/pricing";
const PROTOCOL_HIVE_V2: &str = "/swarm/hive/2.0.0/peers";
const PROTOCOL_HIVE_V1: &str = "/swarm/hive/1.1.0/peers";

#[derive(Debug, Clone)]
pub struct CrawlOptions {
    pub network_id: u64,
    /// Hard cap on distinct peers dialed.
    pub max_dials: usize,
    /// Minimum spacing between dial attempts (rate limit).
    pub dial_interval: Duration,
    /// Per-peer connect+handshake budget.
    pub dial_timeout: Duration,
    /// How long to linger after handshake harvesting gossip.
    pub harvest_window: Duration,
    /// Overall wall-clock cap on the whole crawl.
    pub wall_cap: Duration,
}

impl Default for CrawlOptions {
    fn default() -> Self {
        Self {
            network_id: 1,
            max_dials: 50,
            dial_interval: Duration::from_millis(500),
            dial_timeout: Duration::from_secs(20),
            harvest_window: Duration::from_secs(4),
            wall_cap: Duration::from_secs(600),
        }
    }
}

#[derive(Debug, Default)]
pub struct CrawlStats {
    pub dials_attempted: usize,
    pub dials_ok: usize,
    pub hints_seen: usize,
    pub stop_reason: String,
}

/// Run the crawl from `seeds`, returning the populated cache and stats.
///
/// # Errors
/// Fails only on unrecoverable setup (swarm build). Per-peer failures
/// are counted, not fatal.
///
/// # Panics
/// Only if a sink protocol is registered more than once (a bug).
#[allow(clippy::too_many_lines)]
pub async fn crawl(
    id: &Identity,
    seeds: Vec<Multiaddr>,
    opts: &CrawlOptions,
) -> Result<(TopologyCache, CrawlStats)> {
    let mut cache = TopologyCache::new(id.overlay);
    let mut stats = CrawlStats::default();

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
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(30)))
        .build();
    let mut control = swarm.behaviour().stream.new_control();

    // Hive gossip lands here from the sink tasks.
    let (hint_tx, mut hint_rx) = mpsc::channel::<PeerHint>(1024);
    mount_sinks(&mut control, hint_tx);

    // Drive the swarm in the background; commands in, events out.
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<Cmd>(16);
    let (evt_tx, mut evt_rx) = mpsc::channel::<Evt>(64);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                evt = swarm.next() => match evt {
                    None => break,
                    Some(SwarmEvent::ConnectionEstablished { peer_id, .. }) => {
                        let _ = evt_tx.send(Evt::Connected(peer_id)).await;
                    }
                    Some(SwarmEvent::OutgoingConnectionError { peer_id: Some(p), .. }) => {
                        let _ = evt_tx.send(Evt::Failed(p)).await;
                    }
                    Some(SwarmEvent::Behaviour(BehaviourEvent::Identify(
                        identify::Event::Received { peer_id, .. },
                    ))) => {
                        let _ = evt_tx.send(Evt::Identified(peer_id)).await;
                    }
                    Some(_) => {}
                },
                cmd = cmd_rx.recv() => match cmd {
                    None => break,
                    Some(Cmd::Dial(addr)) => { let _ = swarm.dial(addr); }
                    Some(Cmd::Disconnect(peer)) => { let _ = swarm.disconnect_peer_id(peer); }
                },
            }
        }
    });

    // BFS frontier of dial candidates.
    let mut frontier: VecDeque<PeerHint> = VecDeque::new();
    let mut queued: HashSet<PeerId> = HashSet::new();
    for seed in &seeds {
        if let Some(peer_id) = extract_peer_id(seed) {
            frontier.push_back(PeerHint {
                peer_id,
                overlay: [0u8; 32], // unknown until handshake
                underlays: vec![seed.clone()],
            });
            queued.insert(peer_id);
        }
    }

    let started = Instant::now();
    let mut last_dial: Option<Instant> = None;

    while let Some(hint) = frontier.pop_front() {
        if started.elapsed() >= opts.wall_cap {
            stats.stop_reason = "wall_cap".into();
            break;
        }
        if stats.dials_attempted >= opts.max_dials {
            stats.stop_reason = "max_dials".into();
            break;
        }
        // Rate limit: one attempt per peer, spaced dials.
        if let Some(at) = last_dial {
            if let Some(wait) = opts.dial_interval.checked_sub(at.elapsed()) {
                tokio::time::sleep(wait).await;
            }
        }
        last_dial = Some(Instant::now());
        stats.dials_attempted += 1;

        let Some(addr) = hint.underlays.first().cloned() else {
            continue;
        };
        let handshake = dial_and_handshake(
            &cmd_tx,
            &mut evt_rx,
            &mut control,
            id,
            hint.peer_id,
            &addr,
            opts,
        )
        .await;

        match handshake {
            Ok((overlay, rtt_ms)) => {
                stats.dials_ok += 1;
                cache.upsert(NodeRecord {
                    overlay,
                    underlays: hint.underlays.iter().map(ToString::to_string).collect(),
                    rtt_ms: Some(rtt_ms),
                    last_seen_tick: elapsed_ticks(started),
                    dialed_ok: true,
                });
                // Harvest gossip for a window, enqueue fresh peers.
                harvest(
                    &mut hint_rx,
                    &mut frontier,
                    &mut queued,
                    &mut cache,
                    &mut stats,
                    started,
                    opts.harvest_window,
                )
                .await;
                let _ = cmd_tx.send(Cmd::Disconnect(hint.peer_id)).await;
            }
            Err(err) => {
                tracing::debug!(peer = %hint.peer_id, "dial/handshake failed: {err}");
                let _ = cmd_tx.send(Cmd::Disconnect(hint.peer_id)).await;
            }
        }
    }
    if stats.stop_reason.is_empty() {
        stats.stop_reason = "frontier_drained".into();
    }
    Ok((cache, stats))
}

fn elapsed_ticks(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

async fn harvest(
    hint_rx: &mut mpsc::Receiver<PeerHint>,
    frontier: &mut VecDeque<PeerHint>,
    queued: &mut HashSet<PeerId>,
    cache: &mut TopologyCache,
    stats: &mut CrawlStats,
    started: Instant,
    window: Duration,
) {
    let deadline = Instant::now() + window;
    while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
        match tokio::time::timeout(remaining, hint_rx.recv()).await {
            Ok(Some(hint)) => {
                stats.hints_seen += 1;
                // Record the gossiped node (not yet dialed).
                cache.upsert(NodeRecord {
                    overlay: hint.overlay,
                    underlays: hint.underlays.iter().map(ToString::to_string).collect(),
                    rtt_ms: None,
                    last_seen_tick: elapsed_ticks(started),
                    dialed_ok: false,
                });
                if queued.insert(hint.peer_id) {
                    frontier.push_back(hint);
                }
            }
            Ok(None) | Err(_) => break,
        }
    }
}

#[derive(libp2p::swarm::NetworkBehaviour)]
struct Behaviour {
    stream: StreamBehaviour,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
}

enum Cmd {
    Dial(Multiaddr),
    Disconnect(PeerId),
}

enum Evt {
    Connected(PeerId),
    Failed(PeerId),
    Identified(PeerId),
}

/// Dial `addr`, wait for connect + identify, run the BZZ handshake
/// (V15 then V14) as an honest light peer. Returns the verified remote
/// overlay and the dial+handshake wall-clock in ms (the reach.csv
/// comparable).
async fn dial_and_handshake(
    cmd_tx: &mpsc::Sender<Cmd>,
    evt_rx: &mut mpsc::Receiver<Evt>,
    control: &mut Control,
    id: &Identity,
    peer_id: PeerId,
    addr: &Multiaddr,
    opts: &CrawlOptions,
) -> Result<([u8; 32], u32)> {
    let started = Instant::now();
    cmd_tx
        .send(Cmd::Dial(addr.clone()))
        .await
        .map_err(|_| anyhow!("swarm drive task gone"))?;

    // Wait for identify (implies connected) or failure.
    let deadline = Instant::now() + opts.dial_timeout;
    let mut connected = false;
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| anyhow!("dial timeout"))?;
        match tokio::time::timeout(remaining, evt_rx.recv()).await {
            Ok(Some(Evt::Connected(p))) if p == peer_id => connected = true,
            Ok(Some(Evt::Identified(p))) if p == peer_id => break,
            Ok(Some(Evt::Failed(p))) if p == peer_id => {
                return Err(anyhow!("dial failed"));
            }
            Ok(Some(_)) => {}
            Ok(None) => return Err(anyhow!("swarm drive task gone")),
            Err(_) => {
                if connected {
                    // identify can be slow; proceed after connect.
                    break;
                }
                return Err(anyhow!("dial timeout"));
            }
        }
    }

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
                tracing::debug!("open {proto}: {err}");
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
            std::slice::from_ref(addr),
            Vec::new(),
            false, // honest light role
            version,
        )
        .await
        {
            Ok(info) => {
                let rtt_ms = u32::try_from(started.elapsed().as_millis()).unwrap_or(u32::MAX);
                return Ok((info.remote_overlay, rtt_ms));
            }
            Err(err) => tracing::debug!("handshake {proto}: {err}"),
        }
    }
    Err(anyhow!("handshake failed on both wire versions"))
}

fn extract_peer_id(addr: &Multiaddr) -> Option<PeerId> {
    addr.iter().find_map(|p| match p {
        libp2p::multiaddr::Protocol::P2p(peer) => Some(peer),
        _ => None,
    })
}

fn mount_sinks(control: &mut Control, hint_tx: mpsc::Sender<PeerHint>) {
    let mut mount = |proto: &'static str| {
        control
            .accept(StreamProtocol::new(proto))
            .expect("protocol registered once")
    };
    let pricing = mount(PROTOCOL_PRICING);
    let hive_v2 = mount(PROTOCOL_HIVE_V2);
    let hive_v1 = mount(PROTOCOL_HIVE_V1);
    let pseudosettle = mount(ant_p2p::pseudosettle::PROTOCOL_PSEUDOSETTLE);
    let swap = mount(ant_p2p::swap::PROTOCOL_SWAP);

    tokio::spawn(run_drain_sink(pricing, 1024));
    tokio::spawn(run_hive_sink(hive_v2, hint_tx.clone()));
    tokio::spawn(run_hive_sink(hive_v1, hint_tx));
    tokio::spawn(ant_p2p::pseudosettle::run_inbound(pseudosettle));
    tokio::spawn(ant_p2p::swap::drain_inbound_unconfigured(swap));
}

async fn run_hive_sink(mut incoming: IncomingStreams, hint_tx: mpsc::Sender<PeerHint>) {
    while let Some((_peer, mut stream)) = incoming.next().await {
        let hint_tx = hint_tx.clone();
        tokio::spawn(async move {
            if let Ok(Ok(body)) = tokio::time::timeout(
                Duration::from_secs(30),
                read_sink_body(&mut stream, 64 * 1024),
            )
            .await
            {
                for hint in decode_peers(&body) {
                    if hint_tx.send(hint).await.is_err() {
                        break;
                    }
                }
            }
        });
    }
}

async fn run_drain_sink(mut incoming: IncomingStreams, max: usize) {
    while let Some((_peer, mut stream)) = incoming.next().await {
        tokio::spawn(async move {
            let _ = tokio::time::timeout(Duration::from_secs(30), read_sink_body(&mut stream, max))
                .await;
        });
    }
}

/// bee-headers framing: read peer headers, reply empty, read body,
/// half-close, drain to EOF.
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
            format!("message too large: {len} (cap {max})"),
        ));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}
