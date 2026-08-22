//! M5 groundwork — `probe-growth`: one long-lived, fully settled storer
//! connection measuring the three numbers the 25 MB/s target hangs on:
//!
//! 1. **Threshold growth.** Bee raises a well-behaved peer's payment
//!    threshold each time the peer's cumulative settled debt crosses a
//!    checkpoint (verified in bee `pkg/accounting`: light peers start at
//!    1.35 M units and gain +450 k per 45 M units settled, linear until
//!    ~9.45 M, near-plateau after), announcing each upgrade via the
//!    pricing stream. Phase A fetches continuously with pacing that
//!    tracks the live threshold and logs the growth curve.
//! 2. **Cheque validation latency λ.** Bee credits a cheque only after
//!    on-chain checks against the peer's own RPC endpoint, so sustained
//!    inflow is bounded by disconnect-headroom ÷ λ — λ is per-peer
//!    infrastructure, the swing variable between 0.04 and 0.23 MB/s per
//!    connection. Measured by quiescing, sweeping the debt with one
//!    cheque, then sending tiny (50 k unit) pseudosettle probes: bee
//!    ACKs `min(attempted, allowance, its-debt-view)`, so the ACK drops
//!    to 0 exactly when the cheque lands. Probe amounts are deliberately
//!    small so the free-tier drain cannot mask λ.
//! 3. **Sustained rate at the grown threshold** (Phase C): λ-aware
//!    pacing that keeps bee's worst-case ledger view (our mirror debt +
//!    cheques emitted within the λ window) under 1.05 × T — safely
//!    below bee's 1.25 × T disconnect limit.
//!
//! Honesty notes carried into the report: the chunk set is CYCLED
//! (re-fetching the same neighborhood chunks, paid at full price every
//! time) — this measures settlement pacing, not storer disk; λ probes
//! settle ≤ 50 k units each via the free tier and any overshoot becomes
//! surplus consumed by the next fetches (counted in `refresh_units`).

use ant_retrieval::accounting::Accounting;
use ant_retrieval::retrieve_chunk;
use std::sync::Mutex;
use anyhow::{anyhow, bail, Context, Result};
use futures::{AsyncWriteExt, StreamExt};
use libp2p::{dns, identify, noise, ping, tcp, yamux};
use libp2p::{PeerId, StreamProtocol, SwarmBuilder};
use libp2p::swarm::SwarmEvent;
use libp2p_stream::Control;
use primitive_types::U256;
use std::collections::VecDeque;
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, watch};

use crate::direct::{
    emit_settlement_cheque, extract_peer_id, handshake_with_fallback_raw, mount_sinks,
    read_chequebook_issuable_raw, read_delimited, wait_connected, Behaviour, ChequeEmit,
};
use crate::identity::Identity;

const REFRESH_MIN_INTERVAL: Duration = Duration::from_millis(1100);
const TICK: Duration = Duration::from_millis(50);
const POST_HANDSHAKE_SETTLE: Duration = Duration::from_secs(2);
/// λ probe cadence: > 1 s so bee's per-second refresh timestamps never
/// collide (a same-second refresh gets a zero allowance and would fake
/// a zero ACK).
const LAMBDA_PROBE_INTERVAL: Duration = Duration::from_millis(1150);
/// λ probe attempted amount — small enough that probing cannot drain
/// the swept debt by free tier before a slow validation lands.
const LAMBDA_PROBE_UNITS: u64 = 50_000;
const LAMBDA_PROBE_TIMEOUT: Duration = Duration::from_secs(25);
/// Phase A cheque spacing (pre-λ, conservative: exceeds the slowest
/// validation observed in M4 so at most one cheque is unvalidated).
const GROWTH_CHEQUE_INTERVAL: Duration = Duration::from_millis(3000);
/// Fallback threshold when the peer never announces (bee light default).
const DEFAULT_THRESHOLD: u64 = 1_350_000;
/// Exchange-rate fallback for spend projection before the first cheque
/// reveals the announced rate (mainnet oracle rate, Phase 0/1 measured).
const DEFAULT_RATE: u128 = 100_000;

#[derive(Debug, Clone)]
pub struct GrowthOptions {
    pub network_id: u64,
    pub chain_id: u64,
    pub chequebook: [u8; 20],
    pub rpc_url: String,
    pub ledger_path: PathBuf,
    pub pipeline_depth: usize,
    /// Hard cap on PLUR issued as cheques this run. Checked at the
    /// FETCH gate (projected, incl. unswept debt), so the final sweep
    /// always has room — the M2 lesson (a guard that blocks settlement
    /// strands debt).
    pub max_issue_plur: u64,
    /// Phase A wall cap (seconds). 0 skips straight to λ sampling.
    pub growth_secs: u64,
    /// Phase C wall cap (seconds). 0 skips the ceiling phase.
    pub ceiling_secs: u64,
    /// Number of λ samples between growth and ceiling.
    pub lambda_samples: u32,
    /// JSONL event log (one line per threshold/cheque/sample/λ event).
    pub jsonl_path: PathBuf,
}

#[derive(Debug, Clone, Default)]
pub struct PhaseStats {
    pub wall_s: f64,
    pub fetches_ok: u64,
    pub fetches_err: u64,
    pub bytes: u64,
    /// Accounting units settled during the phase (cheques + refreshes).
    pub units_settled: u64,
    pub mbs: f64,
    /// Threshold at phase end.
    pub threshold_end: u64,
}

#[derive(Debug)]
pub struct GrowthReport {
    pub peer_id: PeerId,
    pub remote_overlay: [u8; 32],
    pub remote_eth: [u8; 20],
    pub handshake_ms: u128,
    /// First announced threshold (a peer that remembers us from earlier
    /// sessions may already announce a grown value).
    pub threshold_first: u64,
    pub threshold_last: u64,
    pub upgrades_observed: u32,
    pub growth: PhaseStats,
    pub ceiling: Option<PhaseStats>,
    /// Per-sample validation latency in ms; None = no zero-ACK within
    /// the probe timeout (validation slower than the timeout, or the
    /// free tier interfered — see the event log).
    pub lambda_ms: Vec<Option<u64>>,
    pub distinct_chunks: usize,
    pub cheques: u64,
    pub cheque_units: u64,
    pub cheque_plur: u128,
    pub refreshes: u64,
    pub refresh_units: u64,
    /// End-of-run zero-debt confirmed from BEE'S side (probe ACK == 0),
    /// stronger than our mirror's claim. None = probe failed.
    pub residual_zero_confirmed: Option<bool>,
    pub spend_capped: bool,
    pub wall: Duration,
}

struct EventLog {
    file: std::fs::File,
    started: Instant,
}

impl EventLog {
    fn open(path: &PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("open {}", path.display()))?;
        Ok(Self {
            file,
            started: Instant::now(),
        })
    }

    /// `body` must be valid JSON fields, e.g. `"ev":"cheque","units":5`.
    fn line(&mut self, body: &str) {
        let t_ms = self.started.elapsed().as_millis();
        let _ = writeln!(self.file, "{{\"t_ms\":{t_ms},{body}}}");
    }
}

/// Single-peer accounting mirror with NO fixed overdraft ceiling: the
/// caller's gates bound exposure against the LIVE announced threshold.
/// (ant's `Accounting` hard-caps `balance + reserved` at the FRESH
/// light disconnect limit — correct for stock fetches, but it silently
/// re-caps a grown-threshold connection to free-tier pacing, which is
/// exactly what this probe exists to escape. Found live in the pilot:
/// ceiling-phase rate pinned at ~450 k units/s regardless of T.)
#[derive(Default)]
struct Mirror {
    state: Arc<Mutex<(u64, u64)>>, // (balance = unsettled debt, reserved)
}

impl Mirror {
    fn snapshot(&self) -> (u64, u64) {
        *self.state.lock().expect("mirror lock")
    }

    /// Reserve `price` if `balance + reserved + price <= limit`.
    fn try_reserve(&self, price: u64, limit: u64) -> bool {
        let mut s = self.state.lock().expect("mirror lock");
        if s.0.saturating_add(s.1).saturating_add(price) > limit {
            return false;
        }
        s.1 += price;
        true
    }

    /// Served: move `price` from reserved into debt.
    fn apply(&self, price: u64) {
        let mut s = self.state.lock().expect("mirror lock");
        s.1 = s.1.saturating_sub(price);
        s.0 = s.0.saturating_add(price);
    }

    /// Fetch failed: release the reservation.
    fn release(&self, price: u64) {
        let mut s = self.state.lock().expect("mirror lock");
        s.1 = s.1.saturating_sub(price);
    }

    /// Settlement accepted (cheque emitted / refresh acknowledged).
    fn credit(&self, amount: u64) {
        let mut s = self.state.lock().expect("mirror lock");
        s.0 = s.0.saturating_sub(amount);
    }
}

struct Conn {
    control: Control,
    peer_id: PeerId,
    storer_overlay: [u8; 32],
    beneficiary: [u8; 20],
    secret: [u8; 32],
    chequebook: [u8; 20],
    chain_id: u64,
    acct: Arc<Mirror>,
    ledger: ant_p2p::swap::OutboundLedger,
    issuable: U256,
    issued_plur: AtomicU64,
    max_issue_plur: u64,
    exchange_rate: Option<u128>,
    alive: Arc<AtomicBool>,
    threshold_rx: watch::Receiver<Option<U256>>,
    // rolling telemetry
    cheques: u64,
    cheque_units: u64,
    cheque_plur: u128,
    refreshes: u64,
    refresh_units: u64,
    last_refresh: Option<Instant>,
    last_cheque: Option<Instant>,
    /// (emit time, units) of cheques possibly still in bee's validation
    /// pipeline; pruned against the λ window in Phase C.
    unvalidated: VecDeque<(Instant, u64)>,
    ev: EventLog,
}

impl Conn {
    fn threshold(&self) -> u64 {
        self.threshold_rx
            .borrow()
            .as_ref()
            .map_or(DEFAULT_THRESHOLD, |t| {
                u64::try_from(*t).unwrap_or(DEFAULT_THRESHOLD)
            })
    }

    fn debt(&self) -> (u64, u64) {
        self.acct.snapshot()
    }

    fn rate(&self) -> u128 {
        self.exchange_rate.unwrap_or(DEFAULT_RATE)
    }

    /// Projected PLUR if everything owed so far were cheque-settled.
    /// Leaves ≥ 8% headroom for the final sweep before the fetch gate
    /// trips.
    fn spend_would_exceed(&self, next_price: u64) -> bool {
        let (debt, reserved) = self.debt();
        let committed = u128::from(self.issued_plur.load(Ordering::SeqCst))
            + u128::from(debt + reserved + next_price) * self.rate();
        committed > u128::from(self.max_issue_plur) * 92 / 100
    }

    async fn refresh(&mut self) {
        let (debt, _) = self.debt();
        if debt == 0 {
            self.last_refresh = Some(Instant::now());
            return;
        }
        match ant_p2p::pseudosettle::refresh_peer(&mut self.control, self.peer_id).await {
            Ok(ok) if ok.accepted > 0 => {
                self.acct.credit(ok.accepted);
                self.refreshes += 1;
                self.refresh_units += ok.accepted;
            }
            Ok(_) => {}
            Err(err) => tracing::debug!("refresh: {err}"),
        }
        self.last_refresh = Some(Instant::now());
    }

    /// Emit a cheque for `units`, credit the mirror instantly (bee's
    /// `NotifyPaymentSent` semantics), track it as unvalidated.
    async fn emit(&mut self, units: u64) -> Result<()> {
        let outcome = emit_settlement_cheque(ChequeEmit {
            control: &mut self.control,
            peer_id: self.peer_id,
            secret: &self.secret,
            chequebook: self.chequebook,
            beneficiary: self.beneficiary,
            chain_id: self.chain_id,
            debt_units: units,
            ledger: &self.ledger,
            issuable: self.issuable,
            issued_plur: &self.issued_plur,
            max_issue_plur: self.max_issue_plur,
        })
        .await?;
        self.acct.credit(units);
        self.cheques += 1;
        self.cheque_units += units;
        self.cheque_plur += outcome.plur;
        if self.exchange_rate.is_none() {
            self.exchange_rate = Some(outcome.rate);
        }
        self.last_cheque = Some(Instant::now());
        self.unvalidated.push_back((Instant::now(), units));
        self.ev.line(&format!(
            "\"ev\":\"cheque\",\"units\":{units},\"plur\":\"{}\",\"cumulative\":\"{}\"",
            outcome.plur_u256, outcome.cumulative
        ));
        Ok(())
    }

    fn unvalidated_within(&mut self, window: Duration) -> u64 {
        while let Some(&(at, _)) = self.unvalidated.front() {
            if at.elapsed() > window {
                self.unvalidated.pop_front();
            } else {
                break;
            }
        }
        self.unvalidated.iter().map(|&(_, u)| u).sum()
    }
}

/// One pseudosettle round trip with an EXPLICIT small attempted amount
/// (the stock `refresh_peer` over-asks, which is right for settlement
/// and wrong for probing — a large ask drains the debt being watched).
/// Returns bee's accepted amount: `min(attempted, allowance, debt)`.
async fn refresh_probe(control: &mut Control, peer: PeerId, attempted: u64) -> Result<u64> {
    let mut stream = control
        .open_stream(
            peer,
            StreamProtocol::new(ant_p2p::pseudosettle::PROTOCOL_PSEUDOSETTLE),
        )
        .await
        .map_err(|e| anyhow!("open pseudosettle: {e}"))?;
    // bee-headers preamble: empty dialer headers, then their headers.
    stream.write_all(&[0u8]).await?;
    stream.flush().await?;
    let _their_headers = read_delimited(&mut stream, 8 * 1024).await?;
    // Payment { bytes Amount = 1 }, amount big-endian minimal bytes.
    let be = attempted.to_be_bytes();
    let first = be.iter().position(|b| *b != 0).unwrap_or(7);
    let amount = &be[first..];
    let mut msg = Vec::with_capacity(amount.len() + 2);
    msg.push(0x0a);
    msg.push(u8::try_from(amount.len()).expect("<=8"));
    msg.extend_from_slice(amount);
    let mut frame = Vec::with_capacity(msg.len() + 1);
    crate::direct::encode_varint(msg.len() as u64, &mut frame);
    frame.extend_from_slice(&msg);
    stream.write_all(&frame).await?;
    stream.flush().await?;
    let ack = read_delimited(&mut stream, 256).await?;
    let _ = stream.close().await;
    parse_ack_amount(&ack).ok_or_else(|| anyhow!("unparseable PaymentAck"))
}

/// `PaymentAck { bytes Amount = 1; int64 Timestamp = 2 }` → Amount as u64.
fn parse_ack_amount(body: &[u8]) -> Option<u64> {
    let mut rest = body;
    let mut amount: Option<u64> = None;
    while !rest.is_empty() {
        let (tag, after) = crate::direct::decode_varint(rest)?;
        rest = after;
        match tag & 0x7 {
            2 => {
                let (len, after_len) = crate::direct::decode_varint(rest)?;
                let len = usize::try_from(len).ok()?;
                let value = after_len.get(..len)?;
                rest = after_len.get(len..)?;
                if tag >> 3 == 1 {
                    if value.len() > 8 {
                        return None;
                    }
                    let mut buf = [0u8; 8];
                    buf[8 - value.len()..].copy_from_slice(value);
                    amount = Some(u64::from_be_bytes(buf));
                }
            }
            0 => {
                let (_, after) = crate::direct::decode_varint(rest)?;
                rest = after;
            }
            _ => return None,
        }
    }
    // Empty Amount bytes decode to 0 (Go big.Int semantics).
    amount.or(Some(0))
}

enum PhaseMode {
    /// Conservative: cap = T/2, cheque = min(debt, T/4) every ≥ 3 s.
    Growth,
    /// λ-aware: bee's worst-case view (debt + reserved + unvalidated
    /// cheques within the λ window) stays ≤ 1.05 × T.
    Ceiling { lambda_window: Duration },
}

struct PhaseOutcome {
    stats: PhaseStats,
    spend_capped: bool,
    errors: Vec<String>,
}

/// Drive one fetch phase: spawn pipelined fetches under the mode's
/// pacing gate, settle inline, sample the rate, and log threshold
/// upgrades. Ends on wall cap, spend cap, plateau (Growth mode: no
/// upgrade for 240 s after minute 5), or a dead connection — then
/// drains in-flight fetches.
#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
async fn run_fetch_phase(
    conn: &mut Conn,
    chunks: &[[u8; 32]],
    cycle_idx: &mut usize,
    wall_cap: Duration,
    mode: &PhaseMode,
    phase_name: &str,
) -> PhaseOutcome {
    let started = Instant::now();
    let mut stats = PhaseStats::default();
    let mut latencies_ms: Vec<u64> = Vec::new();
    let mut errors = Vec::new();
    let mut spend_capped = false;
    let units_at_start = conn.cheque_units + conn.refresh_units;
    let mut last_threshold = conn.threshold();
    let mut last_upgrade = Instant::now();
    let mut sample_at = Instant::now();
    let mut sample_fetches = 0u64;
    let mut tasks: tokio::task::JoinSet<Result<(u64, u64), String>> = tokio::task::JoinSet::new();
    conn.ev
        .line(&format!("\"ev\":\"phase\",\"name\":\"{phase_name}\""));

    let mut stopping = false;
    loop {
        // -- threshold tracking --
        let t = conn.threshold();
        if t != last_threshold {
            conn.ev.line(&format!(
                "\"ev\":\"threshold\",\"units\":{t},\"prev\":{last_threshold}"
            ));
            tracing::info!(threshold = t, prev = last_threshold, "threshold upgraded");
            last_threshold = t;
            last_upgrade = Instant::now();
        }

        // -- stop conditions (stop spawning; keep settling + draining) --
        if !stopping {
            let plateau = matches!(mode, PhaseMode::Growth)
                && started.elapsed() >= Duration::from_secs(300)
                && last_upgrade.elapsed() >= Duration::from_secs(240);
            if started.elapsed() >= wall_cap || plateau || !conn.alive.load(Ordering::Relaxed) {
                stopping = true;
            }
        }

        // -- spawn fetches under the gate --
        while !stopping && tasks.len() < conn_pipeline_depth(mode) {
            let addr = chunks[*cycle_idx % chunks.len()];
            let price = Accounting::peer_price(&conn.storer_overlay, &addr);
            if conn.spend_would_exceed(price) {
                spend_capped = true;
                stopping = true;
                conn.ev.line("\"ev\":\"spend_cap\"");
                break;
            }
            let limit = match mode {
                PhaseMode::Growth => last_threshold / 2,
                PhaseMode::Ceiling { lambda_window } => {
                    let unvalidated = conn.unvalidated_within(*lambda_window);
                    (last_threshold * 105 / 100).saturating_sub(unvalidated)
                }
            };
            if !conn.acct.try_reserve(price, limit) {
                break;
            }
            *cycle_idx += 1;
            let mut control = conn.control.clone();
            let peer_id = conn.peer_id;
            let acct = conn.acct.clone();
            tasks.spawn(async move {
                let t = Instant::now();
                match retrieve_chunk(&mut control, peer_id, addr).await {
                    Ok(chunk) => {
                        acct.apply(price);
                        Ok((
                            u64::try_from(t.elapsed().as_millis()).unwrap_or(u64::MAX),
                            chunk.data.len() as u64,
                        ))
                    }
                    Err(err) => {
                        acct.release(price);
                        Err(format!("chunk {}: {err}", hex::encode(addr)))
                    }
                }
            });
        }

        // -- drain finished fetches --
        while let Some(joined) = tasks.try_join_next() {
            match joined {
                Ok(Ok((ms, bytes))) => {
                    stats.fetches_ok += 1;
                    sample_fetches += 1;
                    stats.bytes += bytes;
                    latencies_ms.push(ms);
                }
                Ok(Err(err)) => {
                    stats.fetches_err += 1;
                    if errors.len() < 20 {
                        errors.push(err);
                    }
                }
                Err(join_err) => {
                    stats.fetches_err += 1;
                    if errors.len() < 20 {
                        errors.push(format!("task: {join_err}"));
                    }
                }
            }
        }

        // -- settle: refresh cadence, then cheques per mode --
        if conn
            .last_refresh
            .is_none_or(|at| at.elapsed() >= REFRESH_MIN_INTERVAL)
        {
            conn.refresh().await;
        }
        let (debt_now, _) = conn.debt();
        let (want_cheque, cheque_units) = match mode {
            PhaseMode::Growth => {
                let due = conn
                    .last_cheque
                    .is_none_or(|at| at.elapsed() >= GROWTH_CHEQUE_INTERVAL);
                let trigger = last_threshold / 4;
                (
                    due && debt_now >= trigger,
                    debt_now.min(last_threshold / 4),
                )
            }
            PhaseMode::Ceiling { .. } => {
                let due = conn
                    .last_cheque
                    .is_none_or(|at| at.elapsed() >= Duration::from_millis(500));
                (due && debt_now >= last_threshold / 5, debt_now)
            }
        };
        if want_cheque && conn.alive.load(Ordering::Relaxed) {
            if let Err(err) = conn.emit(cheque_units).await {
                tracing::warn!("cheque emit failed: {err}");
            }
        }

        // -- 30 s rate samples --
        if sample_at.elapsed() >= Duration::from_secs(30) {
            let dt = sample_at.elapsed().as_secs_f64();
            let mbs = sample_fetches as f64 * 4096.0 / 1e6 / dt;
            conn.ev.line(&format!(
                "\"ev\":\"sample\",\"phase\":\"{phase_name}\",\"fetches\":{sample_fetches},\"mbs\":{mbs:.4},\"threshold\":{last_threshold}"
            ));
            sample_at = Instant::now();
            sample_fetches = 0;
        }

        if stopping && tasks.is_empty() {
            break;
        }
        tokio::time::sleep(TICK).await;
    }

    stats.wall_s = started.elapsed().as_secs_f64();
    stats.units_settled = (conn.cheque_units + conn.refresh_units) - units_at_start;
    stats.mbs = if stats.wall_s > 0.0 {
        stats.bytes as f64 / 1e6 / stats.wall_s
    } else {
        0.0
    };
    stats.threshold_end = conn.threshold();
    latencies_ms.sort_unstable();
    let p = |q: usize| latencies_ms.get(latencies_ms.len() * q / 100).copied();
    conn.ev.line(&format!(
        "\"ev\":\"phase_end\",\"name\":\"{phase_name}\",\"lat_p50_ms\":{},\"lat_p95_ms\":{}",
        p(50).unwrap_or(0),
        p(95).unwrap_or(0)
    ));
    PhaseOutcome {
        stats,
        spend_capped,
        errors,
    }
}

fn conn_pipeline_depth(mode: &PhaseMode) -> usize {
    match mode {
        PhaseMode::Growth => 8,
        PhaseMode::Ceiling { .. } => 16,
    }
}

/// One λ sample: build up debt sequentially, quiesce, sweep it with a
/// single cheque, then small-probe until bee ACKs zero. Returns the
/// elapsed ms from sweep emit to the first of two consecutive zero
/// ACKs (double-confirmed against bee's per-second refresh timestamps).
async fn lambda_sample(conn: &mut Conn, chunks: &[[u8; 32]], cycle_idx: &mut usize) -> Option<u64> {
    let threshold = conn.threshold();
    let target_debt = threshold * 4 / 10; // 0.4 T, safely under the 0.5 T cap
    let build_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (debt, _) = conn.debt();
        if debt >= target_debt || Instant::now() >= build_deadline {
            break;
        }
        if !conn.alive.load(Ordering::Relaxed) {
            return None;
        }
        let addr = chunks[*cycle_idx % chunks.len()];
        let price = Accounting::peer_price(&conn.storer_overlay, &addr);
        if conn.spend_would_exceed(price) {
            break;
        }
        if !conn.acct.try_reserve(price, threshold / 2) {
            break;
        }
        *cycle_idx += 1;
        match retrieve_chunk(&mut conn.control, conn.peer_id, addr).await {
            Ok(_) => conn.acct.apply(price),
            Err(err) => {
                conn.acct.release(price);
                tracing::debug!("λ debt build fetch: {err}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    let (debt, _) = conn.debt();
    if debt < LAMBDA_PROBE_UNITS * 4 {
        conn.ev
            .line("\"ev\":\"lambda\",\"result\":\"skipped-no-debt\"");
        return None;
    }
    // Let bee's refresh-timestamp second tick over so the first probe
    // never lands in the same second as the last cadence refresh.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    if conn.emit(debt).await.is_err() {
        return None;
    }
    let swept = debt;
    let t0 = Instant::now();
    let mut zero_at: Option<u64> = None;
    while t0.elapsed() < LAMBDA_PROBE_TIMEOUT {
        tokio::time::sleep(LAMBDA_PROBE_INTERVAL).await;
        let accepted = match refresh_probe(&mut conn.control, conn.peer_id, LAMBDA_PROBE_UNITS)
            .await
        {
            Ok(a) => a,
            Err(err) => {
                tracing::debug!("λ probe: {err}");
                continue;
            }
        };
        // Probe acceptances settle real (free-tier) units of bee's view.
        if accepted > 0 {
            conn.refreshes += 1;
            conn.refresh_units += accepted;
        }
        let ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX);
        conn.ev.line(&format!(
            "\"ev\":\"lambda_probe\",\"accepted\":{accepted},\"ms\":{ms}"
        ));
        match (accepted, zero_at) {
            (0, Some(first)) => {
                conn.ev.line(&format!(
                    "\"ev\":\"lambda\",\"ms\":{first},\"swept_units\":{swept}"
                ));
                conn.last_refresh = Some(Instant::now());
                return Some(first);
            }
            (0, None) => zero_at = Some(ms),
            (_, _) => zero_at = None,
        }
    }
    conn.ev.line(&format!(
        "\"ev\":\"lambda\",\"result\":\"timeout\",\"swept_units\":{swept}"
    ));
    conn.last_refresh = Some(Instant::now());
    None
}

/// Dial one storer and run the growth → λ → ceiling probe sequence,
/// fully settled end to end.
///
/// # Errors
/// Fails on dial/handshake/chain-RPC/event-log failure; fetch and
/// settlement hiccups are recorded in the report instead.
///
/// # Panics
/// Only on poisoned internal state (infallible behaviour construction).
#[allow(clippy::too_many_lines)]
pub async fn probe_growth(
    id: &Identity,
    target: &crate::direct::ProbeTarget,
    chunks: Vec<[u8; 32]>,
    opts: &GrowthOptions,
) -> Result<GrowthReport> {
    let peer_id = extract_peer_id(&target.underlay)
        .ok_or_else(|| anyhow!("underlay must end in /p2p/<peer-id>"))?;
    if chunks.len() < 20 {
        bail!(
            "only {} chunks selected for this storer — too few to probe",
            chunks.len()
        );
    }
    let ev = EventLog::open(&opts.jsonl_path)?;

    let issuable = read_chequebook_issuable_raw(&opts.rpc_url, opts.chequebook).await?;
    let ledger = ant_p2p::swap::OutboundLedger::open(Some(opts.ledger_path.clone()));

    let behaviour = Behaviour {
        stream: libp2p_stream::Behaviour::default(),
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
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(3600)))
        .build();
    let mut control = swarm.behaviour().stream.new_control();

    let (threshold_tx, threshold_rx) = watch::channel::<Option<U256>>(None);
    mount_sinks(&mut control, threshold_tx)?;

    swarm.dial(target.underlay.clone())?;
    let dialed_addr = wait_connected(&mut swarm, peer_id).await?;

    let alive = Arc::new(AtomicBool::new(true));
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
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        alive.store(false, Ordering::Relaxed);
                        if let Ok(done) = cmd { let _ = done.send(()); }
                        break;
                    }
                }
            }
        });
    }

    let hs_started = Instant::now();
    let info =
        handshake_with_fallback_raw(&mut control, id, peer_id, &dialed_addr, opts.network_id)
            .await?;
    let handshake_ms = hs_started.elapsed().as_millis();
    if info.remote_overlay != target.overlay {
        bail!(
            "peer overlay mismatch: expected {} got {}",
            hex::encode(target.overlay),
            hex::encode(info.remote_overlay)
        );
    }
    tokio::time::sleep(POST_HANDSHAKE_SETTLE).await;

    let run_started = Instant::now();
    let mut conn = Conn {
        control,
        peer_id,
        storer_overlay: target.overlay,
        beneficiary: info.remote_eth_address,
        secret: id.secret,
        chequebook: opts.chequebook,
        chain_id: opts.chain_id,
        acct: Arc::new(Mirror::default()),
        ledger,
        issuable,
        issued_plur: AtomicU64::new(0),
        max_issue_plur: opts.max_issue_plur,
        exchange_rate: None,
        alive: alive.clone(),
        threshold_rx,
        cheques: 0,
        cheque_units: 0,
        cheque_plur: 0,
        refreshes: 0,
        refresh_units: 0,
        last_refresh: None,
        last_cheque: None,
        unvalidated: VecDeque::new(),
        ev,
    };
    let threshold_first = conn.threshold();
    conn.ev.line(&format!(
        "\"ev\":\"start\",\"overlay\":\"{}\",\"threshold\":{threshold_first},\"distinct_chunks\":{}",
        hex::encode(target.overlay),
        chunks.len()
    ));

    let mut cycle_idx = 0usize;
    let mut lambda_ms: Vec<Option<u64>> = Vec::new();
    let mut spend_capped = false;
    let mut all_errors: Vec<String> = Vec::new();

    // --- Phase A: growth ---
    let growth = if opts.growth_secs > 0 {
        let out = run_fetch_phase(
            &mut conn,
            &chunks,
            &mut cycle_idx,
            Duration::from_secs(opts.growth_secs),
            &PhaseMode::Growth,
            "growth",
        )
        .await;
        spend_capped |= out.spend_capped;
        all_errors.extend(out.errors);
        out.stats
    } else {
        PhaseStats::default()
    };

    // --- Phase B: λ samples ---
    for _ in 0..opts.lambda_samples {
        if !conn.alive.load(Ordering::Relaxed) || spend_capped {
            break;
        }
        lambda_ms.push(lambda_sample(&mut conn, &chunks, &mut cycle_idx).await);
    }

    // --- Phase C: λ-aware ceiling ---
    let ceiling = if opts.ceiling_secs > 0 && conn.alive.load(Ordering::Relaxed) && !spend_capped {
        let lambda_max = lambda_ms.iter().flatten().max().copied().unwrap_or(3000);
        let window = Duration::from_millis(lambda_max.max(800) * 3 / 2);
        let out = run_fetch_phase(
            &mut conn,
            &chunks,
            &mut cycle_idx,
            Duration::from_secs(opts.ceiling_secs),
            &PhaseMode::Ceiling {
                lambda_window: window,
            },
            "ceiling",
        )
        .await;
        spend_capped |= out.spend_capped;
        all_errors.extend(out.errors);
        Some(out.stats)
    } else {
        None
    };
    // --- final sweep + bee-side zero confirmation ---
    let mut residual_zero_confirmed = None;
    for _ in 0..3 {
        let (debt, _) = conn.debt();
        if debt == 0 {
            break;
        }
        if !conn.alive.load(Ordering::Relaxed) {
            break;
        }
        if let Err(err) = conn.emit(debt).await {
            tracing::warn!("final sweep cheque failed: {err}");
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
    if conn.alive.load(Ordering::Relaxed) {
        // Wait out the slowest plausible validation, then ask bee.
        let wait = lambda_ms
            .iter()
            .flatten()
            .max()
            .copied()
            .unwrap_or(3000)
            .max(2500)
            + 1500;
        tokio::time::sleep(Duration::from_millis(wait)).await;
        match refresh_probe(&mut conn.control, conn.peer_id, LAMBDA_PROBE_UNITS).await {
            Ok(accepted) => {
                residual_zero_confirmed = Some(accepted == 0);
                conn.ev.line(&format!(
                    "\"ev\":\"final_confirm\",\"accepted\":{accepted}"
                ));
            }
            Err(err) => tracing::warn!("final confirmation probe failed: {err}"),
        }
    }

    let (done_tx, done_rx) = oneshot::channel();
    let _ = bye_tx.send(done_tx);
    let _ = tokio::time::timeout(Duration::from_secs(3), done_rx).await;

    let threshold_last = conn.threshold();
    // Upgrades = threshold delta over the light-peer step (450 k), the
    // robust count even if the watch coalesced an announcement.
    let upgrades = u32::try_from(threshold_last.saturating_sub(threshold_first) / 450_000)
        .unwrap_or(u32::MAX);
    conn.ev.line(&format!(
        "\"ev\":\"end\",\"threshold\":{threshold_last},\"cheques\":{},\"cheque_units\":{},\"cheque_plur\":\"{}\",\"refresh_units\":{}",
        conn.cheques, conn.cheque_units, conn.cheque_plur, conn.refresh_units
    ));
    if !all_errors.is_empty() {
        tracing::info!("first fetch errors: {:?}", &all_errors[..all_errors.len().min(5)]);
    }

    Ok(GrowthReport {
        peer_id,
        remote_overlay: info.remote_overlay,
        remote_eth: info.remote_eth_address,
        handshake_ms,
        threshold_first,
        threshold_last,
        upgrades_observed: upgrades,
        growth,
        ceiling,
        lambda_ms,
        distinct_chunks: chunks.len(),
        cheques: conn.cheques,
        cheque_units: conn.cheque_units,
        cheque_plur: conn.cheque_plur,
        refreshes: conn.refreshes,
        refresh_units: conn.refresh_units,
        residual_zero_confirmed,
        spend_capped,
        wall: run_started.elapsed(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ack_amount() {
        // Amount = 0x0c350 (50000), Timestamp = 1234
        let body = [0x0a, 0x03, 0x00, 0xc3, 0x50, 0x10, 0xd2, 0x09];
        assert_eq!(parse_ack_amount(&body), Some(50_000));
    }

    #[test]
    fn parses_zero_ack() {
        // Empty Amount bytes (Go big.Int zero), Timestamp only.
        let body = [0x0a, 0x00, 0x10, 0x01];
        assert_eq!(parse_ack_amount(&body), Some(0));
        // Entirely empty ack body is also zero.
        assert_eq!(parse_ack_amount(&[]), Some(0));
    }
}
