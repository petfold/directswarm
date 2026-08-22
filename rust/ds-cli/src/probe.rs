//! `directswarm probe-storer` — M2 dev command: one direct, settled
//! storer stream, measured. Reads the Phase-0 chunk inventory, selects
//! the chunks the target storer's neighborhood holds, retrieves them
//! over a direct libp2p connection with accounting + pseudosettle +
//! SWAP cheques, and reports svcrate-comparable numbers.

use clap::Args;
use ds_core::proximity;
use std::path::PathBuf;

#[derive(Args)]
pub struct ProbeArgs {
    /// Target underlay multiaddr, ending in /p2p/<peer-id>.
    #[arg(long)]
    pub underlay: String,
    /// Target overlay (64 hex).
    #[arg(long)]
    pub overlay: String,
    /// Chunk inventory CSV with an `address_hex` column (Phase-0 format).
    #[arg(long, default_value = "../.phase0/chunks.csv")]
    pub chunks_csv: PathBuf,
    /// Only request chunks with proximity(chunk, overlay) >= this
    /// (the storer's neighborhood depth; mainnet ~9-10).
    #[arg(long, default_value_t = 9)]
    pub min_po: u8,
    /// Max chunks to request.
    #[arg(long, default_value_t = 200)]
    pub max_chunks: usize,
    /// Outstanding requests on the stream (Phase-0 etiquette cap: 32).
    #[arg(long, default_value_t = 16)]
    pub pipeline_depth: usize,
    /// Web3 v3 keystore holding the settlement key.
    #[arg(long, default_value = "../.phase0/reach-data/keys/swarm.key")]
    pub swarm_key: PathBuf,
    /// Keystore password.
    #[arg(long, default_value = "directswarm-reach")]
    pub password: String,
    /// Overlay nonce file (created on first run).
    #[arg(long, default_value = "../.phase1/identity/overlay-nonce.hex")]
    pub nonce_file: PathBuf,
    /// Outbound cheque ledger path.
    #[arg(long, default_value = "../.phase1/identity/outbound-cheques.json")]
    pub ledger: PathBuf,
    /// Our chequebook contract (0x hex).
    #[arg(long, default_value = "0xE8C7aD1Af8CAb91E2695EfD1a12dBfCc186dFD41")]
    pub chequebook: String,
    /// Gnosis RPC for the one-time cached-invariant read.
    #[arg(long, default_value = "https://rpc.gnosischain.com")]
    pub rpc_url: String,
    /// Spend guard: max PLUR issued as cheques this run
    /// (default 5e12 PLUR = 0.0005 xBZZ; a 200-chunk run cheques
    /// ~1e12). TODO(M4): a tripped guard must stop FETCHING —
    /// blocking the final sweep leaves debt unsettled (run 6 left
    /// 450k units when the guard was 1e12).
    #[arg(long, default_value_t = 5_000_000_000_000)]
    pub max_issue_plur: u64,
    #[arg(long, default_value_t = 1)]
    pub network_id: u64,
    #[arg(long, default_value_t = 100)]
    pub chain_id: u64,
    /// Append a result row to this CSV (header written if new).
    #[arg(long, default_value = "../.phase1/m2-probe.csv")]
    pub csv_out: PathBuf,
}

fn parse_hex<const N: usize>(label: &str, s: &str) -> Result<[u8; N], String> {
    let s = s.trim().trim_start_matches("0x").trim_start_matches("0X");
    let mut out = [0u8; N];
    hex::decode_to_slice(s, &mut out).map_err(|e| format!("{label}: {e}"))?;
    Ok(out)
}

fn load_chunk_addresses(args: &ProbeArgs, overlay: &[u8; 32]) -> Result<Vec<[u8; 32]>, String> {
    let text = std::fs::read_to_string(&args.chunks_csv)
        .map_err(|e| format!("read {}: {e}", args.chunks_csv.display()))?;
    let mut lines = text.lines();
    let header = lines.next().ok_or("chunks csv is empty")?;
    let addr_col = header
        .split(',')
        .position(|c| c.trim() == "address_hex")
        .ok_or("chunks csv has no address_hex column")?;
    let mut selected = Vec::new();
    for line in lines {
        let Some(cell) = line.split(',').nth(addr_col) else {
            continue;
        };
        let Ok(addr) = parse_hex::<32>("chunk", cell) else {
            continue;
        };
        if proximity(&addr, overlay) >= args.min_po {
            selected.push(addr);
            if selected.len() >= args.max_chunks {
                break;
            }
        }
    }
    Ok(selected)
}

fn median(sorted: &[u64]) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[sorted.len() / 2]
}

fn percentile95(sorted: &[u64]) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[(sorted.len() * 95 / 100).min(sorted.len() - 1)]
}

#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
pub async fn run(args: ProbeArgs) -> i32 {
    let overlay = match parse_hex::<32>("overlay", &args.overlay) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: {e}");
            return 2;
        }
    };
    let chequebook = match parse_hex::<20>("chequebook", &args.chequebook) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: {e}");
            return 2;
        }
    };
    let underlay: libp2p::Multiaddr = match args.underlay.parse() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: underlay: {e}");
            return 2;
        }
    };
    let chunks = match load_chunk_addresses(&args, &overlay) {
        Ok(v) if !v.is_empty() => v,
        Ok(_) => {
            eprintln!(
                "error: no chunks with PO >= {} for overlay {}",
                args.min_po, args.overlay
            );
            return 2;
        }
        Err(e) => {
            eprintln!("error: {e}");
            return 2;
        }
    };
    eprintln!(
        "selected {} chunks with PO >= {} for storer {}…",
        chunks.len(),
        args.min_po,
        &args.overlay[..12]
    );

    let identity = match ds_net::Identity::load(
        &args.swarm_key,
        &args.password,
        &args.nonce_file,
        args.network_id,
    ) {
        Ok(id) => id,
        Err(e) => {
            eprintln!("error: identity: {e:#}");
            return 1;
        }
    };
    eprintln!(
        "identity: eth 0x{} overlay {} (light)",
        hex::encode(identity.eth),
        hex::encode(identity.overlay)
    );

    let target = ds_net::ProbeTarget { underlay, overlay };
    let opts = ds_net::ProbeOptions {
        network_id: args.network_id,
        chain_id: args.chain_id,
        chequebook,
        rpc_url: args.rpc_url.clone(),
        ledger_path: args.ledger.clone(),
        pipeline_depth: args.pipeline_depth.min(32),
        max_issue_plur: args.max_issue_plur,
    };

    let requested = chunks.len();
    let report = match ds_net::probe_storer(&identity, &target, chunks, &opts).await {
        Ok(report) => report,
        Err(e) => {
            eprintln!("error: probe: {e:#}");
            return 1;
        }
    };

    let mut lat = report.latencies_ms.clone();
    lat.sort_unstable();
    let secs = report.wall.as_secs_f64();
    let mbs = if secs > 0.0 {
        report.bytes as f64 / 1e6 / secs
    } else {
        0.0
    };
    println!("== M2 probe report ==");
    println!("peer:            {}", report.peer_id);
    println!("remote overlay:  {}", hex::encode(report.remote_overlay));
    println!("remote eth:      0x{}", hex::encode(report.remote_eth));
    println!("handshake:       {} ms", report.handshake_ms);
    println!(
        "chunks:          {}/{} ok ({} errors)",
        report.chunks_ok, requested, report.chunks_err
    );
    println!(
        "throughput:      {} bytes in {secs:.1}s = {mbs:.3} MB/s (pipeline {})",
        report.bytes, opts.pipeline_depth
    );
    println!(
        "latency ms:      p50 {} p95 {}",
        median(&lat),
        percentile95(&lat)
    );
    let s = &report.settlement;
    println!(
        "settlement:      {} cheques = {} units = {} PLUR (rate {}); {} refreshes = {} units; residual debt {} units",
        s.cheques_issued,
        s.cheque_units,
        s.cheque_plur,
        s.exchange_rate.map_or_else(|| "-".into(), |r| r.to_string()),
        s.refreshes_accepted,
        s.refresh_units,
        s.residual_debt_units
    );
    match &s.announced_threshold {
        Some(t) => println!("announced payment threshold: {t} PLUR"),
        None => println!("announced payment threshold: (none received)"),
    }
    if !report.errors.is_empty() {
        eprintln!("first errors:");
        for err in report.errors.iter().take(5) {
            eprintln!("  {err}");
        }
    }

    if let Err(e) = append_csv(&args, requested, &report, mbs, &lat) {
        eprintln!("warning: csv append failed: {e}");
    }
    i32::from(report.chunks_ok == 0)
}

#[allow(clippy::cast_precision_loss)]
fn append_csv(
    args: &ProbeArgs,
    requested: usize,
    report: &ds_net::ProbeReport,
    mbs: f64,
    sorted_lat: &[u64],
) -> Result<(), String> {
    use std::io::Write;
    if let Some(parent) = args.csv_out.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let new = !args.csv_out.exists();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&args.csv_out)
        .map_err(|e| e.to_string())?;
    if new {
        writeln!(file, "peer_overlay,requested,chunks_ok,chunks_err,bytes,wall_secs,mb_per_s,pipeline_depth,handshake_ms,lat_p50_ms,lat_p95_ms,cheques,cheque_units,cheque_plur,exchange_rate,refreshes,refresh_units,residual_debt_units,announced_threshold").map_err(|e| e.to_string())?;
    }
    writeln!(
        file,
        "{},{requested},{},{},{},{:.2},{mbs:.4},{},{},{},{},{},{},{},{},{},{},{},{}",
        hex::encode(report.remote_overlay),
        report.chunks_ok,
        report.chunks_err,
        report.bytes,
        report.wall.as_secs_f64(),
        args.pipeline_depth.min(32),
        report.handshake_ms,
        median(sorted_lat),
        percentile95(sorted_lat),
        report.settlement.cheques_issued,
        report.settlement.cheque_units,
        report.settlement.cheque_plur,
        report
            .settlement
            .exchange_rate
            .map_or_else(String::new, |r| r.to_string()),
        report.settlement.refreshes_accepted,
        report.settlement.refresh_units,
        report.settlement.residual_debt_units,
        report
            .settlement
            .announced_threshold
            .map_or_else(String::new, |t| t.to_string()),
    )
    .map_err(|e| e.to_string())
}
