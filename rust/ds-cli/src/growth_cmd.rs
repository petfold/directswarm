//! `directswarm probe-growth` — M5 groundwork: one long-lived settled
//! storer connection measuring bee's per-peer threshold growth, the
//! peer's cheque-validation latency λ, and the sustained rate at the
//! grown threshold. See `ds_net::growth` for the mechanism.

use clap::Args;
use ds_core::proximity;
use std::path::PathBuf;

#[derive(Args)]
pub struct GrowthArgs {
    /// Target underlay multiaddr, ending in /p2p/<peer-id>.
    #[arg(long)]
    pub underlay: String,
    /// Target overlay (64 hex).
    #[arg(long)]
    pub overlay: String,
    /// Chunk inventory CSV with an `address_hex` column (Phase-0 format).
    #[arg(long, default_value = "../.phase0/chunks.csv")]
    pub chunks_csv: PathBuf,
    /// Only request chunks with proximity(chunk, overlay) >= this.
    #[arg(long, default_value_t = 9)]
    pub min_po: u8,
    /// Cap on DISTINCT chunks loaded (they are cycled for sustained load).
    #[arg(long, default_value_t = 600)]
    pub max_chunks: usize,
    /// Web3 v3 keystore holding the settlement key.
    #[arg(long, default_value = "../.phase0/reach-data/keys/swarm.key")]
    pub swarm_key: PathBuf,
    /// Keystore password.
    #[arg(long, default_value = "directswarm-reach")]
    pub password: String,
    /// Overlay nonce file.
    #[arg(long, default_value = "../.phase1/identity/overlay-nonce.hex")]
    pub nonce_file: PathBuf,
    /// Outbound cheque ledger path (persisted cumulative per beneficiary).
    #[arg(long, default_value = "../.phase1/identity/outbound-cheques.json")]
    pub ledger: PathBuf,
    /// Our chequebook contract (0x hex).
    #[arg(long, default_value = "0xE8C7aD1Af8CAb91E2695EfD1a12dBfCc186dFD41")]
    pub chequebook: String,
    /// Gnosis RPC for the one-time cached-invariant read.
    #[arg(long, default_value = "https://rpc.gnosischain.com")]
    pub rpc_url: String,
    /// Spend guard: max PLUR issued as cheques this run. Default 3e14
    /// PLUR = 0.03 xBZZ — full linear threshold growth settles ~8.1e13,
    /// the ceiling phase adds rate-dependent spend on top. The guard
    /// gates FETCHING (projected), never the final sweep.
    #[arg(long, default_value_t = 300_000_000_000_000)]
    pub max_issue_plur: u64,
    /// Phase A (growth) wall cap in seconds. 0 skips to λ sampling —
    /// use for quick per-peer λ measurements.
    #[arg(long, default_value_t = 1200)]
    pub growth_secs: u64,
    /// Phase C (λ-aware ceiling) wall cap in seconds. 0 skips it.
    #[arg(long, default_value_t = 120)]
    pub ceiling_secs: u64,
    /// Number of validation-latency samples between growth and ceiling.
    #[arg(long, default_value_t = 3)]
    pub lambda_samples: u32,
    /// Prepay this many CHUNKS' worth of units as one up-front cheque
    /// before the ceiling phase (0 = off). Tests surplus-funded,
    /// throttle-free serving.
    #[arg(long, default_value_t = 0)]
    pub prepay_chunks: u64,
    /// Concurrent in-flight chunk requests (etiquette cap 32).
    #[arg(long, default_value_t = 16)]
    pub pipeline: usize,
    #[arg(long, default_value_t = 1)]
    pub network_id: u64,
    #[arg(long, default_value_t = 100)]
    pub chain_id: u64,
    /// JSONL event log; default ../.phase1/growth/<overlay16>.jsonl.
    #[arg(long)]
    pub jsonl_out: Option<PathBuf>,
    /// Append a summary row to this CSV (header written if new).
    #[arg(long, default_value = "../.phase1/m5-growth.csv")]
    pub csv_out: PathBuf,
}

fn parse_hex<const N: usize>(label: &str, s: &str) -> Result<[u8; N], String> {
    let s = s.trim().trim_start_matches("0x").trim_start_matches("0X");
    let mut out = [0u8; N];
    hex::decode_to_slice(s, &mut out).map_err(|e| format!("{label}: {e}"))?;
    Ok(out)
}

fn load_chunk_addresses(
    csv: &PathBuf,
    overlay: &[u8; 32],
    min_po: u8,
    max: usize,
) -> Result<Vec<[u8; 32]>, String> {
    let text =
        std::fs::read_to_string(csv).map_err(|e| format!("read {}: {e}", csv.display()))?;
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
        if proximity(&addr, overlay) >= min_po {
            selected.push(addr);
            if selected.len() >= max {
                break;
            }
        }
    }
    Ok(selected)
}

#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
pub async fn run(args: GrowthArgs) -> i32 {
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
    let chunks = match load_chunk_addresses(&args.chunks_csv, &overlay, args.min_po, args.max_chunks)
    {
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
    let jsonl_path = args.jsonl_out.clone().unwrap_or_else(|| {
        PathBuf::from(format!("../.phase1/growth/{}.jsonl", &args.overlay[..16]))
    });
    eprintln!(
        "probe-growth: storer {}…, {} distinct chunks (cycled), growth<= {}s, λ×{}, ceiling<= {}s, events -> {}",
        &args.overlay[..12],
        chunks.len(),
        args.growth_secs,
        args.lambda_samples,
        args.ceiling_secs,
        jsonl_path.display()
    );

    let target = ds_net::ProbeTarget { underlay, overlay };
    let opts = ds_net::GrowthOptions {
        network_id: args.network_id,
        chain_id: args.chain_id,
        chequebook,
        rpc_url: args.rpc_url.clone(),
        ledger_path: args.ledger.clone(),
        pipeline_depth: args.pipeline.clamp(1, 32),
        max_issue_plur: args.max_issue_plur,
        growth_secs: args.growth_secs,
        ceiling_secs: args.ceiling_secs,
        lambda_samples: args.lambda_samples,
        prepay_units: args.prepay_chunks
            * chunks
                .first()
                .map_or(220_000, |a| ds_net::peer_price_for(&overlay, a)),
        jsonl_path,
    };
    let report = match ds_net::probe_growth(&identity, &target, chunks, &opts).await {
        Ok(report) => report,
        Err(e) => {
            eprintln!("error: probe-growth: {e:#}");
            return 1;
        }
    };

    let lam: Vec<String> = report
        .lambda_ms
        .iter()
        .map(|l| l.map_or_else(|| "timeout".into(), |ms| format!("{ms}ms")))
        .collect();
    println!("== probe-growth report ==");
    println!("peer:              {}", report.peer_id);
    println!("remote overlay:    {}", hex::encode(report.remote_overlay));
    println!(
        "threshold:         {} -> {} ({} upgrades observed)",
        report.threshold_first, report.threshold_last, report.upgrades_observed
    );
    println!(
        "growth phase:      {:.0}s, {} fetches ok / {} err, {:.4} MB/s avg, {} units settled",
        report.growth.wall_s,
        report.growth.fetches_ok,
        report.growth.fetches_err,
        report.growth.mbs,
        report.growth.units_settled
    );
    println!("validation λ:      [{}]", lam.join(", "));
    match &report.ceiling {
        Some(c) => println!(
            "ceiling phase:     {:.0}s, {} fetches ok / {} err, {:.4} MB/s avg at T={}",
            c.wall_s, c.fetches_ok, c.fetches_err, c.mbs, c.threshold_end
        ),
        None => println!("ceiling phase:     (skipped)"),
    }
    println!(
        "settlement:        {} cheques = {} units = {} PLUR; {} refreshes = {} units",
        report.cheques,
        report.cheque_units,
        report.cheque_plur,
        report.refreshes,
        report.refresh_units
    );
    println!(
        "zero-debt (bee):   {}",
        report.residual_zero_confirmed.map_or_else(
            || "unconfirmed (probe failed)".into(),
            |z| if z {
                "CONFIRMED by peer".to_owned()
            } else {
                "NOT ZERO — check event log".to_owned()
            }
        )
    );
    if report.spend_capped {
        println!("spend guard:       HIT (fetching stopped early, sweep still ran)");
    }

    if let Err(e) = append_csv(&args, &report) {
        eprintln!("warning: csv append failed: {e}");
    }
    0
}

fn append_csv(args: &GrowthArgs, r: &ds_net::GrowthReport) -> Result<(), String> {
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
        writeln!(file, "peer_overlay,threshold_first,threshold_last,upgrades,growth_wall_s,growth_fetches_ok,growth_fetches_err,growth_mbs,growth_units,lambda1_ms,lambda2_ms,lambda3_ms,ceiling_wall_s,ceiling_fetches_ok,ceiling_mbs,cheques,cheque_units,cheque_plur,refresh_units,zero_confirmed,spend_capped").map_err(|e| e.to_string())?;
    }
    let lam = |i: usize| -> String {
        r.lambda_ms
            .get(i)
            .copied()
            .flatten()
            .map_or_else(String::new, |v| v.to_string())
    };
    writeln!(
        file,
        "{},{},{},{},{:.1},{},{},{:.4},{},{},{},{},{:.1},{},{:.4},{},{},{},{},{},{}",
        hex::encode(r.remote_overlay),
        r.threshold_first,
        r.threshold_last,
        r.upgrades_observed,
        r.growth.wall_s,
        r.growth.fetches_ok,
        r.growth.fetches_err,
        r.growth.mbs,
        r.growth.units_settled,
        lam(0),
        lam(1),
        lam(2),
        r.ceiling.as_ref().map_or(0.0, |c| c.wall_s),
        r.ceiling.as_ref().map_or(0, |c| c.fetches_ok),
        r.ceiling.as_ref().map_or(0.0, |c| c.mbs),
        r.cheques,
        r.cheque_units,
        r.cheque_plur,
        r.refresh_units,
        r.residual_zero_confirmed.map_or(-1i8, i8::from),
        i8::from(r.spend_capped),
    )
    .map_err(|e| e.to_string())
}
