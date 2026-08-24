//! `directswarm fetch-direct` — M4: fetch a payload's chunks across N
//! settled storer connections (topology cache + bee fallback), then
//! optionally reassemble + byte-verify. Reports aggregate throughput
//! for the connection-scaling measurement.

use clap::Args;
use ds_core::{NodeRecord, SwarmAddress, TopologyCache};
use std::path::PathBuf;

#[derive(Args)]
#[allow(clippy::struct_excessive_bools)] // clap flags, not state
pub struct FetchDirectArgs {
    /// 64-hex bytes root of the payload (for reassembly/verify).
    #[arg(long)]
    pub reference: Option<String>,
    /// Chunk inventory CSV (`address_hex` column).
    #[arg(long, default_value = "../.phase0/chunks.csv")]
    pub chunks_csv: PathBuf,
    /// Merged topology cache CSV from M3.
    #[arg(long, default_value = "../.phase1/topology.csv")]
    pub topology_csv: PathBuf,
    /// Cap on chunks to fetch this run (spend/time budget; 0 = all).
    #[arg(long, default_value_t = 0)]
    pub max_chunks: usize,
    /// Number of concurrent storer connections.
    #[arg(long, default_value_t = 20)]
    pub connections: usize,
    /// Per-connection pipeline depth (measured: a storer saturates one
    /// client connection at ~8 in flight; deeper only queues there).
    #[arg(long, default_value_t = 8)]
    pub depth: usize,
    /// Neighborhood depth for storer↔chunk matching.
    #[arg(long, default_value_t = 9)]
    pub nbhd_depth: u8,
    /// Global cheque spend cap in PLUR (safety budget). Default 1e10 =
    /// 0.0000001 xBZZ — a *bounded-slice* measurement budget, not a
    /// full-payload run.
    #[arg(long, default_value_t = 10_000_000_000)]
    pub max_issue_plur: u64,
    /// On-disk chunk store base path.
    #[arg(long, default_value = "../.phase1/m4-store")]
    pub store_base: PathBuf,
    /// Persisted per-peer settlement state (threshold, λ, volume).
    #[arg(long, default_value = "../.phase1/peerstate.csv")]
    pub peerstate: PathBuf,
    /// Measure cheque-validation latency inline on first contact with
    /// unknown peers (~5–30 s once per peer, persisted).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub measure_lambda: bool,
    /// Storers per bucket: 2+ hedges slow-storer tails by
    /// work-stealing within the shared bucket.
    #[arg(long, default_value_t = 1)]
    pub redundancy: usize,
    /// Wind down gracefully (sweep + exit, leftovers to fallback) when
    /// the direct plane trickles below 40 chunks/20 s.
    #[arg(long, default_value_t = false)]
    pub stall_exit: bool,
    /// Prepay-first settlement (one up-front cheque per storer + slice
    /// top-ups, converging to the exact consumed amount).
    #[arg(long, default_value_t = false)]
    pub prepay: bool,
    /// Fetch the payload this many times over PERSISTENT connections
    /// (daemon-warm benchmark: iteration 2+ skips all connection
    /// setup). The local store is cleared between iterations.
    #[arg(long, default_value_t = 1)]
    pub repeat: u32,
    /// Measurement mode: drop chunks no connection covers instead of
    /// using the bee fallback, so reported throughput is the direct
    /// plane's alone.
    #[arg(long, default_value_t = false)]
    pub direct_only: bool,
    /// Reassemble + byte-verify after fetch (needs --reference and a
    /// complete chunk set in the store).
    #[arg(long, default_value_t = false)]
    pub verify: bool,
    /// Output file for --verify.
    #[arg(long)]
    pub output: Option<PathBuf>,
    #[arg(long, default_value = "http://localhost:1633")]
    pub bee_url: String,
    #[arg(long, default_value = "../.phase0/reach-data/keys/swarm.key")]
    pub swarm_key: PathBuf,
    #[arg(long, default_value = "directswarm-reach")]
    pub password: String,
    #[arg(long, default_value = "../.phase1/identity/overlay-nonce.hex")]
    pub nonce_file: PathBuf,
    #[arg(long, default_value = "../.phase1/identity/outbound-cheques.json")]
    pub ledger: PathBuf,
    #[arg(long, default_value = "0xE8C7aD1Af8CAb91E2695EfD1a12dBfCc186dFD41")]
    pub chequebook: String,
    #[arg(long, default_value = "https://rpc.gnosischain.com")]
    pub rpc_url: String,
    #[arg(long, default_value_t = 1)]
    pub network_id: u64,
    #[arg(long, default_value_t = 100)]
    pub chain_id: u64,
    /// Append a result row here.
    #[arg(long, default_value = "../.phase1/m5-scaling.csv")]
    pub csv_out: PathBuf,
}

fn parse_hex<const N: usize>(label: &str, s: &str) -> Result<[u8; N], String> {
    let s = s.trim().trim_start_matches("0x").trim_start_matches("0X");
    let mut out = [0u8; N];
    hex::decode_to_slice(s, &mut out).map_err(|e| format!("{label}: {e}"))?;
    Ok(out)
}

fn load_chunks(path: &std::path::Path, max: usize) -> Result<Vec<SwarmAddress>, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let mut lines = text.lines();
    let header = lines.next().ok_or("chunks csv empty")?;
    let col = header
        .split(',')
        .position(|c| c.trim() == "address_hex")
        .ok_or("no address_hex column")?;
    let mut out = Vec::new();
    for line in lines {
        if let Some(cell) = line.split(',').nth(col) {
            if let Ok(a) = parse_hex::<32>("chunk", cell) {
                out.push(a);
                if max > 0 && out.len() >= max {
                    break;
                }
            }
        }
    }
    Ok(out)
}

fn load_topology(path: &std::path::Path, reference: SwarmAddress) -> Result<TopologyCache, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let mut cache = TopologyCache::new(reference);
    let mut lines = text.lines();
    let _ = lines.next(); // header
    for line in lines {
        let cols: Vec<&str> = line.split(',').collect();
        let (Some(oh), Some(dialed), Some(rtt), Some(seen), Some(under)) = (
            cols.first(),
            cols.get(1),
            cols.get(2),
            cols.get(3),
            cols.get(4),
        ) else {
            continue;
        };
        let Ok(overlay) = parse_hex::<32>("overlay", oh) else {
            continue;
        };
        cache.upsert(NodeRecord {
            overlay,
            underlays: under.split('|').map(ToString::to_string).collect(),
            rtt_ms: rtt.parse().ok(),
            last_seen_tick: seen.parse().unwrap_or(0),
            dialed_ok: *dialed == "1",
        });
    }
    Ok(cache)
}

#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
pub async fn run(args: FetchDirectArgs) -> i32 {
    let chunks = match load_chunks(&args.chunks_csv, args.max_chunks) {
        Ok(c) if !c.is_empty() => c,
        Ok(_) => {
            eprintln!("error: no chunks loaded");
            return 2;
        }
        Err(e) => {
            eprintln!("error: {e}");
            return 2;
        }
    };
    let chequebook = match parse_hex::<20>("chequebook", &args.chequebook) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}");
            return 2;
        }
    };
    // The topology reference overlay is our own; recompute after identity.
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
    let cache = match load_topology(&args.topology_csv, identity.overlay) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: topology: {e}");
            return 2;
        }
    };
    eprintln!(
        "fetch-direct: {} chunks, {} connections, depth {}, cache {} storers, spend cap {} PLUR",
        chunks.len(),
        args.connections,
        args.depth,
        cache.len(),
        args.max_issue_plur
    );

    let opts = ds_net::schedule::ScheduleOptions {
        network_id: args.network_id,
        chain_id: args.chain_id,
        chequebook,
        rpc_url: args.rpc_url.clone(),
        ledger_path: args.ledger.clone(),
        bee_url: args.bee_url.clone(),
        store_base: args.store_base.clone(),
        peerstate_path: args.peerstate.clone(),
        connections: args.connections,
        start_depth: args.depth,
        max_depth: args.depth,
        depth: args.nbhd_depth,
        max_issue_plur: args.max_issue_plur,
        measure_lambda: args.measure_lambda,
        redundancy: args.redundancy,
        direct_only: args.direct_only,
        stall_exit: args.stall_exit,
        prepay: args.prepay,
    };

    let warm_pool = std::sync::Arc::new(ds_net::schedule::ConnPool::default());
    let mut report = None;
    for iter in 1..=args.repeat.max(1) {
        if iter > 1 {
            // Cold local store per iteration; connections stay warm.
            let _ = std::fs::remove_file(args.store_base.with_extension("dat"));
            let _ = std::fs::remove_file(args.store_base.with_extension("idx"));
        }
        let started = std::time::Instant::now();
        match ds_net::schedule::fetch_scheduled(
            &identity,
            &cache,
            chunks.clone(),
            &opts,
            Some(warm_pool.clone()),
        )
        .await
        {
            Ok(r) => {
                eprintln!(
                    "iteration {iter}: {} direct in {:.1}s = {:.3} MB/s ({} warm conns parked)",
                    r.chunks_from_direct,
                    started.elapsed().as_secs_f64(),
                    r.direct_mbps(),
                    warm_pool.lock().map_or(0, |g| g.len())
                );
                report = Some(r);
            }
            Err(e) => {
                eprintln!("error: schedule (iteration {iter}): {e:#}");
                return 1;
            }
        }
    }
    let report = report.expect("at least one iteration");

    let direct_mbps = report.direct_mbps();
    let per_conn = if report.connections_opened > 0 {
        direct_mbps / report.connections_opened as f64
    } else {
        0.0
    };
    println!("== M4 fetch-direct report ==");
    println!("connections:     {} opened", report.connections_opened);
    println!(
        "chunks:          {} direct + {} fallback + {} dropped-uncovered + {} failed / {} total",
        report.chunks_from_direct,
        report.chunks_from_fallback,
        report.chunks_dropped_uncovered,
        report.chunks_failed,
        report.chunks_total
    );
    println!(
        "DIRECT plane:    {} bytes in {:.1}s = {:.3} MB/s aggregate ({:.4} MB/s per connection)",
        report.direct_bytes,
        report.wall.as_secs_f64(),
        direct_mbps,
        per_conn
    );
    println!(
        "fallback plane:  {} bytes = {:.3} MB/s (bee forwarding)",
        report.fallback_bytes,
        report.total_mbps() - direct_mbps
    );
    println!(
        "settlement:      {} cheques = {} PLUR; {} refresh units; residual {} units",
        report.cheques_issued, report.cheque_plur, report.refresh_units, report.residual_debt_units
    );
    println!(
        "peer learning:   {} λ measured this run; {}/{} connections zero-debt CONFIRMED by peer",
        report.lambdas_measured, report.zero_confirmed_conns, report.connections_opened
    );
    if report.surplus_parked_units > 0 {
        println!(
            "surplus parked:  {} units (~{:.5} xBZZ) prepaid-unconsumed, reusable at those peers",
            report.surplus_parked_units,
            report.surplus_parked_units as f64 * 1e5 / 1e16
        );
    }
    if !report.errors.is_empty() {
        eprintln!(
            "errors ({} shown of {}):",
            report.errors.len().min(5),
            report.errors.len()
        );
        for e in report.errors.iter().take(5) {
            eprintln!("  {e}");
        }
    }

    if let Err(e) = append_csv(&args, &report, direct_mbps, per_conn) {
        eprintln!("warning: csv append failed: {e}");
    }

    if args.verify {
        return verify(&args).await;
    }
    i32::from(report.chunks_from_direct == 0)
}

async fn verify(args: &FetchDirectArgs) -> i32 {
    let Some(reference) = &args.reference else {
        eprintln!("--verify needs --reference");
        return 2;
    };
    let root = match parse_hex::<32>("reference", reference) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: {e}");
            return 2;
        }
    };
    let store = match ds_net::store::ChunkStore::open(&args.store_base) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: store: {e}");
            return 1;
        }
    };
    let out_path = args
        .output
        .clone()
        .unwrap_or_else(|| PathBuf::from(format!("{}.bin", &reference[..16])));
    let fetcher = ds_net::store::StoreFetcher(&store);
    eprintln!(
        "reassembling from store ({} chunks) -> {}",
        store.len(),
        out_path.display()
    );
    match ds_net::fetch_to_file(&fetcher, root, &out_path, &|_, _| {}).await {
        Ok(outcome) => {
            eprintln!(
                "reassembled {} bytes -> {} (sha256 it against the reference)",
                outcome.total_span,
                out_path.display()
            );
            0
        }
        Err(e) => {
            eprintln!("error: reassembly failed (store likely incomplete): {e}");
            1
        }
    }
}

#[allow(clippy::cast_precision_loss)]
fn append_csv(
    args: &FetchDirectArgs,
    report: &ds_net::schedule::ScheduleReport,
    mbps: f64,
    per_conn: f64,
) -> Result<(), String> {
    use std::io::Write;
    if let Some(parent) = args.csv_out.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let new = !args.csv_out.exists();
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&args.csv_out)
        .map_err(|e| e.to_string())?;
    if new {
        writeln!(f, "connections,depth,chunks_direct,chunks_fallback,chunks_dropped,direct_bytes,wall_secs,direct_mb_per_s,mb_per_s_per_conn,cheques,cheque_plur,refresh_units,residual_units").map_err(|e| e.to_string())?;
    }
    writeln!(
        f,
        "{},{},{},{},{},{},{:.2},{mbps:.4},{per_conn:.4},{},{},{},{}",
        report.connections_opened,
        args.depth,
        report.chunks_from_direct,
        report.chunks_from_fallback,
        report.chunks_dropped_uncovered,
        report.direct_bytes,
        report.wall.as_secs_f64(),
        report.cheques_issued,
        report.cheque_plur,
        report.refresh_units,
        report.residual_debt_units,
    )
    .map_err(|e| e.to_string())
}
