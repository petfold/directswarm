//! `directswarm crawl` — M3: build the topology cache with a bounded
//! polite hive crawl, then report coverage of the Phase-0 payload's
//! chunk set. Seeds from known dialable full nodes (Phase-0 reach.csv
//! by default) and snowballs via gossip.

use clap::Args;
use ds_core::SwarmAddress;
use libp2p::Multiaddr;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Args)]
pub struct CrawlArgs {
    /// Seed multiaddrs (repeatable). If none, seeds are read from
    /// `--seed-csv`.
    #[arg(long)]
    pub seed: Vec<String>,
    /// CSV with an `underlay` column of `|`-separated multiaddrs
    /// (Phase-0 reach.csv format).
    #[arg(long, default_value = "../.phase0/reach.csv")]
    pub seed_csv: PathBuf,
    /// How many seeds to take from the CSV.
    #[arg(long, default_value_t = 4)]
    pub seed_count: usize,
    /// Chunk inventory CSV (for the coverage report).
    #[arg(long, default_value = "../.phase0/chunks.csv")]
    pub chunks_csv: PathBuf,
    /// Neighborhood depth for coverage (mainnet ~9).
    #[arg(long, default_value_t = 9)]
    pub depth: u8,
    /// Hard cap on distinct peers dialed.
    #[arg(long, default_value_t = 40)]
    pub max_dials: usize,
    /// Milliseconds between dial attempts (rate limit).
    #[arg(long, default_value_t = 500)]
    pub dial_interval_ms: u64,
    /// Seconds to harvest gossip per peer.
    #[arg(long, default_value_t = 4)]
    pub harvest_secs: u64,
    /// Overall wall-clock cap in seconds.
    #[arg(long, default_value_t = 600)]
    pub wall_secs: u64,
    #[arg(long, default_value = "../.phase0/reach-data/keys/swarm.key")]
    pub swarm_key: PathBuf,
    #[arg(long, default_value = "directswarm-reach")]
    pub password: String,
    #[arg(long, default_value = "../.phase1/identity/overlay-nonce.hex")]
    pub nonce_file: PathBuf,
    #[arg(long, default_value_t = 1)]
    pub network_id: u64,
    /// Write the topology cache here as CSV.
    #[arg(long, default_value = "../.phase1/m3-topology.csv")]
    pub out_csv: PathBuf,
}

fn public_dialable(underlay_field: &str) -> Option<Multiaddr> {
    underlay_field
        .split('|')
        .filter(|u| {
            u.starts_with("/ip4/")
                && !u.starts_with("/ip4/10.")
                && !u.starts_with("/ip4/192.168")
                && !u.starts_with("/ip4/172.")
                && !u.contains("/ws")
        })
        .find_map(|u| u.parse::<Multiaddr>().ok())
}

fn load_seeds(args: &CrawlArgs) -> Result<Vec<Multiaddr>, String> {
    if !args.seed.is_empty() {
        return args
            .seed
            .iter()
            .map(|s| s.parse::<Multiaddr>().map_err(|e| format!("{s}: {e}")))
            .collect();
    }
    let text = std::fs::read_to_string(&args.seed_csv)
        .map_err(|e| format!("read {}: {e}", args.seed_csv.display()))?;
    let mut lines = text.lines();
    let header = lines.next().ok_or("seed csv empty")?;
    let (ucol, okcol) = {
        let cols: Vec<&str> = header.split(',').map(str::trim).collect();
        (
            cols.iter().position(|c| *c == "underlay"),
            cols.iter().position(|c| *c == "dial_ok"),
        )
    };
    let ucol = ucol.ok_or("seed csv has no underlay column")?;
    let mut seeds = Vec::new();
    for line in lines {
        let cells: Vec<&str> = line.split(',').collect();
        if let Some(ok) = okcol {
            if cells.get(ok).map(|c| c.trim()) != Some("1") {
                continue;
            }
        }
        if let Some(addr) = cells.get(ucol).and_then(|f| public_dialable(f)) {
            seeds.push(addr);
            if seeds.len() >= args.seed_count {
                break;
            }
        }
    }
    if seeds.is_empty() {
        return Err("no dialable seeds found".into());
    }
    Ok(seeds)
}

fn load_chunks(path: &std::path::Path) -> Result<Vec<SwarmAddress>, String> {
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
            let mut addr = [0u8; 32];
            if hex::decode_to_slice(cell.trim(), &mut addr).is_ok() {
                out.push(addr);
            }
        }
    }
    Ok(out)
}

#[allow(clippy::cast_precision_loss)]
pub async fn run(args: CrawlArgs) -> i32 {
    let seeds = match load_seeds(&args) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: {e}");
            return 2;
        }
    };
    let chunks = match load_chunks(&args.chunks_csv) {
        Ok(c) => c,
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
    eprintln!(
        "crawl: {} seeds, max {} dials at 1/{}ms, harvest {}s/peer, cap {}s",
        seeds.len(),
        args.max_dials,
        args.dial_interval_ms,
        args.harvest_secs,
        args.wall_secs
    );

    let opts = ds_net::crawl::CrawlOptions {
        network_id: args.network_id,
        max_dials: args.max_dials,
        dial_interval: Duration::from_millis(args.dial_interval_ms),
        dial_timeout: Duration::from_secs(20),
        harvest_window: Duration::from_secs(args.harvest_secs),
        wall_cap: Duration::from_secs(args.wall_secs),
    };
    let (cache, stats) = match ds_net::crawl::crawl(&identity, seeds, &opts).await {
        Ok(pair) => pair,
        Err(e) => {
            eprintln!("error: crawl: {e:#}");
            return 1;
        }
    };

    let cov = cache.coverage(&chunks, args.depth);
    let bins = cache.bin_counts();
    println!("== M3 crawl report ==");
    println!("stop reason:     {}", stats.stop_reason);
    println!(
        "dials:           {}/{} ok; {} gossip hints seen",
        stats.dials_ok, stats.dials_attempted, stats.hints_seen
    );
    println!(
        "cache:           {} nodes ({} dialed+verified)",
        cache.len(),
        cache.records().filter(|r| r.dialed_ok).count()
    );
    let nonempty: Vec<String> = bins
        .iter()
        .enumerate()
        .filter(|(_, n)| **n > 0)
        .map(|(po, n)| format!("po{po}:{n}"))
        .collect();
    println!("bins:            {}", nonempty.join(" "));
    println!(
        "payload coverage @depth {}: {}/{} chunks ({:.1}%), {}/{} neighborhoods",
        args.depth,
        cov.chunks_covered,
        cov.chunks_total,
        100.0 * cov.chunks_covered as f64 / cov.chunks_total.max(1) as f64,
        cov.neighborhoods_covered,
        cov.neighborhoods_total
    );

    if let Err(e) = write_cache_csv(&args.out_csv, &cache) {
        eprintln!("warning: cache csv write failed: {e}");
    } else {
        eprintln!("topology cache written to {}", args.out_csv.display());
    }
    i32::from(cache.is_empty())
}

fn write_cache_csv(path: &std::path::Path, cache: &ds_core::TopologyCache) -> Result<(), String> {
    use std::io::Write;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let mut f = std::fs::File::create(path).map_err(|e| e.to_string())?;
    writeln!(f, "overlay_hex,dialed_ok,rtt_ms,last_seen_ms,underlays")
        .map_err(|e| e.to_string())?;
    for r in cache.records() {
        writeln!(
            f,
            "{},{},{},{},{}",
            hex::encode(r.overlay),
            u8::from(r.dialed_ok),
            r.rtt_ms.map_or_else(String::new, |v| v.to_string()),
            r.last_seen_tick,
            r.underlays.join("|"),
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}
