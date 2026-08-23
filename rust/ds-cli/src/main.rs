//! directswarm CLI.
//!
//! M1 scope: `directswarm fetch <ref> [-o file]` over the forwarding
//! fallback (local bee node); the fast plane layers in from M2.

mod crawl;
mod fetchdirect;
mod fund;
mod growth_cmd;
mod probe;

use clap::{Parser, Subcommand};
use ds_net::BeeApiFetcher;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(
    name = "directswarm",
    version,
    about = "fast data plane for Ethereum Swarm"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Fetch the file behind a Swarm reference, verified chunk by chunk.
    Fetch {
        /// 64-hex Swarm reference (bytes root).
        reference: String,
        /// Output file (default: <first 16 hex of ref>.bin).
        #[arg(short, long)]
        output: Option<PathBuf>,
        /// Bee API of the local node used as the retrieval path (M1)
        /// and forwarding fallback (M2+).
        #[arg(long, default_value = "http://localhost:1633")]
        bee_url: String,
    },
    /// M2 dev probe: one direct, settled storer stream, measured.
    ProbeStorer(Box<probe::ProbeArgs>),
    /// M3: bounded polite hive crawl building the topology cache.
    Crawl(Box<crawl::CrawlArgs>),
    /// M4: multi-connection settled fetch across the topology cache.
    FetchDirect(Box<fetchdirect::FetchDirectArgs>),
    /// M5 groundwork: threshold-growth + cheque-validation-latency probe
    /// against one storer (long-lived settled connection).
    ProbeGrowth(Box<growth_cmd::GrowthArgs>),
    /// Deposit BZZ from the settlement wallet into its chequebook
    /// (spends real xBZZ; prints balances and the tx hash).
    FundChequebook(Box<fund::FundArgs>),
}

fn parse_ref(reference: &str) -> Result<[u8; 32], String> {
    let bytes = hex::decode(reference).map_err(|err| format!("reference is not hex: {err}"))?;
    <[u8; 32]>::try_from(bytes).map_err(|bytes| {
        format!(
            "reference must be 32 bytes (64 hex chars), got {} bytes \
             (encrypted 64-byte references land in a later milestone)",
            bytes.len()
        )
    })
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    match cli.command {
        Command::Fetch {
            reference,
            output,
            bee_url,
        } => {
            let code = fetch_command(&reference, output, &bee_url).await;
            std::process::exit(code);
        }
        Command::ProbeStorer(args) => {
            let code = probe::run(*args).await;
            std::process::exit(code);
        }
        Command::Crawl(args) => {
            let code = crawl::run(*args).await;
            std::process::exit(code);
        }
        Command::FetchDirect(args) => {
            let code = fetchdirect::run(*args).await;
            std::process::exit(code);
        }
        Command::ProbeGrowth(args) => {
            let code = growth_cmd::run(*args).await;
            std::process::exit(code);
        }
        Command::FundChequebook(args) => {
            let code = fund::run(*args).await;
            std::process::exit(code);
        }
    }
}

async fn fetch_command(reference: &str, output: Option<PathBuf>, bee_url: &str) -> i32 {
    let root = match parse_ref(reference) {
        Ok(root) => root,
        Err(err) => {
            eprintln!("error: {err}");
            return 2;
        }
    };
    let out_path = output.unwrap_or_else(|| PathBuf::from(format!("{}.bin", &reference[..16])));
    let fetcher = match BeeApiFetcher::new(bee_url) {
        Ok(fetcher) => fetcher,
        Err(err) => {
            eprintln!("error: building HTTP client: {err}");
            return 1;
        }
    };

    let started = Instant::now();
    let last_report = Mutex::new((Instant::now(), 0u64));
    let progress = move |done: u64, total: u64| {
        let mut guard = last_report.lock().expect("progress mutex");
        let (last_at, last_done) = *guard;
        let now = Instant::now();
        let dt = now.duration_since(last_at);
        if dt < Duration::from_secs(2) {
            return;
        }
        let rate = rate_mbs(done - last_done, dt);
        *guard = (now, done);
        drop(guard);
        if total > 0 {
            let pct = percent(done, total);
            eprintln!("{done}/{total} bytes ({pct:.1}%), {rate:.2} MB/s");
        } else {
            eprintln!("{done} bytes, {rate:.2} MB/s");
        }
    };

    eprintln!(
        "fetching {reference} -> {} via {bee_url}",
        out_path.display()
    );
    match ds_net::fetch_to_file(&fetcher, root, &out_path, &progress).await {
        Ok(outcome) => {
            let elapsed = started.elapsed();
            let rate = rate_mbs(outcome.bytes_written, elapsed);
            if outcome.resumed_from > 0 {
                eprintln!("resumed from byte {}", outcome.resumed_from);
            }
            eprintln!(
                "done: {} bytes verified -> {} in {:.1}s ({rate:.2} MB/s, {} chunks; \
                 path: forwarding fallback via local bee, settled by the bee node)",
                outcome.total_span,
                out_path.display(),
                elapsed.as_secs_f64(),
                fetcher.chunks_fetched(),
            );
            0
        }
        Err(err) => {
            eprintln!("error: {err}");
            eprintln!("(progress is committed; rerunning the same command resumes)");
            1
        }
    }
}

#[allow(clippy::cast_precision_loss)]
fn percent(done: u64, total: u64) -> f64 {
    100.0 * done as f64 / total as f64
}

#[allow(clippy::cast_precision_loss)]
fn rate_mbs(bytes: u64, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return 0.0;
    }
    bytes as f64 / 1e6 / elapsed.as_secs_f64()
}
