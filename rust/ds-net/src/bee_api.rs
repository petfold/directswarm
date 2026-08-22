//! The forwarding-fallback chunk source: a [`ChunkFetcher`] backed by
//! the local bee node's HTTP chunk API.
//!
//! `GET /chunks/{addr}` returns exactly the wire bytes the trait wants
//! (8-byte LE span ‖ payload). The bee node retrieves through stock
//! forwarding kademlia and settles the traffic itself — this path is
//! invariant 4's total fallback, and in M1 it is the *only* path.
//! Every chunk is CAC/SOC-validated here regardless of trust in the
//! local node.

use ant_crypto::{cac_valid, soc_valid};
use ant_retrieval::ChunkFetcher;
use async_trait::async_trait;
use std::error::Error as StdError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Attempts per chunk. A 404 means bee's own retrieval timed out or
/// found nothing this pass — worth a couple of retries with backoff.
const ATTEMPTS: u32 = 3;
/// Per-request cap. Bee's origin retrieval timeout is 30 s; give the
/// full forwarding path headroom on top.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// [`ChunkFetcher`] over a bee node's HTTP API.
pub struct BeeApiFetcher {
    base: String,
    client: reqwest::Client,
    chunks_fetched: AtomicU64,
    bytes_fetched: AtomicU64,
}

impl BeeApiFetcher {
    /// Build a fetcher for the bee API at `base_url`
    /// (e.g. `http://localhost:1633`).
    ///
    /// # Errors
    /// Fails only if the HTTP client cannot be constructed.
    pub fn new(base_url: &str) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()?;
        Ok(Self {
            base: base_url.trim_end_matches('/').to_owned(),
            client,
            chunks_fetched: AtomicU64::new(0),
            bytes_fetched: AtomicU64::new(0),
        })
    }

    /// Chunks successfully fetched and validated so far.
    #[must_use]
    pub fn chunks_fetched(&self) -> u64 {
        self.chunks_fetched.load(Ordering::Relaxed)
    }

    /// Wire bytes successfully fetched so far (span headers included).
    #[must_use]
    pub fn bytes_fetched(&self) -> u64 {
        self.bytes_fetched.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl ChunkFetcher for BeeApiFetcher {
    async fn fetch(&self, addr: [u8; 32]) -> Result<Vec<u8>, Box<dyn StdError + Send + Sync>> {
        let addr_hex = hex::encode(addr);
        let url = format!("{}/chunks/{addr_hex}", self.base);
        let mut last = String::new();
        for attempt in 0..ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(500 << attempt)).await;
            }
            let resp = match self.client.get(&url).send().await {
                Ok(resp) => resp,
                Err(err) => {
                    last = format!("transport: {err}");
                    continue;
                }
            };
            let status = resp.status();
            if !status.is_success() {
                last = format!("HTTP {status}");
                continue;
            }
            let body = match resp.bytes().await {
                Ok(body) => body,
                Err(err) => {
                    last = format!("body: {err}");
                    continue;
                }
            };
            if cac_valid(&addr, &body) || soc_valid(&addr, &body) {
                self.chunks_fetched.fetch_add(1, Ordering::Relaxed);
                self.bytes_fetched
                    .fetch_add(body.len() as u64, Ordering::Relaxed);
                return Ok(body.to_vec());
            }
            // The local node handed us bytes that don't hash to the
            // address — retrying won't help and the corruption must
            // surface loudly.
            return Err(format!(
                "chunk {addr_hex}: {} bytes failed CAC/SOC validation from {}",
                body.len(),
                self.base
            )
            .into());
        }
        Err(format!("chunk {addr_hex}: {last} after {ATTEMPTS} attempts").into())
    }
}
