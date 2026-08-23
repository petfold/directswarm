//! Outbound cheque ledger with the fsync OFF the emit path.
//!
//! Diagnosed 2026-08-23 from the M5 acceptance battery: ant's
//! `OutboundLedger::record_issued` re-serializes the whole map and
//! fsyncs a temp file UNDER ONE GLOBAL MUTEX on every cheque —
//! ~20–30 ms of serialized blocking per cheque across all connections,
//! a hard node-wide ceiling of ~30–55 cheques/s (measured flat across
//! all five battery runs regardless of threshold regime), and each
//! blocked emit also stalls its connection's fetch loop. The exact
//! shape of the Phase-0 bee finding (global chequebook mutex,
//! ethersphere/bee#5570), reproduced in our own client.
//!
//! This ledger keeps the in-memory map authoritative (mutex held only
//! for the map insert), marks a dirty flag, and lets a background
//! persister snapshot + atomically rewrite the SAME JSON file format
//! (hex beneficiary → decimal cumulative) every 200 ms, off the tokio
//! workers via `spawn_blocking`. `flush()` persists synchronously at
//! run end.
//!
//! Crash-safety trade: up to ~200 ms of cheques can be recorded in
//! memory but not on disk when the process dies. After a restart the
//! ledger then trails bee's persisted highest-validated cumulative for
//! those few peers, so their first cheques are rejected until the
//! cumulative catches back up — the bounded, self-healing failure the
//! M4 diagnosis mapped (and the sweep-at-exit makes the window empty
//! on any clean run). ant's order of operations (persist AFTER the
//! wire send, warn on failure) already had a crash window; this one is
//! bounded and documented.

use primitive_types::U256;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub struct FastLedger {
    path: PathBuf,
    map: Mutex<HashMap<String, U256>>,
    dirty: Arc<AtomicBool>,
}

impl FastLedger {
    /// Open the ledger, loading ant's JSON format if the file exists,
    /// and start the background persister (owned by the runtime; it
    /// exits with the process — call [`Self::flush`] at run end).
    #[must_use]
    pub fn open(path: PathBuf) -> Arc<Self> {
        let map = load_map(&path);
        let ledger = Arc::new(Self {
            path,
            map: Mutex::new(map),
            dirty: Arc::new(AtomicBool::new(false)),
        });
        let weak = Arc::downgrade(&ledger);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let Some(l) = weak.upgrade() else { break };
                if l.dirty.swap(false, Ordering::SeqCst) {
                    let snapshot = l.snapshot();
                    let path = l.path.clone();
                    let dirty = l.dirty.clone();
                    drop(l);
                    let res =
                        tokio::task::spawn_blocking(move || persist_map(&path, &snapshot)).await;
                    if !matches!(res, Ok(Ok(()))) {
                        // Try again next tick rather than losing state.
                        dirty.store(true, Ordering::SeqCst);
                    }
                }
            }
        });
        ledger
    }

    /// Highest cumulative recorded for `beneficiary` (zero if none).
    #[must_use]
    pub fn cumulative_for(&self, beneficiary: &[u8; 20]) -> U256 {
        let key = hex::encode(beneficiary);
        self.map
            .lock()
            .map_or_else(|_| U256::zero(), |g| g.get(&key).copied().unwrap_or_default())
    }

    /// Record `beneficiary` → `new_cumulative`. Memory-only + dirty
    /// mark; the persister writes within ~200 ms.
    pub fn record_issued(&self, beneficiary: &[u8; 20], new_cumulative: U256) {
        let key = hex::encode(beneficiary);
        if let Ok(mut g) = self.map.lock() {
            g.insert(key, new_cumulative);
        }
        self.dirty.store(true, Ordering::SeqCst);
    }

    fn snapshot(&self) -> Vec<(String, U256)> {
        self.map
            .lock()
            .map_or_else(|_| Vec::new(), |g| g.iter().map(|(k, v)| (k.clone(), *v)).collect())
    }

    /// Persist synchronously (run end / final sweep done).
    ///
    /// # Errors
    /// Propagates the file write/rename error.
    pub fn flush(&self) -> std::io::Result<()> {
        self.dirty.store(false, Ordering::SeqCst);
        persist_map(&self.path, &self.snapshot())
    }
}

fn load_map(path: &Path) -> HashMap<String, U256> {
    let mut out = HashMap::new();
    let Ok(text) = std::fs::read_to_string(path) else {
        return out;
    };
    // ant's format: {"<hex40>": "<decimal>", ...} — parse leniently
    // without serde: split on quotes.
    let mut parts = text.split('"');
    while let (Some(_), Some(key), Some(_), Some(value)) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    {
        if key.len() == 40 && key.chars().all(|c| c.is_ascii_hexdigit()) {
            if let Ok(v) = U256::from_dec_str(value.trim()) {
                out.insert(key.to_ascii_lowercase(), v);
            }
        }
    }
    out
}

fn persist_map(path: &Path, entries: &[(String, U256)]) -> std::io::Result<()> {
    use std::fmt::Write as _;
    use std::io::Write;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    let tmp = path.with_extension("json.tmp");
    let mut body = String::with_capacity(entries.len() * 64 + 8);
    body.push_str("{\n");
    for (i, (k, v)) in entries.iter().enumerate() {
        body.push_str(if i == 0 { "" } else { ",\n" });
        let _ = write!(body, "  \"{k}\": \"{v}\"");
    }
    body.push_str("\n}");
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(body.as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn roundtrip_ant_format() {
        let dir = std::env::temp_dir().join(format!("ds-ledger-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("outbound-cheques.json");
        let _ = std::fs::remove_file(&path);
        let ben = [0xabu8; 20];
        {
            let l = FastLedger::open(path.clone());
            assert_eq!(l.cumulative_for(&ben), U256::zero());
            l.record_issued(&ben, U256::from(123_456_789u64));
            l.flush().unwrap();
        }
        let l2 = FastLedger::open(path.clone());
        assert_eq!(l2.cumulative_for(&ben), U256::from(123_456_789u64));
        // ant's loader must also be able to read what we wrote — the
        // format is its own (hex → decimal-string JSON map).
        let ant = ant_p2p::swap::OutboundLedger::open(Some(path.clone()));
        assert_eq!(ant.cumulative_for(&ben), U256::from(123_456_789u64));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
