//! Per-peer settlement state, persisted across runs — the M5 memory
//! that turns threshold growth and validation-latency measurements
//! into a lasting advantage: `.phase1/peerstate.csv` keyed by overlay.
//!
//! What we remember and why:
//! - `threshold_last`: the peer's last announced payment threshold.
//!   Bee grows it with settled volume and re-announces the grown value
//!   on reconnect (while the peer's bee stays up), so it predicts the
//!   pacing headroom a reconnect gets — and ranks storers by earned
//!   trust.
//! - `lambda_ms`: measured cheque-validation latency (the peer's
//!   on-chain RPC speed). Sets the exposure window; ranks storers.
//! - `settled_units`: lifetime units we settled with this peer (spend
//!   audit + growth bookkeeping).
//! - `last_ok_unix`: when we last closed a connection cleanly.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, Default)]
pub struct PeerState {
    pub threshold_last: u64,
    /// Measured cheque-validation latency; None = never measured.
    pub lambda_ms: Option<u64>,
    pub settled_units: u64,
    pub last_ok_unix: u64,
}

pub struct PeerStateStore {
    path: PathBuf,
    map: Mutex<HashMap<[u8; 32], PeerState>>,
}

const HEADER: &str = "overlay_hex,threshold_last,lambda_ms,settled_units,last_ok_unix";

impl PeerStateStore {
    /// Load the store (missing file = empty store).
    #[must_use]
    pub fn open(path: &Path) -> Self {
        let mut map = HashMap::new();
        if let Ok(text) = std::fs::read_to_string(path) {
            for line in text.lines().skip(1) {
                let cols: Vec<&str> = line.split(',').collect();
                let (Some(oh), Some(t), Some(l), Some(s), Some(ok)) = (
                    cols.first(),
                    cols.get(1),
                    cols.get(2),
                    cols.get(3),
                    cols.get(4),
                ) else {
                    continue;
                };
                let mut overlay = [0u8; 32];
                if hex::decode_to_slice(oh, &mut overlay).is_err() {
                    continue;
                }
                map.insert(
                    overlay,
                    PeerState {
                        threshold_last: t.parse().unwrap_or(0),
                        lambda_ms: l.parse().ok(),
                        settled_units: s.parse().unwrap_or(0),
                        last_ok_unix: ok.parse().unwrap_or(0),
                    },
                );
            }
        }
        Self {
            path: path.to_path_buf(),
            map: Mutex::new(map),
        }
    }

    #[must_use]
    pub fn get(&self, overlay: &[u8; 32]) -> Option<PeerState> {
        self.map.lock().ok()?.get(overlay).copied()
    }

    /// Merge an observation for `overlay` (max threshold, latest λ,
    /// summed settled units) and persist the whole store atomically.
    pub fn record(
        &self,
        overlay: &[u8; 32],
        threshold: u64,
        lambda_ms: Option<u64>,
        settled_units_delta: u64,
        clean_close: bool,
    ) {
        let Ok(mut map) = self.map.lock() else {
            return;
        };
        let e = map.entry(*overlay).or_default();
        e.threshold_last = e.threshold_last.max(threshold);
        if lambda_ms.is_some() {
            e.lambda_ms = lambda_ms;
        }
        e.settled_units = e.settled_units.saturating_add(settled_units_delta);
        if clean_close {
            e.last_ok_unix = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs());
        }
        let snapshot: Vec<([u8; 32], PeerState)> = map.iter().map(|(k, v)| (*k, *v)).collect();
        drop(map);
        let _ = self.save(&snapshot);
    }

    fn save(&self, entries: &[([u8; 32], PeerState)]) -> std::io::Result<()> {
        use std::io::Write;
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = self.path.with_extension("csv.tmp");
        let mut f = std::fs::File::create(&tmp)?;
        writeln!(f, "{HEADER}")?;
        for (overlay, s) in entries {
            writeln!(
                f,
                "{},{},{},{},{}",
                hex::encode(overlay),
                s.threshold_last,
                s.lambda_ms.map_or_else(String::new, |l| l.to_string()),
                s.settled_units,
                s.last_ok_unix
            )?;
        }
        f.sync_all()?;
        std::fs::rename(&tmp, &self.path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_merge() {
        let dir = std::env::temp_dir().join(format!("ds-peerstate-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("peerstate.csv");
        let _ = std::fs::remove_file(&path);
        let ov = [7u8; 32];
        {
            let st = PeerStateStore::open(&path);
            st.record(&ov, 9_450_000, Some(1210), 1000, true);
            st.record(&ov, 1_350_000, None, 500, false); // lower T must not regress
        }
        let st = PeerStateStore::open(&path);
        let e = st.get(&ov).unwrap();
        assert_eq!(e.threshold_last, 9_450_000);
        assert_eq!(e.lambda_ms, Some(1210));
        assert_eq!(e.settled_units, 1500);
        assert!(e.last_ok_unix > 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
