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
    /// Measured service rate in tenths of chunks/s (EWMA of saturated
    /// samples) — the direct bandwidth signal for member selection.
    pub service_cps_x10: u32,
    /// Prepaid-but-unconsumed units parked at this peer (bee persists
    /// them as our surplus). A reconnect starts with this as spendable
    /// credit instead of re-prepaying.
    pub surplus_units: u64,
    pub last_ok_unix: u64,
}

/// One connection's worth of learning about a peer.
#[derive(Debug, Clone, Copy, Default)]
pub struct PeerObservation {
    pub threshold: u64,
    pub lambda_ms: Option<u64>,
    pub settled_units_delta: u64,
    pub service_cps_x10: Option<u32>,
    /// Absolute parked surplus after this connection (prepay mode).
    pub surplus_units_abs: Option<u64>,
    pub clean_close: bool,
}

pub struct PeerStateStore {
    path: PathBuf,
    map: Mutex<HashMap<[u8; 32], PeerState>>,
}

const HEADER: &str = "overlay_hex,threshold_last,lambda_ms,settled_units,last_ok_unix,service_cps_x10,surplus_units";

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
                        // column added later; old rows lack it
                        service_cps_x10: cols.get(5).and_then(|v| v.parse().ok()).unwrap_or(0),
                        surplus_units: cols.get(6).and_then(|v| v.parse().ok()).unwrap_or(0),
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
    /// summed settled units, absolute surplus) and persist the whole
    /// store atomically.
    pub fn record(&self, overlay: &[u8; 32], obs: &PeerObservation) {
        let Ok(mut map) = self.map.lock() else {
            return;
        };
        let e = map.entry(*overlay).or_default();
        e.threshold_last = e.threshold_last.max(obs.threshold);
        if obs.lambda_ms.is_some() {
            e.lambda_ms = obs.lambda_ms;
        }
        e.settled_units = e.settled_units.saturating_add(obs.settled_units_delta);
        if let Some(su) = obs.surplus_units_abs {
            // Absolute: the caller accounted for any pre-existing
            // surplus it started from.
            e.surplus_units = su;
        }
        if let Some(r) = obs.service_cps_x10 {
            // EWMA (α = 0.5): responsive but smooths one-off outliers.
            e.service_cps_x10 = if e.service_cps_x10 == 0 {
                r
            } else {
                u32::midpoint(e.service_cps_x10, r)
            };
        }
        if obs.clean_close {
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
                "{},{},{},{},{},{},{}",
                hex::encode(overlay),
                s.threshold_last,
                s.lambda_ms.map_or_else(String::new, |l| l.to_string()),
                s.settled_units,
                s.last_ok_unix,
                s.service_cps_x10,
                s.surplus_units
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
            st.record(&ov, &PeerObservation {
                threshold: 9_450_000,
                lambda_ms: Some(1210),
                settled_units_delta: 1000,
                service_cps_x10: Some(200),
                surplus_units_abs: Some(42),
                clean_close: true,
            });
            st.record(&ov, &PeerObservation {
                threshold: 1_350_000, // lower T must not regress
                settled_units_delta: 500,
                ..Default::default()
            });
        }
        let st = PeerStateStore::open(&path);
        let e = st.get(&ov).unwrap();
        assert_eq!(e.threshold_last, 9_450_000);
        assert_eq!(e.lambda_ms, Some(1210));
        assert_eq!(e.settled_units, 1500);
        assert_eq!(e.surplus_units, 42);
        assert!(e.last_ok_unix > 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
