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
//!
//! ## On-disk format and upstream ant (since ant v0.5.77, 2026-11)
//!
//! We WRITE the flat `{"<beneficiary hex>": "<decimal>"}` map — the
//! format of every ledger directswarm has ever written — and READ both
//! that and upstream ant's current layout. Upstream `OutboundLedger`
//! now keys entries `<chequebook hex>:<beneficiary hex>`, appends each
//! cheque as a one-object line to a `<name>.journal` (folded into the
//! file at compaction, via `<name>.journal.old`), adopts bare entries
//! into the first chequebook that opens the file and rewrites them
//! under it at once, and leaves a `<name>.lost` marker when it moves an
//! unreadable file aside (ant issue #140, PRs #126/#141).
//!
//! So if upstream ant ever opens our file, our bare keys come back
//! sectioned and the newest cheques may sit only in its journal. The
//! old loader skipped any key it didn't recognize, which would have
//! restarted every cumulative at ZERO: bee refuses a cheque below the
//! cumulative it already holds, so we would have run up real debt with
//! every peer we had paid and been disconnected by them. The loader
//! therefore takes the larger figure across the file and both journals,
//! and fails closed — `open` errors before anything dials or pays —
//! on anything it cannot account for: an unreadable file, a malformed
//! entry, another chequebook's section, a lost marker. (Reading is a
//! one-shot merge at open: never run upstream ant on this file while a
//! directswarm process holds it.)

use primitive_types::U256;
use std::collections::HashMap;
use std::io::ErrorKind;
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
    /// Open `chequebook`'s outbound ledger at `path` and start the
    /// background persister (owned by the runtime; it exits with the
    /// process — call [`Self::flush`] at run end). A missing file is a
    /// fresh, empty ledger.
    ///
    /// # Errors
    /// Fails closed (see the module docs) when the file or a journal
    /// next to it exists but can't be read in full, holds an entry that
    /// isn't a beneficiary → decimal cumulative of THIS chequebook, or
    /// upstream ant left a `<name>.lost` marker beside it. Issuing
    /// cheques from cumulatives we can't see would get them refused.
    pub fn open(path: PathBuf, chequebook: &[u8; 20]) -> std::io::Result<Arc<Self>> {
        let map = load_map(&path, chequebook)?;
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
        Ok(ledger)
    }

    /// Highest cumulative recorded for `beneficiary` (zero if none).
    #[must_use]
    pub fn cumulative_for(&self, beneficiary: &[u8; 20]) -> U256 {
        let key = hex::encode(beneficiary);
        self.map.lock().map_or_else(
            |_| U256::zero(),
            |g| g.get(&key).copied().unwrap_or_default(),
        )
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
        self.map.lock().map_or_else(
            |_| Vec::new(),
            |g| g.iter().map(|(k, v)| (k.clone(), *v)).collect(),
        )
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

/// `<name><suffix>` next to `path` (upstream ant's sibling naming).
fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(suffix);
    path.with_file_name(name)
}

fn corrupt(path: &Path, why: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(
        ErrorKind::InvalidData,
        format!(
            "cheque ledger {}: {why} — refusing to issue cheques from cumulatives \
             we can't see (bee would refuse them); repair the file first",
            path.display()
        ),
    )
}

/// `Some(bytes)` if `path` exists, `None` if it doesn't; any other read
/// error propagates.
fn read_if_exists(path: &Path) -> std::io::Result<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn load_map(path: &Path, chequebook: &[u8; 20]) -> std::io::Result<HashMap<String, U256>> {
    let lost = sibling(path, ".lost");
    if lost.exists() {
        return Err(corrupt(
            path,
            format!("{} says ant moved its figures aside", lost.display()),
        ));
    }
    let ours = hex::encode(chequebook);
    let mut out = HashMap::new();
    if let Some(bytes) = read_if_exists(path)? {
        merge_object(&bytes, &ours, &mut out).map_err(|why| corrupt(path, why))?;
    }
    // Journals: one JSON object per line, each group commit closed by a
    // `#<n>` mark line. Every other line must parse except a torn final
    // line (no trailing newline): upstream sends a cheque only once its
    // group commit is fsynced, so a torn tail never went out. A NUL-
    // zeroed range (a crash on XFS/ext4 without data=ordered) fails the
    // parse and so fails closed — upstream tells torn from damaged
    // there by its commit marks; we don't re-implement that, and its
    // compaction (start ant once on the file) settles it.
    for journal in [sibling(path, ".journal.old"), sibling(path, ".journal")] {
        let Some(bytes) = read_if_exists(&journal)? else {
            continue;
        };
        let lines: Vec<&[u8]> = bytes.split(|b| *b == b'\n').collect();
        let torn_tail = !bytes.ends_with(b"\n");
        for (i, line) in lines.iter().enumerate() {
            if line.iter().all(u8::is_ascii_whitespace) || is_commit_mark(line) {
                continue;
            }
            if let Err(why) = merge_object(line, &ours, &mut out) {
                if torn_tail && i + 1 == lines.len() {
                    continue;
                }
                return Err(corrupt(&journal, format!("line {}: {why}", i + 1)));
            }
        }
    }
    Ok(out)
}

/// Upstream's journal group-commit mark: `#<n>`.
fn is_commit_mark(line: &[u8]) -> bool {
    line.strip_prefix(b"#")
        .is_some_and(|d| !d.is_empty() && d.iter().all(u8::is_ascii_digit))
}

/// Merge one JSON object of ledger entries into `out`, keeping the
/// larger figure per beneficiary (cumulatives only grow). Keys are a
/// bare `<beneficiary hex>` (ours, and pre-section ant) or
/// `<chequebook hex>:<beneficiary hex>` (ant ≥ the sectioned ledger);
/// values are decimal strings (a bare JSON integer is accepted too).
fn merge_object(bytes: &[u8], ours: &str, out: &mut HashMap<String, U256>) -> Result<(), String> {
    let is_addr = |s: &str| s.len() == 40 && s.bytes().all(|b| b.is_ascii_hexdigit());
    let entries: HashMap<String, serde_json::Value> =
        serde_json::from_slice(bytes).map_err(|e| format!("not a JSON object of entries: {e}"))?;
    for (key, value) in entries {
        let beneficiary = match key.split_once(':') {
            None if is_addr(&key) => key.as_str(),
            Some((cb, ben)) if is_addr(cb) && is_addr(ben) => {
                if !cb.eq_ignore_ascii_case(ours) {
                    return Err(format!(
                        "entry {key:?} belongs to chequebook 0x{cb}, not ours (0x{ours})"
                    ));
                }
                ben
            }
            _ => return Err(format!("unrecognized key {key:?}")),
        };
        let digits = match &value {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) if n.is_u64() => n.to_string(),
            other => {
                return Err(format!(
                    "entry {key:?}: cumulative {other} is not a decimal"
                ))
            }
        };
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err(format!(
                "entry {key:?}: cumulative {digits:?} is not a decimal"
            ));
        }
        let cumulative =
            U256::from_dec_str(&digits).map_err(|e| format!("entry {key:?}: {e:?}"))?;
        let slot = out.entry(beneficiary.to_ascii_lowercase()).or_default();
        *slot = (*slot).max(cumulative);
    }
    Ok(())
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

    const CB: [u8; 20] = [0xcb; 20];

    fn fresh_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("ds-ledger-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn roundtrip_own_format() {
        let dir = fresh_dir("rt");
        let path = dir.join("outbound-cheques.json");
        let ben = [0xabu8; 20];
        {
            let l = FastLedger::open(path.clone(), &CB).unwrap();
            assert_eq!(l.cumulative_for(&ben), U256::zero(), "missing file = empty");
            l.record_issued(&ben, U256::from(123_456_789u64));
            l.flush().unwrap();
        }
        let l2 = FastLedger::open(path.clone(), &CB).unwrap();
        assert_eq!(l2.cumulative_for(&ben), U256::from(123_456_789u64));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The pinned upstream ant adopts our flat file into its sectioned
    /// layout and journals new cheques; every figure must stay visible
    /// to us — before the journal is folded and after.
    #[tokio::test]
    async fn reads_upstream_ant_sections_and_journal() {
        let dir = fresh_dir("ant");
        let path = dir.join("outbound-cheques.json");
        let (a, b) = ([0xa1u8; 20], [0xb2u8; 20]);
        {
            let l = FastLedger::open(path.clone(), &CB).unwrap();
            l.record_issued(&a, U256::from(5_000u64));
            l.flush().unwrap();
        }
        let ant = ant_p2p::swap::OutboundLedger::open(Some(path.clone()), CB);
        assert_eq!(
            ant.cumulative_for(&a),
            U256::from(5_000u64),
            "ant adopts bare entries"
        );
        ant.record_issued(&b, U256::from(7_000u64)).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(
            text.contains(&format!("{}:", hex::encode(CB))),
            "ant sectioned it: {text}"
        );
        {
            let l = FastLedger::open(path.clone(), &CB).unwrap();
            assert_eq!(
                l.cumulative_for(&a),
                U256::from(5_000u64),
                "from the section"
            );
            assert_eq!(
                l.cumulative_for(&b),
                U256::from(7_000u64),
                "from the journal"
            );
        }
        drop(ant); // the last close folds the journal into the file
        for _ in 0..100 {
            if !sibling(&path, ".journal").exists() && !sibling(&path, ".journal.old").exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let l = FastLedger::open(path.clone(), &CB).unwrap();
        assert_eq!(l.cumulative_for(&a), U256::from(5_000u64));
        assert_eq!(l.cumulative_for(&b), U256::from(7_000u64));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn larger_figure_wins_across_formats() {
        let dir = fresh_dir("max");
        let path = dir.join("outbound-cheques.json");
        let (cb, ben) = (hex::encode(CB), "ab".repeat(20));
        std::fs::write(&path, format!(r#"{{"{ben}":"5","{cb}:{ben}":"8"}}"#)).unwrap();
        std::fs::write(
            sibling(&path, ".journal"),
            format!("{{\"{cb}:{ben}\":\"6\"}}\n#1\n"),
        )
        .unwrap();
        let l = FastLedger::open(path.clone(), &CB).unwrap();
        assert_eq!(l.cumulative_for(&[0xab; 20]), U256::from(8u64));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn fails_closed_on_what_it_cannot_account_for() {
        let dir = fresh_dir("bad");
        let path = dir.join("outbound-cheques.json");
        let (cb, ben, other) = (hex::encode(CB), "ab".repeat(20), "cc".repeat(20));
        for (body, why) in [
            (format!(r#"{{"{ben}":"12x"}}"#), "non-decimal cumulative"),
            (
                format!(r#"{{"{ben}":""}}"#),
                "empty cumulative (bee#5656 shape)",
            ),
            (
                format!(r#"{{"{other}:{ben}":"5"}}"#),
                "another chequebook's section",
            ),
            (r#"{"not-an-address":"5"}"#.to_owned(), "unrecognized key"),
            ("garbage".to_owned(), "not JSON"),
        ] {
            std::fs::write(&path, body).unwrap();
            assert!(FastLedger::open(path.clone(), &CB).is_err(), "{why}");
        }

        std::fs::write(&path, format!(r#"{{"{ben}":"5"}}"#)).unwrap();
        std::fs::write(sibling(&path, ".lost"), "{}").unwrap();
        assert!(
            FastLedger::open(path.clone(), &CB).is_err(),
            "ant's lost marker"
        );
        std::fs::remove_file(sibling(&path, ".lost")).unwrap();

        // A torn final journal line never went out; damage before the
        // tail is not a torn write.
        let journal = sibling(&path, ".journal");
        std::fs::write(
            &journal,
            format!("{{\"{cb}:{ben}\":\"9\"}}\n{{\"{cb}:{ben}\":\"1"),
        )
        .unwrap();
        let l = FastLedger::open(path.clone(), &CB).unwrap();
        assert_eq!(
            l.cumulative_for(&[0xab; 20]),
            U256::from(9u64),
            "torn tail ignored"
        );
        drop(l);
        std::fs::write(
            &journal,
            format!("{{\"{cb}:{ben}\":\"1\n{{\"{cb}:{ben}\":\"9\"}}\n"),
        )
        .unwrap();
        assert!(
            FastLedger::open(path.clone(), &CB).is_err(),
            "mid-journal damage"
        );
        std::fs::write(
            &journal,
            format!("{{\"{cb}:{ben}\":\"9\"}}\n#1\n\0\0\0\n#2\n"),
        )
        .unwrap();
        assert!(
            FastLedger::open(path.clone(), &CB).is_err(),
            "NUL-zeroed range"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
