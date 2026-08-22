//! A simple on-disk chunk store: one append-only data file plus an
//! in-memory address→(offset,len) index persisted as a sidecar, so a
//! scheduled fetch can land chunks from many connections and a later
//! reassembly pass (the M1 joiner) reads them back by address without
//! refetching. Resume = reopen the index and skip present chunks.

use ant_retrieval::ChunkFetcher;
use async_trait::async_trait;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Append-only keyed chunk store.
pub struct ChunkStore {
    data_path: PathBuf,
    index_path: PathBuf,
    inner: Mutex<Inner>,
}

struct Inner {
    data: std::fs::File,
    end: u64,
    index: HashMap<[u8; 32], (u64, u32)>,
    /// Index entries not yet flushed to the sidecar.
    dirty: usize,
}

const INDEX_MAGIC: &str = "directswarm-chunkstore v1";
const FLUSH_EVERY: usize = 2048;

impl ChunkStore {
    /// Open (or create) a store at `base` (`base.dat` + `base.idx`),
    /// recovering any prior index for resume.
    ///
    /// # Errors
    /// Fails if the data file or its parent cannot be opened/created.
    pub fn open(base: &Path) -> std::io::Result<Self> {
        let data_path = with_ext(base, "dat");
        let index_path = with_ext(base, "idx");
        if let Some(parent) = data_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let data = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&data_path)?;
        let end = data.metadata()?.len();
        let index = load_index(&index_path, end);
        Ok(Self {
            data_path,
            index_path,
            inner: Mutex::new(Inner {
                data,
                end,
                index,
                dirty: 0,
            }),
        })
    }

    /// Number of chunks stored.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().map_or(0, |g| g.index.len())
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether `addr` is already stored (resume check).
    #[must_use]
    pub fn contains(&self, addr: &[u8; 32]) -> bool {
        self.inner.lock().is_ok_and(|g| g.index.contains_key(addr))
    }

    /// Append a chunk's wire bytes. Idempotent: a duplicate address is
    /// ignored (the first copy was already CAC-validated).
    ///
    /// # Errors
    /// Fails on a write or seek error.
    ///
    /// # Panics
    /// Panics if the store mutex was poisoned by a prior panic.
    pub fn put(&self, addr: [u8; 32], wire: &[u8]) -> std::io::Result<()> {
        let mut g = self.inner.lock().expect("store mutex");
        if g.index.contains_key(&addr) {
            return Ok(());
        }
        let offset = g.end;
        g.data.seek(SeekFrom::Start(offset))?;
        g.data.write_all(wire)?;
        let len = u32::try_from(wire.len())
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "chunk too large"))?;
        g.end += u64::from(len);
        g.index.insert(addr, (offset, len));
        g.dirty += 1;
        if g.dirty >= FLUSH_EVERY {
            let _ = flush_locked(&mut g, &self.index_path);
        }
        Ok(())
    }

    /// Read a chunk's wire bytes back.
    ///
    /// # Errors
    /// Fails if the address is absent or the read fails.
    ///
    /// # Panics
    /// Panics if the store mutex was poisoned by a prior panic.
    pub fn get(&self, addr: &[u8; 32]) -> std::io::Result<Vec<u8>> {
        let mut g = self.inner.lock().expect("store mutex");
        let (offset, len) = *g.index.get(addr).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "chunk not in store")
        })?;
        g.data.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; len as usize];
        g.data.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Persist the index sidecar and fsync the data file.
    ///
    /// # Errors
    /// Fails on a flush/sync error.
    ///
    /// # Panics
    /// Panics if the store mutex was poisoned by a prior panic.
    pub fn flush(&self) -> std::io::Result<()> {
        let mut g = self.inner.lock().expect("store mutex");
        flush_locked(&mut g, &self.index_path)
    }

    /// Remove both files (call after a verified reassembly).
    ///
    /// # Errors
    /// Fails if a file cannot be removed.
    pub fn cleanup(self) -> std::io::Result<()> {
        let _ = std::fs::remove_file(&self.data_path);
        let _ = std::fs::remove_file(&self.index_path);
        Ok(())
    }
}

fn flush_locked(g: &mut Inner, index_path: &Path) -> std::io::Result<()> {
    g.data.flush()?;
    g.data.sync_data()?;
    let tmp = index_path.with_extension("idx.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    writeln!(f, "{INDEX_MAGIC}")?;
    writeln!(f, "end={}", g.end)?;
    for (addr, (off, len)) in &g.index {
        writeln!(f, "{},{off},{len}", hex::encode(addr))?;
    }
    f.sync_all()?;
    std::fs::rename(&tmp, index_path)?;
    g.dirty = 0;
    Ok(())
}

fn load_index(index_path: &Path, data_end: u64) -> HashMap<[u8; 32], (u64, u32)> {
    let Ok(text) = std::fs::read_to_string(index_path) else {
        return HashMap::new();
    };
    let mut lines = text.lines();
    if lines.next() != Some(INDEX_MAGIC) {
        return HashMap::new();
    }
    let _ = lines.next(); // end= line
    let mut index = HashMap::new();
    for line in lines {
        let mut parts = line.split(',');
        let (Some(addr_hex), Some(off), Some(len)) = (parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        let mut addr = [0u8; 32];
        if hex::decode_to_slice(addr_hex, &mut addr).is_err() {
            continue;
        }
        let (Ok(off), Ok(len)) = (off.parse::<u64>(), len.parse::<u32>()) else {
            continue;
        };
        // Guard against a torn data file: skip entries past its end.
        if off + u64::from(len) <= data_end {
            index.insert(addr, (off, len));
        }
    }
    index
}

fn with_ext(base: &Path, ext: &str) -> PathBuf {
    let mut os = base.as_os_str().to_owned();
    os.push(".");
    os.push(ext);
    PathBuf::from(os)
}

/// [`ChunkFetcher`] reading only from the store — used to drive the M1
/// joiner over already-fetched chunks (no network).
pub struct StoreFetcher<'a>(pub &'a ChunkStore);

#[async_trait]
impl ChunkFetcher for StoreFetcher<'_> {
    async fn fetch(&self, addr: [u8; 32]) -> Result<Vec<u8>, Box<dyn StdError + Send + Sync>> {
        self.0.get(&addr).map_err(std::convert::Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_persist_resume() {
        let dir = std::env::temp_dir().join(format!("ds-store-test-{}", std::process::id()));
        let base = dir.join("chunks");
        let _ = std::fs::remove_dir_all(&dir);
        let a = [1u8; 32];
        let b = [2u8; 32];
        {
            let store = ChunkStore::open(&base).unwrap();
            store.put(a, b"hello-chunk-a").unwrap();
            store.put(b, b"chunk-b-data").unwrap();
            store.put(a, b"dup-ignored").unwrap(); // idempotent
            assert_eq!(store.len(), 2);
            store.flush().unwrap();
        }
        // reopen: resume from sidecar
        let store = ChunkStore::open(&base).unwrap();
        assert_eq!(store.len(), 2);
        assert!(store.contains(&a));
        assert_eq!(store.get(&a).unwrap(), b"hello-chunk-a");
        assert_eq!(store.get(&b).unwrap(), b"chunk-b-data");
        store.cleanup().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
