//! Fetch orchestration: root → streaming join → file sink, with
//! sequential resume via the `ds-core` sidecar.

use ant_retrieval::joiner::{join_to_sender_range, ByteRange, JoinError, JoinOptions};
use ant_retrieval::rs::decode_span;
use ant_retrieval::ChunkFetcher;
use ds_core::resume::{ResumeState, SIDECAR_SUFFIX};
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;

/// Refuse to join anything claiming to be larger than this.
pub const MAX_FETCH_BYTES: usize = 64 << 30;
/// Commit the resume sidecar every this many flushed bytes.
const COMMIT_INTERVAL: u64 = 8 << 20;
/// Streaming channel depth between joiner and file sink, in buffers.
const SINK_PIPE_DEPTH: usize = 64;

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("root chunk: {0}")]
    Root(Box<dyn std::error::Error + Send + Sync>),
    #[error("root chunk shorter than a span header ({0} bytes)")]
    MalformedRoot(usize),
    #[error("join: {0}")]
    Join(#[from] JoinError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// What a completed fetch did.
#[derive(Debug, Clone, Copy)]
pub struct FetchOutcome {
    /// Total body size the root span declares.
    pub total_span: u64,
    /// Bytes written this run (excludes the resumed prefix).
    pub bytes_written: u64,
    /// Verified offset the run resumed from (0 for a fresh fetch).
    pub resumed_from: u64,
}

/// Sidecar path for an output path.
#[must_use]
pub fn sidecar_path(out_path: &Path) -> PathBuf {
    let mut os = out_path.as_os_str().to_owned();
    os.push(SIDECAR_SUFFIX);
    PathBuf::from(os)
}

/// Fetch the file behind `root` into `out_path`, resuming from a
/// matching sidecar if one exists. `progress(done_bytes, total_bytes)`
/// fires after every flushed buffer.
///
/// # Errors
/// Fails if the root is unfetchable or malformed, any chunk in the
/// tree cannot be fetched and validated, or the file cannot be written.
/// On failure the sidecar holds the last committed verified offset, so
/// a rerun resumes instead of restarting.
pub async fn fetch_to_file(
    fetcher: &dyn ChunkFetcher,
    root: [u8; 32],
    out_path: &Path,
    progress: &(dyn Fn(u64, u64) + Send + Sync),
) -> Result<FetchOutcome, FetchError> {
    let root_wire = fetcher.fetch(root).await.map_err(FetchError::Root)?;
    let Some(span_raw) = root_wire.get(..8).and_then(|s| <[u8; 8]>::try_from(s).ok()) else {
        return Err(FetchError::MalformedRoot(root_wire.len()));
    };
    let (_rs_level, plain_span) = decode_span(span_raw);
    let total_span = u64::from_le_bytes(plain_span);

    let sidecar = sidecar_path(out_path);
    let start = read_resume_offset(&sidecar, root, total_span).await;

    if total_span > 0 && start >= total_span {
        // A previous run finished writing but died before cleanup.
        let _ = tokio::fs::remove_file(&sidecar).await;
        return Ok(FetchOutcome {
            total_span,
            bytes_written: 0,
            resumed_from: start,
        });
    }

    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(out_path)
        .await?;
    // Drop any unverified tail past the committed offset.
    file.set_len(start).await?;
    file.seek(std::io::SeekFrom::Start(start)).await?;

    let range = if start == 0 || total_span == 0 {
        None
    } else {
        ByteRange::clamp(start, total_span - 1, total_span)
    };

    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(SINK_PIPE_DEPTH);
    let producer = join_to_sender_range(
        fetcher,
        &root_wire,
        MAX_FETCH_BYTES,
        JoinOptions::default(),
        range,
        tx,
    );
    let consumer = async {
        let mut written = start;
        let mut last_commit = start;
        while let Some(buf) = rx.recv().await {
            file.write_all(&buf).await?;
            written += buf.len() as u64;
            if written - last_commit >= COMMIT_INTERVAL {
                file.flush().await?;
                file.sync_data().await?;
                commit_sidecar(&sidecar, root, written).await?;
                last_commit = written;
            }
            progress(written, total_span);
        }
        file.flush().await?;
        file.sync_data().await?;
        Ok::<u64, std::io::Error>(written)
    };

    let (join_result, sink_result) = tokio::join!(producer, consumer);
    let written = sink_result?;
    if let Err(err) = join_result {
        // Preserve what the sink verified before the join died.
        commit_sidecar(&sidecar, root, written).await?;
        return Err(err.into());
    }
    let _ = tokio::fs::remove_file(&sidecar).await;
    Ok(FetchOutcome {
        total_span,
        bytes_written: written - start,
        resumed_from: start,
    })
}

async fn read_resume_offset(sidecar: &Path, root: [u8; 32], total_span: u64) -> u64 {
    let Ok(text) = tokio::fs::read_to_string(sidecar).await else {
        return 0;
    };
    match ResumeState::parse(&text) {
        Some(state) if state.root == root && state.offset <= total_span => state.offset,
        _ => 0,
    }
}

async fn commit_sidecar(sidecar: &Path, root: [u8; 32], offset: u64) -> std::io::Result<()> {
    tokio::fs::write(sidecar, ResumeState { root, offset }.render()).await
}
