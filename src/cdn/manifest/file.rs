// Modified by Aurelia; see README.
use futures::{Stream, StreamExt, stream::FuturesUnordered};
use itertools::Itertools;
use sha1::Digest;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{fmt::Write, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Semaphore,
};
use tracing;

use crate::{Error, cdn::inner::InnerClient};

/// Steam EDepotFileFlag::Executable bit.
#[cfg(unix)]
pub const FLAG_EXECUTABLE: u32 = 32;

/// Steam EDepotFileFlag::Directory bit.
pub const FLAG_DIRECTORY: u32 = 64;

/// Steam EDepotFileFlag::Symlink bit.
pub const FLAG_SYMLINK: u32 = 512;

/// First bytes of executables a depot may ship without `FLAG_EXECUTABLE`.
#[cfg(unix)]
const EXECUTABLE_MAGIC: &[&[u8]] = &[
    b"\x7fELF",
    b"#!",
    b"\xcf\xfa\xed\xfe", // Mach-O, 64-bit
    b"\xce\xfa\xed\xfe", // Mach-O, 32-bit
    b"\xca\xfe\xba\xbe", // universal Mach-O, and harmlessly Java class files
    b"\xca\xfe\xba\xbf", // universal Mach-O, 64-bit offsets
];

/// Whether a file starting with `head` is an executable.
#[cfg(unix)]
fn looks_executable(head: &[u8]) -> bool {
    EXECUTABLE_MAGIC.iter().any(|magic| head.starts_with(magic))
}

/// Largest file `download_to_memory` accepts.
pub const MAX_IN_MEMORY_LEN: u64 = 64 << 20;

/// Refuse manifest sizes too big for memory.
fn check_in_memory_len(size: u64) -> Result<(), Error> {
    if size > MAX_IN_MEMORY_LEN {
        return Err(Error::Unexpected(format!(
            "file of {size} bytes is too large to read into memory"
        )));
    }
    Ok(())
}

#[derive(Debug)]
pub struct ChunkData {
    pub(super) sha: Vec<u8>,
    pub(super) crc: u32,
    pub(super) offset: u64,
    pub(super) original_size: u32,
    pub(super) compressed_size: u32,
}

impl ChunkData {
    pub fn sha(&self) -> Vec<u8> {
        self.sha.clone()
    }

    pub fn id(&self) -> String {
        self.sha.iter().fold(String::new(), |mut output, b| {
            let _ = write!(output, "{b:02x}");
            output
        })
    }

    pub fn crc(&self) -> u32 {
        self.crc
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn original_size(&self) -> u32 {
        self.original_size
    }

    pub fn compressed_size(&self) -> u32 {
        self.compressed_size
    }
}

#[derive(Debug)]
pub struct ManifestFile {
    pub(super) inner: Arc<InnerClient>,
    pub(super) app_id: u32,
    pub(super) depot_id: u32,
    pub(super) filename: String,
    pub(super) size: u64,
    pub(super) flags: u32,
    pub(super) sha_filename: Vec<u8>,
    pub(super) sha_content: Vec<u8>,
    pub(super) chunks: Vec<ChunkData>,
    pub(super) linktarget: String,
}

impl ManifestFile {
    pub fn full_path(&self) -> String {
        self.filename
            .trim_matches(|c: char| c.is_whitespace() || c == '\0')
            .replace("\\", "/")
    }

    pub fn path(&self) -> String {
        let full_path = self.full_path();
        if let Some(pos) = full_path.rfind('/') {
            full_path[..pos].to_string()
        } else {
            full_path
        }
    }

    pub fn filename(&self) -> String {
        self.full_path()
            .rsplit("/")
            .next()
            .unwrap_or("")
            .to_string()
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn flags(&self) -> u32 {
        self.flags
    }

    pub fn sha_filename(&self) -> Vec<u8> {
        self.sha_filename.clone()
    }

    pub fn sha_content(&self) -> Vec<u8> {
        self.sha_content.clone()
    }

    pub fn chunks(&self) -> &Vec<ChunkData> {
        self.chunks.as_ref()
    }

    pub fn linktarget(&self) -> String {
        self.linktarget.clone()
    }

    /// Fetch this file's full content into memory. Symlink entries store
    /// their link target as chunk data, so they need content without a
    /// destination file on disk.
    ///
    /// Refuses files over [`MAX_IN_MEMORY_LEN`].
    pub async fn download_to_memory(&self, depot_key: [u8; 32]) -> Result<Vec<u8>, Error> {
        check_in_memory_len(self.size)?;
        let mut buffer = vec![0u8; self.size as usize];
        for chunk in self
            .chunks()
            .iter()
            .sorted_by(|a, b| a.offset.cmp(&b.offset))
        {
            let data = self
                .inner
                .get_chunk(
                    self.app_id,
                    self.depot_id,
                    depot_key,
                    chunk.id(),
                    &chunk.sha,
                    chunk.original_size,
                )
                .await?;
            let offset = chunk.offset as usize;
            let end = offset.saturating_add(data.len()).min(buffer.len());
            if offset < end {
                buffer[offset..end].copy_from_slice(&data[..end - offset]);
            }
        }
        Ok(buffer)
    }

    /// Download into `target_path`, reusing matching chunks.
    ///
    /// `on_progress(bytes, reused)` fires per chunk.
    pub async fn download(
        &self,
        depot_key: [u8; 32],
        target_path: &std::path::Path,
        verify_mode: bool,
        abort_signal: Option<Arc<AtomicBool>>,
        limit: Option<Arc<Semaphore>>,
        on_progress: Option<ProgressFn>,
    ) -> Result<(), Error> {
        let semaphore = limit.unwrap_or_else(|| Arc::new(Semaphore::new(4)));

        let metadata_before = tokio::fs::metadata(target_path).await;
        let file_exists_before = metadata_before.is_ok();
        let current_len_before = metadata_before.map(|m| m.len()).unwrap_or(0);

        let mut out = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(target_path)
            .await
            .map_err(Error::Io)?;

        if current_len_before != self.size {
            out.set_len(self.size).await.map_err(Error::Io)?;
        }

        let tasks = self
            .chunks()
            .iter()
            .sorted_by(|&a, &b| a.offset.cmp(&b.offset))
            .map(|chunk_data| {
                let job = ChunkJob {
                    inner: self.inner.clone(),
                    app_id: self.app_id,
                    depot_id: self.depot_id,
                    depot_key,
                    id: chunk_data.id(),
                    sha: chunk_data.sha.clone(),
                    offset: chunk_data.offset,
                    size: chunk_data.original_size,
                    target_path: target_path.to_path_buf(),
                    // Only existing files hold chunks.
                    check_disk: file_exists_before,
                    verify_mode,
                };
                AbortOnDrop(tokio::spawn(job.run(
                    semaphore.clone(),
                    abort_signal.clone(),
                    on_progress.clone(),
                )))
            })
            // Unordered: write chunks as they finish.
            .collect::<FuturesUnordered<_>>();

        let results = tasks.map(|joined| joined.map_err(Error::from).and_then(|r| r));
        write_chunks(&mut out, results, abort_signal.as_deref()).await?;

        #[cfg(unix)]
        self.apply_unix_mode(target_path).await?;

        Ok(())
    }

    #[cfg(unix)]
    pub(crate) async fn apply_unix_mode(&self, target_path: &std::path::Path) -> Result<(), Error> {
        use std::os::unix::fs::PermissionsExt;

        if self.flags & FLAG_EXECUTABLE == 0 {
            // Flagless depots: detect executables by their first bytes.
            let mut head = [0u8; 4];
            let read = match tokio::fs::File::open(target_path).await {
                Ok(mut f) => f.read(&mut head).await.unwrap_or(0),
                Err(_) => 0,
            };
            if !looks_executable(&head[..read]) {
                return Ok(());
            }
        }
        let metadata = tokio::fs::metadata(target_path).await.map_err(Error::Io)?;
        let mut permissions = metadata.permissions();
        if permissions.mode() & 0o111 != 0 {
            return Ok(());
        }
        permissions.set_mode(permissions.mode() | 0o755);
        tokio::fs::set_permissions(target_path, permissions)
            .await
            .map_err(Error::Io)?;
        Ok(())
    }
}

/// `(bytes, reused_from_disk)` progress callback.
pub type ProgressFn = Arc<dyn Fn(u64, bool) + Send + Sync + 'static>;

/// Spawned task, aborted when dropped.
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> std::future::Future for AbortOnDrop<T> {
    type Output = Result<T, tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.0).poll(cx)
    }
}

/// One chunk: reuse or fetch.
struct ChunkJob {
    inner: Arc<InnerClient>,
    app_id: u32,
    depot_id: u32,
    depot_key: [u8; 32],
    id: String,
    sha: Vec<u8>,
    offset: u64,
    size: u32,
    target_path: std::path::PathBuf,
    check_disk: bool,
    /// Mismatches are only news when verifying.
    verify_mode: bool,
}

impl ChunkJob {
    /// `(offset, None)` when reused or aborted.
    async fn run(
        self,
        semaphore: Arc<Semaphore>,
        abort_signal: Option<Arc<AtomicBool>>,
        on_progress: Option<ProgressFn>,
    ) -> Result<(u64, Option<Vec<u8>>), Error> {
        let aborted = || {
            abort_signal
                .as_ref()
                .is_some_and(|s| s.load(Ordering::Relaxed))
        };
        if aborted() {
            return Ok((self.offset, None));
        }
        let _permit = semaphore.acquire_owned().await?;
        if aborted() {
            return Ok((self.offset, None));
        }
        if self.check_disk && self.on_disk().await? {
            tracing::debug!("Verified chunk {}", self.id);
            if let Some(cb) = &on_progress {
                cb(self.size as u64, true);
            }
            return Ok((self.offset, None));
        }
        let data = self
            .inner
            .get_chunk(
                self.app_id,
                self.depot_id,
                self.depot_key,
                self.id.clone(),
                &self.sha,
                self.size,
            )
            .await?;
        // On arrival, not in file order.
        if let Some(cb) = &on_progress {
            cb(data.len() as u64, false);
        }
        Ok((self.offset, Some(data)))
    }

    /// Whether the bytes on disk match.
    async fn on_disk(&self) -> Result<bool, Error> {
        match disk_chunk(&self.target_path, self.offset, self.size, &self.sha).await? {
            DiskChunk::Matches => Ok(true),
            DiskChunk::Missing => Ok(false),
            DiskChunk::Differs => {
                if mismatch_is_corruption(self.verify_mode) {
                    tracing::warn!("Corruption detected in chunk {}", self.id);
                } else {
                    tracing::debug!("Chunk {} not on disk yet", self.id);
                }
                Ok(false)
            }
        }
    }
}

/// What the file holds at a chunk's range.
#[derive(Debug, PartialEq, Eq)]
enum DiskChunk {
    /// File too short or unreadable.
    Missing,
    Matches,
    Differs,
}

/// Fresh or partial writes differ normally.
fn mismatch_is_corruption(verify_mode: bool) -> bool {
    verify_mode
}

async fn disk_chunk(
    path: &std::path::Path,
    offset: u64,
    size: u32,
    sha: &[u8],
) -> Result<DiskChunk, Error> {
    let Ok(mut file) = tokio::fs::File::open(path).await else {
        return Ok(DiskChunk::Missing);
    };
    let len = file.metadata().await.map(|m| m.len()).unwrap_or(0);
    // Oversized chunks: refetch, then reject.
    if size as usize > crate::utils::lzma::MAX_CHUNK_LEN {
        return Ok(DiskChunk::Missing);
    }
    if len < offset + size as u64 {
        return Ok(DiskChunk::Missing);
    }
    file.seek(tokio::io::SeekFrom::Start(offset))
        .await
        .map_err(Error::Io)?;
    let mut buffer = vec![0u8; size as usize];
    if file.read_exact(&mut buffer).await.is_err() {
        return Ok(DiskChunk::Missing);
    }
    Ok(if sha1::Sha1::digest(&buffer).as_slice() == sha {
        DiskChunk::Matches
    } else {
        DiskChunk::Differs
    })
}

/// Write `(offset, data)` results in arrival order.
///
/// `None` data means the chunk was already on disk.
async fn write_chunks<W, S>(
    out: &mut W,
    results: S,
    abort_signal: Option<&AtomicBool>,
) -> Result<(), Error>
where
    W: AsyncWriteExt + AsyncSeekExt + Unpin,
    S: Stream<Item = Result<(u64, Option<Vec<u8>>), Error>>,
{
    let mut results = std::pin::pin!(results);
    while let Some(result) = results.next().await {
        if abort_signal.is_some_and(|s| s.load(Ordering::Relaxed)) {
            break;
        }
        let (offset, data) = result?;
        // Counted by the task on arrival.
        if let Some(data) = data {
            out.seek(tokio::io::SeekFrom::Start(offset))
                .await
                .map_err(Error::Io)?;
            out.write_all(&data).await.map_err(Error::Io)?;
        }
    }
    out.flush().await.map_err(Error::Io)
}

#[cfg(test)]
mod in_memory_tests {
    use super::*;

    #[test]
    fn in_memory_len_is_capped() {
        assert!(check_in_memory_len(0).is_ok());
        assert!(check_in_memory_len(MAX_IN_MEMORY_LEN).is_ok());
        assert!(check_in_memory_len(MAX_IN_MEMORY_LEN + 1).is_err());
        assert!(check_in_memory_len(u64::MAX).is_err());
    }
}

#[cfg(all(test, unix))]
mod executable_tests {
    use super::*;

    #[test]
    fn elf_and_scripts_look_executable() {
        assert!(looks_executable(b"\x7fELF"));
        assert!(looks_executable(b"#!/b"));
    }

    #[test]
    fn mach_o_binaries_look_executable() {
        for head in [
            b"\xcf\xfa\xed\xfe",
            b"\xce\xfa\xed\xfe",
            b"\xca\xfe\xba\xbe",
            b"\xca\xfe\xba\xbf",
        ] {
            assert!(looks_executable(head), "{head:x?}");
        }
    }

    #[test]
    fn data_files_do_not_look_executable() {
        for head in [&b""[..], b"#", b"\x7fEL", b"PK\x03\x04", b"\x89PNG"] {
            assert!(!looks_executable(head), "{head:x?}");
        }
    }
}

#[cfg(test)]
mod write_tests {
    use super::*;

    fn chunk(offset: u64, byte: u8, len: usize) -> Result<(u64, Option<Vec<u8>>), Error> {
        Ok((offset, Some(vec![byte; len])))
    }

    #[tokio::test]
    async fn out_of_order_chunks_land_at_their_offsets() {
        let mut out = std::io::Cursor::new(vec![0u8; 12]);
        let results = futures::stream::iter(vec![
            chunk(8, b'c', 4),
            Ok((4, None)), // already on disk
            chunk(0, b'a', 4),
        ]);
        write_chunks(&mut out, results, None).await.unwrap();
        assert_eq!(out.into_inner(), b"aaaa\0\0\0\0cccc".to_vec());
    }

    #[tokio::test]
    async fn abort_stops_before_the_next_write() {
        let mut out = std::io::Cursor::new(vec![0u8; 4]);
        let abort = AtomicBool::new(true);
        let results = futures::stream::iter(vec![chunk(0, b'a', 4)]);
        write_chunks(&mut out, results, Some(&abort)).await.unwrap();
        assert_eq!(out.into_inner(), vec![0u8; 4]);
    }

    #[tokio::test]
    async fn a_failed_chunk_is_reported() {
        let mut out = std::io::Cursor::new(vec![0u8; 4]);
        let results = futures::stream::iter(vec![Err(Error::Unexpected("boom".into()))]);
        assert!(write_chunks(&mut out, results, None).await.is_err());
    }
}

#[cfg(test)]
mod disk_chunk_tests {
    use super::*;

    fn sha(data: &[u8]) -> Vec<u8> {
        sha1::Sha1::digest(data).to_vec()
    }

    #[tokio::test]
    async fn preallocated_zeros_differ_but_are_not_corruption() {
        let dir = std::env::temp_dir().join(format!("cdn-disk-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("prealloc.bin");
        std::fs::write(&path, vec![0u8; 8]).unwrap();
        let got = disk_chunk(&path, 0, 4, &sha(b"real")).await.unwrap();
        assert_eq!(got, DiskChunk::Differs);
        assert!(!mismatch_is_corruption(false));
        assert!(mismatch_is_corruption(true));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn matching_and_short_files() {
        let dir = std::env::temp_dir().join(format!("cdn-disk2-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("data.bin");
        std::fs::write(&path, b"xxxxreal").unwrap();
        assert_eq!(
            disk_chunk(&path, 4, 4, &sha(b"real")).await.unwrap(),
            DiskChunk::Matches
        );
        assert_eq!(
            disk_chunk(&path, 6, 4, &sha(b"real")).await.unwrap(),
            DiskChunk::Missing
        );
        assert_eq!(
            disk_chunk(&dir.join("absent"), 0, 4, &sha(b"real"))
                .await
                .unwrap(),
            DiskChunk::Missing
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
