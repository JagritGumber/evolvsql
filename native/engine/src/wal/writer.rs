use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(test)]
use std::sync::atomic::AtomicBool;

use parking_lot::Mutex;

use super::entry::WalOp;
use super::{Lsn, WalEntry};

/// Append-only WAL writer. Single mutex serializes writes; group commit
/// refinement lives in PR 8.
///
/// Durability invariant: bytes [0, durable_len) on the underlying file
/// are known to be fsynced to disk. On any I/O failure we truncate the
/// file back to durable_len so a failed append or flush never becomes
/// visible via a later successful sync. The BufWriter was removed for
/// this reason: it silently retained buffered bytes across a failed
/// flush_sync, which then became durable on the next successful one
/// even though the caller had already returned the earlier frame's
/// error to its own caller and aborted the logical operation. Recovery
/// would then replay a frame whose user-visible work had been rolled
/// back at the SQL layer — classic lost-update / phantom-write territory.
pub struct WalWriter {
    path: PathBuf,
    inner: Mutex<WriterState>,
    next_lsn: AtomicU64,
    #[cfg(test)]
    pub(crate) fail_next_sync: AtomicBool,
}

struct WriterState {
    file: File,
    /// File length that is known to be on disk. Bytes after this are
    /// either mid-write or waiting in the OS page cache and may be
    /// discarded via `set_len` on failure.
    durable_len: u64,
    /// First LSN in the current un-flushed batch. Set on the first
    /// successful append since the last durable boundary; cleared on
    /// a successful flush_sync. On failure we use this to rewind
    /// `next_lsn` so a retry starts from the durable frontier again.
    undurable_start_lsn: Option<Lsn>,
}

impl WalWriter {
    /// Open or create a WAL file at `path`. Existing content is preserved
    /// (appended to); `next_lsn` starts at 1 unless `starting_lsn` is set
    /// by the caller after reading the existing WAL.
    pub fn open<P: AsRef<Path>>(path: P, starting_lsn: Lsn) -> Result<Self, String> {
        let path = path.as_ref().to_path_buf();
        // Open with write (not append) so `set_len` has the access
        // rights to truncate on rollback. On Windows, append-mode
        // opens use FILE_APPEND_DATA which does NOT grant
        // FILE_WRITE_DATA, so SetEndOfFile (the Rust `set_len` backend)
        // silently fails and the rollback leaves buffered bytes in
        // the file. With plain write we manage the write position
        // ourselves — simple since we're the only writer and only
        // ever append to end.
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&path)
            .map_err(|e| format!("WAL open {:?}: {}", path, e))?;
        let durable_len = file
            .seek(SeekFrom::End(0))
            .map_err(|e| format!("WAL seek end: {}", e))?;
        Ok(Self {
            path,
            inner: Mutex::new(WriterState {
                file,
                durable_len,
                undurable_start_lsn: None,
            }),
            next_lsn: AtomicU64::new(starting_lsn.max(1)),
            #[cfg(test)]
            fail_next_sync: AtomicBool::new(false),
        })
    }

    /// Append an operation and return the assigned LSN. Does NOT fsync.
    /// Call `flush_sync` to make writes durable.
    ///
    /// LSN assignment is done INSIDE the writer lock so that the order
    /// of LSNs always matches the order of frames in the file. An
    /// earlier version assigned LSNs via a separate atomic before the
    /// lock; under concurrent appends that allowed a thread with a
    /// lower LSN to write its frame after a thread with a higher one,
    /// which would leave recovery replaying entries out of logical
    /// order — and, worse, a torn-write tail could drop the
    /// lower-LSN entry while keeping the higher-LSN one durable.
    ///
    /// On write failure the file is truncated back to `durable_len`
    /// and `next_lsn` rewound to the start of the current un-flushed
    /// batch, so the caller can retry without leaving a phantom frame.
    pub fn append(&self, op: WalOp) -> Result<Lsn, String> {
        let payload = op.encode_payload()?;
        let tag = op.tag();
        let mut state = self.inner.lock();
        let lsn = self.next_lsn.load(Ordering::SeqCst);
        let frame = encode_frame(lsn, tag, &payload);
        if let Err(e) = state.file.write_all(&frame) {
            rollback(&mut state, &self.next_lsn);
            return Err(format!("WAL write: {}", e));
        }
        if state.undurable_start_lsn.is_none() {
            state.undurable_start_lsn = Some(lsn);
        }
        self.next_lsn.store(lsn + 1, Ordering::SeqCst);
        Ok(lsn)
    }

    /// Flush any pending writes and fsync to disk. This is the
    /// durability boundary: on success, every frame appended since the
    /// last successful flush_sync is guaranteed on disk. On failure
    /// those frames are discarded via a truncate-back so a later
    /// successful flush cannot resurrect them.
    pub fn flush_sync(&self) -> Result<(), String> {
        let mut state = self.inner.lock();
        #[cfg(test)]
        if self.fail_next_sync.swap(false, Ordering::SeqCst) {
            rollback(&mut state, &self.next_lsn);
            return Err("WAL fsync: test injected failure".into());
        }
        if let Err(e) = state.file.sync_data() {
            rollback(&mut state, &self.next_lsn);
            return Err(format!("WAL fsync: {}", e));
        }
        let len = state
            .file
            .stream_position()
            .map_err(|e| format!("WAL stream_position: {}", e))?;
        state.durable_len = len;
        state.undurable_start_lsn = None;
        Ok(())
    }

    /// Path of the underlying WAL file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// LSN that will be assigned to the next append.
    pub fn peek_next_lsn(&self) -> Lsn {
        self.next_lsn.load(Ordering::SeqCst)
    }

    /// Append a frame AND fsync it in a single locked section. This is
    /// the concurrency-safe path and the one production callers must
    /// use. The naive composition of `append` + `flush_sync` releases
    /// the inner mutex between the two and is unsafe: thread A's
    /// failing `flush_sync` can roll back to `durable_len`, truncating
    /// thread B's successfully-appended-but-not-yet-flushed frame. B's
    /// subsequent `flush_sync` would then trivially succeed (nothing
    /// left to sync) and return Ok, silently violating its durability
    /// contract. Holding the mutex across both steps eliminates the
    /// interleaving entirely — no other thread can observe or be
    /// affected by a partial state, and the rollback path only ever
    /// sees the one frame this call wrote.
    pub fn append_sync(&self, op: WalOp) -> Result<WalEntry, String> {
        let payload = op.encode_payload()?;
        let tag = op.tag();
        let mut state = self.inner.lock();
        let lsn = self.next_lsn.load(Ordering::SeqCst);
        let frame = encode_frame(lsn, tag, &payload);
        // On failure, always use the full `rollback` — not a
        // single-frame truncate. If a prior `append` call left
        // un-flushed frames in the file, truncating to `durable_len`
        // destroys them too, and without rewinding `next_lsn` the
        // next write would skip those destroyed LSNs and produce a
        // permanent gap recovery would treat as a torn tail.
        if let Err(e) = state.file.write_all(&frame) {
            rollback(&mut state, &self.next_lsn);
            return Err(format!("WAL write: {}", e));
        }
        #[cfg(test)]
        if self.fail_next_sync.swap(false, Ordering::SeqCst) {
            rollback(&mut state, &self.next_lsn);
            return Err("WAL fsync: test injected failure".into());
        }
        if let Err(e) = state.file.sync_data() {
            rollback(&mut state, &self.next_lsn);
            return Err(format!("WAL fsync: {}", e));
        }
        let new_len = state
            .file
            .stream_position()
            .map_err(|e| format!("WAL stream_position: {}", e))?;
        state.durable_len = new_len;
        // `sync_data` fsyncs the whole file, so any frames left over
        // from prior `append` calls are now durable too. Clear the
        // batch marker so a later failing `flush_sync` doesn't rewind
        // `next_lsn` past frames that are already on disk.
        state.undurable_start_lsn = None;
        self.next_lsn.store(lsn + 1, Ordering::SeqCst);
        Ok(WalEntry { lsn, op })
    }
}

/// Roll the writer's state back to the last durable boundary. Used
/// by both `append` + `flush_sync` and `append_sync` on any I/O
/// failure so a later successful flush cannot resurrect a frame the
/// caller was told had failed. Truncates the file, rewinds the write
/// cursor (so the next append lands at the right offset instead of
/// creating a hole), and — if there was an un-flushed batch in
/// progress — rewinds `next_lsn` to the start of that batch so
/// retries reclaim the lost LSNs rather than skipping past them and
/// leaving a permanent gap recovery would mistake for a torn tail.
fn rollback(state: &mut WriterState, next_lsn: &AtomicU64) {
    let _ = state.file.set_len(state.durable_len);
    let _ = state.file.seek(SeekFrom::Start(state.durable_len));
    if let Some(start) = state.undurable_start_lsn.take() {
        next_lsn.store(start, Ordering::SeqCst);
    }
}

/// Encode a single WAL frame:
/// [frame_len: u32 LE] [lsn: u64 LE] [tag: u8] [payload] [crc32: u32 LE]
pub(super) fn encode_frame(lsn: Lsn, tag: u8, payload: &[u8]) -> Vec<u8> {
    // frame_len covers: lsn(8) + tag(1) + payload + crc(4)
    let frame_len: u32 = (8 + 1 + payload.len() + 4) as u32;
    let mut buf = Vec::with_capacity(4 + frame_len as usize);
    buf.extend_from_slice(&frame_len.to_le_bytes());
    buf.extend_from_slice(&lsn.to_le_bytes());
    buf.push(tag);
    buf.extend_from_slice(payload);
    // CRC covers lsn + tag + payload (bytes 4..end-4 of final frame)
    let crc = crc32fast::hash(&buf[4..]);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}
