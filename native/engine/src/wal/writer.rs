use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use super::entry::WalOp;
use super::{Lsn, WalEntry};

/// Append-only WAL writer. Single mutex serializes writes; group commit
/// refinement lives in PR 8.
pub struct WalWriter {
    path: PathBuf,
    inner: Mutex<BufWriter<File>>,
    next_lsn: AtomicU64,
}

impl WalWriter {
    /// Open or create a WAL file at `path`. Existing content is preserved
    /// (appended to); `next_lsn` starts at 1 unless `starting_lsn` is set
    /// by the caller after reading the existing WAL.
    pub fn open<P: AsRef<Path>>(path: P, starting_lsn: Lsn) -> Result<Self, String> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| format!("WAL open {:?}: {}", path, e))?;
        Ok(Self {
            path,
            inner: Mutex::new(BufWriter::with_capacity(64 * 1024, file)),
            next_lsn: AtomicU64::new(starting_lsn.max(1)),
        })
    }

    /// Append an operation and return the assigned LSN. Does NOT fsync.
    /// Call `flush_sync` to make writes durable.
    pub fn append(&self, op: WalOp) -> Result<Lsn, String> {
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        let payload = op.encode_payload()?;
        let tag = op.tag();
        let frame = encode_frame(lsn, tag, &payload);
        let mut w = self.inner.lock();
        w.write_all(&frame).map_err(|e| format!("WAL write: {}", e))?;
        Ok(lsn)
    }

    /// Flush buffered writes and fsync to disk. This is the durability
    /// boundary: after this returns, appended entries survive a crash.
    pub fn flush_sync(&self) -> Result<(), String> {
        let mut w = self.inner.lock();
        w.flush().map_err(|e| format!("WAL flush: {}", e))?;
        w.get_ref().sync_data().map_err(|e| format!("WAL fsync: {}", e))?;
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

    /// Convenience: append + flush in one call.
    pub fn append_sync(&self, op: WalOp) -> Result<WalEntry, String> {
        let lsn = self.append(op.clone())?;
        self.flush_sync()?;
        Ok(WalEntry { lsn, op })
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
