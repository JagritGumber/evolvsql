use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use super::entry::WalOp;
use super::{Lsn, WalEntry};

/// Iterator over WAL entries on disk. Stops at the first torn/corrupt
/// frame (common case: crash mid-write) so recovery gets all durable
/// entries up to the damage point.
pub struct WalReader {
    reader: BufReader<File>,
}

impl WalReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let file = File::open(path.as_ref())
            .map_err(|e| format!("WAL reader open: {}", e))?;
        Ok(Self {
            reader: BufReader::with_capacity(64 * 1024, file),
        })
    }

    /// Read the next entry, or Ok(None) at clean EOF, or Err on
    /// corruption beyond the durable prefix.
    ///
    /// Torn writes (partial frame at tail) are treated as EOF: the
    /// caller should trust the last successfully-returned entry as the
    /// durable frontier.
    pub fn next_entry(&mut self) -> Result<Option<WalEntry>, String> {
        // Read frame_len (4 bytes)
        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(format!("WAL read len: {}", e)),
        }
        let frame_len = u32::from_le_bytes(len_buf) as usize;
        // Bound-check before allocating: torn writes can produce u32::MAX here
        // which would OOM. 64MB is larger than any legitimate frame (vectors
        // and rows are far smaller), so this safely rejects garbage.
        const MAX_FRAME: usize = 64 * 1024 * 1024;
        if frame_len < 8 + 1 + 4 || frame_len > MAX_FRAME {
            return Ok(None); // malformed frame, treat as torn tail
        }

        let mut frame = vec![0u8; frame_len];
        if let Err(e) = self.reader.read_exact(&mut frame) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None); // torn write at tail
            }
            return Err(format!("WAL read frame: {}", e));
        }

        // Split: lsn(8) | tag(1) | payload | crc(4)
        let payload_end = frame_len - 4;
        let crc_stored = u32::from_le_bytes(frame[payload_end..].try_into().unwrap());
        let crc_actual = crc32fast::hash(&frame[..payload_end]);
        if crc_stored != crc_actual {
            return Ok(None); // corrupt frame, treat as end of durable log
        }

        let lsn = Lsn::from_le_bytes(frame[0..8].try_into().unwrap());
        let tag = frame[8];
        let payload = &frame[9..payload_end];
        let op = WalOp::decode(tag, payload)?;
        Ok(Some(WalEntry { lsn, op }))
    }

    /// Collect all entries into a Vec. Convenience for recovery.
    pub fn read_all(mut self) -> Result<Vec<WalEntry>, String> {
        let mut entries = Vec::new();
        while let Some(e) = self.next_entry()? {
            entries.push(e);
        }
        Ok(entries)
    }
}
