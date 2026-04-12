use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use crate::types::Value;

use super::format::{decode_header, ColumnMeta, SegmentFooter, HEADER_SIZE};

/// Opens a segment file and exposes column-level random access.
///
/// The reader loads the header + footer on open (cheap, O(1) in file
/// size) but does not read any column data until requested. This lets
/// query execution pull only the columns it needs.
pub struct SegmentReader {
    file: BufReader<File>,
    footer: SegmentFooter,
}

impl SegmentReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let file = File::open(path.as_ref())
            .map_err(|e| format!("segment open: {}", e))?;
        let mut reader = BufReader::with_capacity(64 * 1024, file);

        // Read fixed header
        let mut header = [0u8; HEADER_SIZE];
        reader.read_exact(&mut header)
            .map_err(|e| format!("segment read header: {}", e))?;
        let (_row_count, _col_count, footer_offset) = decode_header(&header)?;

        // Seek to footer and decode
        reader.seek(SeekFrom::Start(footer_offset))
            .map_err(|e| format!("segment seek footer: {}", e))?;
        let mut footer_bytes = Vec::new();
        reader.read_to_end(&mut footer_bytes)
            .map_err(|e| format!("segment read footer: {}", e))?;
        let footer: SegmentFooter = bincode::deserialize(&footer_bytes)
            .map_err(|e| format!("segment decode footer: {}", e))?;

        Ok(Self { file: reader, footer })
    }

    /// Total row count stored in this segment.
    pub fn row_count(&self) -> u32 {
        self.footer.row_count
    }

    /// All column metadata (schema + zone maps).
    pub fn columns(&self) -> &[ColumnMeta] {
        &self.footer.columns
    }

    /// Find a column by name. Case-sensitive.
    pub fn column_meta(&self, name: &str) -> Option<&ColumnMeta> {
        self.footer.columns.iter().find(|c| c.name == name)
    }

    /// Read a single column's values. This is the primary columnar
    /// access path: callers scan only the columns they need.
    pub fn read_column(&mut self, name: &str) -> Result<Vec<Value>, String> {
        let meta = self.column_meta(name)
            .ok_or_else(|| format!("segment: column \"{}\" not found", name))?;
        let offset = meta.offset;
        let length = meta.length as usize;
        self.file.seek(SeekFrom::Start(offset))
            .map_err(|e| format!("segment seek col: {}", e))?;
        let mut buf = vec![0u8; length];
        self.file.read_exact(&mut buf)
            .map_err(|e| format!("segment read col: {}", e))?;
        bincode::deserialize(&buf)
            .map_err(|e| format!("segment decode col: {}", e))
    }

    /// Read all rows by reconstructing the row-oriented view. Useful
    /// for full-table scans and tests. For production SELECTs, prefer
    /// `read_column` to scan only needed columns.
    pub fn read_all_rows(&mut self) -> Result<Vec<Vec<Value>>, String> {
        let names: Vec<String> = self.footer.columns.iter().map(|c| c.name.clone()).collect();
        let mut cols: Vec<Vec<Value>> = Vec::with_capacity(names.len());
        for n in &names {
            cols.push(self.read_column(n)?);
        }
        let row_count = self.footer.row_count as usize;
        let mut rows = Vec::with_capacity(row_count);
        for i in 0..row_count {
            let row: Vec<Value> = cols.iter().map(|c| c[i].clone()).collect();
            rows.push(row);
        }
        Ok(rows)
    }
}
