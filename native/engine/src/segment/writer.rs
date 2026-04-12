use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

use crate::types::Value;

use super::format::{encode_header, ColumnMeta, SegmentFooter, HEADER_SIZE};
use super::zone_map::compute_zone_map;

/// Build a segment file from a batch of rows. Rows are transposed into
/// columnar layout, each column is encoded independently with bincode,
/// zone maps (min/max/null_count) are computed, and the footer is
/// written at the end.
pub struct SegmentWriter;

impl SegmentWriter {
    /// Write `rows` to `path` using `columns` as the schema. All rows
    /// must have the same arity as `columns`.
    pub fn write<P: AsRef<Path>>(
        path: P,
        columns: &[(String, i32)],
        rows: &[Vec<Value>],
    ) -> Result<(), String> {
        let file = File::create(path.as_ref())
            .map_err(|e| format!("segment create: {}", e))?;
        let mut w = BufWriter::with_capacity(64 * 1024, file);

        // Reserve header space; we'll fill it in after we know the footer offset.
        w.write_all(&[0u8; HEADER_SIZE])
            .map_err(|e| format!("segment header reserve: {}", e))?;

        // Validate row shape before transposing.
        for (i, row) in rows.iter().enumerate() {
            if row.len() != columns.len() {
                return Err(format!(
                    "segment row {} arity {} != column count {}",
                    i, row.len(), columns.len()
                ));
            }
        }

        // Transpose rows into columns and write each column block.
        let mut col_metas = Vec::with_capacity(columns.len());
        for (col_idx, (name, type_oid)) in columns.iter().enumerate() {
            let col_values: Vec<Value> = rows.iter().map(|r| r[col_idx].clone()).collect();
            let (min, max, null_count) = compute_zone_map(&col_values);

            let offset = w.stream_position()
                .map_err(|e| format!("segment stream pos: {}", e))?;
            let encoded = bincode::serialize(&col_values)
                .map_err(|e| format!("segment col encode: {}", e))?;
            w.write_all(&encoded)
                .map_err(|e| format!("segment col write: {}", e))?;
            let length = encoded.len() as u64;

            col_metas.push(ColumnMeta {
                name: name.clone(),
                type_oid: *type_oid,
                offset, length, min, max, null_count,
            });
        }

        // Write the footer at the current position.
        let footer_offset = w.stream_position()
            .map_err(|e| format!("segment footer pos: {}", e))?;
        let footer = SegmentFooter { columns: col_metas, row_count: rows.len() as u32 };
        let footer_bytes = bincode::serialize(&footer)
            .map_err(|e| format!("segment footer encode: {}", e))?;
        w.write_all(&footer_bytes)
            .map_err(|e| format!("segment footer write: {}", e))?;

        // Seek back and fill the header with the now-known footer_offset.
        let header = encode_header(rows.len() as u32, columns.len() as u16, footer_offset);
        w.seek(SeekFrom::Start(0))
            .map_err(|e| format!("segment seek header: {}", e))?;
        w.write_all(&header)
            .map_err(|e| format!("segment header write: {}", e))?;

        w.flush().map_err(|e| format!("segment flush: {}", e))?;
        w.get_ref().sync_data()
            .map_err(|e| format!("segment fsync: {}", e))?;
        Ok(())
    }
}
