// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use bytes::Bytes;
use flate2::Compression;
use flate2::write::GzEncoder;
use std::io::Write;

use super::error::Error;

const ONE_MB: usize = 1024 * 1024; // 1 MB
const MAX_GZIP_FLUSH_COUNT: usize = 100;
/// Safety margin for gzip overhead:
///
/// - gzip header + trailer: ~18 bytes
/// - worst-case deflate stored-block headers for ~1MB: ~320 bytes
/// - sync flush overhead: ~5 bytes per flush × 100 flushes = ~500 bytes
/// - slack: ~3KB
///
/// Total: ~4KB is generous.
///
/// Measured flush counts (worst case: 31 flushes for single-byte entries at level 9):
///
/// | Profile     | Entry Size | Level 1 | Level 6 | Level 9 |
/// |-------------|------------|---------|---------|---------|
/// | single_byte | 1 B        | 21      | 30      | 31      |
/// | hex         | 10 B       | 12      | 12      | 12      |
/// | json        | 256 B      | 10      | 10      | 10      |
/// | ascii       | 256 B      | 5       | 6       | 5       |
/// | hex         | 1 KB       | 10      | 9       | 9       |
/// | json        | 1 KB       | 8       | 7       | 8       |
/// | ascii       | 1 KB       | 4       | 5       | 5       |
/// | json        | 2 KB       | 7       | 7       | 7       |
/// | hex         | 16 KB      | 7       | 6       | 6       |
/// | json        | 16 KB      | 6       | 5       | 6       |
/// | ascii       | 16 KB      | 3       | 3       | 3       |
/// | mixed       | 1B–16KB    | 6       | 6       | 6       |
const GZIP_SAFETY_MARGIN: usize = 4096;

/// Accumulates JSON entries into gzip-compressed batches that stay under a size limit.
pub struct GzipBatcher {
    buf: GzEncoder<Vec<u8>>,
    compression: Compression,
    remaining_size: usize,
    uncompressed_size: usize,
    total_uncompressed_size: usize,
    row_count: u64,
    flush_count: usize,
    batch_id: u64,
    pending_batch: Option<GzipResult>,
}

/// Result of pushing an entry into the batcher.
pub enum PushResult {
    /// Entry accepted into the current batch (returns batch id).
    Ok(u64),
    /// Entry exceeds the maximum allowed size.
    TooLarge,
    /// A batch is ready to be taken (returns the new batch id).
    BatchReady(u64),
}

/// Result of finalizing the current batch.
pub enum FinalizeResult {
    /// No data was present to finalize.
    Empty,
    /// Batch finalized successfully.
    Ok,
}

/// A completed gzip-compressed batch.
pub struct GzipResult {
    /// Unique identifier for this batch.
    pub batch_id: u64,
    /// The gzip-compressed payload.
    pub compressed_data: Bytes,
    /// Number of entries in this batch.
    pub row_count: u64,
    /// Number of gzip sync flushes performed while building this batch.
    pub flush_count: usize,
}

impl GzipBatcher {
    /// Create a new batcher with the given gzip compression level (0-9).
    #[must_use]
    pub fn new(compression_level: u32) -> Self {
        let compression = Compression::new(compression_level);
        Self {
            buf: Self::new_encoder(compression),
            compression,
            remaining_size: ONE_MB - GZIP_SAFETY_MARGIN,
            uncompressed_size: 0,
            total_uncompressed_size: 0,
            row_count: 0,
            flush_count: 0,
            batch_id: 0,
            pending_batch: None,
        }
    }

    fn new_encoder(compression: Compression) -> GzEncoder<Vec<u8>> {
        GzEncoder::new(Vec::with_capacity(ONE_MB), compression)
    }

    /// Returns `true` if the encoder buffer contains uncommitted data.
    #[inline]
    pub fn has_pending_data(&self) -> bool {
        !self.buf.get_ref().is_empty()
    }

    /// Push an entry into the batcher. Returns the push result.
    #[inline]
    pub fn push(&mut self, data: &[u8]) -> Result<PushResult, Error> {
        if self.pending_batch.is_some() {
            return Ok(PushResult::BatchReady(self.batch_id));
        }

        self.push_internal(data)
    }

    fn push_internal(&mut self, data: &[u8]) -> Result<PushResult, Error> {
        // Account for structural JSON bytes: '[' or ',' prefix + ']' for finalization.
        // Reject entries that can't possibly fit in a single batch.
        if data.len() + 2 > (ONE_MB - GZIP_SAFETY_MARGIN) {
            return Ok(PushResult::TooLarge);
        }

        let is_first_entry = self.uncompressed_size == 0;

        if is_first_entry {
            self.batch_id += 1;
            self.buf.write_all(b"[").map_err(Error::BatchPushFailed)?;
        }

        // Include structural overhead: ',' for non-first entries, ']' for finalization.
        let structural_overhead = if is_first_entry { 0 } else { 1 }; // ','
        let finalize_overhead = 1; // ']'
        let next_size =
            self.uncompressed_size + structural_overhead + data.len() + finalize_overhead;
        let must_flush = next_size > self.remaining_size;

        if must_flush {
            self.buf.flush().map_err(Error::BatchPushFailed)?;

            self.flush_count += 1;
            let compressed_size = self.buf.get_ref().len();

            // Use the constant here
            self.remaining_size = ONE_MB.saturating_sub(compressed_size + GZIP_SAFETY_MARGIN);
            self.uncompressed_size = 0;
        }

        let next_size =
            self.uncompressed_size + structural_overhead + data.len() + finalize_overhead;
        let must_finalize =
            next_size > self.remaining_size || self.flush_count >= MAX_GZIP_FLUSH_COUNT;

        if must_finalize {
            let finalize_result = self.finalize()?;
            // We attempt to push the data to the next batch.
            // If this fails, we propagate the error.
            // Note: If finalize succeeded, we have a pending batch ready.
            // The recursive push will start a new batch (id+1).
            let _ = self.push_internal(data)?;

            match finalize_result {
                FinalizeResult::Empty => Ok(PushResult::Ok(self.batch_id)),
                FinalizeResult::Ok => {
                    // this is the new batch id that we are currently building
                    // the pending batch id is available in the pending_batch field
                    Ok(PushResult::BatchReady(self.batch_id))
                }
            }
        } else {
            if !is_first_entry {
                self.buf.write_all(b",").map_err(Error::BatchPushFailed)?;
                self.total_uncompressed_size += 1;
                self.uncompressed_size += 1;
            }
            self.buf.write_all(data).map_err(Error::BatchPushFailed)?;
            self.uncompressed_size += data.len();
            self.total_uncompressed_size += data.len();
            self.row_count += 1;

            Ok(PushResult::Ok(self.batch_id))
        }
    }

    /// Finalize the current batch, making it available via [`take_pending_batch`](Self::take_pending_batch).
    pub fn finalize(&mut self) -> Result<FinalizeResult, Error> {
        if self.buf.get_ref().is_empty() {
            return Ok(FinalizeResult::Empty);
        }

        self.buf
            .write_all(b"]")
            .map_err(Error::BatchFinalizeFailed)?;

        let old_buf = std::mem::replace(&mut self.buf, Self::new_encoder(self.compression));

        let compressed_data = old_buf.finish().map_err(Error::BatchFinalizeFailed)?;
        let row_count = self.row_count;
        let flush_count = self.flush_count;

        // Reset state
        self.remaining_size = ONE_MB - GZIP_SAFETY_MARGIN;
        self.uncompressed_size = 0;
        self.total_uncompressed_size = 0;
        self.row_count = 0;
        self.flush_count = 0;

        self.pending_batch = Some(GzipResult {
            batch_id: self.batch_id,
            compressed_data: Bytes::from(compressed_data),
            row_count,
            flush_count,
        });

        Ok(FinalizeResult::Ok)
    }

    /// Take the pending completed batch, if any.
    #[inline]
    pub fn take_pending_batch(&mut self) -> Option<GzipResult> {
        self.pending_batch.take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use rand::RngExt;
    use std::io::Read;

    // ==================== Test Helpers ====================

    fn generate_data(size: usize) -> Vec<u8> {
        let mut rng = rand::rng();
        let id = rng.random_range(10000..99999);
        let timestamp = rng.random_range(1600000000..1700000000);

        let base = format!(r#"{{"id":{},"ts":{},"msg":""#, id, timestamp);
        let closing = r#""}"#;

        let padding_needed = size.saturating_sub(base.len() + closing.len());
        let padding: String = (0..padding_needed)
            .map(|_| rng.random_range(b'a'..=b'z') as char)
            .collect();

        format!("{}{}{}", base, padding, closing).into_bytes()
    }

    fn generate_1kb_data() -> Vec<u8> {
        generate_data(1024)
    }

    fn decompress_and_validate(data: &Bytes) -> String {
        let mut decoder = GzDecoder::new(&data[..]);
        let mut decompressed = String::new();
        _ = decoder
            .read_to_string(&mut decompressed)
            .expect("Should decompress");

        let trimmed = decompressed.trim();
        assert!(trimmed.starts_with('['), "Should start with [");
        assert!(trimmed.ends_with(']'), "Should end with ]");

        // Remove all whitespace to check for structural issues like [, or ,]
        let no_whitespace: String = decompressed
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();

        // Ensure no invalid comma placement (ignoring whitespace)
        assert!(
            !no_whitespace.contains("[,") && !no_whitespace.contains(",]"),
            "Invalid comma placement found in JSON: {}",
            decompressed
        );

        decompressed
    }

    // ==================== Construction & State Tests ====================

    #[test]
    fn test_new_creates_empty_batcher() {
        let batcher = GzipBatcher::new(1);
        assert!(!batcher.has_pending_data());
        assert!(batcher.pending_batch.is_none());
    }

    #[test]
    fn test_has_pending_data_lifecycle() {
        let mut batcher = GzipBatcher::new(1);
        assert!(!batcher.has_pending_data());

        let _ = batcher.push(&generate_1kb_data()).unwrap();
        assert!(batcher.has_pending_data());

        let _ = batcher.finalize().unwrap();
        assert!(!batcher.has_pending_data());
    }

    #[test]
    fn test_take_pending_batch_lifecycle() {
        let mut batcher = GzipBatcher::new(1);
        assert!(batcher.take_pending_batch().is_none());

        let _ = batcher.push(&generate_1kb_data()).unwrap();
        let _ = batcher.finalize().unwrap();

        let batch = batcher.take_pending_batch();
        assert!(batch.is_some());
        assert!(batcher.take_pending_batch().is_none());
    }

    // ==================== Push Logic Tests ====================

    #[test]
    fn test_push_single_entry() {
        let mut batcher = GzipBatcher::new(1);
        match batcher.push(&generate_1kb_data()).unwrap() {
            PushResult::Ok(id) => assert_eq!(id, 1),
            _ => panic!("Should be Ok"),
        }
    }

    #[test]
    fn test_push_too_large_entry() {
        let mut batcher = GzipBatcher::new(1);
        let large_data = vec![b'x'; ONE_MB];
        match batcher.push(&large_data).unwrap() {
            PushResult::TooLarge => {} // Expected
            _ => panic!("Should be TooLarge"),
        }
    }

    #[test]
    fn test_push_just_under_limit() {
        let mut batcher = GzipBatcher::new(1);
        // Max allowed: ONE_MB - GZIP_SAFETY_MARGIN - 2 (for '[' or ',' and ']')
        let data = vec![b'x'; ONE_MB - GZIP_SAFETY_MARGIN - 2];
        match batcher.push(&data).unwrap() {
            PushResult::Ok(_) | PushResult::BatchReady(_) => {} // Expected
            PushResult::TooLarge => panic!("Should fit"),
        }
    }

    #[test]
    fn test_push_returns_batch_ready_when_pending_exists() {
        let mut batcher = GzipBatcher::new(1);

        // Force a pending batch
        loop {
            if let PushResult::BatchReady(_) = batcher.push(&generate_1kb_data()).unwrap() {
                break;
            }
        }

        // Subsequent pushes should return BatchReady
        match batcher.push(&generate_1kb_data()).unwrap() {
            PushResult::BatchReady(_) => {}
            _ => panic!("Should return BatchReady"),
        }
    }

    #[test]
    fn test_push_batch_id_increments() {
        let mut batcher = GzipBatcher::new(1);
        let mut last_id = 0;

        for _ in 0..3 {
            loop {
                match batcher.push(&generate_1kb_data()).unwrap() {
                    PushResult::Ok(_) => continue,
                    PushResult::BatchReady(id) => {
                        assert!(id > last_id);
                        last_id = id;
                        let _ = batcher.take_pending_batch();
                        break;
                    }
                    _ => panic!("Unexpected"),
                }
            }
        }
    }

    // ==================== Flush & Finalize Tests ====================

    #[test]
    fn test_flush_empty_batcher() {
        let mut batcher = GzipBatcher::new(1);
        match batcher.finalize().unwrap() {
            FinalizeResult::Empty => {}
            _ => panic!("Should be Empty"),
        }
    }

    #[test]
    fn test_flush_with_data() {
        let mut batcher = GzipBatcher::new(1);
        let _ = batcher.push(&generate_1kb_data()).unwrap();

        match batcher.finalize().unwrap() {
            FinalizeResult::Ok => {
                let batch = batcher.take_pending_batch().unwrap();
                assert!(batch.row_count > 0);
                assert!(!batch.compressed_data.is_empty());
            }
            _ => panic!("Should be Ok"),
        }
    }

    #[test]
    fn test_flush_multiple_times() {
        let mut batcher = GzipBatcher::new(1);

        // Batch 1
        let _ = batcher.push(&generate_1kb_data()).unwrap();
        let _ = batcher.finalize().unwrap();
        let b1 = batcher.take_pending_batch().unwrap();

        // Batch 2
        let _ = batcher.push(&generate_1kb_data()).unwrap();
        let _ = batcher.finalize().unwrap();
        let b2 = batcher.take_pending_batch().unwrap();

        assert!(b2.batch_id > b1.batch_id);
    }

    // ==================== Integration & Format Tests ====================

    #[test]
    fn test_output_is_valid_gzip_json_array() {
        let mut batcher = GzipBatcher::new(1);
        for _ in 0..10 {
            let _ = batcher.push(&generate_1kb_data()).unwrap();
        }
        let _ = batcher.finalize().unwrap();

        let batch = batcher.take_pending_batch().unwrap();
        let decompressed = decompress_and_validate(&batch.compressed_data);

        let parsed: Vec<serde_json::Value> = serde_json::from_str(&decompressed).unwrap();
        assert_eq!(parsed.len(), 10);
    }

    #[test]
    fn test_row_count_accuracy() {
        let mut batcher = GzipBatcher::new(1);
        for _ in 0..42 {
            let _ = batcher.push(&generate_1kb_data()).unwrap();
        }
        let _ = batcher.finalize().unwrap();
        assert_eq!(batcher.take_pending_batch().unwrap().row_count, 42);
    }

    #[test]
    fn test_interleaved_push_and_take() {
        let mut batcher = GzipBatcher::new(1);

        let _ = batcher.push(&generate_1kb_data()).unwrap();
        let _ = batcher.finalize().unwrap();
        let _ = batcher.take_pending_batch();

        let _ = batcher.push(&generate_1kb_data()).unwrap();
        let _ = batcher.finalize().unwrap();
        let b2 = batcher.take_pending_batch().unwrap();

        assert_eq!(b2.row_count, 1);
    }

    // ==================== Comma Handling Regression Tests ====================

    #[test]
    fn test_no_leading_comma_after_bracket() {
        let mut batcher = GzipBatcher::new(1);
        let _ = batcher.push(b"1").unwrap();
        let _ = batcher.push(b"2").unwrap();
        let _ = batcher.finalize().unwrap();

        let json = decompress_and_validate(&batcher.take_pending_batch().unwrap().compressed_data);
        assert_eq!(json, "[1,2]");
    }

    #[test]
    fn test_no_trailing_comma_before_bracket() {
        let mut batcher = GzipBatcher::new(1);
        let _ = batcher.push(b"1").unwrap();
        let _ = batcher.finalize().unwrap();

        let json = decompress_and_validate(&batcher.take_pending_batch().unwrap().compressed_data);
        assert_eq!(json, "[1]");
    }

    #[test]
    fn test_format_valid_after_auto_finalize() {
        let mut batcher = GzipBatcher::new(1);

        // Fill until split
        loop {
            if let PushResult::BatchReady(_) = batcher.push(&generate_1kb_data()).unwrap() {
                break;
            }
        }

        let batch = batcher.take_pending_batch().unwrap();
        let json = decompress_and_validate(&batch.compressed_data);

        assert!(!json.contains("[,"));
        assert!(!json.contains(",]"));
        assert!(serde_json::from_str::<Vec<serde_json::Value>>(&json).is_ok());
    }

    #[test]
    fn test_format_valid_for_second_batch() {
        let mut batcher = GzipBatcher::new(1);

        // Fill first batch and discard
        loop {
            if let PushResult::BatchReady(_) = batcher.push(&generate_1kb_data()).unwrap() {
                break;
            }
        }
        let _ = batcher.take_pending_batch();

        // Second batch
        // Note: This batch will start with the "spillover" entry that triggered the previous BatchReady.
        // We append more data to it.
        let _ = batcher.push(b"1").unwrap();
        let _ = batcher.push(b"2").unwrap();
        let _ = batcher.finalize().unwrap();

        // decompress_and_validate checks for [, and ,] and [] wrapping
        let json = decompress_and_validate(&batcher.take_pending_batch().unwrap().compressed_data);

        // If it deserializes successfully, the format is valid.
        let parsed: Result<Vec<serde_json::Value>, _> = serde_json::from_str(&json);
        assert!(
            parsed.is_ok(),
            "Second batch must be valid JSON. Got error: {:?}. Content: {}",
            parsed.err(),
            json
        );

        // We can also verify it contains at least the elements we explicitly added
        let array = parsed.unwrap();
        assert!(array.len() >= 2);
        assert_eq!(array[array.len() - 2], serde_json::json!(1));
        assert_eq!(array[array.len() - 1], serde_json::json!(2));
    }

    // ==================== Size Limit Tests ====================

    struct BatchStats {
        size: usize,
        flush_count: usize,
    }

    /// Helper: fill a batcher until BatchReady, return batch stats.
    fn fill_to_batch_ready(compression_level: u32, gen_chunk: &dyn Fn() -> Vec<u8>) -> BatchStats {
        let mut batcher = GzipBatcher::new(compression_level);
        loop {
            let chunk = gen_chunk();
            match batcher.push(&chunk).unwrap() {
                PushResult::Ok(_) => continue,
                PushResult::BatchReady(_) => break,
                PushResult::TooLarge => panic!("Should not happen with small chunks"),
            }
        }
        let batch = batcher.take_pending_batch().unwrap();
        BatchStats {
            size: batch.compressed_data.len(),
            flush_count: batch.flush_count,
        }
    }

    /// Maximum allowed waste: 2% of 1MB.
    /// Larger entries (e.g. 16KB) have coarser finalization granularity,
    /// so waste can reach ~1% at the boundary.
    const MAX_WASTE_PERCENT: f64 = 2.0;

    fn assert_batch_utilization(stats: &BatchStats, label: &str) {
        assert!(
            stats.size <= ONE_MB,
            "{label}: batch size {} exceeds 1MB limit",
            stats.size
        );
        let utilization = stats.size as f64 / ONE_MB as f64 * 100.0;
        let waste = 100.0 - utilization;
        assert!(
            waste <= MAX_WASTE_PERCENT,
            "{label}: batch size {} ({utilization:.1}% utilization, \
             {waste:.1}% waste) exceeds {MAX_WASTE_PERCENT}% waste threshold",
            stats.size
        );
        assert!(
            stats.flush_count <= MAX_GZIP_FLUSH_COUNT,
            "{label}: flush count {} exceeds limit {MAX_GZIP_FLUSH_COUNT}",
            stats.flush_count
        );
    }

    #[test]
    fn test_batch_utilization_hex_data() {
        let hex = b"0123456789abcdef";
        for level in [1u32, 6, 9] {
            for entry_size in [10, 1024, 16384] {
                let stats = fill_to_batch_ready(level, &|| {
                    let mut rng = rand::rng();
                    (0..entry_size)
                        .map(|_| hex[rng.random_range(0..16usize)])
                        .collect()
                });
                assert_batch_utilization(&stats, &format!("hex/{entry_size}B/level_{level}"));
            }
        }
    }

    /// High-entropy random printable ASCII — barely compressible, worst realistic case.
    #[test]
    fn test_batch_utilization_high_entropy_data() {
        for level in [1u32, 6, 9] {
            for entry_size in [256, 1024, 16384] {
                let stats = fill_to_batch_ready(level, &|| {
                    let mut rng = rand::rng();
                    (0..entry_size)
                        .map(|_| rng.random_range(b' '..=b'~'))
                        .collect()
                });
                assert_batch_utilization(&stats, &format!("ascii/{entry_size}B/level_{level}"));
            }
        }
    }

    #[test]
    fn test_batch_utilization_json_data() {
        for level in [1u32, 6, 9] {
            for entry_size in [256, 1024, 2048, 16384] {
                let stats = fill_to_batch_ready(level, &|| generate_data(entry_size));
                assert_batch_utilization(&stats, &format!("json/{entry_size}B/level_{level}"));
            }
        }
    }

    /// Stress test: single-byte entries maximize the ratio of commas to data.
    /// ~50% of the uncompressed stream is commas, exercising the structural
    /// byte accounting heavily.
    #[test]
    fn test_batch_utilization_single_byte_entries() {
        for level in [1u32, 6, 9] {
            let stats = fill_to_batch_ready(level, &|| {
                let mut rng = rand::rng();
                vec![rng.random_range(b'0'..=b'9')]
            });
            assert_batch_utilization(&stats, &format!("single_byte/level_{level}"));
        }
    }

    /// Stress test: wildly varying entry sizes within a single batch.
    /// Mixes tiny (1–10B), medium (256–1KB), and large (8–16KB) entries
    /// to exercise size accounting across granularity transitions.
    #[test]
    fn test_batch_utilization_mixed_entry_sizes() {
        let sizes = [1, 5, 10, 50, 256, 512, 1024, 4096, 8192, 16384];
        for level in [1u32, 6, 9] {
            let counter = std::cell::Cell::new(0usize);
            let stats = fill_to_batch_ready(level, &|| {
                let i = counter.get();
                counter.set(i + 1);
                generate_data(sizes[i % sizes.len()])
            });
            assert_batch_utilization(&stats, &format!("mixed_sizes/level_{level}"));
        }
    }

    /// Regression test: fill with random (incompressible) data to verify the
    /// accounting correctly prevents overflow even when deflate expands data.
    /// This was the exact scenario that caused a CI failure with the old 30-byte margin.
    #[test]
    fn test_1mb_limit_with_incompressible_data() {
        let mut rng = rand::rng();
        // Run multiple iterations to reduce flakiness from randomness.
        for _ in 0..5 {
            let mut batcher = GzipBatcher::new(1);
            loop {
                let chunk: Vec<u8> = (0..10).map(|_| rng.random()).collect();
                match batcher.push(&chunk).unwrap() {
                    PushResult::Ok(_) => continue,
                    PushResult::BatchReady(_) => break,
                    PushResult::TooLarge => panic!("Should not happen with small chunks"),
                }
            }

            let batch = batcher.take_pending_batch().unwrap();
            assert!(
                batch.compressed_data.len() <= ONE_MB,
                "Batch size {} exceeds 1MB limit with incompressible data",
                batch.compressed_data.len()
            );
        }
    }

    /// Verify structural bytes ('[', ',', ']') are correctly accounted for
    /// by checking that a single entry produces valid JSON with no overflow.
    #[test]
    fn test_structural_bytes_accounting() {
        let mut batcher = GzipBatcher::new(6);
        // Push data that's just under the limit minus structural overhead
        let data = vec![b'a'; ONE_MB - GZIP_SAFETY_MARGIN - 3]; // -3 for '[', ']', and slack
        match batcher.push(&data).unwrap() {
            PushResult::Ok(_) => {}
            other => panic!("Expected Ok, got {:?}", std::mem::discriminant(&other)),
        }
        let _ = batcher.finalize().unwrap();
        let batch = batcher.take_pending_batch().unwrap();
        assert!(
            batch.compressed_data.len() <= ONE_MB,
            "Single large entry batch {} exceeds 1MB",
            batch.compressed_data.len()
        );
    }

    /// Verify the TooLarge check accounts for structural bytes.
    #[test]
    fn test_too_large_includes_structural_overhead() {
        let mut batcher = GzipBatcher::new(1);
        // Exactly ONE_MB - GZIP_SAFETY_MARGIN - 1: too large because +2 for structural bytes
        let data = vec![b'x'; ONE_MB - GZIP_SAFETY_MARGIN - 1];
        match batcher.push(&data).unwrap() {
            PushResult::TooLarge => {} // Expected: data.len() + 2 > ONE_MB - GZIP_SAFETY_MARGIN
            _ => panic!("Should be TooLarge"),
        }
    }
}
