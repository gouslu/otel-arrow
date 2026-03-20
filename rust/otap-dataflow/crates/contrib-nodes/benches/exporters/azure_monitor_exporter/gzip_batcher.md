# GzipBatcher Compression Benchmark

## Motivation

The `GzipBatcher` streams log entries into a gzip compressor and produces ~1 MB
compressed batches for upload. The gzip compression level directly affects CPU
cost per batch. This benchmark measures the trade-off between compression level,
throughput, and compression ratio to determine the optimal default.

## Methodology

**Compression levels tested:** 1 (fastest), 6 (default), and 9 (maximum).
Gzip levels range from 1-9, where 1 prioritizes speed and 9 prioritizes
compression ratio.

**Procedure:** For each combination of (data type, entry size, compression
level), the benchmark:

1. Pre-generates a pool of unique random entries of the given size and type.
2. Pushes entries into a fresh `GzipBatcher` until the ~1 MB batch threshold is
   reached (`BatchReady`).
3. Records the number of log records, uncompressed size, compressed size, and
   compression ratio.
4. Measures the wall-clock time to fill one complete batch using
   [Criterion](https://github.com/bheisler/criterion.rs) (100 samples per
   benchmark).

**Data profiles:**

Three data profiles are used to sweep across entropy levels and therefore
compressibility. This lets us isolate how compression level interacts with
data compressibility rather than measuring only a single operating point:

- **json_log** (low entropy)  --  Structured JSON with random field values and a
  random lowercase-letter message body. The repeating key names, braces, and
  quoted strings create high structural redundancy, making this the most
  compressible profile. It also serves as the most realistic example, closely
  resembling actual Azure Monitor ingestion payloads.
- **hex** (medium entropy)  --  Random hexadecimal characters (`0-9a-f`). With
  only 16 possible byte values the entropy is ~4 bits per byte, placing this
  profile in the middle of the compressibility spectrum.
- **ascii** (high entropy)  --  Uniformly random printable ASCII (`0x20-0x7E`).
  With 95 possible byte values drawn uniformly, entropy is close to the
  theoretical maximum, making the data near-incompressible. This establishes
  the worst-case performance floor.

**Entry sizes:**

Three entry sizes are tested to capture how per-entry overhead and compression
window utilization change with payload length:

- **256 B**  --  Small log entries. Typical of short structured events (e.g., a
  metric data point or a brief log line with metadata). At this size,
  per-entry framing overhead is proportionally higher.
- **512 B**  --  Medium log entries. Representative of a typical log record with
  a moderate-length message body and several attributes.
- **1024 B**  --  Large log entries. Represents verbose log records with stack
  traces, detailed error messages, or rich attribute sets. Larger entries give
  the compressor more context per push, which can improve throughput.

**Column definitions:**

| Column | Description |
| ------------ | --------------------------------------------------------- |
| Entry Size | Size of each uncompressed log entry |
| Level | Gzip compression level (1 = fastest, 9 = max compression) |
| Log Records | Number of entries that fit in one ~1 MB compressed batch |
| Uncompressed | Total raw size of all entries in the batch |
| Compressed | Resulting gzip-compressed batch size (~1 MB target) |
| Ratio | Compressed / Uncompressed (lower is better) |
| Time | Wall-clock time to fill one batch |
| Throughput | Uncompressed data processed per second |

## Results

### JSON Log Data

| Entry Size | Level | Log Records | Uncompressed | Compressed | Ratio | Time | Throughput |
| ---------- | ----- | ----------- | ------------ | ---------- | ----- | ---- | ---------- |
| 256 B | 1 | 9,051 | 2.21 MB | 1.00 MB | 45.2% | 12.3 ms | 179 MiB/s |
| 256 B | 6 | 9,129 | 2.23 MB | 1.00 MB | 44.9% | 29.8 ms | 74.7 MiB/s |
| 256 B | 9 | 9,130 | 2.23 MB | 1.00 MB | 44.9% | 31.4 ms | 71.0 MiB/s |
| 512 B | 1 | 3,814 | 1.86 MB | 1.00 MB | 53.7% | 8.4 ms | 222 MiB/s |
| 512 B | 6 | 3,706 | 1.81 MB | 1.00 MB | 55.2% | 28.0 ms | 64.6 MiB/s |
| 512 B | 9 | 3,705 | 1.81 MB | 1.00 MB | 55.2% | 28.6 ms | 63.3 MiB/s |
| 1024 B | 1 | 1,780 | 1.74 MB | 1.00 MB | 57.5% | 6.4 ms | 271 MiB/s |
| 1024 B | 6 | 1,702 | 1.66 MB | 1.00 MB | 60.1% | 23.9 ms | 69.5 MiB/s |
| 1024 B | 9 | 1,702 | 1.66 MB | 1.00 MB | 60.1% | 24.8 ms | 67.1 MiB/s |

## Hex Data (medium entropy)

| Entry Size | Level | Log Records | Uncompressed | Compressed | Ratio | Time | Throughput |
| ---------- | ----- | ----------- | ------------ | ---------- | ----- | ---- | ---------- |
| 256 B | 1 | 7,469 | 1.82 MB | 1.00 MB | 54.8% | 13.2 ms | 138 MiB/s |
| 256 B | 6 | 7,109 | 1.74 MB | 1.00 MB | 57.6% | 30.3 ms | 57.3 MiB/s |
| 256 B | 9 | 7,108 | 1.74 MB | 1.00 MB | 57.6% | 30.1 ms | 57.6 MiB/s |
| 512 B | 1 | 3,748 | 1.83 MB | 1.00 MB | 54.6% | 10.0 ms | 183 MiB/s |
| 512 B | 6 | 3,570 | 1.74 MB | 1.00 MB | 57.4% | 29.1 ms | 59.9 MiB/s |
| 512 B | 9 | 3,569 | 1.74 MB | 1.00 MB | 57.4% | 29.0 ms | 60.2 MiB/s |
| 1024 B | 1 | 1,877 | 1.83 MB | 1.00 MB | 54.5% | 8.5 ms | 216 MiB/s |
| 1024 B | 6 | 1,789 | 1.75 MB | 1.00 MB | 57.2% | 28.5 ms | 61.3 MiB/s |
| 1024 B | 9 | 1,789 | 1.75 MB | 1.00 MB | 57.2% | 28.4 ms | 61.4 MiB/s |

## ASCII Data (high entropy)

| Entry Size | Level | Log Records | Uncompressed | Compressed | Ratio | Time | Throughput |
| ---------- | ----- | ----------- | ------------ | ---------- | ----- | ---- | ---------- |
| 256 B | 1 | 4,906 | 1.20 MB | 1.00 MB | 83.5% | 7.5 ms | 160 MiB/s |
| 256 B | 6 | 4,899 | 1.20 MB | 1.00 MB | 83.6% | 19.2 ms | 62.2 MiB/s |
| 256 B | 9 | 4,899 | 1.20 MB | 1.00 MB | 83.6% | 19.2 ms | 62.2 MiB/s |
| 512 B | 1 | 2,458 | 1.20 MB | 1.00 MB | 83.3% | 5.4 ms | 220 MiB/s |
| 512 B | 6 | 2,454 | 1.20 MB | 1.00 MB | 83.4% | 18.7 ms | 64.0 MiB/s |
| 512 B | 9 | 2,454 | 1.20 MB | 1.00 MB | 83.4% | 18.7 ms | 64.0 MiB/s |
| 1024 B | 1 | 1,230 | 1.20 MB | 1.00 MB | 83.2% | 4.4 ms | 274 MiB/s |
| 1024 B | 6 | 1,228 | 1.20 MB | 1.00 MB | 83.3% | 18.3 ms | 65.4 MiB/s |
| 1024 B | 9 | 1,228 | 1.20 MB | 1.00 MB | 83.3% | 18.3 ms | 65.4 MiB/s |

## Analysis

- **Level 1 vs 6:** Level 1 is 2.4-4.2x faster with only ~2-3 percentage points
  worse compression ratio. The throughput difference is dramatic (e.g., 271 vs
  69 MiB/s for json_log/1024B).
- **Level 6 vs 9:** Virtually identical compression ratios across all data types,
  but level 9 is consistently 2-5% slower. Level 9 provides no measurable
  compression benefit over level 6.
- **Data type impact:** JSON logs compress best (45-60% ratio), hex is moderate
  (55-58%), and random ASCII barely compresses (83-84%). The data type matters
  far more than the compression level.

## Caveats

- These benchmarks measure CPU-bound compression throughput in isolation.
  In this exporter, the bottleneck is outgoing HTTP request rate  --  compression
  is not the limiting factor. However, reducing CPU time per batch frees up
  cycles for other pipeline work and reduces back-pressure on upstream
  components.
- When the bottleneck is HTTP request rate (e.g., high-latency network path),
  higher compression levels pack more log records per batch, reducing the
  number of requests needed and potentially improving end-to-end throughput.
- Compression ratios were measured with synthetic data. Production payloads
  may compress differently depending on field cardinality, message repetition,
  and attribute diversity.
