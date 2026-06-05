# Paimon Vector Index

Pure Rust IVF-PQ implementation for Apache Paimon, aligned with Faiss/LanceDB. Designed for data lake (S3/HDFS/OSS) with seek-based I/O, supporting both 8-bit and 4-bit PQ with SIMD acceleration.

## Performance (100K vectors, d=128, nprobe=8)

| Metric | 8-bit (M=16) | 4-bit (M=32) | Faiss | LanceDB |
|--------|-------------|-------------|-------|---------|
| Query latency | 9 μs/q | 8 μs/q | 8 μs/q | 15 μs/q |
| Recall@10 | 40% | 31% | 45% | 40% |
| Storage/vec | 16 bytes | 16 bytes | 16 bytes | 16 bytes |
| Build (100K) | 4.2s | 2.8s | 2.5s | 3s |

Remote storage: I/O adds ~5ms per list read. All implementations converge to the same end-to-end latency.

## Rust API

### Build Index

```rust
use paimon_vindex_core::distance::MetricType;
use paimon_vindex_core::ivfpq::IVFPQIndex;

// 8-bit PQ (default)
let mut index = IVFPQIndex::new(d, nlist, m, MetricType::L2, false);

// 4-bit PQ (half storage, faster scan, lower recall)
let mut index = IVFPQIndex::with_nbits(d, nlist, m, 4, MetricType::L2, false);

// Auto nlist from target partition size
let mut index = IVFPQIndex::with_target_partition_size(d, n, 5000, m, MetricType::L2, false);

// Train + add
index.train(&data, n);
index.add(&data, &ids, n);

// Build fastscan block layout (4-bit only, lightweight)
index.build_search_structures();

// Optional: precomputed table for high-QPS services (costs memory + 10ms build)
// Only call if querying thousands of times on the same index
index.build_precomputed_table();
```

### Search

```rust
// Single query
let mut dists = vec![0.0f32; k];
let mut labels = vec![0i64; k];
index.search(&query, 1, k, nprobe, &mut dists, &mut labels);

// Batch queries (parallel via rayon)
index.search(&queries, nq, k, nprobe, &mut dists, &mut labels);

// Search with ID filter (predicate pushdown)
let filter: HashSet<i64> = get_valid_ids();
index.search_with_filter(&queries, nq, k, nprobe, Some(&filter), &mut dists, &mut labels);
```

### Write/Read Index File

```rust
use paimon_vindex_core::io::{write_index, IVFPQIndexReader, PosWriter};

// Write
let mut buf = Vec::new();
write_index(&index, &mut PosWriter::new(&mut buf))?;

// Read (lazy: only loads metadata, reads inverted lists on demand)
let mut reader = IVFPQIndexReader::open(file)?;
let (ids, dists) = reader.search(&query, k, nprobe)?;
```

### Streaming Training (large datasets)

```rust
use paimon_vindex_core::kmeans::StreamingKMeans;

// For datasets too large to fit in memory
let mut streaming = StreamingKMeans::new(d, nlist, chunk_size, config);
for chunk in data_stream {
    streaming.add_chunk(&chunk, chunk_n);  // compress each chunk to coreset
}
let centroids = streaming.finalize();  // train final centroids on weighted coreset
```

### Batch Reader Search (big_batch_search)

```rust
use paimon_vindex_core::ivfpq::search_batch_reader;

// Multiple queries share list reads: 800 I/O ops → ~100 I/O ops
let (ids, dists) = search_batch_reader(&mut reader, &queries, nq, k, nprobe)?;
```

## Java API

```java
// Build index
try (NativeIVFPQIndexWriter writer =
        new NativeIVFPQIndexWriter(128, 256, 16, DistanceType.L2, false)) {
    writer.train(data, n);
    writer.addVectors(ids, data, n);
    writer.write(outputStream);
}

// Search (single)
try (NativeIVFPQIndexReader reader = NativeIVFPQIndexReader.open(seekableInputStream)) {
    IVFPQResult result = reader.search(queryVector, 10, 8);
}

// Search (batch)
IVFPQBatchResult batch = reader.searchBatch(queries, nq, 10, 8);
for (int i = 0; i < nq; i++) {
    IVFPQResult r = batch.resultForQuery(i);
}
```

### GlobalIndexer Integration (Paimon SPI)

```sql
CREATE TABLE vectors (
    id BIGINT,
    embedding VECTOR<FLOAT, 128>
) WITH (
    'global-index.type' = 'ivfpq',
    'ivfpq.nlist' = '256',
    'ivfpq.m' = '16',
    'ivfpq.nbits' = '8',        -- or '4' for 4-bit PQ
    'ivfpq.metric' = 'l2',      -- l2 | ip | cosine
    'ivfpq.nprobe' = '8',
    'ivfpq.opq' = 'false',
    'ivfpq.target-partition-size' = '5000'  -- alternative to nlist
);
```

## Python API (reader only)

```python
from paimon_vindex import IVFPQReader
import numpy as np

with open("index.ivfpq", "rb") as f:
    reader = IVFPQReader(f)
    ids, distances = reader.search(
        np.array(query, dtype=np.float32), top_k=10, nprobe=4
    )
```

## Architecture

```
paimon-vector-index/          (~5000 lines Rust)
├── core/
│   ├── distance.rs           L2, IP, cosine + AVX2 gather + NEON tbl + fvec_madd
│   ├── blas.rs               sgemm via matrixmultiply (cache blocked, pure Rust)
│   ├── kmeans.rs             k-means++, hierarchical (nlist>256), streaming coreset,
│   │                         sgemm batch assignment, balance factor, chunked allocation
│   ├── pq.rs                 8-bit + 4-bit PQ, parallel train/encode, centroid norms cache
│   ├── opq.rs                OPQ rotation (Procrustes + SVD, hot-start, data centering)
│   ├── fastscan.rs           Block layout (bbs=32) + AVX2 vpshufb / NEON tbl for 4-bit
│   ├── ivfpq.rs              IVF-PQ index: batch add, precomputed tables, fastscan,
│   │                         big_batch_search, ID filtering, rayon parallel
│   ├── io.rs                 SeekRead/SeekWrite + pread, delta-varint IDs, transposed codes
│   └── shuffler.rs           Disk-based shuffler for large-scale build (avoids OOM)
├── jni/
│   ├── lib.rs                JNI: createWriter/train/add/write + openReader/search/searchBatch
│   └── stream.rs             JNI SeekableInputStream callback + pread (VectoredReadable)
└── python/
    └── lib.rs                PyO3: IVFPQReader with numpy zero-copy
```

## Design Decisions for Paimon

| Decision | Rationale |
|----------|-----------|
| **Precomputed table off by default** | Paimon tasks query few times then exit. Building the table costs ~10ms, only pays off after ~800 queries. Explicitly opt-in via `build_precomputed_table()`. |
| **Delta-varint ID encoding** | Paimon RowIDs are sequential integers. Delta encoding compresses IDs 7.9x (87% savings), reducing remote I/O. |
| **Transposed codes** | Column-major layout keeps distance table sub-slice in L1 cache during scan. |
| **FastScan block layout** | 4-bit codes packed in 32-vector blocks for vpshufb/tbl 32-way parallel lookup. |
| **Hierarchical k-means** | For nlist > 256, avoids O(n×k) per iteration on large k. |
| **Streaming coreset** | For 100M+ vectors, trains in chunks without loading all data into memory. |
| **32MB threshold** | Precomputed table auto-disabled when > 32MB (nlist > 2000 with 8-bit). |
| **16MB sgemm chunking** | ip_matrix capped at 4M elements to avoid huge allocations. |
| **Big batch search** | Batch queries share list reads: 100 queries × 8 probes → ~100 unique reads (not 800). |
| **pread support** | JNI detects VectoredReadable for thread-safe positional reads without cursor. |

## File Format (v2)

```
HEADER (64 bytes)
  magic "IVPQ", version 2, d, nlist, m, ksub, dsub, metric, totalVecs, flags

FLAGS:
  bit 0: has_opq          (OPQ rotation matrix follows header)
  bit 1: by_residual      (PQ encodes residuals)
  bit 2: delta_ids        (IDs stored as delta-varint, sorted)
  bit 3: transposed_codes (codes stored column-major)

SECTIONS:
  [OPQ matrix?] [Centroids] [PQ Codebooks] [Offset Table] [Inverted Lists]
```

Remote read pattern: 1 bulk read for metadata → nprobe seeks for selected lists only.

## Build

```bash
# Core library + tests
cargo test -p paimon-vindex-core

# Release benchmark
cargo bench --bench pq4_bench

# JNI shared library
cargo build --release -p paimon-vindex-jni
# → target/release/libpaimon_vindex_jni.{so,dylib}

# Python module (requires maturin)
cd python && maturin develop --release
```

## Dependencies (all pure Rust, zero system libraries)

| Crate | Purpose |
|-------|---------|
| `matrixmultiply` | sgemm (cache blocked + SIMD) |
| `nalgebra` | SVD for OPQ |
| `rand` | k-means++ initialization |
| `rayon` | Thread-level parallelism |
| `jni` (jni crate only) | Java Native Interface |
| `pyo3` + `numpy` (python crate only) | Python bindings |
