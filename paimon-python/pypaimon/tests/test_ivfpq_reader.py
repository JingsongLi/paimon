#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.

import io
import struct
import tempfile

import numpy as np
import pytest

from pypaimon.index.ivfpq.ivfpq_index_reader import (
    HEADER_SIZE,
    MAGIC,
    VERSION,
    DistanceType,
    IVFPQIndexReader,
)


def _simple_kmeans(vectors, k, max_iter=25, seed=42):
    """Minimal k-means for building test indexes in pure Python/numpy."""
    rng = np.random.RandomState(seed)
    n = len(vectors)
    if n <= k:
        centroids = np.zeros((k, vectors.shape[1]), dtype=np.float32)
        for i in range(k):
            centroids[i] = vectors[i % n]
        return centroids

    indices = rng.choice(n, k, replace=False)
    centroids = vectors[indices].copy()

    for _ in range(max_iter):
        dists = np.sum((vectors[:, None, :] - centroids[None, :, :]) ** 2, axis=2)
        assignments = np.argmin(dists, axis=1)
        new_centroids = np.zeros_like(centroids)
        for c in range(k):
            mask = assignments == c
            if np.any(mask):
                new_centroids[c] = vectors[mask].mean(axis=0)
            else:
                new_centroids[c] = vectors[rng.randint(n)]
        if np.allclose(centroids, new_centroids):
            break
        centroids = new_centroids

    return centroids


def _build_ivfpq_index(
    vectors,
    ids,
    dimension,
    nlist,
    m,
    ksub,
    metric=DistanceType.L2,
    seed=42,
):
    """Build an IVF-PQ index file in memory, compatible with the Java writer format."""
    dsub = dimension // m
    n = len(vectors)

    vecs = vectors.copy().astype(np.float32)
    if metric == DistanceType.COSINE:
        norms = np.linalg.norm(vecs, axis=1, keepdims=True)
        norms[norms == 0] = 1
        vecs = vecs / norms

    # Train coarse centroids
    centroids = _simple_kmeans(vecs, nlist, seed=seed)

    # Assign to centroids and compute residuals
    dists = np.sum((vecs[:, None, :] - centroids[None, :, :]) ** 2, axis=2)
    assignments = np.argmin(dists, axis=1)
    residuals = vecs - centroids[assignments]

    # Train PQ codebooks on residuals
    codebooks = np.zeros((m, ksub, dsub), dtype=np.float32)
    for i in range(m):
        sub_residuals = residuals[:, i * dsub : (i + 1) * dsub]
        codebooks[i] = _simple_kmeans(sub_residuals, ksub, seed=seed + i)

    # Encode residuals
    codes = np.zeros((n, m), dtype=np.uint8)
    for i in range(m):
        sub_residuals = residuals[:, i * dsub : (i + 1) * dsub]
        sub_dists = np.sum(
            (sub_residuals[:, None, :] - codebooks[i][None, :, :]) ** 2, axis=2
        )
        codes[:, i] = np.argmin(sub_dists, axis=1).astype(np.uint8)

    # Build inverted lists
    inverted_ids = [[] for _ in range(nlist)]
    inverted_codes = [[] for _ in range(nlist)]
    for j in range(n):
        list_id = int(assignments[j])
        inverted_ids[list_id].append(ids[j])
        inverted_codes[list_id].append(codes[j])

    # Write file
    buf = io.BytesIO()

    # Header
    header = struct.pack(
        "<IIIIIIIIq24x",
        MAGIC, VERSION, dimension, nlist, m, ksub, dsub, int(metric), n,
    )
    buf.write(header)

    # Centroids
    buf.write(centroids.astype("<f4").tobytes())

    # Codebooks
    buf.write(codebooks.astype("<f4").tobytes())

    # Compute offsets
    centroids_bytes = nlist * dimension * 4
    codebooks_bytes = m * ksub * dsub * 4
    offset_table_bytes = nlist * 16
    data_start = HEADER_SIZE + centroids_bytes + codebooks_bytes + offset_table_bytes

    list_offsets = []
    list_counts = []
    current_offset = data_start
    for i in range(nlist):
        list_offsets.append(current_offset)
        count = len(inverted_ids[i])
        list_counts.append(count)
        current_offset += count * 8 + count * m

    # Write offset table
    for i in range(nlist):
        buf.write(struct.pack("<qiI", list_offsets[i], list_counts[i], 0))

    # Write inverted list data
    for i in range(nlist):
        count = list_counts[i]
        if count == 0:
            continue
        for vid in inverted_ids[i]:
            buf.write(struct.pack("<q", vid))
        for code in inverted_codes[i]:
            buf.write(bytes(code))

    return buf.getvalue()


def _brute_force_topk(query, vectors, ids, k):
    """Brute-force L2 top-k search."""
    dists = np.sum((vectors - query) ** 2, axis=1)
    top_indices = np.argpartition(dists, k)[:k]
    top_indices = top_indices[np.argsort(dists[top_indices])]
    return set(int(ids[i]) for i in top_indices)


class TestIVFPQIndexReader:

    def test_read_header(self):
        dim, nlist, m, ksub = 16, 4, 4, 8
        rng = np.random.RandomState(42)
        vectors = rng.randn(100, dim).astype(np.float32)
        ids = np.arange(100, dtype=np.int64)

        data = _build_ivfpq_index(vectors, ids, dim, nlist, m, ksub)
        stream = io.BytesIO(data)

        with IVFPQIndexReader(stream) as reader:
            assert reader.dimension == dim
            assert reader.nlist == nlist
            assert reader.m == m
            assert reader.ksub == ksub
            assert reader.dsub == dim // m
            assert reader.metric == DistanceType.L2
            assert reader.total_vectors == 100

    def test_search_l2(self):
        dim, nlist, m, ksub = 16, 4, 4, 8
        rng = np.random.RandomState(42)
        vectors = rng.randn(500, dim).astype(np.float32)
        ids = np.arange(500, dtype=np.int64)

        data = _build_ivfpq_index(vectors, ids, dim, nlist, m, ksub)
        stream = io.BytesIO(data)

        with IVFPQIndexReader(stream) as reader:
            result = reader.search(vectors[0], top_k=5, nprobe=2)

            assert result.size == 5
            # Distances should be sorted ascending
            for i in range(1, result.size):
                assert result.distances[i] >= result.distances[i - 1]

    def test_search_inner_product(self):
        dim, nlist, m, ksub = 16, 4, 4, 8
        rng = np.random.RandomState(123)
        vectors = rng.randn(500, dim).astype(np.float32)
        ids = np.arange(500, dtype=np.int64)

        data = _build_ivfpq_index(
            vectors, ids, dim, nlist, m, ksub, metric=DistanceType.INNER_PRODUCT
        )
        stream = io.BytesIO(data)

        with IVFPQIndexReader(stream) as reader:
            result = reader.search(vectors[0], top_k=5, nprobe=2)
            assert result.size == 5
            for i in range(1, result.size):
                assert result.distances[i] >= result.distances[i - 1]

    def test_search_cosine(self):
        dim, nlist, m, ksub = 16, 4, 4, 8
        rng = np.random.RandomState(456)
        vectors = rng.randn(500, dim).astype(np.float32)
        ids = np.arange(500, dtype=np.int64)

        data = _build_ivfpq_index(
            vectors, ids, dim, nlist, m, ksub, metric=DistanceType.COSINE
        )
        stream = io.BytesIO(data)

        with IVFPQIndexReader(stream) as reader:
            result = reader.search(vectors[0], top_k=5, nprobe=2)
            assert result.size == 5

    def test_self_retrieval(self):
        """When probing all lists, searching for an indexed vector should find itself."""
        dim, nlist, m, ksub = 8, 2, 2, 4
        rng = np.random.RandomState(789)
        vectors = rng.randn(50, dim).astype(np.float32) * 10
        ids = np.arange(50, dtype=np.int64)

        data = _build_ivfpq_index(vectors, ids, dim, nlist, m, ksub)
        stream = io.BytesIO(data)

        with IVFPQIndexReader(stream) as reader:
            # With all lists probed, the query vector's own ID should appear in results
            result = reader.search(vectors[0], top_k=5, nprobe=nlist)
            assert result.size == 5
            assert 0 in result.ids

    def test_multiple_queries(self):
        """Multiple searches on the same reader should work correctly."""
        dim, nlist, m, ksub = 16, 4, 4, 8
        rng = np.random.RandomState(789)
        vectors = rng.randn(200, dim).astype(np.float32)
        ids = np.arange(200, dtype=np.int64)

        data = _build_ivfpq_index(vectors, ids, dim, nlist, m, ksub)
        stream = io.BytesIO(data)

        with IVFPQIndexReader(stream) as reader:
            for q in range(10):
                result = reader.search(vectors[q * 10], top_k=5, nprobe=2)
                assert result.size == 5
                # Distances should be sorted ascending
                for i in range(1, result.size):
                    assert result.distances[i] >= result.distances[i - 1]

    def test_topk_larger_than_vectors(self):
        dim, nlist, m, ksub = 8, 2, 2, 4
        rng = np.random.RandomState(42)
        vectors = rng.randn(10, dim).astype(np.float32)
        ids = np.arange(10, dtype=np.int64)

        data = _build_ivfpq_index(vectors, ids, dim, nlist, m, ksub)
        stream = io.BytesIO(data)

        with IVFPQIndexReader(stream) as reader:
            result = reader.search(vectors[0], top_k=100, nprobe=nlist)
            assert result.size == 10

    def test_file_based_stream(self):
        """Test with a real file stream instead of BytesIO."""
        dim, nlist, m, ksub = 16, 4, 4, 8
        rng = np.random.RandomState(42)
        vectors = rng.randn(200, dim).astype(np.float32)
        ids = np.arange(200, dtype=np.int64)

        data = _build_ivfpq_index(vectors, ids, dim, nlist, m, ksub)

        with tempfile.NamedTemporaryFile(suffix=".ivfpq", delete=True) as f:
            f.write(data)
            f.flush()
            f.seek(0)

            with IVFPQIndexReader(f) as reader:
                result = reader.search(vectors[0], top_k=5, nprobe=2)
                assert result.size == 5

    def test_invalid_magic(self):
        data = struct.pack("<I", 0xDEADBEEF) + b"\x00" * 60
        stream = io.BytesIO(data)
        with pytest.raises(ValueError, match="Invalid IVFPQ magic"):
            IVFPQIndexReader(stream)

    def test_invalid_version(self):
        data = struct.pack("<II", MAGIC, 99) + b"\x00" * 56
        stream = io.BytesIO(data)
        with pytest.raises(ValueError, match="Unsupported IVFPQ version"):
            IVFPQIndexReader(stream)
