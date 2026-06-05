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

import heapq
import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import List, Optional, Tuple

import numpy as np


MAGIC = 0x49565051  # "IVPQ"
VERSION = 1
HEADER_SIZE = 64


class DistanceType(IntEnum):
    L2 = 0
    INNER_PRODUCT = 1
    COSINE = 2


@dataclass
class IVFPQResult:
    """Result of an IVF-PQ nearest neighbor search."""

    ids: np.ndarray
    distances: np.ndarray

    @property
    def size(self) -> int:
        return len(self.ids)


class IVFPQIndexReader:
    """Reads and queries an IVF-PQ index from a seekable file stream.

    Compatible with the Java IVFPQIndexWriter file format.
    The stream must support seek() and read(), or read_at() for positional reads.
    """

    def __init__(self, stream, file_size: Optional[int] = None):
        self._stream = stream
        self._has_read_at = hasattr(stream, "read_at")

        self._read_header()
        self._read_centroids()
        self._read_codebooks()
        self._read_offset_table()

    def _pread(self, offset: int, length: int) -> bytes:
        """Positional read without changing stream position."""
        if self._has_read_at:
            return self._stream.read_at(length, offset)
        else:
            self._stream.seek(offset)
            return self._read_fully(length)

    def _read_fully(self, length: int) -> bytes:
        """Read exactly `length` bytes from the current position."""
        data = b""
        while len(data) < length:
            chunk = self._stream.read(length - len(data))
            if not chunk:
                raise IOError(
                    f"Unexpected end of stream: expected {length} bytes, got {len(data)}"
                )
            data += chunk
        return data

    def _read_header(self):
        header_bytes = self._pread(0, HEADER_SIZE)
        fields = struct.unpack_from("<IIIIIIIIq", header_bytes, 0)

        magic = fields[0]
        if magic != MAGIC:
            raise ValueError(f"Invalid IVFPQ magic: expected 0x{MAGIC:08X}, got 0x{magic:08X}")

        version = fields[1]
        if version != VERSION:
            raise ValueError(f"Unsupported IVFPQ version: {version}")

        self.dimension = fields[2]
        self.nlist = fields[3]
        self.m = fields[4]
        self.ksub = fields[5]
        self.dsub = fields[6]
        self.metric = DistanceType(fields[7])
        self.total_vectors = fields[8]

    def _read_centroids(self):
        offset = HEADER_SIZE
        nbytes = self.nlist * self.dimension * 4
        data = self._pread(offset, nbytes)
        self._centroids = np.frombuffer(data, dtype="<f4").reshape(self.nlist, self.dimension)

    def _read_codebooks(self):
        offset = HEADER_SIZE + self.nlist * self.dimension * 4
        nbytes = self.m * self.ksub * self.dsub * 4
        data = self._pread(offset, nbytes)
        self._codebooks = np.frombuffer(data, dtype="<f4").reshape(self.m, self.ksub, self.dsub)

    def _read_offset_table(self):
        offset = (
            HEADER_SIZE
            + self.nlist * self.dimension * 4
            + self.m * self.ksub * self.dsub * 4
        )
        nbytes = self.nlist * 16
        data = self._pread(offset, nbytes)

        self._list_offsets = np.zeros(self.nlist, dtype=np.int64)
        self._list_counts = np.zeros(self.nlist, dtype=np.int32)
        for i in range(self.nlist):
            base = i * 16
            self._list_offsets[i] = struct.unpack_from("<q", data, base)[0]
            self._list_counts[i] = struct.unpack_from("<i", data, base + 8)[0]

    def _read_inverted_list(self, list_id: int) -> Tuple[np.ndarray, np.ndarray]:
        """Read IDs and PQ codes for a single inverted list.

        Returns:
            (ids, codes) where ids is int64[count] and codes is uint8[count, m]
        """
        count = int(self._list_counts[list_id])
        if count == 0:
            return np.array([], dtype=np.int64), np.array([], dtype=np.uint8).reshape(0, self.m)

        offset = int(self._list_offsets[list_id])
        id_bytes = count * 8
        code_bytes = count * self.m
        data = self._pread(offset, id_bytes + code_bytes)

        ids = np.frombuffer(data[:id_bytes], dtype="<i8")
        codes = np.frombuffer(data[id_bytes:], dtype=np.uint8).reshape(count, self.m)
        return ids, codes

    def search(self, query: np.ndarray, top_k: int, nprobe: int) -> IVFPQResult:
        """Search for the top-k nearest neighbors of the query vector.

        Args:
            query: query vector of shape (dimension,)
            top_k: number of nearest neighbors to return
            nprobe: number of inverted lists to probe

        Returns:
            IVFPQResult with ids and distances sorted by distance ascending
        """
        query = np.asarray(query, dtype=np.float32)

        if self.metric == DistanceType.COSINE:
            norm = np.linalg.norm(query)
            if norm > 0:
                query = query / norm

        # Find top-nprobe nearest coarse centroids
        nprobe = min(nprobe, self.nlist)
        dists_to_centroids = np.sum((self._centroids - query) ** 2, axis=1)
        if nprobe >= self.nlist:
            probe_list = np.argsort(dists_to_centroids)
        else:
            probe_list = np.argpartition(dists_to_centroids, nprobe)[:nprobe]
            probe_list = probe_list[np.argsort(dists_to_centroids[probe_list])]

        # Max-heap for top-k (negate distances for max-heap via heapq min-heap)
        heap: List[Tuple[float, int]] = []

        table_metric = DistanceType.L2 if self.metric == DistanceType.COSINE else self.metric

        for list_id in probe_list:
            list_id = int(list_id)
            count = int(self._list_counts[list_id])
            if count == 0:
                continue

            # Compute residual query
            residual_query = query - self._centroids[list_id]

            # Compute distance table: [m, ksub]
            dist_table = self._compute_distance_table(residual_query, table_metric)

            # Read inverted list
            ids, codes = self._read_inverted_list(list_id)

            # Vectorized PQ distance computation
            distances = self._compute_pq_distances(dist_table, codes)

            # Update top-k heap
            for i in range(count):
                dist = float(distances[i])
                vec_id = int(ids[i])
                if len(heap) < top_k:
                    heapq.heappush(heap, (-dist, vec_id))
                elif dist < -heap[0][0]:
                    heapq.heapreplace(heap, (-dist, vec_id))

        # Sort results by distance ascending
        heap.sort(key=lambda x: -x[0])
        result_ids = np.array([h[1] for h in heap], dtype=np.int64)
        result_dists = np.array([-h[0] for h in heap], dtype=np.float32)

        return IVFPQResult(ids=result_ids, distances=result_dists)

    def _compute_distance_table(self, query: np.ndarray, metric: DistanceType) -> np.ndarray:
        """Precompute distance table [m, ksub] from query sub-vectors to codebook centroids."""
        table = np.zeros((self.m, self.ksub), dtype=np.float32)
        for i in range(self.m):
            sub_query = query[i * self.dsub : (i + 1) * self.dsub]
            if metric == DistanceType.L2 or metric == DistanceType.COSINE:
                # Squared L2 distance
                diff = self._codebooks[i] - sub_query
                table[i] = np.sum(diff * diff, axis=1)
            elif metric == DistanceType.INNER_PRODUCT:
                # Negative inner product
                table[i] = -np.dot(self._codebooks[i], sub_query)
        return table

    def _compute_pq_distances(self, dist_table: np.ndarray, codes: np.ndarray) -> np.ndarray:
        """Compute distances for all codes using the precomputed distance table.

        Uses advanced indexing for vectorized lookup.
        """
        m_indices = np.arange(self.m)
        # codes shape: [count, m], values are uint8 indices into dist_table
        # dist_table shape: [m, ksub]
        # result: sum over m of dist_table[m_i, codes[:, m_i]]
        distances = np.zeros(codes.shape[0], dtype=np.float32)
        for i in range(self.m):
            distances += dist_table[i, codes[:, i]]
        return distances

    def close(self):
        if hasattr(self._stream, "close"):
            self._stream.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
