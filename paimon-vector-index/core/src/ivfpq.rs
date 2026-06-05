// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::distance::{
    fvec_madd, fvec_normalize, pq_distance_four_codes, pq_distance_from_table, MetricType,
};
use crate::io::{IVFPQIndexReader, SeekRead};
use crate::kmeans::{self, KMeansConfig};
use crate::opq::OPQMatrix;
use crate::pq::ProductQuantizer;
use rayon::prelude::*;
use std::collections::HashSet;
use std::io;

/// IVF-PQ index aligned with Faiss's IndexIVFPQ.
pub struct IVFPQIndex {
    pub d: usize,
    pub nlist: usize,
    pub metric: MetricType,
    pub by_residual: bool,

    pub quantizer_centroids: Vec<f32>,
    pub pq: ProductQuantizer,
    pub opq: Option<OPQMatrix>,

    pub ids: Vec<Vec<i64>>,
    pub codes: Vec<Vec<u8>>,

    /// Precomputed table [nlist * M * ksub] for L2+by_residual mode.
    /// Avoids recomputing distance table per list during search.
    precomputed_table: Vec<f32>,
    /// Block-layout packed codes for 4-bit FastScan. One per list.
    fastscan_codes: Vec<Vec<u8>>,
}

impl IVFPQIndex {
    pub fn new(d: usize, nlist: usize, m: usize, metric: MetricType, use_opq: bool) -> Self {
        Self::with_nbits(d, nlist, m, 8, metric, use_opq)
    }

    pub fn with_nbits(
        d: usize,
        nlist: usize,
        m: usize,
        nbits: usize,
        metric: MetricType,
        use_opq: bool,
    ) -> Self {
        let by_residual = metric == MetricType::L2;
        IVFPQIndex {
            d,
            nlist,
            metric,
            by_residual,
            quantizer_centroids: Vec::new(),
            pq: ProductQuantizer::with_nbits(d, m, nbits),
            opq: if use_opq {
                Some(OPQMatrix::new(d, m))
            } else {
                None
            },
            ids: vec![Vec::new(); nlist],
            codes: vec![Vec::new(); nlist],
            precomputed_table: Vec::new(),
            fastscan_codes: Vec::new(),
        }
    }

    /// Create an index with automatic nlist based on target partition size.
    /// nlist = max(1, n / target_partition_size), clamped to reasonable bounds.
    pub fn with_target_partition_size(
        d: usize,
        n: usize,
        target_partition_size: usize,
        m: usize,
        metric: MetricType,
        use_opq: bool,
    ) -> Self {
        let nlist = (n / target_partition_size.max(1)).max(1).min(65536);
        Self::new(d, nlist, m, metric, use_opq)
    }

    pub fn train(&mut self, data: &[f32], n: usize) {
        let d = self.d;

        let train_data = if self.metric == MetricType::Cosine {
            let mut normalized = data[..n * d].to_vec();
            for i in 0..n {
                fvec_normalize(&mut normalized[i * d..(i + 1) * d]);
            }
            normalized
        } else {
            data[..n * d].to_vec()
        };

        let effective_data = if let Some(ref mut opq) = self.opq {
            opq.train(&train_data, n, &mut self.pq);
            &train_data
        } else {
            &train_data
        };

        let km_config = KMeansConfig::default();
        self.quantizer_centroids =
            kmeans::kmeans_train(&km_config, effective_data, n, d, self.nlist);

        if self.opq.is_none() {
            let pq_train_data = if self.by_residual {
                compute_residuals(effective_data, n, d, &self.quantizer_centroids, self.nlist)
            } else {
                effective_data.to_vec()
            };
            self.pq.train(&pq_train_data, n);
        } else if self.by_residual {
            let mut projected = vec![0.0f32; n * d];
            self.opq
                .as_ref()
                .unwrap()
                .apply_batch(effective_data, &mut projected, n);
            let residuals =
                compute_residuals(&projected, n, d, &self.quantizer_centroids, self.nlist);
            self.pq.train(&residuals, n);
        }
    }

    /// Add vectors in batches (Faiss-style: batch assign → batch residual → batch encode).
    pub fn add(&mut self, data: &[f32], ids: &[i64], n: usize) {
        const BATCH_SIZE: usize = 32768;
        let mut offset = 0;
        while offset < n {
            let batch_n = (n - offset).min(BATCH_SIZE);
            self.add_batch(
                &data[offset * self.d..(offset + batch_n) * self.d],
                &ids[offset..offset + batch_n],
                batch_n,
            );
            offset += batch_n;
        }
    }

    fn add_batch(&mut self, data: &[f32], ids: &[i64], n: usize) {
        let d = self.d;

        // Step 1: Preprocess (normalize + OPQ rotate)
        let processed = self.preprocess_queries(data, n);

        // Step 2: Batch assign to coarse centroids (uses sgemm)
        let assignments: Vec<usize> = (0..n)
            .into_par_iter()
            .map(|i| {
                kmeans::find_nearest(
                    &processed[i * d..(i + 1) * d],
                    &self.quantizer_centroids,
                    self.nlist,
                    d,
                )
            })
            .collect();

        // Step 3: Batch compute residuals (parallel)
        let to_encode = if self.by_residual {
            let mut residuals = vec![0.0f32; n * d];
            residuals
                .par_chunks_mut(d)
                .enumerate()
                .for_each(|(i, res)| {
                    let list_id = assignments[i];
                    for j in 0..d {
                        res[j] = processed[i * d + j]
                            - self.quantizer_centroids[list_id * d + j];
                    }
                });
            residuals
        } else {
            processed
        };

        // Step 4: Batch PQ encode (parallel)
        let cs = self.pq.code_size();
        let mut codes = vec![0u8; n * cs];
        self.pq.encode_batch(&to_encode, n, &mut codes);

        // Step 5: Distribute to inverted lists
        for i in 0..n {
            let list_id = assignments[i];
            self.ids[list_id].push(ids[i]);
            self.codes[list_id].extend_from_slice(&codes[i * cs..(i + 1) * cs]);
        }
    }

    /// Build fastscan block codes for 4-bit search acceleration.
    /// Call after all vectors are added. Lightweight — only reorganizes existing codes.
    pub fn build_search_structures(&mut self) {
        // Build fastscan block layout for 4-bit codes (no extra memory, just reorganize)
        if self.pq.nbits == 4 {
            let cs = self.pq.code_size();
            self.fastscan_codes = self
                .codes
                .iter()
                .enumerate()
                .map(|(list_id, codes)| {
                    let count = self.ids[list_id].len();
                    if count == 0 {
                        Vec::new()
                    } else {
                        crate::fastscan::pack_codes_block_layout(codes, count, cs)
                    }
                })
                .collect();
        }
    }

    /// Build precomputed distance tables for faster repeated queries.
    /// Only useful for long-running services with many queries on the same index.
    /// Costs ~10ms to build and uses nlist × M × ksub × 4 bytes of memory.
    pub fn build_precomputed_table(&mut self) {
        let d = self.d;
        let m = self.pq.m;
        let ksub = self.pq.ksub;
        let nlist = self.nlist;

        if self.metric != MetricType::L2 || !self.by_residual {
            return;
        }
        // No threshold here — caller explicitly opted in
        {
            let pq_norms = self.pq.compute_centroid_norms();
            let mut table = vec![0.0f32; nlist * m * ksub];

            for i in 0..nlist {
                let centroid = &self.quantizer_centroids[i * d..(i + 1) * d];
                let tab_base = i * m * ksub;

                for sub in 0..m {
                    let sub_centroid = &centroid[sub * self.pq.dsub..(sub + 1) * self.pq.dsub];
                    let pq_base = sub * ksub * self.pq.dsub;

                    for j in 0..ksub {
                        let pq_off = pq_base + j * self.pq.dsub;
                        let mut ip = 0.0f32;
                        for dd in 0..self.pq.dsub {
                            ip += sub_centroid[dd] * self.pq.centroids[pq_off + dd];
                        }
                        table[tab_base + sub * ksub + j] = pq_norms[sub * ksub + j] + 2.0 * ip;
                    }
                }
            }
            self.precomputed_table = table;
        }
    }

    /// Search for top-k nearest neighbors.
    /// Uses rayon to parallelize across queries.
    pub fn search(
        &self,
        queries: &[f32],
        nq: usize,
        k: usize,
        nprobe: usize,
        result_distances: &mut [f32],
        result_labels: &mut [i64],
    ) {
        self.search_with_filter(queries, nq, k, nprobe, None, result_distances, result_labels);
    }

    /// Search with optional ID filter.
    pub fn search_with_filter(
        &self,
        queries: &[f32],
        nq: usize,
        k: usize,
        nprobe: usize,
        filter: Option<&HashSet<i64>>,
        result_distances: &mut [f32],
        result_labels: &mut [i64],
    ) {
        let d = self.d;
        let m = self.pq.m;
        let ksub = self.pq.ksub;

        // Preprocess all queries: normalize + OPQ rotation
        let processed_queries = self.preprocess_queries(queries, nq);

        // Batch coarse search: one sgemm for all queries
        let (all_probe_indices, _all_coarse_dists) = kmeans::find_topk_batch(
            &processed_queries,
            nq,
            &self.quantizer_centroids,
            self.nlist,
            d,
            nprobe,
        );

        // Precompute ip_table for precomputed-table mode
        let use_precomputed = !self.precomputed_table.is_empty();
        let use_fastscan = !self.fastscan_codes.is_empty() && self.pq.nbits == 4;

        // Parallel scan across queries
        let results: Vec<Vec<(f32, i64)>> = (0..nq)
            .into_par_iter()
            .map(|qi| {
                let query = &processed_queries[qi * d..(qi + 1) * d];
                let probe_indices = &all_probe_indices[qi];

                let mut heap = TopKHeap::new(k);
                let mut sim_table = vec![0.0f32; m * ksub];

                // For precomputed mode: compute ip_table once per query
                let ip_table = if use_precomputed {
                    let mut t = vec![0.0f32; m * ksub];
                    self.pq.compute_inner_product_table(query, &mut t);
                    t
                } else {
                    Vec::new()
                };

                for &list_id in probe_indices {
                    let count = self.ids[list_id].len();
                    if count == 0 {
                        continue;
                    }

                    // Build distance table for this list
                    if use_precomputed {
                        // Faiss mode 1: sim_table = precomputed[list] - 2 * ip_table
                        let tab_base = list_id * m * ksub;
                        fvec_madd(
                            &self.precomputed_table[tab_base..tab_base + m * ksub],
                            &ip_table,
                            -2.0,
                            &mut sim_table,
                        );
                    } else {
                        self.compute_list_table(query, list_id, &mut sim_table);
                    }

                    // Scan
                    if use_fastscan {
                        // 4-bit FastScan with block layout
                        let mut dists = vec![0.0f32; count];
                        crate::fastscan::fastscan_4bit(
                            &sim_table,
                            &self.fastscan_codes[list_id],
                            count,
                            m,
                            &mut dists,
                        );
                        for i in 0..count {
                            if let Some(f) = filter {
                                if !f.contains(&self.ids[list_id][i]) {
                                    continue;
                                }
                            }
                            heap.push(dists[i], self.ids[list_id][i]);
                        }
                    } else if self.pq.nbits == 4 {
                        scan_codes_4bit(
                            &sim_table, &self.codes[list_id], &self.ids[list_id],
                            count, m, ksub, 0.0, filter, &mut heap,
                        );
                    } else {
                        scan_codes_batched(
                            &sim_table, &self.codes[list_id], &self.ids[list_id],
                            count, m, ksub, 0.0, filter, &mut heap,
                        );
                    }
                }

                heap.into_sorted()
            })
            .collect();

        for (qi, result) in results.into_iter().enumerate() {
            let out_base = qi * k;
            for (i, &(dist, id)) in result.iter().enumerate() {
                result_distances[out_base + i] = dist;
                result_labels[out_base + i] = id;
            }
            for i in result.len()..k {
                result_distances[out_base + i] = f32::MAX;
                result_labels[out_base + i] = -1;
            }
        }
    }

    fn preprocess_queries(&self, queries: &[f32], nq: usize) -> Vec<f32> {
        let d = self.d;
        let mut processed = queries[..nq * d].to_vec();

        if self.metric == MetricType::Cosine {
            for i in 0..nq {
                fvec_normalize(&mut processed[i * d..(i + 1) * d]);
            }
        }

        if let Some(ref opq) = self.opq {
            let mut rotated = vec![0.0f32; nq * d];
            opq.apply_batch(&processed, &mut rotated, nq);
            return rotated;
        }

        processed
    }

    fn compute_list_table(&self, query: &[f32], list_id: usize, sim_table: &mut [f32]) {
        let d = self.d;
        if self.by_residual {
            let mut residual_query = vec![0.0f32; d];
            for j in 0..d {
                residual_query[j] = query[j] - self.quantizer_centroids[list_id * d + j];
            }
            self.pq
                .compute_distance_table(&residual_query, self.metric, sim_table);
        } else {
            self.pq
                .compute_distance_table(query, self.metric, sim_table);
        }
    }
}

/// Scan 4-bit packed codes using u8-domain accumulation.
/// Supports both row-major [n][cs] and transposed [cs][n] layouts.
fn scan_codes_4bit(
    sim_table: &[f32],
    codes: &[u8],
    ids: &[i64],
    count: usize,
    m: usize,
    _ksub: usize,
    dis0: f32,
    filter: Option<&HashSet<i64>>,
    heap: &mut TopKHeap,
) {
    let mut dists = vec![0.0f32; count];
    crate::distance::scan_4bit_simd(sim_table, codes, count, m, &mut dists);

    for i in 0..count {
        if let Some(f) = filter {
            if !f.contains(&ids[i]) {
                continue;
            }
        }
        heap.push(dis0 + dists[i], ids[i]);
    }
}

/// Scan 4-bit transposed codes: layout [M/2][n].
/// Each sub-quantizer pair's codes are contiguous — ideal for SIMD.
fn scan_codes_4bit_transposed(
    sim_table: &[f32],
    codes: &[u8], // layout: [cs][count] where cs = m/2
    ids: &[i64],
    count: usize,
    m: usize,
    dis0: f32,
    filter: Option<&HashSet<i64>>,
    heap: &mut TopKHeap,
) {
    let cs = m / 2;

    // Same algorithm as scan_4bit_simd but codes are already in column layout
    const FLAT_NUM: usize = 200;
    let flat_end = count.min(FLAT_NUM);

    let mut dists = vec![0.0f32; count];

    // Step 1: first FLAT_NUM with f32 precision
    for i in 0..flat_end {
        let mut d = 0.0f32;
        for pair in 0..cs {
            let byte = codes[pair * count + i];
            let lo = (byte & 0x0F) as usize;
            let hi = ((byte >> 4) & 0x0F) as usize;
            d += sim_table[(pair * 2) * 16 + lo];
            d += sim_table[(pair * 2 + 1) * 16 + hi];
        }
        dists[i] = d;
    }

    if count > FLAT_NUM {
        // Step 2: quantize table
        let qmin = sim_table.iter().cloned().fold(f32::INFINITY, f32::min);
        let qmax = dists[..flat_end]
            .iter()
            .cloned()
            .fold(f32::MIN, f32::max);
        let range = (qmax - qmin).max(1e-10);
        let factor = 255.0 / range;

        let qtable: Vec<u8> = sim_table
            .iter()
            .map(|&d| ((d - qmin) * factor).min(255.0).max(0.0) as u8)
            .collect();

        // Step 3: u16 accumulation on transposed codes (sequential access per sub-quant)
        let mut q_dists = vec![0u16; count];
        for pair in 0..cs {
            let qtab_lo = &qtable[(pair * 2) * 16..(pair * 2 + 1) * 16];
            let qtab_hi = &qtable[(pair * 2 + 1) * 16..(pair * 2 + 2) * 16];
            let col = &codes[pair * count..]; // contiguous codes for this pair

            for i in flat_end..count {
                let byte = col[i];
                let lo = (byte & 0x0F) as usize;
                let hi = ((byte >> 4) & 0x0F) as usize;
                q_dists[i] += qtab_lo[lo] as u16 + qtab_hi[hi] as u16;
            }
        }

        // Step 4: dequantize
        let inv_factor = range / 255.0;
        let base_dist = qmin * m as f32;
        for i in flat_end..count {
            dists[i] = q_dists[i] as f32 * inv_factor + base_dist;
        }
    }

    // Push to heap
    for i in 0..count {
        if let Some(f) = filter {
            if !f.contains(&ids[i]) {
                continue;
            }
        }
        heap.push(dis0 + dists[i], ids[i]);
    }
}

/// Scan transposed (column-major) codes: layout is [M][n].
/// The distance table sub-slice stays in L1 cache for the entire inner loop.
fn scan_codes_transposed(
    sim_table: &[f32],
    codes: &[u8],  // layout: [M][count], i.e., codes[sub * count + vec_idx]
    ids: &[i64],
    count: usize,
    m: usize,
    ksub: usize,
    dis0: f32,
    filter: Option<&HashSet<i64>>,
    heap: &mut TopKHeap,
) {
    // Column-oriented: accumulate distances per sub-quantizer
    let mut dists = vec![dis0; count];
    for sub in 0..m {
        let tab_base = sub * ksub;
        let col_base = sub * count;
        for i in 0..count {
            dists[i] += sim_table[tab_base + codes[col_base + i] as usize];
        }
    }

    // Push to heap
    for i in 0..count {
        if let Some(f) = filter {
            if !f.contains(&ids[i]) {
                continue;
            }
        }
        heap.push(dists[i], ids[i]);
    }
}

/// Scan inverted list codes with 4-code batching for ILP (row-major layout).
fn scan_codes_batched(
    sim_table: &[f32],
    codes: &[u8],
    ids: &[i64],
    count: usize,
    m: usize,
    ksub: usize,
    dis0: f32,
    filter: Option<&HashSet<i64>>,
    heap: &mut TopKHeap,
) {
    let mut i = 0;

    // Process 4 codes at a time
    while i + 4 <= count {
        let dists = pq_distance_four_codes(
            sim_table,
            codes,
            m,
            ksub,
            [i * m, (i + 1) * m, (i + 2) * m, (i + 3) * m],
        );

        for j in 0..4 {
            let idx = i + j;
            let id = ids[idx];
            if let Some(f) = filter {
                if !f.contains(&id) {
                    continue;
                }
            }
            heap.push(dis0 + dists[j], id);
        }
        i += 4;
    }

    // Process remaining codes
    while i < count {
        let code = &codes[i * m..(i + 1) * m];
        let dist = dis0 + pq_distance_from_table(sim_table, code, m, ksub);
        let id = ids[i];
        if let Some(f) = filter {
            if !f.contains(&id) {
                i += 1;
                continue;
            }
        }
        heap.push(dist, id);
        i += 1;
    }
}

/// Search using a lazy reader (reads inverted lists on demand).
pub fn search_with_reader<R: SeekRead>(
    reader: &mut IVFPQIndexReader<R>,
    query: &[f32],
    k: usize,
    nprobe: usize,
) -> io::Result<(Vec<i64>, Vec<f32>)> {
    search_with_reader_filter(reader, query, k, nprobe, None)
}

/// Search with optional ID filter using a lazy reader.
pub fn search_with_reader_filter<R: SeekRead>(
    reader: &mut IVFPQIndexReader<R>,
    query: &[f32],
    k: usize,
    nprobe: usize,
    filter: Option<&HashSet<i64>>,
) -> io::Result<(Vec<i64>, Vec<f32>)> {
    reader.ensure_loaded()?;
    let d = reader.d;
    let m = reader.m;
    let ksub = reader.ksub;
    let metric = reader.metric;
    let by_residual = reader.by_residual;

    let mut q = query.to_vec();
    if metric == MetricType::Cosine {
        fvec_normalize(&mut q);
    }

    if let Some(ref opq) = reader.opq {
        let mut rotated = vec![0.0f32; d];
        opq.apply(&q, &mut rotated);
        q = rotated;
    }

    let (probe_indices, coarse_dists) =
        kmeans::find_topk(&q, &reader.quantizer_centroids, reader.nlist, d, nprobe);

    let use_precomputed =
        metric == MetricType::L2 && by_residual && !reader.precomputed_table.is_empty();
    let sim_table_2 = if use_precomputed {
        let mut t = vec![0.0f32; m * ksub];
        reader.pq.compute_inner_product_table(&q, &mut t);
        t
    } else {
        Vec::new()
    };

    // Pre-read all inverted lists upfront so we can scan in parallel
    let mut list_data: Vec<(usize, usize, f32, Vec<i64>, Vec<u8>)> = Vec::new();
    for (probe_idx, &list_id) in probe_indices.iter().enumerate() {
        let count = reader.list_counts[list_id] as usize;
        if count == 0 {
            continue;
        }
        let dis0 = if use_precomputed {
            coarse_dists[probe_idx]
        } else {
            0.0
        };
        let (ids, codes) = reader.read_inverted_list(list_id)?;
        list_data.push((list_id, count, dis0, ids, codes));
    }

    // Parallel scan across pre-read inverted lists
    let per_list_results: Vec<Vec<(f32, i64)>> = list_data
        .par_iter()
        .map(|(list_id, count, dis0, ids, codes)| {
            let mut sim_table_local = vec![0.0f32; m * ksub];

            if use_precomputed {
                let tab_base = list_id * m * ksub;
                fvec_madd(
                    &reader.precomputed_table[tab_base..tab_base + m * ksub],
                    &sim_table_2,
                    -2.0,
                    &mut sim_table_local,
                );
            } else if by_residual {
                let mut residual_query = vec![0.0f32; d];
                for j in 0..d {
                    residual_query[j] = q[j] - reader.quantizer_centroids[list_id * d + j];
                }
                reader
                    .pq
                    .compute_distance_table(&residual_query, metric, &mut sim_table_local);
            } else {
                reader
                    .pq
                    .compute_distance_table(&q, metric, &mut sim_table_local);
            }

            let mut local_heap = TopKHeap::new(k);
            let use_transposed = reader.transposed_codes;
            let is_4bit = reader.pq.nbits == 4;

            if is_4bit && use_transposed {
                scan_codes_4bit_transposed(
                    &sim_table_local, codes, ids, *count, m, *dis0, filter,
                    &mut local_heap,
                );
            } else if is_4bit {
                scan_codes_4bit(
                    &sim_table_local, codes, ids, *count, m, ksub, *dis0, filter,
                    &mut local_heap,
                );
            } else if use_transposed {
                scan_codes_transposed(
                    &sim_table_local, codes, ids, *count, m, ksub, *dis0, filter,
                    &mut local_heap,
                );
            } else {
                scan_codes_batched(
                    &sim_table_local, codes, ids, *count, m, ksub, *dis0, filter,
                    &mut local_heap,
                );
            }
            local_heap.into_sorted()
        })
        .collect();

    // Merge per-list heaps
    let mut heap = TopKHeap::new(k);
    for results in per_list_results {
        for (dist, id) in results {
            heap.push(dist, id);
        }
    }

    let sorted = heap.into_sorted();
    let result_ids: Vec<i64> = sorted.iter().map(|&(_, id)| id).collect();
    let result_dists: Vec<f32> = sorted.iter().map(|&(d, _)| d).collect();

    Ok((result_ids, result_dists))
}

/// Big batch search: batch queries share list reads.
/// Instead of nq*nprobe I/O ops, reads each unique list once and scans for all queries.
pub fn search_batch_reader<R: SeekRead>(
    reader: &mut IVFPQIndexReader<R>,
    queries: &[f32],
    nq: usize,
    k: usize,
    nprobe: usize,
) -> io::Result<(Vec<i64>, Vec<f32>)> {
    reader.ensure_loaded()?;
    let d = reader.d;
    let m = reader.m;
    let ksub = reader.ksub;
    let metric = reader.metric;
    let by_residual = reader.by_residual;

    // Step 1: Preprocess all queries
    let mut processed = queries[..nq * d].to_vec();
    if metric == MetricType::Cosine {
        for i in 0..nq {
            fvec_normalize(&mut processed[i * d..(i + 1) * d]);
        }
    }
    if let Some(ref opq) = reader.opq {
        let mut rotated = vec![0.0f32; nq * d];
        opq.apply_batch(&processed, &mut rotated, nq);
        processed = rotated;
    }

    // Step 2: Batch coarse search (one sgemm)
    let (all_probe_indices, all_coarse_dists) = kmeans::find_topk_batch(
        &processed,
        nq,
        &reader.quantizer_centroids,
        reader.nlist,
        d,
        nprobe,
    );

    // Step 3: Group (query_idx, probe_rank) pairs by list_id
    // list_id → Vec<(query_idx, coarse_dist)>
    let mut list_to_queries: Vec<Vec<(usize, f32)>> = vec![Vec::new(); reader.nlist];
    for qi in 0..nq {
        for (rank, &list_id) in all_probe_indices[qi].iter().enumerate() {
            let coarse_dist = all_coarse_dists[qi][rank];
            list_to_queries[list_id].push((qi, coarse_dist));
        }
    }

    // Step 4: For each unique list that has queries, read once and scan for all
    let use_precomputed =
        metric == MetricType::L2 && by_residual && !reader.precomputed_table.is_empty();

    // Precompute ip tables for all queries (needed for precomputed table mode)
    let all_sim_table_2: Vec<Vec<f32>> = if use_precomputed {
        (0..nq)
            .map(|qi| {
                let mut t = vec![0.0f32; m * ksub];
                reader
                    .pq
                    .compute_inner_product_table(&processed[qi * d..(qi + 1) * d], &mut t);
                t
            })
            .collect()
    } else {
        Vec::new()
    };

    let mut heaps: Vec<TopKHeap> = (0..nq).map(|_| TopKHeap::new(k)).collect();

    // Iterate over lists that have at least one query
    for list_id in 0..reader.nlist {
        if list_to_queries[list_id].is_empty() {
            continue;
        }
        let count = reader.list_counts[list_id] as usize;
        if count == 0 {
            continue;
        }

        // Read list once (shared across all queries that probe it)
        let (ids, codes) = reader.read_inverted_list(list_id)?;

        // Scan this list for every query that probes it
        for &(qi, coarse_dist) in &list_to_queries[list_id] {
            let query = &processed[qi * d..(qi + 1) * d];

            let mut sim_table = vec![0.0f32; m * ksub];
            if use_precomputed {
                let tab_base = list_id * m * ksub;
                fvec_madd(
                    &reader.precomputed_table[tab_base..tab_base + m * ksub],
                    &all_sim_table_2[qi],
                    -2.0,
                    &mut sim_table,
                );
            } else if by_residual {
                let mut residual_query = vec![0.0f32; d];
                for j in 0..d {
                    residual_query[j] = query[j] - reader.quantizer_centroids[list_id * d + j];
                }
                reader
                    .pq
                    .compute_distance_table(&residual_query, metric, &mut sim_table);
            } else {
                reader.pq.compute_distance_table(query, metric, &mut sim_table);
            }

            let dis0 = if use_precomputed { coarse_dist } else { 0.0 };

            let is_4bit = reader.pq.nbits == 4;
            if is_4bit && reader.transposed_codes {
                scan_codes_4bit_transposed(
                    &sim_table, &codes, &ids, count, m, dis0, None, &mut heaps[qi],
                );
            } else if is_4bit {
                scan_codes_4bit(
                    &sim_table, &codes, &ids, count, m, ksub, dis0, None, &mut heaps[qi],
                );
            } else if reader.transposed_codes {
                scan_codes_transposed(
                    &sim_table, &codes, &ids, count, m, ksub, dis0, None, &mut heaps[qi],
                );
            } else {
                scan_codes_batched(
                    &sim_table, &codes, &ids, count, m, ksub, dis0, None, &mut heaps[qi],
                );
            }
        }
    }

    // Collect results
    let mut result_ids = vec![-1i64; nq * k];
    let mut result_dists = vec![f32::MAX; nq * k];
    for qi in 0..nq {
        let sorted = std::mem::replace(&mut heaps[qi], TopKHeap::new(0)).into_sorted();
        let base = qi * k;
        for (i, &(dist, id)) in sorted.iter().enumerate() {
            result_ids[base + i] = id;
            result_dists[base + i] = dist;
        }
    }

    Ok((result_ids, result_dists))
}

// --- Top-K Heap ---

struct TopKHeap {
    k: usize,
    data: Vec<(f32, i64)>,
    built: bool,
}

impl TopKHeap {
    fn new(k: usize) -> Self {
        TopKHeap {
            k,
            data: Vec::with_capacity(k),
            built: false,
        }
    }

    #[inline]
    fn push(&mut self, dist: f32, id: i64) {
        if self.data.len() < self.k {
            self.data.push((dist, id));
            if self.data.len() == self.k {
                build_max_heap(&mut self.data);
                self.built = true;
            }
        } else if dist < self.data[0].0 {
            self.data[0] = (dist, id);
            sift_down(&mut self.data, 0);
        }
    }

    fn into_sorted(mut self) -> Vec<(f32, i64)> {
        self.data.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        self.data
    }
}

// --- Utilities ---

fn compute_residuals(
    data: &[f32],
    n: usize,
    d: usize,
    centroids: &[f32],
    nlist: usize,
) -> Vec<f32> {
    let mut residuals = vec![0.0f32; n * d];
    for i in 0..n {
        let point = &data[i * d..(i + 1) * d];
        let list_id = kmeans::find_nearest(point, centroids, nlist, d);
        for j in 0..d {
            residuals[i * d + j] = point[j] - centroids[list_id * d + j];
        }
    }
    residuals
}

fn build_max_heap(heap: &mut [(f32, i64)]) {
    let n = heap.len();
    for i in (0..n / 2).rev() {
        sift_down(heap, i);
    }
}

fn sift_down(heap: &mut [(f32, i64)], mut i: usize) {
    let n = heap.len();
    loop {
        let mut largest = i;
        let left = 2 * i + 1;
        let right = 2 * i + 2;

        if left < n && heap[left].0 > heap[largest].0 {
            largest = left;
        }
        if right < n && heap[right].0 > heap[largest].0 {
            largest = right;
        }
        if largest == i {
            break;
        }
        heap.swap(i, largest);
        i = largest;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    fn generate_clustered_data(n: usize, d: usize, num_clusters: usize, seed: u64) -> Vec<f32> {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut centers = vec![0.0f32; num_clusters * d];
        for i in 0..num_clusters * d {
            centers[i] = rng.gen::<f32>() * 100.0;
        }

        let mut data = vec![0.0f32; n * d];
        for i in 0..n {
            let cluster = i % num_clusters;
            for j in 0..d {
                data[i * d + j] = centers[cluster * d + j] + rng.gen::<f32>() * 2.0 - 1.0;
            }
        }
        data
    }

    #[test]
    fn test_build_and_search_l2() {
        let d = 16;
        let nlist = 4;
        let m = 4;
        let n = 1000;
        let k = 5;
        let nprobe = 2;

        let data = generate_clustered_data(n, d, 4, 42);
        let ids: Vec<i64> = (0..n as i64).collect();

        let mut index = IVFPQIndex::new(d, nlist, m, MetricType::L2, false);
        index.train(&data, n);
        index.add(&data, &ids, n);

        let query = &data[0..d];
        let mut dists = vec![0.0f32; k];
        let mut labels = vec![0i64; k];
        index.search(query, 1, k, nprobe, &mut dists, &mut labels);

        assert_eq!(labels[0], 0);
        for i in 1..k {
            assert!(dists[i] >= dists[i - 1]);
        }
    }

    #[test]
    fn test_build_and_search_ip() {
        let d = 16;
        let nlist = 4;
        let m = 4;
        let n = 1000;

        let data = generate_clustered_data(n, d, 4, 123);
        let ids: Vec<i64> = (0..n as i64).collect();

        let mut index = IVFPQIndex::new(d, nlist, m, MetricType::InnerProduct, false);
        index.train(&data, n);
        index.add(&data, &ids, n);

        let mut dists = vec![0.0f32; 5];
        let mut labels = vec![0i64; 5];
        index.search(&data[0..d], 1, 5, 2, &mut dists, &mut labels);

        for i in 1..5 {
            assert!(dists[i] >= dists[i - 1]);
        }
    }

    #[test]
    fn test_search_with_filter() {
        let d = 16;
        let nlist = 4;
        let m = 4;
        let n = 1000;
        let k = 5;

        let data = generate_clustered_data(n, d, 4, 42);
        let ids: Vec<i64> = (0..n as i64).collect();

        let mut index = IVFPQIndex::new(d, nlist, m, MetricType::L2, false);
        index.train(&data, n);
        index.add(&data, &ids, n);

        // Only allow even IDs
        let filter: HashSet<i64> = (0..n as i64).filter(|id| id % 2 == 0).collect();
        let mut dists = vec![0.0f32; k];
        let mut labels = vec![0i64; k];
        index.search_with_filter(&data[0..d], 1, k, 4, Some(&filter), &mut dists, &mut labels);

        for &label in &labels[..k] {
            if label >= 0 {
                assert!(label % 2 == 0, "Filter violated: got odd ID {}", label);
            }
        }
    }

    #[test]
    fn test_batch_search() {
        let d = 16;
        let nlist = 4;
        let m = 4;
        let n = 1000;
        let k = 5;
        let nq = 10;

        let data = generate_clustered_data(n, d, 4, 42);
        let ids: Vec<i64> = (0..n as i64).collect();

        let mut index = IVFPQIndex::new(d, nlist, m, MetricType::L2, false);
        index.train(&data, n);
        index.add(&data, &ids, n);

        let queries: Vec<f32> = data[..nq * d].to_vec();
        let mut dists = vec![0.0f32; nq * k];
        let mut labels = vec![0i64; nq * k];
        index.search(&queries, nq, k, 2, &mut dists, &mut labels);

        // Each query's first result should be itself
        for qi in 0..nq {
            assert_eq!(labels[qi * k], qi as i64);
        }
    }

    #[test]
    fn test_write_read_search() {
        use crate::io::{write_index, IVFPQIndexReader, PosWriter};
        use std::io::Cursor;

        let d = 16;
        let nlist = 4;
        let m = 4;
        let n = 500;
        let k = 10;

        let data = generate_clustered_data(n, d, 4, 789);
        let ids: Vec<i64> = (0..n as i64).collect();

        let mut index = IVFPQIndex::new(d, nlist, m, MetricType::L2, false);
        index.train(&data, n);
        index.add(&data, &ids, n);

        let mut buf = Vec::new();
        let mut writer = PosWriter::new(&mut buf);
        write_index(&index, &mut writer).unwrap();

        let mut cursor = Cursor::new(buf);
        let mut reader = IVFPQIndexReader::open(&mut cursor).unwrap();

        let (result_ids, result_dists) = reader.search(&data[0..d], k, 4).unwrap();

        assert!(!result_ids.is_empty());
        assert!(result_ids.contains(&0));
        for i in 1..result_dists.len() {
            assert!(result_dists[i] >= result_dists[i - 1]);
        }
    }

    #[test]
    fn test_write_read_search_with_filter() {
        use crate::io::{write_index, IVFPQIndexReader, PosWriter};
        use std::io::Cursor;

        let d = 16;
        let nlist = 4;
        let m = 4;
        let n = 500;
        let k = 5;

        let data = generate_clustered_data(n, d, 4, 789);
        let ids: Vec<i64> = (0..n as i64).collect();

        let mut index = IVFPQIndex::new(d, nlist, m, MetricType::L2, false);
        index.train(&data, n);
        index.add(&data, &ids, n);

        let mut buf = Vec::new();
        let mut writer = PosWriter::new(&mut buf);
        write_index(&index, &mut writer).unwrap();

        let mut cursor = Cursor::new(buf);
        let mut reader = IVFPQIndexReader::open(&mut cursor).unwrap();

        let filter: HashSet<i64> = (0..n as i64).filter(|id| id % 3 == 0).collect();
        let (result_ids, _) =
            crate::ivfpq::search_with_reader_filter(&mut reader, &data[0..d], k, 4, Some(&filter))
                .unwrap();

        for &id in &result_ids {
            assert!(id % 3 == 0, "Filter violated: got ID {}", id);
        }
    }

    #[test]
    fn test_big_batch_search() {
        use crate::io::{write_index, IVFPQIndexReader, PosWriter};
        use std::io::Cursor;

        let d = 16;
        let nlist = 4;
        let m = 4;
        let n = 1000;
        let k = 5;
        let nq = 20;
        let nprobe = 2;

        let data = generate_clustered_data(n, d, 4, 42);
        let ids: Vec<i64> = (0..n as i64).collect();

        let mut index = IVFPQIndex::new(d, nlist, m, MetricType::L2, false);
        index.train(&data, n);
        index.add(&data, &ids, n);

        let mut buf = Vec::new();
        let mut writer = PosWriter::new(&mut buf);
        write_index(&index, &mut writer).unwrap();

        let mut cursor = Cursor::new(&buf);
        let mut reader = IVFPQIndexReader::open(&mut cursor).unwrap();

        let queries = &data[..nq * d];
        let (batch_ids, batch_dists) =
            search_batch_reader(&mut reader, queries, nq, k, nprobe).unwrap();

        // Each query's first result should be itself
        for qi in 0..nq {
            let base = qi * k;
            assert_eq!(batch_ids[base], qi as i64);
            // Distances should be sorted
            for i in 1..k {
                if batch_ids[base + i] >= 0 {
                    assert!(batch_dists[base + i] >= batch_dists[base + i - 1]);
                }
            }
        }
    }

    #[test]
    fn test_4bit_ivfpq() {
        let d = 16;
        let nlist = 4;
        let m = 8; // must be even for 4-bit
        let n = 1000;
        let k = 5;
        let nprobe = 2;

        let data = generate_clustered_data(n, d, 4, 42);
        let ids: Vec<i64> = (0..n as i64).collect();

        let mut index = IVFPQIndex::with_nbits(d, nlist, m, 4, MetricType::L2, false);
        assert_eq!(index.pq.ksub, 16);
        assert_eq!(index.pq.code_size(), 4); // m/2

        index.train(&data, n);
        index.add(&data, &ids, n);

        let mut dists = vec![0.0f32; k];
        let mut labels = vec![0i64; k];
        index.search(&data[0..d], 1, k, nprobe, &mut dists, &mut labels);

        // Should find the query vector itself
        assert_eq!(labels[0], 0);
        for i in 1..k {
            assert!(dists[i] >= dists[i - 1]);
        }

        // Compare storage with 8-bit: 4-bit should be ~50% smaller
        let codes_8bit_size = n * m; // 8-bit: m bytes per vector
        let codes_4bit_size: usize = index.codes.iter().map(|c| c.len()).sum();
        // 4-bit is m/2 bytes per vector = half of 8-bit
        assert!(
            codes_4bit_size < codes_8bit_size,
            "4-bit ({}) should be smaller than 8-bit ({})",
            codes_4bit_size,
            codes_8bit_size,
        );
    }
}
