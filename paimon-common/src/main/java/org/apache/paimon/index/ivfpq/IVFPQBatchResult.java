/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.index.ivfpq;

/** Result of a batch IVF-PQ search over multiple query vectors. */
public class IVFPQBatchResult {

    private final long[] ids;
    private final float[] distances;
    private final int nq;
    private final int k;

    public IVFPQBatchResult(long[] ids, float[] distances, int nq, int k) {
        this.ids = ids;
        this.distances = distances;
        this.nq = nq;
        this.k = k;
    }

    /** Get the result for a single query. */
    public IVFPQResult resultForQuery(int queryIndex) {
        int offset = queryIndex * k;
        int count = 0;
        for (int i = 0; i < k; i++) {
            if (ids[offset + i] >= 0) {
                count++;
            }
        }
        long[] qIds = new long[count];
        float[] qDists = new float[count];
        System.arraycopy(ids, offset, qIds, 0, count);
        System.arraycopy(distances, offset, qDists, 0, count);
        return new IVFPQResult(qIds, qDists);
    }

    /** Number of queries. */
    public int numQueries() {
        return nq;
    }

    /** Top-k per query. */
    public int topK() {
        return k;
    }

    /** Raw flat ID array of shape [nq * k]. Use {@link #resultForQuery} for per-query access. */
    public long[] ids() {
        return ids;
    }

    /** Raw flat distance array of shape [nq * k]. */
    public float[] distances() {
        return distances;
    }
}
