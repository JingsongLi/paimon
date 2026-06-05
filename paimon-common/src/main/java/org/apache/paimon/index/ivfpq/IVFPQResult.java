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

/** Result of an IVF-PQ nearest neighbor search. */
public class IVFPQResult {

    private final long[] ids;
    private final float[] distances;

    public IVFPQResult(long[] ids, float[] distances) {
        this.ids = ids;
        this.distances = distances;
    }

    /** Vector IDs of the top-k nearest neighbors, sorted by distance ascending. */
    public long[] ids() {
        return ids;
    }

    /** Distances of the top-k nearest neighbors, sorted ascending. */
    public float[] distances() {
        return distances;
    }

    public int size() {
        return ids.length;
    }
}
