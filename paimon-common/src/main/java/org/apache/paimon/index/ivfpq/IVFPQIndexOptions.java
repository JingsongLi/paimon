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

import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.VectorType;

/** Parsed options for IVF-PQ index. */
public class IVFPQIndexOptions {

    private final int dimension;
    private final int nlist;
    private final int m;
    private final DistanceType metric;
    private final boolean useOpq;
    private final int nprobe;

    public IVFPQIndexOptions(DataType fieldType, Options options) {
        this.dimension = resolveDimension(fieldType, options);

        this.nlist = options.getInteger("ivfpq.nlist", 256);
        int defaultM = Math.max(1, this.dimension / 8);
        this.m = options.getInteger("ivfpq.m", defaultM);
        this.nprobe = options.getInteger("ivfpq.nprobe", 8);
        this.useOpq = options.getBoolean("ivfpq.opq", false);

        String metricStr = options.getString("ivfpq.metric", "l2").toLowerCase();
        switch (metricStr) {
            case "ip":
            case "inner_product":
                this.metric = DistanceType.INNER_PRODUCT;
                break;
            case "cosine":
                this.metric = DistanceType.COSINE;
                break;
            default:
                this.metric = DistanceType.L2;
        }

        if (this.dimension % this.m != 0) {
            throw new IllegalArgumentException(
                    "Dimension "
                            + this.dimension
                            + " must be divisible by m="
                            + this.m
                            + ". Set ivfpq.m to a valid divisor.");
        }
    }

    private int resolveDimension(DataType fieldType, Options options) {
        int dim = options.getInteger("ivfpq.dimension", -1);
        if (dim > 0) {
            return dim;
        }
        if (fieldType instanceof VectorType) {
            return ((VectorType) fieldType).getLength();
        }
        throw new IllegalArgumentException(
                "Cannot determine vector dimension. Set ivfpq.dimension explicitly.");
    }

    public int dimension() {
        return dimension;
    }

    public int nlist() {
        return nlist;
    }

    public int m() {
        return m;
    }

    public DistanceType metric() {
        return metric;
    }

    public boolean useOpq() {
        return useOpq;
    }

    public int nprobe() {
        return nprobe;
    }

    public IVFPQIndexMeta toMeta() {
        return new IVFPQIndexMeta(dimension, nlist, m, metric, useOpq);
    }
}
