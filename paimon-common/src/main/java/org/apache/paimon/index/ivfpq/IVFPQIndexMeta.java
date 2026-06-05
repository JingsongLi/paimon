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

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/** Serializable metadata for an IVF-PQ index file. Stored as JSON in ResultEntry.meta(). */
public class IVFPQIndexMeta {

    private final int dimension;
    private final int nlist;
    private final int m;
    private final DistanceType metric;
    private final boolean useOpq;

    public IVFPQIndexMeta(int dimension, int nlist, int m, DistanceType metric, boolean useOpq) {
        this.dimension = dimension;
        this.nlist = nlist;
        this.m = m;
        this.metric = metric;
        this.useOpq = useOpq;
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

    public byte[] serialize() {
        // Simple key=value format
        Map<String, String> map = new LinkedHashMap<>();
        map.put("dim", String.valueOf(dimension));
        map.put("nlist", String.valueOf(nlist));
        map.put("m", String.valueOf(m));
        map.put("metric", metric.name().toLowerCase());
        map.put("opq", String.valueOf(useOpq));
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : map.entrySet()) {
            if (sb.length() > 0) {
                sb.append('\n');
            }
            sb.append(e.getKey()).append('=').append(e.getValue());
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static IVFPQIndexMeta deserialize(byte[] data) {
        String content = new String(data, StandardCharsets.UTF_8);
        Map<String, String> map = new LinkedHashMap<>();
        for (String line : content.split("\n")) {
            int eq = line.indexOf('=');
            if (eq > 0) {
                map.put(line.substring(0, eq).trim(), line.substring(eq + 1).trim());
            }
        }

        int dim = Integer.parseInt(map.getOrDefault("dim", "0"));
        int nlist = Integer.parseInt(map.getOrDefault("nlist", "256"));
        int m = Integer.parseInt(map.getOrDefault("m", "16"));
        String metricStr = map.getOrDefault("metric", "l2");
        DistanceType metric;
        switch (metricStr) {
            case "inner_product":
                metric = DistanceType.INNER_PRODUCT;
                break;
            case "cosine":
                metric = DistanceType.COSINE;
                break;
            default:
                metric = DistanceType.L2;
        }
        boolean useOpq = Boolean.parseBoolean(map.getOrDefault("opq", "false"));

        return new IVFPQIndexMeta(dim, nlist, m, metric, useOpq);
    }
}
