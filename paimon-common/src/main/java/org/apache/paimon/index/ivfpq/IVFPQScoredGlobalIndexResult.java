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

import org.apache.paimon.globalindex.ScoreGetter;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.util.HashMap;
import java.util.Map;

/** IVF-PQ search result with row ID bitmap and per-row scores. */
public class IVFPQScoredGlobalIndexResult implements ScoredGlobalIndexResult {

    private final RoaringNavigableMap64 bitmap;
    private final Map<Long, Float> scores;

    public IVFPQScoredGlobalIndexResult(RoaringNavigableMap64 bitmap, Map<Long, Float> scores) {
        this.bitmap = bitmap;
        this.scores = scores;
    }

    @Override
    public RoaringNavigableMap64 results() {
        return bitmap;
    }

    @Override
    public ScoreGetter scoreGetter() {
        return rowId -> {
            Float score = scores.get(rowId);
            if (score == null) {
                throw new IllegalArgumentException("No score for row ID: " + rowId);
            }
            return score;
        };
    }

    /** Create from native search result, converting distances to scores. */
    public static IVFPQScoredGlobalIndexResult fromNativeResult(
            IVFPQResult result, DistanceType metric) {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        Map<Long, Float> scores = new HashMap<>();

        for (int i = 0; i < result.size(); i++) {
            long id = result.ids()[i];
            float distance = result.distances()[i];
            float score = distanceToScore(distance, metric);

            bitmap.add(id);
            scores.put(id, score);
        }

        return new IVFPQScoredGlobalIndexResult(bitmap, scores);
    }

    private static float distanceToScore(float distance, DistanceType metric) {
        switch (metric) {
            case L2:
                return 1.0f / (1.0f + distance);
            case COSINE:
                return 1.0f - distance;
            case INNER_PRODUCT:
                return -distance;
            default:
                return 1.0f / (1.0f + distance);
        }
    }
}
