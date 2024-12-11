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

package org.apache.paimon.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class Range {

    private final int start;
    private final int end;

    public Range(int start, int end) {
        this.start = start;
        this.end = end;
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }

    public boolean noOverlaps(Range other) {
        return this.start > other.end || this.end < other.start;
    }

    public Range[] subtract(Range other) {
        if (this.noOverlaps(other)) {
            // no overlapping, return self
            return new Range[]{this};
        }

        List<Range> result = new ArrayList<>();
        if (this.start < other.start) {
            // before other
            result.add(new Range(this.start, other.start));
        }
        if (this.end > other.end) {
            // after other
            result.add(new Range(other.end, this.end));
        }
        return result.toArray(new Range[0]);
    }

    @Override
    public String toString() {
        return "Range{" + "start=" + start + ", end=" + end + '}';
    }

    public static List<Range> subtractRanges(List<Range> ranges, List<Range> subtracts) {
        List<Range> result = new ArrayList<>();

        for (Range range : ranges) {
            Range remaining = range;

            for (Range subtract : subtracts) {
                Range[] subtracted = remaining.subtract(subtract);
                if (subtracted.length > 0) {
                    // update remaining
                    remaining = subtracted[0];
                    result.addAll(Arrays.asList(subtracted).subList(1, subtracted.length));
                } else {
                    // totally overlapping, set it to null
                    remaining = null;
                    break;
                }
            }

            if (remaining != null) {
                // add remaining to Ranges
                result.add(remaining);
            }
        }

        result.sort(Comparator.comparingInt(Range::getStart));
        return result;
    }
}