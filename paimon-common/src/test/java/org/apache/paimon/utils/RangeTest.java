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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.Range.subtractRanges;
import static org.assertj.core.api.Assertions.assertThat;

class RangeTest {

    @Test
    public void testSubtractRanges() {
        List<Range> ranges = new ArrayList<>();
        ranges.add(new Range(1, 5));
        ranges.add(new Range(6, 10));
        ranges.add(new Range(11, 15));

        List<Range> subtracts = new ArrayList<>();
        subtracts.add(new Range(2, 4));
        subtracts.add(new Range(12, 14));

        List<Range> result = subtractRanges(ranges, subtracts);

        assertThat(result.toString()).isEqualTo("[" +
                "Range{start=1, end=2}, " +
                "Range{start=4, end=5}, " +
                "Range{start=6, end=10}, " +
                "Range{start=11, end=12}, " +
                "Range{start=14, end=15}]");
    }
}
