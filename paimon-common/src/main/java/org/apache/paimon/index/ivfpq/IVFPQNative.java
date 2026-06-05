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

import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;

/**
 * JNI bridge to the Rust IVF-PQ implementation. All methods are static and operate on native
 * pointers (returned as {@code long}).
 *
 * <p>Users should not call these methods directly. Use {@link NativeIVFPQIndexWriter} and {@link
 * NativeIVFPQIndexReader} instead.
 */
class IVFPQNative {

    static {
        NativeLoader.load();
    }

    // --- Writer ---

    static native long createWriter(int dimension, int nlist, int m, int metric, boolean useOpq);

    static native void train(long ptr, float[] data, int n);

    static native void addVectors(long ptr, long[] ids, float[] data, int n);

    static native void writeIndex(long ptr, PositionOutputStream output);

    static native void freeWriter(long ptr);

    // --- Reader ---

    static native long openReader(SeekableInputStream input);

    static native int getDimension(long ptr);

    static native long getTotalVectors(long ptr);

    static native IVFPQResult search(long ptr, float[] query, int k, int nprobe);

    static native IVFPQBatchResult searchBatch(
            long ptr, float[] queries, int nq, int k, int nprobe);

    static native void freeReader(long ptr);
}
