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

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.VectorSearch;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * IVF-PQ index reader following the GlobalIndexReader protocol. Lazy-loads the native index on
 * first search. Only supports vector search; all scalar predicates return empty.
 */
public class IVFPQVectorGlobalIndexReader implements GlobalIndexReader {

    private final GlobalIndexFileReader fileReader;
    private final GlobalIndexIOMeta ioMeta;
    private final IVFPQIndexOptions options;
    private final ExecutorService executor;

    private volatile long nativePtr = 0;
    private volatile SeekableInputStream stream;
    private IVFPQIndexMeta indexMeta;

    IVFPQVectorGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            IVFPQIndexOptions options,
            ExecutorService executor) {
        if (files.isEmpty()) {
            throw new IllegalArgumentException("No index files provided");
        }
        this.fileReader = fileReader;
        this.ioMeta = files.get(0);
        this.options = options;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Optional<ScoredGlobalIndexResult>> visitVectorSearch(
            VectorSearch vectorSearch) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        ensureLoaded();
                        return Optional.of(doSearch(vectorSearch));
                    } catch (IOException e) {
                        throw new RuntimeException("IVF-PQ search failed", e);
                    }
                },
                executor);
    }

    // --- Scalar predicates: not applicable for vector index ---

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNotNull(FieldRef fieldRef) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNull(FieldRef fieldRef) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitStartsWith(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessThan(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterOrEqual(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessOrEqual(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEqual(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterThan(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIn(
            FieldRef fieldRef, List<Object> literals) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotIn(
            FieldRef fieldRef, List<Object> literals) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotEqual(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEndsWith(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitContains(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLike(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    // --- Internal ---

    private synchronized void ensureLoaded() throws IOException {
        if (nativePtr != 0) {
            return;
        }
        indexMeta = IVFPQIndexMeta.deserialize(ioMeta.metadata());
        stream = fileReader.getInputStream(ioMeta);
        nativePtr = IVFPQNative.openReader(stream);
    }

    private ScoredGlobalIndexResult doSearch(VectorSearch vectorSearch) {
        float[] query = vectorSearch.vector();
        int topK = Math.min(vectorSearch.limit(), (int) IVFPQNative.getTotalVectors(nativePtr));
        int nprobe = options.nprobe();

        if (topK <= 0) {
            return IVFPQScoredGlobalIndexResult.fromNativeResult(
                    new IVFPQResult(new long[0], new float[0]), indexMeta.metric());
        }

        IVFPQResult result = IVFPQNative.search(nativePtr, query, topK, nprobe);
        return IVFPQScoredGlobalIndexResult.fromNativeResult(result, indexMeta.metric());
    }

    @Override
    public void close() throws IOException {
        if (nativePtr != 0) {
            IVFPQNative.freeReader(nativePtr);
            nativePtr = 0;
        }
        if (stream != null) {
            stream.close();
            stream = null;
        }
    }
}
