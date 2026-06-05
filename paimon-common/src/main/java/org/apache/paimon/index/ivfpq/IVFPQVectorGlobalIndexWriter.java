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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.VectorType;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * IVF-PQ index writer following the GlobalIndexSingletonWriter protocol.
 *
 * <p>Vectors are buffered to a temp file during write(), then the native Rust index is trained and
 * built during finish().
 */
public class IVFPQVectorGlobalIndexWriter implements GlobalIndexSingletonWriter, Closeable {

    private static final int BUFFER_SIZE = 8 * 1024 * 1024;

    private final GlobalIndexFileWriter fileWriter;
    private final IVFPQIndexOptions options;
    private final int dimension;

    private final Path tempFile;
    private final FileChannel writeChannel;
    private final ByteBuffer writeBuffer;

    private long logicalRowId;
    private long count;

    IVFPQVectorGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter, DataType fieldType, IVFPQIndexOptions options)
            throws IOException {
        this.fileWriter = fileWriter;
        this.options = options;
        this.dimension = options.dimension();

        validateFieldType(fieldType);

        this.tempFile = Files.createTempFile("ivfpq-vectors-", ".bin");
        this.writeChannel =
                FileChannel.open(
                        tempFile, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        this.writeBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE).order(ByteOrder.nativeOrder());
        this.logicalRowId = 0;
        this.count = 0;
    }

    @Override
    public void write(@Nullable Object fieldData) {
        if (fieldData == null) {
            logicalRowId++;
            return;
        }

        float[] vector = materializeVector(fieldData);
        if (vector.length != dimension) {
            throw new IllegalArgumentException(
                    "Expected dimension " + dimension + " but got " + vector.length);
        }

        // Write [logicalRowId (long)] [vector (float * dim)] to temp file
        int recordSize = 8 + dimension * 4;
        if (writeBuffer.remaining() < recordSize) {
            flushBuffer();
        }

        writeBuffer.putLong(logicalRowId);
        for (float v : vector) {
            writeBuffer.putFloat(v);
        }

        logicalRowId++;
        count++;
    }

    @Override
    public List<ResultEntry> finish() {
        if (count == 0) {
            cleanup();
            return Collections.emptyList();
        }

        try {
            flushBuffer();
            writeChannel.close();
            return buildIndex();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            cleanup();
        }
    }

    private List<ResultEntry> buildIndex() throws IOException {
        // Read all vectors from temp file
        int recordSize = 8 + dimension * 4;
        long[] ids = new long[(int) count];
        float[] data = new float[(int) count * dimension];

        try (FileChannel readChannel = FileChannel.open(tempFile, StandardOpenOption.READ)) {
            ByteBuffer readBuffer =
                    ByteBuffer.allocateDirect(BUFFER_SIZE).order(ByteOrder.nativeOrder());
            int idx = 0;

            while (idx < count) {
                readBuffer.clear();
                int bytesRead = readChannel.read(readBuffer);
                if (bytesRead <= 0) {
                    break;
                }
                readBuffer.flip();

                while (readBuffer.remaining() >= recordSize && idx < count) {
                    ids[idx] = readBuffer.getLong();
                    for (int d = 0; d < dimension; d++) {
                        data[idx * dimension + d] = readBuffer.getFloat();
                    }
                    idx++;
                }
            }
        }

        // Build index via native Rust
        long writerPtr =
                IVFPQNative.createWriter(
                        dimension,
                        options.nlist(),
                        options.m(),
                        options.metric().code(),
                        options.useOpq());
        try {
            IVFPQNative.train(writerPtr, data, (int) count);
            IVFPQNative.addVectors(writerPtr, ids, data, (int) count);

            // Write index file
            String fileName = fileWriter.newFileName("ivfpq");
            try (PositionOutputStream out = fileWriter.newOutputStream(fileName)) {
                IVFPQNative.writeIndex(writerPtr, out);
            }

            IVFPQIndexMeta meta = options.toMeta();
            List<ResultEntry> results = new ArrayList<>();
            results.add(new ResultEntry(fileName, logicalRowId, meta.serialize()));
            return results;
        } finally {
            IVFPQNative.freeWriter(writerPtr);
        }
    }

    private float[] materializeVector(Object fieldData) {
        if (fieldData instanceof float[]) {
            return (float[]) fieldData;
        } else if (fieldData instanceof InternalVector) {
            InternalVector vec = (InternalVector) fieldData;
            float[] result = new float[vec.size()];
            for (int i = 0; i < vec.size(); i++) {
                result[i] = vec.getFloat(i);
            }
            return result;
        } else if (fieldData instanceof InternalArray) {
            InternalArray arr = (InternalArray) fieldData;
            float[] result = new float[arr.size()];
            for (int i = 0; i < arr.size(); i++) {
                result[i] = arr.getFloat(i);
            }
            return result;
        }
        throw new IllegalArgumentException(
                "Unsupported vector type: " + fieldData.getClass().getName());
    }

    private void flushBuffer() {
        try {
            writeBuffer.flip();
            while (writeBuffer.hasRemaining()) {
                writeChannel.write(writeBuffer);
            }
            writeBuffer.clear();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void cleanup() {
        try {
            if (writeChannel.isOpen()) {
                writeChannel.close();
            }
        } catch (IOException ignored) {
        }
        try {
            Files.deleteIfExists(tempFile);
        } catch (IOException ignored) {
        }
    }

    @Override
    public void close() {
        cleanup();
    }

    private void validateFieldType(DataType fieldType) {
        if (fieldType instanceof VectorType) {
            return;
        }
        if (fieldType instanceof ArrayType) {
            return;
        }
        throw new IllegalArgumentException(
                "IVF-PQ index requires VECTOR or ARRAY<FLOAT> field type, got: " + fieldType);
    }
}
