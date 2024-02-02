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

package org.apache.paimon.mergetree;

import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.RandomAccessInputView;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.InMemoryBuffer;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.memory.ReservedMemorySegmentPool;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.mergetree.compact.ReducerMergeFunctionWrapper;
import org.apache.paimon.sort.BinaryInMemorySortBuffer;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

/** A {@link WriteBuffer} which stores records in {@link BinaryInMemorySortBuffer}. */
public class QueryableWriteBuffer implements WriteBuffer {

    private final InternalRowSerializer keySerializer;
    private final BinaryRowSerializer valueDeserializer;
    private final int pageSize;
    private final InMemoryBuffer buffer;

    private final Map<BinaryRow, NavigableSet<WriteValue>> index;

    public QueryableWriteBuffer(RowType keyType, RowType valueType, MemorySegmentPool memoryPool) {
        this.keySerializer = new InternalRowSerializer(keyType);
        this.valueDeserializer = new BinaryRowSerializer(valueType.getFieldCount());
        this.pageSize = memoryPool.pageSize();
        // reserve memory for key index heap
        this.buffer =
                new InMemoryBuffer(
                        new ReservedMemorySegmentPool(memoryPool, memoryPool.freePages() / 2),
                        new InternalRowSerializer(valueType));

        RecordComparator keyComparator =
                CodeGenUtils.newRecordComparator(keyType.getFieldTypes(), "KeyComparator");
        this.index = new ConcurrentSkipListMap<>(keyComparator);
    }

    @Override
    public boolean put(long sequenceNumber, RowKind valueKind, InternalRow key, InternalRow value)
            throws IOException {
        long offset = this.buffer.getCurrentDataBufferOffset();
        boolean success = this.buffer.put(value);
        if (!success) {
            return false;
        }

        BinaryRow binaryKey = keySerializer.toBinaryRow(key).copy();
        WriteValue writeValue = new WriteValue(sequenceNumber, valueKind, offset);
        index.computeIfAbsent(binaryKey, k -> new ConcurrentSkipListSet<>()).add(writeValue);
        return true;
    }

    @Override
    public int size() {
        return buffer.size();
    }

    @Override
    public long memoryOccupancy() {
        // reserve memory for key index heap
        return buffer.memoryOccupancy() * 2;
    }

    @Override
    public boolean flushMemory() throws IOException {
        // in memory, can not be flushed
        return false;
    }

    @Override
    public void forEach(
            Comparator<InternalRow> keyComparator,
            MergeFunction<KeyValue> mergeFunction,
            @Nullable WriteBuffer.KvConsumer rawConsumer,
            KvConsumer mergedConsumer)
            throws IOException {
        ReducerMergeFunctionWrapper merger = new ReducerMergeFunctionWrapper(mergeFunction);
        RandomAccessInputView inputView = createView();
        for (Map.Entry<BinaryRow, NavigableSet<WriteValue>> entry : index.entrySet()) {
            BinaryRow key = entry.getKey();
            Set<WriteValue> values = entry.getValue();
            merger.reset();
            for (WriteValue v : values) {
                inputView.setReadPosition(v.offset);
                BinaryRow row = valueDeserializer.deserialize(inputView);
                KeyValue kv = new KeyValue().replace(key, v.sequenceNumber, v.valueKind, row);
                if (rawConsumer != null) {
                    rawConsumer.accept(kv);
                }
                merger.add(kv);
            }
            KeyValue result = merger.getResult();
            if (result != null) {
                mergedConsumer.accept(result);
            }
        }
    }

    @Override
    public void clear() {
        this.buffer.reset();
        this.index.clear();
    }

    private RandomAccessInputView createView() {
        return new RandomAccessInputView(buffer.getRecordBufferSegments(), pageSize);
    }

    @Nullable
    public BinaryRow lookupLatest(BinaryRow key) {
        NavigableSet<WriteValue> values = index.get(key);
        if (values == null || values.isEmpty()) {
            return null;
        }

        WriteValue value = values.last();
        RandomAccessInputView view = createView();
        view.setReadPosition(value.offset);
        try {
            return valueDeserializer.deserialize(view);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class WriteValue implements Comparable<WriteValue> {

        private final long sequenceNumber;
        private final RowKind valueKind;
        private final long offset;

        private WriteValue(long sequenceNumber, RowKind valueKind, long offset) {
            this.sequenceNumber = sequenceNumber;
            this.valueKind = valueKind;
            this.offset = offset;
        }

        @Override
        public int compareTo(@NotNull QueryableWriteBuffer.WriteValue o) {
            return Long.compare(sequenceNumber, o.sequenceNumber);
        }
    }
}
