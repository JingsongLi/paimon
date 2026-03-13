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

package org.apache.paimon.mergetree.compact.separated;

import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.compact.CompactFutureManager;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.sort.db.SimpleLsmKvDb;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.utils.VarLengthIntUtils.decodeInt;
import static org.apache.paimon.utils.VarLengthIntUtils.encodeInt;

/** Key Value separated compact manager for {@link KeyValueFileStore}. */
public class SeparatedCompactManager extends CompactFutureManager {

    private final RowType keyType;
    private final KeyValueFileReaderFactory keyOnlyReaderFactory;
    private final ExecutorService executor;
    private final BucketedDvMaintainer dvMaintainer;
    private final SimpleLsmKvDb kvDb;
    private final boolean lazyGenDeletionFile;
    @Nullable private final CompactionMetrics.Reporter metricsReporter;

    private final SeparatedFileLevels fileLevels;

    public SeparatedCompactManager(
            RowType keyType,
            String tmpDir,
            CacheManager cacheManager,
            KeyValueFileReaderFactory keyOnlyReaderFactory,
            ExecutorService executor,
            BucketedDvMaintainer dvMaintainer,
            boolean lazyGenDeletionFile,
            List<DataFileMeta> restoreFiles,
            @Nullable CompactionMetrics.Reporter metricsReporter) {
        this.keyType = keyType;
        this.keyOnlyReaderFactory = keyOnlyReaderFactory;
        this.executor = executor;
        this.dvMaintainer = dvMaintainer;
        this.lazyGenDeletionFile = lazyGenDeletionFile;
        this.metricsReporter = metricsReporter;
        this.fileLevels = new SeparatedFileLevels();
        restoreFiles.forEach(this::addNewFile);

        this.kvDb =
                SimpleLsmKvDb.builder(new File(tmpDir))
                        .cacheManager(cacheManager)
                        .keyComparator(new RowCompactedSerializer(keyType).createSliceComparator())
                        .build();
        bootstrapKeyIndex(restoreFiles);
    }

    private void bootstrapKeyIndex(List<DataFileMeta> restoreFiles) {
        RowCompactedSerializer keySerializer = new RowCompactedSerializer(keyType);
        for (DataFileMeta file : restoreFiles) {
            if (file.level() == 0) {
                continue;
            }
            int fileId = fileLevels.getFileIdByName(file.fileName());
            int position = 0;
            try (CloseableIterator<InternalRow> iterator = readKeyIterator(file)) {
                while (iterator.hasNext()) {
                    byte[] key = keySerializer.serializeToBytes(iterator.next());
                    ByteArrayOutputStream value = new ByteArrayOutputStream(8);
                    encodeInt(value, fileId);
                    encodeInt(value, position);
                    kvDb.put(key, value.toByteArray());
                    position++;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private CloseableIterator<InternalRow> readKeyIterator(DataFileMeta file) throws IOException {
        //noinspection resource
        return keyOnlyReaderFactory
                .createRecordReader(file)
                .transform(KeyValue::key)
                .toCloseableIterator();
    }

    @Override
    public boolean shouldWaitForLatestCompaction() {
        return false;
    }

    @Override
    public boolean shouldWaitForPreparingCheckpoint() {
        return false;
    }
    @Override
    public void addNewFile(DataFileMeta file) {
        fileLevels.addNewFile(file);
    }

    @Override
    public List<DataFileMeta> allFiles() {
        return fileLevels.allFiles();
    }

    @Override
    public void triggerCompaction(boolean fullCompaction) {
        if (!fileLevels.compactNotCompleted()) {
            return;
        }

        taskFuture =
                executor.submit(
                        new CompactTask(metricsReporter) {
                            @Override
                            protected CompactResult doCompact() throws Exception {
                                return compact();
                            }
                        });
    }

    private CompactResult compact() throws Exception {
        RowCompactedSerializer keySerializer = new RowCompactedSerializer(keyType);
        List<DataFileMeta> level0Files = fileLevels.level0Files();
        CompactResult result = new CompactResult();
        for (DataFileMeta file : level0Files) {
            DataFileMeta newFile = fileLevels.upgrade(file, 1);
            int fileId = fileLevels.getFileIdByName(file.fileName());
            int position = 0;
            try (CloseableIterator<InternalRow> iterator = readKeyIterator(file)) {
                while (iterator.hasNext()) {
                    byte[] key = keySerializer.serializeToBytes(iterator.next());
                    byte[] oldValue = kvDb.get(key);
                    if (oldValue != null) {
                        ByteArrayInputStream valueIn = new ByteArrayInputStream(oldValue);
                        DataFileMeta oldFile = fileLevels.getFileById(decodeInt(valueIn));
                        dvMaintainer.notifyNewDeletion(oldFile.fileName(), decodeInt(valueIn));
                    }
                    ByteArrayOutputStream value = new ByteArrayOutputStream(8);
                    encodeInt(value, fileId);
                    encodeInt(value, position);
                    kvDb.put(key, value.toByteArray());
                    position++;
                }
            }
            result.before().add(file);
            result.after().add(newFile);
        }
        CompactDeletionFile deletionFile =
                lazyGenDeletionFile
                        ? CompactDeletionFile.lazyGeneration(dvMaintainer)
                        : CompactDeletionFile.generateFiles(dvMaintainer);
        result.setDeletionFile(deletionFile);
        return result;
    }

    @Override
    public Optional<CompactResult> getCompactionResult(boolean blocking)
            throws ExecutionException, InterruptedException {
        return innerGetCompactionResult(blocking);
    }

    @Override
    public boolean compactNotCompleted() {
        return super.compactNotCompleted() || fileLevels.compactNotCompleted();
    }

    @Override
    public void close() throws IOException {
        kvDb.close();
    }
}
