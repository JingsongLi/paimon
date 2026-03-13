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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.compact.NoopCompactManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.mergetree.compact.KvCompactionManagerFactory;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Factory to create {@link SeparatedCompactManager}. */
public class SeparatedCompactManagerFactory implements KvCompactionManagerFactory {

    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;
    private final CoreOptions options;
    private final RowType keyType;
    private final CacheManager cacheManager;

    @Nullable private IOManager ioManager;
    @Nullable private CompactionMetrics compactionMetrics;

    public SeparatedCompactManagerFactory(
            KeyValueFileReaderFactory.Builder readerFactoryBuilder,
            CoreOptions options,
            RowType keyType,
            CacheManager cacheManager) {
        this.readerFactoryBuilder = readerFactoryBuilder;
        this.options = options;
        this.keyType = keyType;
        this.cacheManager = cacheManager;

        if (!options.deletionVectorsEnabled()) {
            throw new UnsupportedOperationException("Cannot support deletion-vectors disabled.");
        }
        if (options.recordLevelExpireTime() != null) {
            throw new UnsupportedOperationException(
                    "Cannot support record level expire time enabled.");
        }
        if (options.mergeEngine() != CoreOptions.MergeEngine.DEDUPLICATE) {
            // TODO support first row
            throw new UnsupportedOperationException(
                    "Cannot support merge engine: " + options.mergeEngine());
        }
        if (!options.sequenceField().isEmpty()) {
            throw new UnsupportedOperationException(
                    "Cannot support sequence field: " + options.sequenceField());
        }
    }

    @Override
    public void withIOManager(@Nullable IOManager ioManager) {
        this.ioManager = ioManager;
    }

    @Override
    public void withCompactionMetrics(@Nullable CompactionMetrics compactionMetrics) {
        this.compactionMetrics = compactionMetrics;
    }

    @Override
    public CompactManager create(
            BinaryRow partition,
            int bucket,
            ExecutorService compactExecutor,
            List<DataFileMeta> restoreFiles,
            @Nullable BucketedDvMaintainer dvMaintainer) {
        if (options.writeOnly()) {
            return new NoopCompactManager();
        }

        checkNotNull(ioManager);
        DeletionVector.Factory dvFactory = DeletionVector.factory(dvMaintainer);
        KeyValueFileReaderFactory keyOnlyReaderFactory =
                readerFactoryBuilder.copyWithoutValue().build(partition, bucket, dvFactory);
        return new SeparatedCompactManager(
                keyType,
                ioManager.pickRandomTempDir(),
                cacheManager,
                keyOnlyReaderFactory,
                compactExecutor,
                dvMaintainer,
                options.prepareCommitWaitCompaction(),
                restoreFiles,
                compactionMetrics == null
                        ? null
                        : compactionMetrics.createReporter(partition, bucket));
    }

    @Override
    public void close() {}
}
