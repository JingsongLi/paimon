/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.io;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.TableStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.statistics.FieldStatsCollector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.StatsCollectorFactories;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/** A factory to create {@link FileWriter}s for writing {@link KeyValue} files. */
public class KeyValueFileWriterFactory {

    private final FileIO fileIO;
    private final long schemaId;
    private final RowType keyType;
    private final RowType valueType;
    private final WriteFormatContext formatContext;
    private final long suggestedFileSize;
    private final CoreOptions options;

    private KeyValueFileWriterFactory(
            FileIO fileIO,
            long schemaId,
            RowType keyType,
            RowType valueType,
            WriteFormatContext formatContext,
            long suggestedFileSize,
            CoreOptions options) {
        this.fileIO = fileIO;
        this.schemaId = schemaId;
        this.keyType = keyType;
        this.valueType = valueType;
        this.formatContext = formatContext;
        this.suggestedFileSize = suggestedFileSize;
        this.options = options;
    }

    public RowType keyType() {
        return keyType;
    }

    public RowType valueType() {
        return valueType;
    }

    @VisibleForTesting
    public DataFilePathFactory pathFactory(int level) {
        return formatContext.pathFactory(level);
    }

    public RollingFileWriter<KeyValue, DataFileMeta> createRollingMergeTreeFileWriter(int level) {
        return new RollingFileWriter<>(
                () -> createDataFileWriter(formatContext.pathFactory(level).newPath(), level),
                suggestedFileSize);
    }

    public RollingFileWriter<KeyValue, DataFileMeta> createRollingChangelogFileWriter(int level) {
        return new RollingFileWriter<>(
                () ->
                        createDataFileWriter(
                                formatContext.pathFactory(level).newChangelogPath(), level),
                suggestedFileSize);
    }

    private KeyValueDataFileWriter createDataFileWriter(Path path, int level) {
        KeyValueSerializer kvSerializer = new KeyValueSerializer(keyType, valueType);
        return new KeyValueDataFileWriter(
                fileIO,
                formatContext.writerFactory(level),
                path,
                kvSerializer::toRow,
                keyType,
                valueType,
                formatContext.extractor(level),
                schemaId,
                level,
                formatContext.compression(level),
                options);
    }

    public void deleteFile(String filename, int level) {
        fileIO.deleteQuietly(formatContext.pathFactory(level).toPath(filename));
    }

    public static Builder builder(
            FileIO fileIO,
            long schemaId,
            RowType keyType,
            RowType valueType,
            FileFormat fileFormat,
            Map<String, FileStorePathFactory> format2PathFactory,
            long suggestedFileSize) {
        return new Builder(
                fileIO,
                schemaId,
                keyType,
                valueType,
                fileFormat,
                format2PathFactory,
                suggestedFileSize);
    }

    /** Builder of {@link KeyValueFileWriterFactory}. */
    public static class Builder {

        private final FileIO fileIO;
        private final long schemaId;
        private final RowType keyType;
        private final RowType valueType;
        private final FileFormat fileFormat;
        private final Map<String, FileStorePathFactory> format2PathFactory;
        private final long suggestedFileSize;

        private Builder(
                FileIO fileIO,
                long schemaId,
                RowType keyType,
                RowType valueType,
                FileFormat fileFormat,
                Map<String, FileStorePathFactory> format2PathFactory,
                long suggestedFileSize) {
            this.fileIO = fileIO;
            this.schemaId = schemaId;
            this.keyType = keyType;
            this.valueType = valueType;
            this.fileFormat = fileFormat;
            this.format2PathFactory = format2PathFactory;
            this.suggestedFileSize = suggestedFileSize;
        }

        public KeyValueFileWriterFactory build(
                BinaryRow partition, int bucket, CoreOptions options) {
            RowType fileRowType = KeyValue.schema(keyType, valueType);
            WriteFormatContext context =
                    new WriteFormatContext(
                            partition,
                            bucket,
                            fileRowType,
                            fileFormat,
                            format2PathFactory,
                            options);
            return new KeyValueFileWriterFactory(
                    fileIO, schemaId, keyType, valueType, context, suggestedFileSize, options);
        }
    }

    private static class WriteFormatContext {

        private final BinaryRow partition;
        private final int bucket;
        private final RowType rowType;
        private final FieldStatsCollector.Factory[] statsCollectorFactories;
        private final Map<String, FileStorePathFactory> format2PathFactory;

        private final Function<Integer, FileFormat> level2Format;
        private final Function<Integer, String> level2Compress;
        private final Map<Integer, Optional<TableStatsExtractor>> level2Extractor = new HashMap<>();
        private final Map<Integer, DataFilePathFactory> level2PathFactory = new HashMap<>();
        private final Map<Integer, FormatWriterFactory> level2WriterFactory = new HashMap<>();

        private WriteFormatContext(
                BinaryRow partition,
                int bucket,
                RowType rowType,
                FileFormat defaultFormat,
                Map<String, FileStorePathFactory> format2PathFactory,
                CoreOptions options) {
            this.partition = partition;
            this.bucket = bucket;
            this.rowType = rowType;
            this.format2PathFactory = format2PathFactory;

            this.statsCollectorFactories =
                    StatsCollectorFactories.createStatsFactories(options, rowType.getFieldNames());

            Map<Integer, String> fileFormatPerLevel = options.fileFormatPerLevel();
            Map<Integer, FileFormat> levelFormatMap = new HashMap<>();
            this.level2Format =
                    level ->
                            levelFormatMap.computeIfAbsent(
                                    level,
                                    k -> {
                                        String format = fileFormatPerLevel.get(level);
                                        if (format == null) {
                                            return defaultFormat;
                                        }
                                        return FileFormat.getFileFormat(
                                                options.toConfiguration(), format);
                                    });

            String defaultCompress = options.fileCompression();
            Map<Integer, String> fileCompressionPerLevel = options.fileCompressionPerLevel();
            Map<Integer, String> levelCompressMap = new HashMap<>();
            this.level2Compress =
                    level ->
                            levelCompressMap.computeIfAbsent(
                                    level,
                                    k -> {
                                        String compress = fileCompressionPerLevel.get(level);
                                        return compress == null ? defaultCompress : compress;
                                    });
        }

        private TableStatsExtractor extractor(int level) {
            return level2Extractor
                    .computeIfAbsent(
                            level,
                            k ->
                                    level2Format
                                            .apply(level)
                                            .createStatsExtractor(rowType, statsCollectorFactories))
                    .orElse(null);
        }

        private DataFilePathFactory pathFactory(int level) {
            return level2PathFactory.computeIfAbsent(
                    level,
                    k ->
                            format2PathFactory
                                    .get(level2Format.apply(level).getFormatIdentifier())
                                    .createDataFilePathFactory(partition, bucket));
        }

        private FormatWriterFactory writerFactory(int level) {
            return level2WriterFactory.computeIfAbsent(
                    level, k -> level2Format.apply(level).createWriterFactory(rowType));
        }

        private String compression(int level) {
            return level2Compress.apply(level);
        }
    }
}
