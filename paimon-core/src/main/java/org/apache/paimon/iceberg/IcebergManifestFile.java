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

package org.apache.paimon.iceberg;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.format.FieldStats;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.TableStatsCollector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.IcebergManifestFileMeta.Content;
import org.apache.paimon.io.SingleFileWriter;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.ObjectsFile;
import org.apache.paimon.utils.PathFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.iceberg.IcebergConversions.toByteBuffer;
import static org.apache.paimon.iceberg.IcebergManifestFileMeta.PARTITION_SPEC_ID;
import static org.apache.paimon.iceberg.IcebergManifestFileMeta.UNASSIGNED_SEQ;

/**
 * This file includes several iceberg {@link ManifestEntry}s, representing the additional changes
 * since last snapshot.
 */
public class IcebergManifestFile extends ObjectsFile<IcebergManifestEntry> {

    private final RowType partitionType;
    private final FormatWriterFactory writerFactory;

    private IcebergManifestFile(
            FileIO fileIO,
            RowType partitionType,
            FormatReaderFactory readerFactory,
            FormatWriterFactory writerFactory,
            PathFactory pathFactory) {
        super(
                fileIO,
                new IcebergManifestEntrySerializer(partitionType),
                readerFactory,
                writerFactory,
                pathFactory,
                null);
        this.partitionType = partitionType;
        this.writerFactory = writerFactory;
    }

    public IcebergManifestFileMeta write(List<IcebergManifestEntry> entries) throws IOException {
        SingleFileWriter<IcebergManifestEntry, IcebergManifestFileMeta> writer = createWriter();
        try {
            writer.write(entries);
            writer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return writer.result();
    }

    public SingleFileWriter<IcebergManifestEntry, IcebergManifestFileMeta> createWriter() {
        return new IcebergManifestEntryWriter(
                writerFactory, pathFactory.newPath(), CoreOptions.FILE_COMPRESSION.defaultValue());
    }

    private class IcebergManifestEntryWriter
            extends SingleFileWriter<IcebergManifestEntry, IcebergManifestFileMeta> {

        private final TableStatsCollector partitionStatsCollector;

        private long addedFilesCount = 0;
        private long existingFilesCount = 0;
        private long deletedFilesCount = 0;
        private long addedRowsCount = 0;
        private long existingRowsCount = 0;
        private long deletedRowsCount = 0;
        private Long minDataSequenceNumber = null;

        IcebergManifestEntryWriter(FormatWriterFactory factory, Path path, String fileCompression) {
            super(
                    IcebergManifestFile.this.fileIO,
                    factory,
                    path,
                    serializer::toRow,
                    fileCompression);
            this.partitionStatsCollector = new TableStatsCollector(partitionType);
        }

        @Override
        public void write(IcebergManifestEntry entry) throws IOException {
            super.write(entry);

            switch (entry.status()) {
                case ADDED:
                    addedFilesCount += 1;
                    addedRowsCount += entry.file().recordCount();
                    break;
                case EXISTING:
                    existingFilesCount += 1;
                    existingRowsCount += entry.file().recordCount();
                    break;
                case DELETED:
                    deletedFilesCount += 1;
                    deletedRowsCount += entry.file().recordCount();
                    break;
            }

            if (entry.isLive()
                    && (minDataSequenceNumber == null
                            || entry.sequenceNumber() < minDataSequenceNumber)) {
                this.minDataSequenceNumber = entry.sequenceNumber();
            }

            partitionStatsCollector.collect(entry.file().partition());
        }

        @Override
        public IcebergManifestFileMeta result() throws IOException {
            FieldStats[] stats = partitionStatsCollector.extract();
            List<IcebergPartitionSummary> partitionSummaries = new ArrayList<>();
            for (int i = 0; i < stats.length; i++) {
                FieldStats fieldStats = stats[i];
                DataType type = partitionType.getTypeAt(i);
                partitionSummaries.add(
                        new IcebergPartitionSummary(
                                Objects.requireNonNull(fieldStats.nullCount()) > 0,
                                false, // TODO correct it?
                                toByteBuffer(type, fieldStats.minValue()).array(),
                                toByteBuffer(type, fieldStats.maxValue()).array()));
            }
            return new IcebergManifestFileMeta(
                    path.toString(),
                    fileIO.getFileSize(path),
                    PARTITION_SPEC_ID,
                    Content.DATA,
                    UNASSIGNED_SEQ,
                    minDataSequenceNumber != null ? minDataSequenceNumber : UNASSIGNED_SEQ,
                    null,
                    addedFilesCount,
                    existingFilesCount,
                    deletedFilesCount,
                    addedRowsCount,
                    existingRowsCount,
                    deletedRowsCount,
                    partitionSummaries);
        }
    }

    /** Creator of {@link IcebergManifestFile}. */
    public static class Factory {

        private final FileIO fileIO;
        private final RowType partitionType;
        private final FileFormat fileFormat;
        private final FileStorePathFactory pathFactory;

        public Factory(
                FileIO fileIO,
                RowType partitionType,
                FileFormat fileFormat,
                FileStorePathFactory pathFactory) {
            this.fileIO = fileIO;
            this.partitionType = partitionType;
            this.fileFormat = fileFormat;
            this.pathFactory = pathFactory;
        }

        public IcebergManifestFile create() {
            RowType entryType = IcebergManifestEntry.schema(partitionType);
            return new IcebergManifestFile(
                    fileIO,
                    partitionType,
                    fileFormat.createReaderFactory(entryType),
                    fileFormat.createWriterFactory(entryType),
                    pathFactory.manifestFileFactory());
        }
    }
}
