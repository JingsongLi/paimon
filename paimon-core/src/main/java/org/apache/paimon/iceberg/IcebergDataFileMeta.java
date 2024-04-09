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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Iceberg data file meta. */
public class IcebergDataFileMeta {

    private final int content;
    private final String filePath;
    private final String fileFormat;
    private final int specId;
    private final BinaryRow partition;
    private final long recordCount;
    private final long fileSizeInBytes;

    public IcebergDataFileMeta(
            int content,
            String filePath,
            String fileFormat,
            int specId,
            BinaryRow partition,
            long recordCount,
            long fileSizeInBytes) {
        this.content = content;
        this.filePath = filePath;
        this.fileFormat = fileFormat;
        this.specId = specId;
        this.partition = partition;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
    }

    public int content() {
        return content;
    }

    public String filePath() {
        return filePath;
    }

    public String fileFormat() {
        return fileFormat;
    }

    public int specId() {
        return specId;
    }

    public BinaryRow partition() {
        return partition;
    }

    public long recordCount() {
        return recordCount;
    }

    public long fileSizeInBytes() {
        return fileSizeInBytes;
    }

    public static RowType schema(RowType partitionType) {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(134, "content", DataTypes.STRING()));
        fields.add(new DataField(100, "file_path", DataTypes.STRING().notNull()));
        fields.add(new DataField(101, "file_format", DataTypes.STRING().notNull()));
        fields.add(new DataField(102, "partition", partitionType));
        fields.add(new DataField(103, "record_count", DataTypes.BIGINT().notNull()));
        fields.add(new DataField(104, "file_size_in_bytes", DataTypes.BIGINT().notNull()));
        return new RowType(fields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IcebergDataFileMeta that = (IcebergDataFileMeta) o;
        return content == that.content
                && specId == that.specId
                && recordCount == that.recordCount
                && fileSizeInBytes == that.fileSizeInBytes
                && Objects.equals(filePath, that.filePath)
                && Objects.equals(fileFormat, that.fileFormat)
                && Objects.equals(partition, that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                content, filePath, fileFormat, specId, partition, recordCount, fileSizeInBytes);
    }
}
