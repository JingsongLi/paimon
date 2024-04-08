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

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Entry of an Iceberg manifest file. */
public class IcebergManifestEntry {

    private final int status;
    private final long snapshotId;
    private final long sequenceNumber;
    private final long fileSequenceNumber;
    private final IcebergDataFile dataFile;

    public IcebergManifestEntry(
            int status,
            long snapshotId,
            long sequenceNumber,
            long fileSequenceNumber,
            IcebergDataFile dataFile) {
        this.status = status;
        this.snapshotId = snapshotId;
        this.sequenceNumber = sequenceNumber;
        this.fileSequenceNumber = fileSequenceNumber;
        this.dataFile = dataFile;
    }

    public int status() {
        return status;
    }

    public long snapshotId() {
        return snapshotId;
    }

    public long sequenceNumber() {
        return sequenceNumber;
    }

    public long fileSequenceNumber() {
        return fileSequenceNumber;
    }

    public IcebergDataFile dataFile() {
        return dataFile;
    }

    public static RowType schema(RowType partitionType) {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "status", DataTypes.INT().notNull()));
        fields.add(new DataField(1, "snapshot_id", DataTypes.BIGINT()));
        fields.add(new DataField(3, "sequence_number", DataTypes.BIGINT()));
        fields.add(new DataField(4, "file_sequence_number", DataTypes.BIGINT()));
        fields.add(new DataField(2, "data_file", IcebergDataFile.schema(partitionType).notNull()));
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
        IcebergManifestEntry that = (IcebergManifestEntry) o;
        return status == that.status
                && snapshotId == that.snapshotId
                && sequenceNumber == that.sequenceNumber
                && fileSequenceNumber == that.fileSequenceNumber
                && Objects.equals(dataFile, that.dataFile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, snapshotId, sequenceNumber, fileSequenceNumber, dataFile);
    }
}
