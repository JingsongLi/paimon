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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ObjectSerializer;

/** Serializer for {@link IcebergDataFile}. */
public class IcebergDataFileSerializer extends ObjectSerializer<IcebergDataFile> {

    private static final long serialVersionUID = 1L;

    private final InternalRowSerializer partSerializer;

    public IcebergDataFileSerializer(RowType partitionType) {
        super(IcebergDataFile.schema(partitionType));
        this.partSerializer = new InternalRowSerializer(partitionType);
    }

    @Override
    public InternalRow toRow(IcebergDataFile file) {
        return GenericRow.of(
                file.content(),
                BinaryString.fromString(file.filePath()),
                BinaryString.fromString(file.fileFormat()),
                file.specId(),
                file.partition(),
                file.recordCount(),
                file.fileSizeInBytes());
    }

    @Override
    public IcebergDataFile fromRow(InternalRow row) {
        return new IcebergDataFile(
                row.getInt(0),
                row.getString(1).toString(),
                row.getString(2).toString(),
                row.getInt(3),
                partSerializer.toBinaryRow(row.getRow(4, partSerializer.getArity())).copy(),
                row.getLong(5),
                row.getLong(6));
    }
}
