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

import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.ObjectsFile;
import org.apache.paimon.utils.PathFactory;

import java.io.IOException;
import java.util.List;

/**
 * This file includes several iceberg {@link ManifestFileMeta}, representing all data of the whole
 * table at the corresponding snapshot.
 */
public class IcebergManifestList extends ObjectsFile<IcebergManifestFileMeta> {

    private IcebergManifestList(
            FileIO fileIO,
            FormatReaderFactory readerFactory,
            FormatWriterFactory writerFactory,
            PathFactory pathFactory) {
        super(
                fileIO,
                new IcebergManifestFileMetaSerializer(),
                readerFactory,
                writerFactory,
                pathFactory,
                null);
    }

    public String write(List<IcebergManifestFileMeta> metas) throws IOException {
        return super.writeWithoutRolling(metas);
    }

    /** Creator of {@link IcebergManifestList}. */
    public static class Factory {

        private final FileIO fileIO;
        private final FileFormat fileFormat;
        private final FileStorePathFactory pathFactory;

        public Factory(FileIO fileIO, FileFormat fileFormat, FileStorePathFactory pathFactory) {
            this.fileIO = fileIO;
            this.fileFormat = fileFormat;
            this.pathFactory = pathFactory;
        }

        public IcebergManifestList create() {
            RowType type = IcebergManifestFileMeta.schema();
            return new IcebergManifestList(
                    fileIO,
                    fileFormat.createReaderFactory(type),
                    fileFormat.createWriterFactory(type),
                    pathFactory.manifestFileFactory());
        }
    }
}
