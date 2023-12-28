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

package org.apache.paimon.service;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.FileUtils.listOriginalVersionedFiles;
import static org.apache.paimon.utils.FileUtils.listVersionedFileStatus;


import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.JsonSerdeUtil;

/** Manage services. */
public class ServiceManager implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String SERVICE_LOOKUP = "SERVICE-LOOKUP";

    private final FileIO fileIO;
    private final Path tablePath;

    public ServiceManager(FileIO fileIO, Path tablePath) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
    }

    public Optional<InetSocketAddress[]> service(String serviceId) {
        try {
            return fileIO.readOverwrittenFileUtf8(servicePath(serviceId)).map(s -> JsonSerdeUtil.fromJson(s, InetSocketAddress[].class));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void resetService(String service, InetSocketAddress[] addresses) {
        try {
            fileIO.overwriteFileUtf8(servicePath(service), JsonSerdeUtil.toJson(addresses));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void deleteService(String serviceId) {
        fileIO.deleteQuietly(servicePath(serviceId));
    }

    private Path servicePath(String serviceId) {
        return new Path(tablePath + "/service/" + serviceId);
    }
}
