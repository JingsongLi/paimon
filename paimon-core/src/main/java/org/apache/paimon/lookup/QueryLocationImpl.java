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

package org.apache.paimon.lookup;

import java.net.InetSocketAddress;
import java.util.Optional;


import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.service.ServiceManager;


import static org.apache.paimon.service.ServiceManager.SERVICE_LOOKUP;

/** */
public class QueryLocationImpl implements QueryLocation {

    private final Path tablePath;
    private final ServiceManager serviceManager;

    private InetSocketAddress[] addressesCache;

    public QueryLocationImpl(FileIO fileIO, Path tablePath) {
        this.tablePath = tablePath;
        this.serviceManager = new ServiceManager(fileIO, tablePath);
    }

    @Override
    public InetSocketAddress getLocation(BinaryRow partition, int bucket, boolean forceUpdate) {
        if (addressesCache == null || forceUpdate) {
            Optional<InetSocketAddress[]> addresses = serviceManager.service(SERVICE_LOOKUP);
            if (!addresses.isPresent()) {
                throw new RuntimeException("Cannot find address for table path: " + tablePath);
            }
            addressesCache = addresses.get();
        }

        return addressesCache[select(partition, bucket, addressesCache.length)];
    }

    private static int select(BinaryRow partition, int bucket, int numChannels) {
        int startChannel = Math.abs(partition.hashCode()) % numChannels;
        return (startChannel + bucket) % numChannels;
    }
}
