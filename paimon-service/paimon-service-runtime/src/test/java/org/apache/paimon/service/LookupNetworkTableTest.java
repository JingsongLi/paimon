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

import org.apache.paimon.catalog.PrimaryKeyTableTestBase;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.query.QueryLocationImpl;
import org.apache.paimon.query.TableQuery;
import org.apache.paimon.service.client.KvQueryClient;
import org.apache.paimon.service.network.stats.DisabledServiceRequestStats;
import org.apache.paimon.service.server.KvQueryServer;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessageImpl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.apache.paimon.service.ServiceManager.SERVICE_PRIMARY_KEY_LOOKUP;
import static org.apache.paimon.table.sink.ChannelComputer.select;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for remote lookup. */
public class LookupNetworkTableTest extends PrimaryKeyTableTestBase {

    private TableQuery query0;
    private TableQuery query1;

    private KvQueryServer server0;
    private KvQueryServer server1;

    private KvQueryClient client;

    @BeforeEach
    public void beforeEach() throws Throwable {
        this.query0 = table.newQuery();
        this.query0.withIOManager(IOManager.create(tempPath.toString()));

        this.query1 = table.newQuery();
        this.query1.withIOManager(IOManager.create(tempPath.toString()));

        this.server0 = createServer(query0);
        this.server1 = createServer(query1);

        ServiceManager serviceManager = table.store().newServiceManager();
        serviceManager.resetService(
                SERVICE_PRIMARY_KEY_LOOKUP,
                new InetSocketAddress[] {server0.getServerAddress(), server1.getServerAddress()});

        this.client = new KvQueryClient(new QueryLocationImpl(serviceManager), 1);
    }

    private KvQueryServer createServer(TableQuery query) throws Throwable {
        KvQueryServer server =
                new KvQueryServer(
                        InetAddress.getLocalHost().getHostName(),
                        Arrays.asList(7777, 7900).iterator(),
                        1,
                        1,
                        query,
                        new DisabledServiceRequestStats());
        server.start();
        return server;
    }

    @AfterEach
    public void afterEach() {
        if (server0 != null) {
            server0.shutdown();
        }
        if (server1 != null) {
            server1.shutdown();
        }
        if (client != null) {
            client.shutdown();
        }
    }

    @Test
    public void test() throws Exception {
        // test not exists
        BinaryRow[] result = client.getValues(row(1), 0, new BinaryRow[] {row(1)}).get();
        assertThat(result).containsOnly((BinaryRow) null);

        // test 1 row
        write(1, 1, 1);
        result = client.getValues(row(1), 0, new BinaryRow[] {row(1)}).get();
        assertThat(result).containsOnly(row(1, 1, 1));

        // test many partitions
        for (int i = 2; i < 10; i++) {
            write(i, 1, 2);
            result = client.getValues(row(i), 0, new BinaryRow[] {row(1)}).get();
            assertThat(result).containsOnly(row(i, 1, 2));
        }

        // test 2 rows
        write(1, 2, 1);
        result = client.getValues(row(1), 0, new BinaryRow[] {row(1), row(2)}).get();
        assertThat(result).containsOnly(row(1, 1, 1), row(1, 2, 1));
    }

    private void write(int partition, int key, int value) throws Exception {
        try (BatchTableWrite write = table.newBatchWriteBuilder().newWrite()) {
            write.write(GenericRow.of(partition, key, value));
            @SuppressWarnings({"unchecked", "rawtypes"})
            List<CommitMessageImpl> commitMessages = (List) write.prepareCommit();
            commitMessages.forEach(
                    m -> {
                        TableQuery query =
                                select(m.partition(), m.bucket(), 2) == 0 ? query0 : query1;
                        query.refreshFiles(
                                m.partition(),
                                m.bucket(),
                                Collections.emptyList(),
                                m.newFilesIncrement().newFiles());
                    });
        }
    }
}
