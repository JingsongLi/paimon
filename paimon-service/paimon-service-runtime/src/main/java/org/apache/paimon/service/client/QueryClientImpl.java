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

package org.apache.paimon.service.client;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.lookup.QueryClient;
import org.apache.paimon.lookup.QueryLocation;
import org.apache.paimon.service.messages.KvRequest;
import org.apache.paimon.service.messages.KvResponse;
import org.apache.paimon.service.network.NetworkClient;
import org.apache.paimon.service.network.messages.MessageSerializer;
import org.apache.paimon.service.network.stats.DisabledServiceRequestStats;
import org.apache.paimon.utils.ExecutorUtils;
import org.apache.paimon.utils.FutureUtils;

import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/** */
public class QueryClientImpl implements QueryClient {

    private static final Logger LOG = LoggerFactory.getLogger(QueryClientImpl.class);

    private final NetworkClient<KvRequest, KvResponse> networkClient;
    private final QueryLocation queryLocation;

    public QueryClientImpl(
            QueryLocation queryLocation, int numEventLoopThreads) {
        this.queryLocation = queryLocation;
        final MessageSerializer<KvRequest, KvResponse> messageSerializer =
                new MessageSerializer<>(
                        new KvRequest.KvRequestDeserializer(),
                        new KvResponse.KvResponseDeserializer());

        this.networkClient =
                new NetworkClient<>(
                        "Paimon Query Nettey Client",
                        numEventLoopThreads,
                        messageSerializer,
                        new DisabledServiceRequestStats());
    }

    @Override
    public CompletableFuture<BinaryRow[]> getValues(
            BinaryRow partition, int bucket, BinaryRow[] keys) {
        CompletableFuture<BinaryRow[]> response = new CompletableFuture<>();
        executeActionAsync(response, new KvRequest(partition, bucket, keys), false);
        return response;
    }

    private void executeActionAsync(
            final CompletableFuture<BinaryRow[]> result,
            final KvRequest request,
            final boolean update) {
        if (!result.isDone()) {
            final CompletableFuture<KvResponse> operationFuture = getResponse(request, update);
            operationFuture.whenCompleteAsync(
                    (t, throwable) -> {
                        if (throwable != null) {
                            if (throwable.getCause() instanceof ConnectException) {

                                // These failures are likely to be caused by out-of-sync location.
                                // Therefore, we retry this query and force lookup the location.

                                LOG.debug(
                                        "Retrying after failing to retrieve state due to: {}.",
                                        throwable.getCause().getMessage());
                                executeActionAsync(result, request, true);
                            } else {
                                result.completeExceptionally(throwable);
                            }
                        } else {
                            result.complete(t.values());
                        }
                    });

            result.whenComplete((t, throwable) -> operationFuture.cancel(false));
        }
    }

    private CompletableFuture<KvResponse> getResponse(
            final KvRequest request, final boolean forceUpdate) {
        InetSocketAddress serverAddress =
                queryLocation.getLocation(request.partition(), request.bucket(), forceUpdate);
        if (serverAddress == null) {
            return FutureUtils.completedExceptionally(
                    new RuntimeException("Cannot find address for bucket: " + request.bucket()));
        }
        return networkClient.sendRequest(serverAddress, request);
    }

    public CompletableFuture<Void> shutdown() {
        return networkClient.shutdown();
    }
}
