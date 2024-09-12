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

package org.apache.paimon.flink.orphan;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.utils.BoundedTwoInputOperator;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.operation.OrphanFilesClean;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.SerializableConsumer;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Flink {@link OrphanFilesClean}, it will submit a job for a table. */
public class FlinkOrphanFilesClean extends OrphanFilesClean {

    @Nullable protected final Integer parallelism;

    public FlinkOrphanFilesClean(
            FileStoreTable table,
            long olderThanMillis,
            SerializableConsumer<Path> fileCleaner,
            @Nullable Integer parallelism) {
        super(table, olderThanMillis, fileCleaner);
        this.parallelism = parallelism;
    }

    @Nullable
    public DataStream<Long> doOrphanClean(StreamExecutionEnvironment env) {
        Configuration flinkConf = new Configuration();
        flinkConf.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        flinkConf.set(ExecutionOptions.SORT_INPUTS, false);
        flinkConf.set(ExecutionOptions.USE_BATCH_STATE_BACKEND, false);
        if (parallelism != null) {
            flinkConf.set(CoreOptions.DEFAULT_PARALLELISM, parallelism);
        }
        env.configure(flinkConf);

        List<String> branches = validBranches();

        // snapshot and changelog files are the root of everything, so they are handled specially
        // here, and subsequently, we will not count their orphan files.
        AtomicLong deletedInLocal = new AtomicLong(0);
        cleanSnapshotDir(branches, p -> deletedInLocal.incrementAndGet());

        // branch and manifest file
        final OutputTag<Tuple2<String, String>> manifestOutputTag =
                new OutputTag<Tuple2<String, String>>("manifest-output") {};

        SingleOutputStreamOperator<String> usedManifestFiles =
                env.fromCollection(branches)
                        .process(
                                new ProcessFunction<String, String>() {
                                    @Override
                                    public void processElement(
                                            String branch, Context ctx, Collector<String> out)
                                            throws Exception {
                                        Consumer<ManifestFileMeta> manifestConsumer =
                                                manifest -> {
                                                    Tuple2<String, String> tuple2 =
                                                            new Tuple2<>(
                                                                    branch, manifest.fileName());
                                                    ctx.output(manifestOutputTag, tuple2);
                                                };
                                        collectWithoutDataFile(
                                                branch, out::collect, manifestConsumer);
                                    }
                                });

        DataStream<String> usedFiles =
                usedManifestFiles
                        .getSideOutput(manifestOutputTag)
                        .keyBy(tuple2 -> tuple2.f0 + ":" + tuple2.f1)
                        .reduce((t1, t2) -> t1)
                        .flatMap(
                                new FlatMapFunction<Tuple2<String, String>, String>() {

                                    private final Map<String, ManifestFile> branchManifests =
                                            new HashMap<>();

                                    @Override
                                    public void flatMap(
                                            Tuple2<String, String> value, Collector<String> out)
                                            throws Exception {
                                        ManifestFile manifestFile =
                                                branchManifests.computeIfAbsent(
                                                        value.f0,
                                                        key ->
                                                                table.switchToBranch(key)
                                                                        .store()
                                                                        .manifestFileFactory()
                                                                        .create());
                                        retryReadingFiles(
                                                        () ->
                                                                manifestFile.readWithIOException(
                                                                        value.f1),
                                                        Collections.<ManifestEntry>emptyList())
                                                .forEach(
                                                        f -> {
                                                            out.collect(f.fileName());
                                                            f.file()
                                                                    .extraFiles()
                                                                    .forEach(out::collect);
                                                        });
                                    }
                                });

        usedFiles = usedFiles.union(usedManifestFiles);

        List<String> fileDirs =
                listPaimonFileDirs().stream()
                        .map(Path::toUri)
                        .map(Object::toString)
                        .collect(Collectors.toList());
        DataStream<String> candidates =
                env.fromCollection(fileDirs)
                        .flatMap(
                                new FlatMapFunction<String, String>() {
                                    @Override
                                    public void flatMap(String dir, Collector<String> out) {
                                        for (FileStatus fileStatus :
                                                tryBestListingDirs(new Path(dir))) {
                                            if (oldEnough(fileStatus)) {
                                                out.collect(
                                                        fileStatus.getPath().toUri().toString());
                                            }
                                        }
                                    }
                                });

        DataStream<Long> deleted =
                usedFiles
                        .keyBy(f -> f)
                        .connect(candidates.keyBy(path -> new Path(path).getName()))
                        .transform(
                                "files_join",
                                LONG_TYPE_INFO,
                                new BoundedTwoInputOperator<String, String, Long>() {

                                    private boolean buildEnd;
                                    private long emitted;

                                    private final Set<String> used = new HashSet<>();

                                    @Override
                                    public InputSelection nextSelection() {
                                        return buildEnd
                                                ? InputSelection.SECOND
                                                : InputSelection.FIRST;
                                    }

                                    @Override
                                    public void endInput(int inputId) {
                                        switch (inputId) {
                                            case 1:
                                                checkState(!buildEnd, "Should not build ended.");
                                                LOG.info("Finish build phase.");
                                                buildEnd = true;
                                                break;
                                            case 2:
                                                checkState(buildEnd, "Should build ended.");
                                                LOG.info("Finish probe phase.");
                                                if (output != null) {
                                                    output.collect(new StreamRecord<>(emitted));
                                                }
                                                break;
                                        }
                                    }

                                    @Override
                                    public void processElement1(StreamRecord<String> element) {
                                        used.add(element.getValue());
                                    }

                                    @Override
                                    public void processElement2(StreamRecord<String> element) {
                                        checkState(buildEnd, "Should build ended.");
                                        String value = element.getValue();
                                        Path path = new Path(value);
                                        if (!used.contains(path.getName())) {
                                            fileCleaner.accept(path);
                                            emitted++;
                                        }
                                    }
                                });

        if (deletedInLocal.get() != 0) {
            deleted = deleted.union(env.fromData(deletedInLocal.get()));
        }
        return deleted;
    }

    public static long executeDatabaseOrphanFiles(
            StreamExecutionEnvironment env,
            Catalog catalog,
            long olderThanMillis,
            SerializableConsumer<Path> fileCleaner,
            @Nullable Integer parallelism,
            String databaseName,
            @Nullable String tableName)
            throws Catalog.DatabaseNotExistException, Catalog.TableNotExistException {
        List<DataStream<Long>> orphanFilesCleans = new ArrayList<>();
        List<String> tableNames = Collections.singletonList(tableName);
        if (tableName == null || "*".equals(tableName)) {
            tableNames = catalog.listTables(databaseName);
        }

        for (String t : tableNames) {
            Identifier identifier = new Identifier(databaseName, t);
            Table table = catalog.getTable(identifier);
            checkArgument(
                    table instanceof FileStoreTable,
                    "Only FileStoreTable supports remove-orphan-files action. The table type is '%s'.",
                    table.getClass().getName());

            DataStream<Long> clean =
                    new FlinkOrphanFilesClean(
                                    (FileStoreTable) table,
                                    olderThanMillis,
                                    fileCleaner,
                                    parallelism)
                            .doOrphanClean(env);
            if (clean != null) {
                orphanFilesCleans.add(clean);
            }
        }

        DataStream<Long> result = null;
        for (DataStream<Long> clean : orphanFilesCleans) {
            if (result == null) {
                result = clean;
            } else {
                result = result.union(clean);
            }
        }

        return sum(result);
    }

    private static long sum(DataStream<Long> deleted) {
        long deleteCount = 0;
        if (deleted != null) {
            try {
                CloseableIterator<Long> iterator =
                        deleted.global().executeAndCollect("OrphanFilesClean");
                while (iterator.hasNext()) {
                    deleteCount += iterator.next();
                }
                iterator.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return deleteCount;
    }
}
