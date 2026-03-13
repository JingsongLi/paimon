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

package org.apache.paimon.mergetree.compact.separated;

import org.apache.paimon.io.DataFileMeta;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.ListUtils.isNullOrEmpty;

/** File levels for compact key value separated table mode. */
@ThreadSafe
public class SeparatedFileLevels {

    private final Map<Integer, List<DataFileMeta>> levelFiles = new HashMap<>();
    private final Map<Integer, DataFileMeta> idToFileMap = new HashMap<>();
    private final Map<String, Integer> fileNameToIdMap = new HashMap<>();

    private int currentFileId = 0;

    public synchronized void addNewFile(DataFileMeta file) {
        levelFiles.computeIfAbsent(file.level(), k -> new ArrayList<>()).add(file);
        if (file.level() == 0) {
            return;
        }

        if (fileNameToIdMap.containsKey(file.fileName())) {
            return;
        }
        idToFileMap.put(currentFileId, file);
        fileNameToIdMap.put(file.fileName(), currentFileId);
        currentFileId++;
    }

    public synchronized DataFileMeta upgrade(DataFileMeta file, int newLevel) {
        levelFiles.get(file.level()).remove(file);
        DataFileMeta newFile = file.upgrade(newLevel);
        addNewFile(newFile);
        return newFile;
    }

    public synchronized DataFileMeta getFileById(int fileId) {
        return idToFileMap.get(fileId);
    }

    public synchronized int getFileIdByName(String fileName) {
        return fileNameToIdMap.get(fileName);
    }

    public synchronized List<DataFileMeta> allFiles() {
        List<DataFileMeta> result = new ArrayList<>();
        levelFiles.forEach((k, v) -> result.addAll(v));
        return result;
    }

    public synchronized boolean compactNotCompleted() {
        return !isNullOrEmpty(levelFiles.get(0));
    }

    public synchronized List<DataFileMeta> level0Files() {
        return new ArrayList<>(levelFiles.get(0));
    }
}
