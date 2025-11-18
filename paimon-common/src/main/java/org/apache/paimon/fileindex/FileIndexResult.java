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

package org.apache.paimon.fileindex;

import org.apache.paimon.index.IndexResult;

/** File index result to decide whether filter a file. */
public interface FileIndexResult extends IndexResult {

    FileIndexResult REMAIN =
            new FileIndexResult() {
                @Override
                public boolean remain() {
                    return true;
                }

                @Override
                public FileIndexResult and(IndexResult fileIndexResult) {
                    return (FileIndexResult) fileIndexResult;
                }

                @Override
                public FileIndexResult or(IndexResult fileIndexResult) {
                    return this;
                }
            };

    FileIndexResult SKIP =
            new FileIndexResult() {
                @Override
                public boolean remain() {
                    return false;
                }

                @Override
                public FileIndexResult and(IndexResult fileIndexResult) {
                    return this;
                }

                @Override
                public FileIndexResult or(IndexResult fileIndexResult) {
                    return (FileIndexResult) fileIndexResult;
                }
            };

    boolean remain();

    @Override
    default FileIndexResult and(IndexResult result) {
        FileIndexResult fileIndexResult = (FileIndexResult) result;
        if (fileIndexResult.remain()) {
            return this;
        } else {
            return SKIP;
        }
    }

    @Override
    default FileIndexResult or(IndexResult result) {
        FileIndexResult fileIndexResult = (FileIndexResult) result;
        if (fileIndexResult.remain()) {
            return REMAIN;
        } else {
            return this;
        }
    }
}
