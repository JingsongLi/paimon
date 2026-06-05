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

package org.apache.paimon.index.ivfpq;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/** Loads the native paimon_vindex_jni library from the JAR or system path. */
class NativeLoader {

    private static volatile boolean loaded = false;

    static void load() {
        if (loaded) {
            return;
        }
        synchronized (NativeLoader.class) {
            if (loaded) {
                return;
            }
            try {
                loadFromResource();
            } catch (UnsatisfiedLinkError | IOException e) {
                try {
                    System.loadLibrary("paimon_vindex_jni");
                } catch (UnsatisfiedLinkError e2) {
                    throw new RuntimeException(
                            "Failed to load paimon_vindex_jni native library. "
                                    + "Ensure it is on java.library.path or bundled in the JAR.",
                            e2);
                }
            }
            loaded = true;
        }
    }

    private static void loadFromResource() throws IOException {
        String os = System.getProperty("os.name", "").toLowerCase();
        String arch = System.getProperty("os.arch", "").toLowerCase();

        String osDir;
        String libName;
        if (os.contains("mac") || os.contains("darwin")) {
            osDir = "darwin-" + normalizeArch(arch);
            libName = "libpaimon_vindex_jni.dylib";
        } else if (os.contains("linux")) {
            osDir = "linux-" + normalizeArch(arch);
            libName = "libpaimon_vindex_jni.so";
        } else {
            throw new UnsatisfiedLinkError("Unsupported OS: " + os);
        }

        String resourcePath = "/native/" + osDir + "/" + libName;
        InputStream in = NativeLoader.class.getResourceAsStream(resourcePath);
        if (in == null) {
            throw new UnsatisfiedLinkError("Native library not found in JAR: " + resourcePath);
        }

        File tempFile =
                File.createTempFile(
                        "paimon_vindex_jni", libName.substring(libName.lastIndexOf('.')));
        tempFile.deleteOnExit();
        Files.copy(in, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        in.close();

        System.load(tempFile.getAbsolutePath());
    }

    private static String normalizeArch(String arch) {
        if (arch.contains("aarch64") || arch.contains("arm64")) {
            return "aarch64";
        } else if (arch.contains("amd64") || arch.contains("x86_64")) {
            return "amd64";
        }
        return arch;
    }
}
