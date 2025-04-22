/*
 * Copyright 2020 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.common.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import org.apache.commons.io.function.IOFunction;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

public enum CompressionType {
    NONE("none", "", in -> in, out -> out), GZIP("gzip", ".gz", GZIPInputStream::new,
            out -> new GZIPOutputStream(out, true)), SNAPPY("snappy", ".snappy", SnappyInputStream::new,
                    SnappyOutputStream::new), ZSTD("zstd", ".zst", ZstdInputStream::new, ZstdOutputStream::new);

    public static final String SUPPORTED_COMPRESSION_TYPES = CompressionType.names()
            .stream()
            .map(c -> String.format("'%s'", c))
            .collect(Collectors.joining(", "));

    public final String name;
    private final String extension;
    private final IOFunction<InputStream, InputStream> inputCreator;
    private final IOFunction<OutputStream, OutputStream> outputCreator;

    CompressionType(final String name, final String extension, final IOFunction<InputStream, InputStream> inputCreator,
            final IOFunction<OutputStream, OutputStream> outputCreator) {
        this.name = name;
        this.extension = extension;
        this.inputCreator = inputCreator;
        this.outputCreator = outputCreator;
    }

    public static CompressionType forName(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        for (final CompressionType ct : CompressionType.values()) {
            if (ct.name.equalsIgnoreCase(name.toLowerCase(Locale.ROOT))) {
                return ct;
            }
        }
        throw new IllegalArgumentException("Unknown compression type: " + name);
    }

    public static Collection<String> names() {
        return Arrays.stream(values()).map(v -> v.name).collect(Collectors.toList());
    }

    public final String extension() {
        return extension;
    }

    public final InputStream decompress(InputStream in) throws IOException {
        return inputCreator.apply(in);
    }

    public final OutputStream compress(OutputStream out) throws IOException {
        return outputCreator.apply(out);
    }

}
