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
import org.apache.commons.io.function.IOSupplier;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

public enum CompressionType {
    /** No compression */
    NONE("none", "", in -> in, out -> out),
    /** GZIP compression */
    GZIP("gzip", ".gz", GZIPInputStream::new, out -> new GZIPOutputStream(out, true)),
    /** Snappy compression */
    SNAPPY("snappy", ".snappy", SnappyInputStream::new, SnappyOutputStream::new),
    /** Zstandard compression */
    ZSTD("zstd", ".zst", ZstdInputStream::new, ZstdOutputStream::new);

    /**
     * A list of supported compression types for display.
     */
    public static final String SUPPORTED_COMPRESSION_TYPES = CompressionType.names()
            .stream()
            .map(c -> String.format("'%s'", c))
            .collect(Collectors.joining(", "));

    /**
     * The common name of the compression.
     */
    public final String name;
    /**
     * The file extension associated with the compression.
     */
    private final String extensionStr;
    /**
     * A function that will return an input stream that decompresses the data in a provided input stream.
     */
    private final IOFunction<InputStream, InputStream> decompressor;
    /**
     * A function that will return an output stream that compresses the data in a provided output stream.
     */
    private final IOFunction<OutputStream, OutputStream> compressor;

    /**
     * Coinstructor
     *
     * @param name
     *            The common name of the compression
     * @param extensionStr
     *            The file extension associated with the compression.
     * @param decompressor
     *            A function that will return an input stream that decompresses the data in a provided input stream.
     * @param compressor
     *            A function that will return an output stream that compresses the data in a provided output stream.
     */
    CompressionType(final String name, final String extensionStr,
            final IOFunction<InputStream, InputStream> decompressor,
            final IOFunction<OutputStream, OutputStream> compressor) {
        this.name = name;
        this.extensionStr = extensionStr;
        this.decompressor = decompressor;
        this.compressor = compressor;
    }

    /**
     * Gets the compression type for the specified name.
     *
     * @param name
     *            the name to lookup
     * @throws IllegalArgumentException
     *             if the name is unknown.
     * @return the Compression type.
     */
    public static CompressionType forName(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        try {
            return CompressionType.valueOf(name.toUpperCase(Locale.ROOT));
        } catch (final IllegalArgumentException ignored) {
            throw new IllegalArgumentException("Unknown compression type: " + name);
        }
    }

    /**
     * The list of all names.
     *
     * @return the list of names.
     */
    public static Collection<String> names() {
        return Arrays.stream(values()).map(v -> v.name).collect(Collectors.toList());
    }

    /**
     * Gets the file name extension associated with the compression.
     *
     * @return the file name extension.
     */
    public final String extension() {
        return extensionStr;
    }

    /**
     * Decompresses an input stream.
     *
     * @param input
     *            the input stream to read compressed data from.
     * @return An input stream that returns decompressed data.
     * @throws IOException
     *             on error.
     */
    public final InputStream decompress(final InputStream input) throws IOException {
        return decompressor.apply(input);
    }

    /**
     * Decompresses an input stream wrapped in an IOSupplier
     *
     * @param input
     *            the input stream to read compressed data from.
     * @return An input stream that returns decompressed data.
     */
    public final IOSupplier<InputStream> decompress(final IOSupplier<InputStream> input) {
        return new IOSupplier<InputStream>() {
            @Override
            public InputStream get() throws IOException {
                return decompress(input.get());
            }
        };
    }

    /**
     * Compresses an output stream.
     *
     * @param output
     *            the output stream to write compressed data to.
     * @return An output stream that writes compressed data.
     * @throws IOException
     *             on error.
     */
    public final OutputStream compress(final OutputStream output) throws IOException {
        return compressor.apply(output);
    }
}
