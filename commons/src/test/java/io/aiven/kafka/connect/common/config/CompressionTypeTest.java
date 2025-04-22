/*
 * Copyright 2025 Aiven Oy
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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class CompressionTypeTest {
    private final String testText = "Now is the time for all good people to come to the aid of their country";

    @ParameterizedTest
    @MethodSource("CompressionTypeArgs")
    void testCompressionType(CompressionType compressionType) throws IOException {
        byte[] compressed = compress(testText.getBytes(), compressionType);
        byte[] decompressed = decompress(compressed, compressionType);
        assertThat(new String(decompressed)).isEqualTo(testText);
    }

    private byte[] compress(final byte[] input, CompressionType compressionType) throws IOException {
        try (final var stream = new ByteArrayInputStream(input);
                final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                final OutputStream compressedStream = compressionType.compress(outputStream);) {
            IOUtils.copy(stream, compressedStream);
            compressedStream.close();
            return outputStream.toByteArray();
        }
    }

    private byte[] decompress(final byte[] input, CompressionType compressionType) throws IOException {
        try (final var stream = new ByteArrayInputStream(input);
                final InputStream decompressedStream = compressionType.decompress(stream);
                final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();) {
            IOUtils.copy(decompressedStream, outputStream);
            outputStream.flush();
            return outputStream.toByteArray();
        }
    }

    private static Stream<CompressionType> CompressionTypeArgs() {
        return Arrays.stream(CompressionType.values());
    }
}
