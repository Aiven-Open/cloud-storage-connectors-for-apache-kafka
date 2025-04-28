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
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class CompressionTypeTest {

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void testCompressionType(final CompressionType compressionType) throws IOException {
        final String testText = "Now is the time for all good people to come to the aid of their country";
        final byte[] compressed = compress(testText.getBytes(StandardCharsets.UTF_8), compressionType);
        final byte[] decompressed = decompress(compressed, compressionType);
        assertThat(new String(decompressed, StandardCharsets.UTF_8)).isEqualTo(testText);
    }

    private byte[] compress(final byte[] input, final CompressionType compressionType) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (var stream = new ByteArrayInputStream(input);
                OutputStream compressedStream = compressionType.compress(outputStream);) {
            IOUtils.copy(stream, compressedStream);
        }
        return outputStream.toByteArray();
    }

    private byte[] decompress(final byte[] input, final CompressionType compressionType) throws IOException {
        try (var stream = new ByteArrayInputStream(input);
                InputStream decompressedStream = compressionType.decompress(stream);
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();) {
            IOUtils.copy(decompressedStream, outputStream);
            return outputStream.toByteArray();
        }
    }
}
