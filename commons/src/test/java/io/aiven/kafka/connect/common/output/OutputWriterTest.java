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

package io.aiven.kafka.connect.common.output;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class OutputWriterTest {

    @ParameterizedTest
    @MethodSource("compressionTypes")
    void checkRoundTrip(CompressionType compression) throws IOException {
        List<OutputField> lst = Arrays.asList(new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputWriter.Builder builder = OutputWriter.builder()
                .withCompressionType(compression)
                // .withExternalProperties(config.originalsStrings())
                .withOutputFields(lst)
                .withEnvelopeEnabled(false);
        try (OutputWriter writer = builder.build(out, FormatType.CSV)) {
            SinkRecord record = new SinkRecord("topic", 1, Schema.STRING_SCHEMA, "key", Schema.BYTES_SCHEMA,
                    "value".getBytes(StandardCharsets.UTF_8), 0L);
            writer.writeRecord(record);
        }
        byte[] resultBytes = decompress(out.toByteArray(), compression);
        String[] s = new String(resultBytes).split(",");
        s[0] = b64Decode(s[0]);
        assertThat(s[0]).isEqualTo("key");
        assertThat(s[1]).isEqualTo("value");
    }

    public static List<Arguments> compressionTypes() {
        List<Arguments> compressionTypes = new ArrayList<>();
        for (CompressionType compressionType : CompressionType.values()) {
            compressionTypes.add(Arguments.of(compressionType));
        }
        return compressionTypes;
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

    private String b64Decode(final String value) {
        Objects.requireNonNull(value, "value cannot be null");

        return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
    }
}
