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
import org.junit.jupiter.params.provider.EnumSource;

class OutputWriterTest {

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void checkRoundTrip(final CompressionType compression) throws IOException {
        final List<OutputField> lst = Arrays.asList(new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final OutputWriter.Builder builder = OutputWriter.builder()
                .withCompressionType(compression)
                // .withExternalProperties(config.originalsStrings())
                .withOutputFields(lst)
                .withEnvelopeEnabled(false);
        try (OutputWriter writer = builder.build(out, FormatType.CSV)) {
            final SinkRecord record = new SinkRecord("topic", 1, Schema.STRING_SCHEMA, "key", Schema.BYTES_SCHEMA,
                    "value".getBytes(StandardCharsets.UTF_8), 0L);
            writer.writeRecord(record);
        }
        final byte[] resultBytes = decompress(out.toByteArray(), compression);
        String[] resultArray = new String(resultBytes, StandardCharsets.UTF_8).split(",");
        resultArray[0] = b64Decode(resultArray[0]);
        assertThat(resultArray[0]).isEqualTo("key");
        assertThat(resultArray[1]).isEqualTo("value");
    }

    private byte[] decompress(final byte[] input, final CompressionType compressionType) throws IOException {
        try (var stream = new ByteArrayInputStream(input);
                InputStream decompressedStream = compressionType.decompress(stream);
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();) {
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
