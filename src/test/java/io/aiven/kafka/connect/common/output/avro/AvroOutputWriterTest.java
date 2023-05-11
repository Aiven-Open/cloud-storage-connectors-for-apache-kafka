/*
 * Copyright 2023 Aiven Oy
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

package io.aiven.kafka.connect.common.output.avro;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

public class AvroOutputWriterTest {

    @TempDir
    private Path tmpDir;

    private static final long TEST_OFFSET = 100L;
    private static final String TEST_KEY_PREFIX = "some-key-";

    private final String getTempFileName() {
        return UUID.randomUUID().toString() + ".avro";
    }

    @ParameterizedTest
    @MethodSource("io.aiven.kafka.connect.common.output.avro.AvroCodecParameters#avroCodecTestParameters")
    void testWriteAllFields(final String avroCodec) throws IOException {
        final Map<String, String> externalConfiguration = Map.of("avro.codec", avroCodec);
        final Path avroFile = tmpDir.resolve(getTempFileName());
        final List<String> values = List.of("a", "b", "c", "d");
        writeRecords(
            avroFile,
            List.of(
                new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.HEADERS, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE)
            ),
            SchemaBuilder.STRING_SCHEMA,
            values,
            externalConfiguration,
            true,
            true
        );
        int counter = 0;
        final int timestamp = 1000;
        final Map<Utf8, ByteBuffer> expectedHeaders = new HashMap<>();
        expectedHeaders.put(new Utf8("a"), ByteBuffer.wrap("b".getBytes(StandardCharsets.UTF_8)));
        expectedHeaders.put(new Utf8("c"), ByteBuffer.wrap("d".getBytes(StandardCharsets.UTF_8)));
        final org.apache.avro.Schema headersMapSchema = org.apache.avro.SchemaBuilder.builder()
            .map().values(org.apache.avro.SchemaBuilder.builder().bytesType());
        final org.apache.avro.Schema expectedAvroSchema = org.apache.avro.SchemaBuilder.builder()
            .record("connector_records").namespace("io.aiven.avro.output.schema").fields()
            .requiredString("key").requiredLong("offset").requiredLong("timestamp")
            .name("headers").type(headersMapSchema).noDefault().requiredString("value").endRecord();
        for (final GenericRecord r : readRecords(avroFile, avroCodec)) {
            assertThat(r.getSchema()).isEqualTo(expectedAvroSchema);
            final GenericData.Record expectedRecord = new GenericData.Record(expectedAvroSchema);
            expectedRecord.put("key", new Utf8(TEST_KEY_PREFIX + counter));
            expectedRecord.put("offset", TEST_OFFSET);
            expectedRecord.put("timestamp", (long) (timestamp + counter));
            expectedRecord.put("headers", expectedHeaders);
            expectedRecord.put("value", new Utf8(values.get(counter)));
            assertThat(r).isEqualTo(expectedRecord);
            counter++;
        }
    }

    @ParameterizedTest
    @MethodSource("io.aiven.kafka.connect.common.output.avro.AvroCodecParameters#avroCodecTestParameters")
    void testWriteValueStruct(final String avroCodec) throws IOException {
        final Map<String, String> externalConfiguration = Map.of("avro.codec", avroCodec);
        final Path avroFile = tmpDir.resolve(getTempFileName());
        final Schema recordSchema =
            SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .build();

        final List<Struct> values =
            List.of(
                new Struct(recordSchema)
                    .put("name", "name-0").put("age", 0),
                new Struct(recordSchema)
                    .put("name", "name-1").put("age", 1),
                new Struct(recordSchema)
                    .put("name", "name-2").put("age", 2),
                new Struct(recordSchema)
                    .put("name", "name-3").put("age", 3)
            );
        writeRecords(
            avroFile,
            List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE)),
            recordSchema,
            values,
            externalConfiguration,
            false,
            true
        );
        int counter = 0;
        final org.apache.avro.Schema valueRecordSchema = org.apache.avro.SchemaBuilder.builder()
            .record("ConnectDefault").namespace("io.confluent.connect.avro").fields()
            .requiredString("name").requiredInt("age").endRecord();
        final org.apache.avro.Schema expectedAvroSchema = org.apache.avro.SchemaBuilder.builder()
            .record("connector_records").namespace("io.aiven.avro.output.schema").fields()
            .name("value").type(valueRecordSchema).noDefault().endRecord();
        for (final GenericRecord r : readRecords(avroFile, avroCodec)) {
            assertThat(r.getSchema()).isEqualTo(expectedAvroSchema);
            final GenericData.Record expectedRecord = new GenericData.Record(r.getSchema());
            final GenericData.Record valueRecord = new GenericData.Record(r.getSchema().getField("value").schema());
            valueRecord.put("name", "name-" + counter);
            valueRecord.put("age", counter);
            expectedRecord.put("value", valueRecord);
            assertThat(r).isEqualTo(expectedRecord);
            counter++;
        }
    }

    @ParameterizedTest
    @MethodSource("io.aiven.kafka.connect.common.output.avro.AvroCodecParameters#avroCodecTestParameters")
    void testWritePlainAvroValue(final String avroCodec) throws IOException {
        final Map<String, String> externalConfiguration = Map.of("avro.codec", avroCodec);
        final Path avroFile = tmpDir.resolve(getTempFileName());
        final Schema recordSchema =
            SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .build();

        final List<Struct> values =
            List.of(
                new Struct(recordSchema)
                    .put("name", "name-0").put("age", 0),
                new Struct(recordSchema)
                    .put("name", "name-1").put("age", 1),
                new Struct(recordSchema)
                    .put("name", "name-2").put("age", 2),
                new Struct(recordSchema)
                    .put("name", "name-3").put("age", 3)
            );
        writeRecords(
            avroFile,
            List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE)),
            recordSchema,
            values,
            externalConfiguration,
            false,
            false
        );
        int counter = 0;
        final org.apache.avro.Schema expectedAvroSchema = org.apache.avro.SchemaBuilder.builder()
            .record("ConnectDefault").namespace("io.confluent.connect.avro").fields()
            .requiredString("name").requiredInt("age").endRecord();
        for (final GenericRecord r : readRecords(avroFile, avroCodec)) {
            assertThat(r.getSchema()).isEqualTo(expectedAvroSchema);
            final GenericData.Record expectedRecord = new GenericData.Record(r.getSchema());
            expectedRecord.put("name", "name-" + counter);
            expectedRecord.put("age", counter);
            assertThat(r).isEqualTo(expectedRecord);
            counter++;
        }
    }

    private <T> void writeRecords(final Path avroFile,
                                  final Collection<OutputField> fields,
                                  final Schema recordSchema,
                                  final List<T> records,
                                  final Map<String, String> externalConfiguration,
                                  final boolean withHeaders,
                                  final boolean withEnvelope) throws IOException {
        final OutputStream out = Files.newOutputStream(avroFile);
        final Headers headers = new ConnectHeaders();
        headers.add("a", "b".getBytes(StandardCharsets.UTF_8), Schema.BYTES_SCHEMA);
        headers.add("c", "d".getBytes(StandardCharsets.UTF_8), Schema.BYTES_SCHEMA);
        try (final OutputStream o = out;
             final AvroOutputWriter avroWriter =
                 new AvroOutputWriter(fields, o, externalConfiguration, withEnvelope)) {
            int counter = 0;
            final List<SinkRecord> sinkRecords = new ArrayList<>();
            for (final T r : records) {
                final SinkRecord sinkRecord =
                    new SinkRecord(
                        "some-topic", 1,
                        Schema.STRING_SCHEMA, TEST_KEY_PREFIX + counter,
                        recordSchema, r,
                        TEST_OFFSET, 1000L + counter,
                        TimestampType.CREATE_TIME,
                        withHeaders ? headers : null);
                sinkRecords.add(sinkRecord);
                counter++;
            }
            avroWriter.writeRecords(sinkRecords);
        }
    }

    private List<GenericRecord> readRecords(final Path avroFile, final String expectedAvroCodec) throws IOException {
        final File inputFile = avroFile.toFile();
        final List<GenericRecord> records = new ArrayList<>();
        final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileReader<GenericRecord> reader = new DataFileReader<>(inputFile, datumReader)) {
            assertThat(reader.getMetaString("avro.codec")).isEqualTo(expectedAvroCodec);
            reader.forEach(records::add);
        }
        assertThat(records).isNotEmpty();
        return records;
    }

}
