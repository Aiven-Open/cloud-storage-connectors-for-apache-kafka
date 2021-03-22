/*
 * Copyright 2021 Aiven Oy
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

package io.aiven.kafka.connect.common.output.parquet;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

import io.confluent.connect.avro.AvroData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SinkRecordConverterTest {

    static final int CACHE_SIZE = 0;

    private final AvroData avroData = new AvroData(CACHE_SIZE);

    @Test
    void testConvertRecordWithOneFieldSimpleType() {
        final var fields =
                List.of(new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, avroData);
        final var converter = new SinkRecordConverter(fields, avroData);

        final var sinkRecord =
                new SinkRecord(
                        "some-topic", 1,
                        Schema.STRING_SCHEMA, "some-key",
                        Schema.STRING_SCHEMA, "some-value",
                        100L, 1000L, TimestampType.CREATE_TIME);

        final var avroRecord = converter.convert(sinkRecord, schemaBuilder.buildSchema(sinkRecord));
        assertNotNull(avroRecord.get(OutputFieldType.OFFSET.name));
        assertNull(avroRecord.get(OutputFieldType.KEY.name));
        assertNull(avroRecord.get(OutputFieldType.TIMESTAMP.name));
        assertNull(avroRecord.get(OutputFieldType.HEADERS.name));
        assertNull(avroRecord.get(OutputFieldType.VALUE.name));

        assertEquals(100L, (Long) avroRecord.get(OutputFieldType.OFFSET.name));
    }

    @Test
    void testConvertRecordValueSimpleType() {
        final var fields =
                List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, avroData);
        final var converter = new SinkRecordConverter(fields, avroData);

        final var sinkRecord =
                new SinkRecord(
                        "some-topic", 1,
                        Schema.STRING_SCHEMA, "some-key",
                        Schema.STRING_SCHEMA, "some-value",
                        100L, 1000L, TimestampType.CREATE_TIME);

        final var avroRecord = converter.convert(sinkRecord, schemaBuilder.buildSchema(sinkRecord));
        assertNull(avroRecord.get(OutputFieldType.OFFSET.name));
        assertNull(avroRecord.get(OutputFieldType.KEY.name));
        assertNull(avroRecord.get(OutputFieldType.TIMESTAMP.name));
        assertNull(avroRecord.get(OutputFieldType.HEADERS.name));
        assertNotNull(avroRecord.get(OutputFieldType.VALUE.name));

        assertEquals("some-value", avroRecord.get(OutputFieldType.VALUE.name));
    }

    @Test
    void testConvertRecordValueStructType() {
        final var fields =
                List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, avroData);
        final var converter = new SinkRecordConverter(fields, avroData);

        final var recordSchema =
                SchemaBuilder.struct()
                        .field("foo", Schema.STRING_SCHEMA)
                        .field("bar", SchemaBuilder.STRING_SCHEMA)
                        .build();

        final var sinkRecord =
                new SinkRecord(
                        "some-topic", 1,
                        Schema.STRING_SCHEMA, "some-key",
                        recordSchema,
                        new Struct(recordSchema)
                                .put("foo", "bar")
                                .put("bar", "foo"),
                        100L, 1000L, TimestampType.CREATE_TIME);

        final var avroRecord = converter.convert(sinkRecord, schemaBuilder.buildSchema(sinkRecord));
        assertNull(avroRecord.get(OutputFieldType.OFFSET.name));
        assertNull(avroRecord.get(OutputFieldType.KEY.name));
        assertNull(avroRecord.get(OutputFieldType.TIMESTAMP.name));
        assertNull(avroRecord.get(OutputFieldType.HEADERS.name));
        assertNotNull(avroRecord.get(OutputFieldType.VALUE.name));

        final var valueRecord = (GenericRecord) avroRecord.get("value");
        assertEquals("bar", valueRecord.get("foo").toString());
        assertEquals("foo", valueRecord.get("bar").toString());
    }

    @Test
    void testConvertRecordValueStruct() {
        final var fields =
                List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, avroData);
        final var converter = new SinkRecordConverter(fields, avroData);

        final var recordSchema = SchemaBuilder.struct()
                .field("user_name", Schema.STRING_SCHEMA)
                .field("user_ip", Schema.STRING_SCHEMA)
                .field("blocked", Schema.BOOLEAN_SCHEMA)
                .build();

        final var sinkRecord =
                new SinkRecord(
                        "some-topic", 1,
                        Schema.STRING_SCHEMA, "some-key",
                        recordSchema, new Struct(recordSchema)
                            .put("user_name", "John Doe")
                            .put("user_ip", "127.0.0.1")
                            .put("blocked", true),
                        100L, 1000L, TimestampType.CREATE_TIME);

        final var avroRecord = converter.convert(sinkRecord, schemaBuilder.buildSchema(sinkRecord));
        assertNull(avroRecord.get(OutputFieldType.OFFSET.name));
        assertNull(avroRecord.get(OutputFieldType.KEY.name));
        assertNull(avroRecord.get(OutputFieldType.TIMESTAMP.name));
        assertNull(avroRecord.get(OutputFieldType.HEADERS.name));
        assertNotNull(avroRecord.get(OutputFieldType.VALUE.name));

        assertEquals(
                List.of("user_name", "user_ip", "blocked"),
                avroRecord.getSchema()
                        .getField("value").schema().getFields().stream()
                        .map(org.apache.avro.Schema.Field::name)
                        .collect(Collectors.toList())
        );

        final var valueRecord = (GenericRecord) avroRecord.get("value");
        assertEquals("John Doe", valueRecord.get("user_name"));
        assertEquals("127.0.0.1", valueRecord.get("user_ip"));
        assertEquals(true, valueRecord.get("blocked"));
    }

    @Test
    void testConvertRecordValueArray() {
        final var fields =
                List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, avroData);
        final var converter = new SinkRecordConverter(fields, avroData);

        final var recordSchema = SchemaBuilder.array(Schema.INT32_SCHEMA).build();

        final var sinkRecord =
                new SinkRecord(
                        "some-topic", 1,
                        Schema.STRING_SCHEMA, "some-key",
                        recordSchema, List.of(1, 2, 3, 4, 5, 6),
                        100L, 1000L, TimestampType.CREATE_TIME);

        final var avroRecord = converter.convert(sinkRecord, schemaBuilder.buildSchema(sinkRecord));
        assertEquals("{\"value\": [1, 2, 3, 4, 5, 6]}", avroRecord.toString());
    }

    @Test
    void testConvertRecordValueMap() {
        final var fields =
                List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, avroData);
        final var converter = new SinkRecordConverter(fields, avroData);

        final var recordSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA).build();

        final var sinkRecord =
                new SinkRecord(
                        "some-topic", 1,
                        Schema.STRING_SCHEMA, "some-key",
                        recordSchema, Map.of("a", true, "b", false, "c", true),
                        100L, 1000L, TimestampType.CREATE_TIME);

        final var avroRecord = converter.convert(sinkRecord, schemaBuilder.buildSchema(sinkRecord));
        assertEquals("{\"value\": {\"a\": true, \"b\": false, \"c\": true}}", avroRecord.toString());
    }

    @Test
    void testConvertRecordWithAllFields() {
        final var fields = List.of(
                new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.HEADERS, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE)
        );

        final var schemaBuilder = new ParquetSchemaBuilder(fields, avroData);
        final var converter = new SinkRecordConverter(fields, avroData);

        final Headers headers = new ConnectHeaders();
        headers.add("a", "b", Schema.STRING_SCHEMA);
        headers.add("c", "d", Schema.STRING_SCHEMA);

        final var recordSchema = SchemaBuilder.struct()
                .field("user_name", Schema.STRING_SCHEMA)
                .field("user_ip", Schema.STRING_SCHEMA)
                .field("blocked", Schema.BOOLEAN_SCHEMA)
                .build();

        final var sinkRecord =
                new SinkRecord(
                        "some-topic", 1,
                        Schema.STRING_SCHEMA, "some-key",
                        recordSchema,
                        new Struct(recordSchema)
                                .put("user_name", "John Doe")
                                .put("user_ip", "127.0.0.1")
                                .put("blocked", true),
                        100L, 1000L, TimestampType.CREATE_TIME, headers);

        final var avroRecord = converter.convert(sinkRecord, schemaBuilder.buildSchema(sinkRecord));
        assertNotNull(avroRecord.get(OutputFieldType.KEY.name));
        assertNotNull(avroRecord.get(OutputFieldType.OFFSET.name));
        assertNotNull(avroRecord.get(OutputFieldType.TIMESTAMP.name));
        assertNotNull(avroRecord.get(OutputFieldType.HEADERS.name));
        assertNotNull(avroRecord.get(OutputFieldType.VALUE.name));

        assertEquals(100L, (Long) avroRecord.get(OutputFieldType.OFFSET.name));
        assertEquals(1000L, (Long) avroRecord.get(OutputFieldType.TIMESTAMP.name));

        assertEquals(
                "some-key",
                avroRecord.get(OutputFieldType.KEY.name)
        );
        assertEquals(
                "{\"user_name\": \"John Doe\", \"user_ip\": \"127.0.0.1\", \"blocked\": true}",
                avroRecord.get(OutputFieldType.VALUE.name).toString()
        );

        final var recordHeaders = (Map<String, String>) avroRecord.get(OutputFieldType.HEADERS.name);
        assertEquals(2, recordHeaders.size());
        assertEquals("b", recordHeaders.get("a"));
        assertEquals("d", recordHeaders.get("c"));
    }

    @Test
    void testConvertRecordWithAllFieldsWithoutHeaders() {
        final var fields = List.of(
                new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.HEADERS, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE)
        );

        final var schemaBuilder = new ParquetSchemaBuilder(fields, avroData);
        final var converter = new SinkRecordConverter(fields, avroData);

        final var recordSchema = SchemaBuilder.struct()
                .field("user_name", Schema.STRING_SCHEMA)
                .field("user_ip", Schema.STRING_SCHEMA)
                .field("blocked", Schema.BOOLEAN_SCHEMA)
                .build();

        final var sinkRecord =
                new SinkRecord(
                        "some-topic", 1,
                        Schema.STRING_SCHEMA, "some-key",
                        recordSchema,
                        new Struct(recordSchema)
                                .put("user_name", "John Doe")
                                .put("user_ip", "127.0.0.1")
                                .put("blocked", true),
                        100L, 1000L, TimestampType.CREATE_TIME);

        final var avroRecord = converter.convert(sinkRecord, schemaBuilder.buildSchema(sinkRecord));
        assertNotNull(avroRecord.get(OutputFieldType.KEY.name));
        assertNotNull(avroRecord.get(OutputFieldType.OFFSET.name));
        assertNotNull(avroRecord.get(OutputFieldType.TIMESTAMP.name));
        assertNotNull(avroRecord.get(OutputFieldType.HEADERS.name));
        assertNotNull(avroRecord.get(OutputFieldType.VALUE.name));

        assertEquals(100L, (Long) avroRecord.get(OutputFieldType.OFFSET.name));
        assertEquals(1000L, (Long) avroRecord.get(OutputFieldType.TIMESTAMP.name));

        assertEquals(
                "some-key",
                avroRecord.get(OutputFieldType.KEY.name)
        );
        assertEquals(
                "{\"user_name\": \"John Doe\", \"user_ip\": \"127.0.0.1\", \"blocked\": true}",
                avroRecord.get(OutputFieldType.VALUE.name).toString()
        );

        final var recordHeaders = (Map<String, String>) avroRecord.get(OutputFieldType.HEADERS.name);
        assertTrue(recordHeaders.isEmpty());
    }

    @Test
    void testConvertRecordWithPartialFields() {
        final var fields = List.of(
                new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE)
        );

        final var schemaBuilder = new ParquetSchemaBuilder(fields, avroData);
        final var converter = new SinkRecordConverter(fields, avroData);

        final var sinkRecord =
                new SinkRecord(
                        "some-topic", 1,
                        Schema.STRING_SCHEMA, "some-key",
                        Schema.STRING_SCHEMA, "some-value",
                        100L, 1000L, TimestampType.CREATE_TIME);

        final var avroRecord = converter.convert(sinkRecord, schemaBuilder.buildSchema(sinkRecord));
        assertNotNull(avroRecord.get(OutputFieldType.KEY.name));
        assertNotNull(avroRecord.get(OutputFieldType.OFFSET.name));
        assertNotNull(avroRecord.get(OutputFieldType.TIMESTAMP.name));
        assertNull(avroRecord.get(OutputFieldType.HEADERS.name));
        assertNull(avroRecord.get(OutputFieldType.VALUE.name));

        assertEquals(100L, (Long) avroRecord.get(OutputFieldType.OFFSET.name));
        assertEquals(1000L, (Long) avroRecord.get(OutputFieldType.TIMESTAMP.name));

        assertEquals("some-key", avroRecord.get(OutputFieldType.KEY.name));
    }

}
