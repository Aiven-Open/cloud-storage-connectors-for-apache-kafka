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

package io.aiven.kafka.connect.common.output;

import static org.apache.avro.Schema.Field;
import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import io.aiven.kafka.connect.common.output.parquet.ParquetSchemaBuilder;

import io.confluent.connect.avro.AvroData;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SinkRecordConverterTest {

    static final int CACHE_SIZE = 0;

    private final AvroData avroData = new AvroData(CACHE_SIZE);

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testConvertRecordWithOneFieldSimpleType(final boolean envelopeEnabled) {
        final var fields = List.of(new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, avroData, envelopeEnabled);
        final var converter = new SinkRecordConverter(fields, avroData, envelopeEnabled);

        final var sinkRecord = new SinkRecord("some-topic", 1, Schema.STRING_SCHEMA, "some-key", Schema.STRING_SCHEMA,
                "some-value", 100L, 1000L, TimestampType.CREATE_TIME);

        final var avroRecord = converter.convert(sinkRecord, schemaBuilder.buildSchema(sinkRecord));
        assertThat(avroRecord.hasField(OutputFieldType.OFFSET.name)).isTrue();
        assertThat(avroRecord.hasField(OutputFieldType.KEY.name)).isFalse();
        assertThat(avroRecord.hasField(OutputFieldType.TIMESTAMP.name)).isFalse();
        assertThat(avroRecord.hasField(OutputFieldType.HEADERS.name)).isFalse();
        assertThat(avroRecord.hasField(OutputFieldType.VALUE.name)).isFalse();

        assertThat(avroRecord.get(OutputFieldType.OFFSET.name)).isEqualTo(100L);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testConvertRecordValueSimpleType(final boolean envelopeEnabled) {
        final var fields = List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, avroData, envelopeEnabled);
        final var converter = new SinkRecordConverter(fields, avroData, envelopeEnabled);

        final var sinkRecord = new SinkRecord("some-topic", 1, Schema.STRING_SCHEMA, "some-key", Schema.STRING_SCHEMA,
                "some-value", 100L, 1000L, TimestampType.CREATE_TIME);

        final var avroRecord = converter.convert(sinkRecord, schemaBuilder.buildSchema(sinkRecord));
        assertThat(avroRecord.hasField(OutputFieldType.OFFSET.name)).isFalse();
        assertThat(avroRecord.hasField(OutputFieldType.KEY.name)).isFalse();
        assertThat(avroRecord.hasField(OutputFieldType.TIMESTAMP.name)).isFalse();
        assertThat(avroRecord.hasField(OutputFieldType.HEADERS.name)).isFalse();
        assertThat(avroRecord.get(OutputFieldType.VALUE.name)).isEqualTo("some-value");
    }

    @Test
    void testConvertRecordValueStructType() {
        final var fields = List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, avroData);
        final var converter = new SinkRecordConverter(fields, avroData);

        final var recordSchema = SchemaBuilder.struct()
                .field("foo", Schema.STRING_SCHEMA)
                .field("bar", SchemaBuilder.STRING_SCHEMA)
                .build();

        final var sinkRecord = new SinkRecord("some-topic", 1, Schema.STRING_SCHEMA, "some-key", recordSchema,
                new Struct(recordSchema).put("foo", "bar").put("bar", "foo"), 100L, 1000L, TimestampType.CREATE_TIME);

        final var avroRecord = converter.convert(sinkRecord, schemaBuilder.buildSchema(sinkRecord));
        assertThat(avroRecord.hasField(OutputFieldType.OFFSET.name)).isFalse();
        assertThat(avroRecord.hasField(OutputFieldType.KEY.name)).isFalse();
        assertThat(avroRecord.hasField(OutputFieldType.TIMESTAMP.name)).isFalse();
        assertThat(avroRecord.hasField(OutputFieldType.HEADERS.name)).isFalse();
        assertThat(avroRecord.hasField(OutputFieldType.VALUE.name)).isTrue();

        final var valueRecord = (GenericRecord) avroRecord.get("value");
        assertThat(valueRecord.get("foo")).hasToString("bar");
        assertThat(valueRecord.get("bar")).hasToString("foo");
    }

    @Test
    void testConvertRecordValueStruct() {
        final var fields = List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, avroData);
        final var converter = new SinkRecordConverter(fields, avroData);

        final var recordSchema = SchemaBuilder.struct()
                .field("user_name", Schema.STRING_SCHEMA)
                .field("user_ip", Schema.STRING_SCHEMA)
                .field("blocked", Schema.BOOLEAN_SCHEMA)
                .build();

        final var sinkRecord = new SinkRecord("some-topic", 1, Schema.STRING_SCHEMA, "some-key", recordSchema,
                new Struct(recordSchema).put("user_name", "John Doe").put("user_ip", "127.0.0.1").put("blocked", true),
                100L, 1000L, TimestampType.CREATE_TIME);

        final var avroRecord = converter.convert(sinkRecord, schemaBuilder.buildSchema(sinkRecord));
        assertThat(avroRecord.hasField(OutputFieldType.OFFSET.name)).isFalse();
        assertThat(avroRecord.hasField(OutputFieldType.KEY.name)).isFalse();
        assertThat(avroRecord.hasField(OutputFieldType.TIMESTAMP.name)).isFalse();
        assertThat(avroRecord.hasField(OutputFieldType.HEADERS.name)).isFalse();
        assertThat(avroRecord.hasField(OutputFieldType.VALUE.name)).isTrue();

        assertThat(avroRecord.getSchema().getField("value").schema().getFields()).map(Field::name)
                .containsExactly("user_name", "user_ip", "blocked");

        final var valueRecord = (GenericRecord) avroRecord.get("value");
        assertThat(valueRecord.get("user_name")).isEqualTo("John Doe");
        assertThat(valueRecord.get("user_ip")).isEqualTo("127.0.0.1");
        assertThat(valueRecord.get("blocked")).isEqualTo(true);
    }

    @Test
    void testConvertRecordValueStructWithoutEnvelope() {
        final var fields = List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, avroData, false);
        final var converter = new SinkRecordConverter(fields, avroData, false);

        final var recordSchema = SchemaBuilder.struct()
                .field("user_name", Schema.STRING_SCHEMA)
                .field("user_ip", Schema.STRING_SCHEMA)
                .field("blocked", Schema.BOOLEAN_SCHEMA)
                .build();

        final var sinkRecord = new SinkRecord("some-topic", 1, Schema.STRING_SCHEMA, "some-key", recordSchema,
                new Struct(recordSchema).put("user_name", "John Doe").put("user_ip", "127.0.0.1").put("blocked", true),
                100L, 1000L, TimestampType.CREATE_TIME);

        final var avroRecord = converter.convert(sinkRecord, schemaBuilder.buildSchema(sinkRecord));
        assertThat(avroRecord.hasField(OutputFieldType.OFFSET.name)).isFalse();
        assertThat(avroRecord.hasField(OutputFieldType.KEY.name)).isFalse();
        assertThat(avroRecord.hasField(OutputFieldType.TIMESTAMP.name)).isFalse();
        assertThat(avroRecord.hasField(OutputFieldType.HEADERS.name)).isFalse();

        assertThat(avroRecord.getSchema().getFields()).map(Field::name)
                .containsExactly("user_name", "user_ip", "blocked");

        assertThat(avroRecord.get("user_name")).isEqualTo("John Doe");
        assertThat(avroRecord.get("user_ip")).isEqualTo("127.0.0.1");
        assertThat(avroRecord.get("blocked")).isEqualTo(true);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testConvertRecordValueArray(final boolean envelopeEnabled) {
        final var fields = List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, avroData, envelopeEnabled);
        final var converter = new SinkRecordConverter(fields, avroData, envelopeEnabled);

        final var recordSchema = SchemaBuilder.array(Schema.INT32_SCHEMA).build();

        final var sinkRecord = new SinkRecord("some-topic", 1, Schema.STRING_SCHEMA, "some-key", recordSchema,
                List.of(1, 2, 3, 4, 5, 6), 100L, 1000L, TimestampType.CREATE_TIME);

        final var avroRecord = converter.convert(sinkRecord, schemaBuilder.buildSchema(sinkRecord));
        assertThat(avroRecord).hasToString("{\"value\": [1, 2, 3, 4, 5, 6]}");
    }

    @Test
    void testConvertRecordValueMap() {
        final var fields = List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, avroData);
        final var converter = new SinkRecordConverter(fields, avroData);

        final var recordSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA).build();

        final var sinkRecord = new SinkRecord("some-topic", 1, Schema.STRING_SCHEMA, "some-key", recordSchema,
                Map.of("a", true, "b", false, "c", true), 100L, 1000L, TimestampType.CREATE_TIME);

        final var avroRecord = converter.convert(sinkRecord, schemaBuilder.buildSchema(sinkRecord));
        assertThat(avroRecord).hasToString("{\"value\": {\"a\": true, \"b\": false, \"c\": true}}");
    }

    @Test
    void testConvertRecordValueMapWithoutEnvelope() {
        final var fields = List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, avroData, false);
        final var converter = new SinkRecordConverter(fields, avroData, false);

        final var recordSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA).build();

        final var sinkRecord = new SinkRecord("some-topic", 1, Schema.STRING_SCHEMA, "some-key", recordSchema,
                new HashMap<>(Map.of("a", true, "b", false, "c", true)), 100L, 1000L, TimestampType.CREATE_TIME);

        final org.apache.avro.Schema schema = schemaBuilder.buildSchema(sinkRecord);
        final GenericData.Record avroRecord = (GenericData.Record) converter.convert(sinkRecord, schema);
        assertThat(avroRecord).hasToString("{\"a\": true, \"b\": false, \"c\": true}");
    }

    @Test
    void testConvertRecordWithAllFields() {
        final var fields = List.of(new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.HEADERS, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));

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

        final var sinkRecord = new SinkRecord("some-topic", 1, Schema.STRING_SCHEMA, "some-key", recordSchema,
                new Struct(recordSchema).put("user_name", "John Doe").put("user_ip", "127.0.0.1").put("blocked", true),
                100L, 1000L, TimestampType.CREATE_TIME, headers);

        final var avroRecord = converter.convert(sinkRecord, schemaBuilder.buildSchema(sinkRecord));
        assertThat(avroRecord.get(OutputFieldType.KEY.name)).isNotNull();
        assertThat(avroRecord.get(OutputFieldType.OFFSET.name)).isNotNull();
        assertThat(avroRecord.get(OutputFieldType.TIMESTAMP.name)).isNotNull();
        assertThat(avroRecord.get(OutputFieldType.HEADERS.name)).isNotNull();
        assertThat(avroRecord.get(OutputFieldType.VALUE.name)).isNotNull();

        assertThat((Long) avroRecord.get(OutputFieldType.OFFSET.name)).isEqualTo(100L);
        assertThat((Long) avroRecord.get(OutputFieldType.TIMESTAMP.name)).isEqualTo(1000L);

        assertThat(avroRecord.get(OutputFieldType.KEY.name)).isEqualTo("some-key");
        assertThat(avroRecord.get(OutputFieldType.VALUE.name))
                .hasToString("{\"user_name\": \"John Doe\", \"user_ip\": \"127.0.0.1\", \"blocked\": true}");

        assertThat(avroRecord)
                .extracting(rec -> rec.get(OutputFieldType.HEADERS.name), as(InstanceOfAssertFactories.MAP))
                .containsOnly(entry("a", "b"), entry("c", "d"));
    }

    @Test
    void testConvertRecordWithAllFieldsWithoutHeaders() {
        final var fields = List.of(new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.HEADERS, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, avroData);
        final var converter = new SinkRecordConverter(fields, avroData);

        final var recordSchema = SchemaBuilder.struct()
                .field("user_name", Schema.STRING_SCHEMA)
                .field("user_ip", Schema.STRING_SCHEMA)
                .field("blocked", Schema.BOOLEAN_SCHEMA)
                .build();

        final var sinkRecord = new SinkRecord("some-topic", 1, Schema.STRING_SCHEMA, "some-key", recordSchema,
                new Struct(recordSchema).put("user_name", "John Doe").put("user_ip", "127.0.0.1").put("blocked", true),
                100L, 1000L, TimestampType.CREATE_TIME);

        final var avroRecord = converter.convert(sinkRecord, schemaBuilder.buildSchema(sinkRecord));
        assertThat(avroRecord.get(OutputFieldType.KEY.name)).isNotNull();
        assertThat(avroRecord.get(OutputFieldType.OFFSET.name)).isNotNull();
        assertThat(avroRecord.get(OutputFieldType.TIMESTAMP.name)).isNotNull();
        assertThat(avroRecord.get(OutputFieldType.HEADERS.name)).isNotNull();
        assertThat(avroRecord.get(OutputFieldType.VALUE.name)).isNotNull();

        assertThat((Long) avroRecord.get(OutputFieldType.OFFSET.name)).isEqualTo(100L);
        assertThat((Long) avroRecord.get(OutputFieldType.TIMESTAMP.name)).isEqualTo(1000L);

        assertThat(avroRecord.get(OutputFieldType.KEY.name)).isEqualTo("some-key");
        assertThat(avroRecord.get(OutputFieldType.VALUE.name))
                .hasToString("{\"user_name\": \"John Doe\", \"user_ip\": \"127.0.0.1\", \"blocked\": true}");

        assertThat(avroRecord)
                .extracting(rec -> rec.get(OutputFieldType.HEADERS.name), as(InstanceOfAssertFactories.MAP))
                .isEmpty();
    }

    @Test
    void testConvertRecordWithPartialFields() {
        final var fields = List.of(new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, avroData);
        final var converter = new SinkRecordConverter(fields, avroData);

        final var sinkRecord = new SinkRecord("some-topic", 1, Schema.STRING_SCHEMA, "some-key", Schema.STRING_SCHEMA,
                "some-value", 100L, 1000L, TimestampType.CREATE_TIME);

        final var avroRecord = converter.convert(sinkRecord, schemaBuilder.buildSchema(sinkRecord));
        assertThat(avroRecord.get(OutputFieldType.KEY.name)).isEqualTo("some-key");
        assertThat(avroRecord.get(OutputFieldType.OFFSET.name)).isEqualTo(100L);
        assertThat(avroRecord.get(OutputFieldType.TIMESTAMP.name)).isEqualTo(1000L);
        assertThat(avroRecord.hasField(OutputFieldType.HEADERS.name)).isFalse();
        assertThat(avroRecord.hasField(OutputFieldType.VALUE.name)).isFalse();
    }

}
