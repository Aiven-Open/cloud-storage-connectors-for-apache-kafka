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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;

import io.confluent.connect.avro.AvroData;
import org.apache.avro.Schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ParquetSchemaBuilderTest {

    static final int AVRO_CACHE_SIZE = 10;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSchemaForSimpleType(final boolean envelopeEnabled) {
        final var fields =
                List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, new AvroData(AVRO_CACHE_SIZE), envelopeEnabled);

        final var sinkRecord =
                new SinkRecord(
                        "some-topic", 1,
                        Schema.STRING_SCHEMA, "some-key",
                        Schema.STRING_SCHEMA, "some-value",
                        100L, 1000L, TimestampType.CREATE_TIME);
        final var avroSchema = schemaBuilder.buildSchema(sinkRecord);

        assertNotNull(avroSchema);
        assertEquals(Type.RECORD, avroSchema.getType());
        assertEquals(Type.STRING, avroSchema.getField(fields.get(0).getFieldType().name).schema().getType());
    }

    @Test
    void testSchemaForRecordValueStruct() {
        final var fields =
                List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, new AvroData(AVRO_CACHE_SIZE));

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
                                .put("user_name", "Vasia Pupkion")
                                .put("user_ip", "127.0.0.1")
                                .put("blocked", false),
                        100L, 1000L, TimestampType.CREATE_TIME);
        final var avroSchema = schemaBuilder.buildSchema(sinkRecord);

        assertNotNull(avroSchema);
        assertEquals(Type.RECORD, avroSchema.getType());
        assertEquals(
                Type.RECORD,
                avroSchema.getField("value").schema().getType()
        );
    }


    @Test
    void testSchemaForRecordValueStructWithoutEnvelope() {
        final var fields =
                List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));

        final AvroData avroData = new AvroData(AVRO_CACHE_SIZE);
        final var schemaBuilder = new ParquetSchemaBuilder(fields, avroData, false);

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
                                .put("user_name", "Vasia Pupkion")
                                .put("user_ip", "127.0.0.1")
                                .put("blocked", false),
                        100L, 1000L, TimestampType.CREATE_TIME);
        final var avroSchema = schemaBuilder.buildSchema(sinkRecord);

        assertThat(avroSchema).isNotNull();
        assertThat(avroSchema.getType()).isEqualTo(Type.RECORD);
        assertThat(avroSchema).isEqualTo(avroData.fromConnectSchema(recordSchema));
    }

    @Test
    void testSchemaForRecordValueMap() {
        final var fields =
                List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, new AvroData(AVRO_CACHE_SIZE));

        final var recordSchema =
                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA).build();
        final var sinkRecord =
                new SinkRecord(
                        "some-topic", 1,
                        Schema.STRING_SCHEMA, "some-key",
                        recordSchema, Map.of("any", true, "beny", false, "raba", true),
                        100L, 1000L, TimestampType.CREATE_TIME);
        final var avroSchema = schemaBuilder.buildSchema(sinkRecord);

        assertNotNull(avroSchema);
        assertEquals(Type.RECORD, avroSchema.getType());
        assertEquals(1, avroSchema.getFields().size());
        assertEquals(Type.MAP, avroSchema.getField(OutputFieldType.VALUE.name).schema().getType());
        assertEquals(Type.BOOLEAN, avroSchema.getField(OutputFieldType.VALUE.name).schema().getValueType().getType());
    }

    @Test
    void testSchemaForRecordValueMapWithoutEnvelope() {
        final var fields =
                List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, new AvroData(AVRO_CACHE_SIZE), false);

        final var recordSchema =
                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA).build();
        final Map<String, Boolean> valueMap = Map.of("any", true, "beny", false, "raba", true);
        final var sinkRecord =
                new SinkRecord(
                        "some-topic", 1,
                        Schema.STRING_SCHEMA, valueMap,
                        recordSchema, valueMap,
                        100L, 1000L, TimestampType.CREATE_TIME);
        final var avroSchema = schemaBuilder.buildSchema(sinkRecord);

        assertThat(avroSchema).isNotNull();
        assertThat(avroSchema.getType()).isEqualTo(Type.RECORD);
        assertThat(avroSchema.getFields()).hasSize(3);
        assertThat(avroSchema.getFields())
                .map(org.apache.avro.Schema.Field::name)
                .containsExactlyElementsOf(valueMap.keySet());
        assertThat(avroSchema.getFields())
                .map(field -> field.schema().getType())
                .allMatch(type -> type == Type.BOOLEAN);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSchemaForRecordValueArray(final boolean envelopeEnabled) {
        final var fields =
                List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, new AvroData(AVRO_CACHE_SIZE), envelopeEnabled);

        final var recordSchema =
                SchemaBuilder.array(Schema.STRING_SCHEMA).build();
        final var sinkRecord =
                new SinkRecord(
                        "some-topic", 1,
                        Schema.STRING_SCHEMA, "some-key",
                        recordSchema, List.of("any", "beny", "raba"),
                        100L, 1000L, TimestampType.CREATE_TIME);
        final var avroSchema = schemaBuilder.buildSchema(sinkRecord);

        assertNotNull(avroSchema);
        assertEquals(Type.RECORD, avroSchema.getType());
        assertEquals(1, avroSchema.getFields().size());
        assertEquals(Type.ARRAY, avroSchema.getField(OutputFieldType.VALUE.name).schema().getType());
        assertEquals(Type.STRING, avroSchema.getField(OutputFieldType.VALUE.name).schema().getElementType().getType());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSchemaForRecordValueSimpleType(final boolean envelopeEnabled) {
        final var fields =
                List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, new AvroData(AVRO_CACHE_SIZE), envelopeEnabled);

        final var sinkRecord =
                new SinkRecord(
                        "some-topic", 1,
                        Schema.STRING_SCHEMA, "some-key",
                        Schema.STRING_SCHEMA, "some-value",
                        100L, 1000L, TimestampType.CREATE_TIME);
        final var avroSchema = schemaBuilder.buildSchema(sinkRecord);

        assertNotNull(avroSchema);
        assertEquals(Type.RECORD, avroSchema.getType());
        assertEquals(1, avroSchema.getFields().size());
        assertEquals(
                Type.STRING,
                avroSchema.getField("value").schema().getType()
        );
    }

    @Test
    void testBuildAivenCustomSchemaForMultipleFields() {
        final var fields = List.of(
                new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.HEADERS, OutputFieldEncodingType.NONE)
        );
        final var schemaBuilder = new ParquetSchemaBuilder(fields, new AvroData(AVRO_CACHE_SIZE));
        final Headers headers = new ConnectHeaders();
        headers.add("a", "b", Schema.STRING_SCHEMA);
        headers.add("c", "d", Schema.STRING_SCHEMA);
        final var sinkRecord =
                new SinkRecord(
                        "some-topic", 1,
                        Schema.STRING_SCHEMA, "some-key",
                        Schema.STRING_SCHEMA, "some-value",
                        100L, 1000L,
                        TimestampType.CREATE_TIME, headers);

        final var avroSchema = schemaBuilder.buildSchema(sinkRecord);

        assertNotNull(avroSchema);
        assertEquals(Type.RECORD, avroSchema.getType());
        assertEquals(
                fields.stream().map(f -> f.getFieldType().name).collect(Collectors.toList()),
                avroSchema.getFields().stream().map(org.apache.avro.Schema.Field::name).collect(Collectors.toList())
        );
        assertEquals(
                List.of(Type.STRING, Type.STRING, Type.LONG, Type.LONG, Type.MAP),
                avroSchema.getFields().stream().map(f -> f.schema().getType()).collect(Collectors.toList())
        );
        assertEquals(Type.STRING, avroSchema.getField(OutputFieldType.HEADERS.name).schema().getValueType().getType());
    }

    @Test
    void testBuildSchemaForMultipleFieldsWithoutHeaders() {
        final var fields = List.of(
                new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.HEADERS, OutputFieldEncodingType.NONE)
        );
        final var schemaBuilder = new ParquetSchemaBuilder(fields, new AvroData(AVRO_CACHE_SIZE));
        final var sinkRecord =
                new SinkRecord(
                        "some-topic", 1,
                        Schema.STRING_SCHEMA, "some-key",
                        Schema.STRING_SCHEMA, "some-value",
                        100L, 1000L,
                        TimestampType.CREATE_TIME);

        final var avroSchema = schemaBuilder.buildSchema(sinkRecord);

        assertNotNull(avroSchema);
        assertEquals(Type.RECORD, avroSchema.getType());
        assertEquals(
                List.of(OutputFieldType.KEY.name,
                        OutputFieldType.VALUE.name,
                        OutputFieldType.TIMESTAMP.name,
                        OutputFieldType.OFFSET.name,
                        OutputFieldType.HEADERS.name),
                avroSchema.getFields().stream().map(org.apache.avro.Schema.Field::name).collect(Collectors.toList())
        );
        assertEquals(
                List.of(Type.STRING, Type.STRING, Type.LONG, Type.LONG, Type.NULL),
                avroSchema.getFields().stream().map(f -> f.schema().getType()).collect(Collectors.toList())
        );
    }

    @Test
    void testThrowsDataExceptionForWrongNoSchemaData() {
        final var fields =
                List.of(new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE));
        final var schemaBuilder = new ParquetSchemaBuilder(fields, new AvroData(10));
        final var sinkRecordWithoutKeySchema =
                new SinkRecord(
                        "some-topic", 1,
                        null, "some-key",
                        Schema.STRING_SCHEMA, "some-value",
                        100L, 1000L,
                        TimestampType.CREATE_TIME);

        final var nullKeyE = assertThrows(
                DataException.class, () -> schemaBuilder.buildSchema(sinkRecordWithoutKeySchema));

        assertEquals(nullKeyE.getMessage(), "Record key without schema");

        final var sinkRecordWithoutRecordSchema =
                new SinkRecord(
                        "some-topic", 1,
                        Schema.STRING_SCHEMA, "some-key",
                        null, "some-value",
                        100L, 1000L,
                        TimestampType.CREATE_TIME);
        final var nullValueE = assertThrows(
                DataException.class, () -> schemaBuilder.buildSchema(sinkRecordWithoutRecordSchema));
        assertEquals(nullValueE.getMessage(), "Record value without schema");
    }

    @Test
    void testThrowsDataExceptionForWrongHeaders() {
        final var fields =
                List.of(new OutputField(OutputFieldType.HEADERS, OutputFieldEncodingType.NONE));

        final var schemaBuilder = new ParquetSchemaBuilder(fields, new AvroData(10));

        final var sinkRecordWithHeadersWithoutSchema =
                new SinkRecord(
                        "some-topic", 1,
                        Schema.STRING_SCHEMA, "some-key",
                        Schema.STRING_SCHEMA, "some-value",
                        100L, 1000L,
                        TimestampType.CREATE_TIME,
                        new ConnectHeaders()
                                .add("a", "b", Schema.STRING_SCHEMA)
                                .add("c", "d", null)
                );

        assertThrows(DataException.class, () -> schemaBuilder.buildSchema(sinkRecordWithHeadersWithoutSchema));

        final var sinkRecordWithHeadersWithDiffSchema =
                new SinkRecord(
                        "some-topic", 1,
                        Schema.STRING_SCHEMA, "some-key",
                        Schema.STRING_SCHEMA, "some-value",
                        100L, 1000L,
                        TimestampType.CREATE_TIME,
                        new ConnectHeaders()
                                .add("a", "b", Schema.STRING_SCHEMA)
                                .add("c", "d".getBytes(StandardCharsets.UTF_8), Schema.BYTES_SCHEMA)
                );

        assertThrows(DataException.class, () -> schemaBuilder.buildSchema(sinkRecordWithHeadersWithDiffSchema));
    }

}
