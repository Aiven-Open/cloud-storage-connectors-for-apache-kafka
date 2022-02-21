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
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

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

        assertThat(avroSchema).isNotNull();
        assertThat(avroSchema.getType()).isEqualTo(Type.RECORD);
        assertThat(avroSchema.getField(fields.get(0).getFieldType().name).schema().getType()).isEqualTo(Type.STRING);
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

        assertThat(avroSchema).isNotNull();
        assertThat(avroSchema.getType()).isEqualTo(Type.RECORD);
        assertThat(avroSchema.getField("value").schema().getType()).isEqualTo(Type.RECORD);
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

        assertThat(avroSchema).isNotNull();
        assertThat(avroSchema.getType()).isEqualTo(Type.RECORD);
        assertThat(avroSchema.getFields()).hasSize(1);
        assertThat(avroSchema.getField(OutputFieldType.VALUE.name).schema().getType()).isEqualTo(Type.MAP);
        assertThat(avroSchema.getField(OutputFieldType.VALUE.name).schema().getValueType().getType())
            .isEqualTo(Type.BOOLEAN);
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
                .map(Field::name)
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

        assertThat(avroSchema).isNotNull();
        assertThat(avroSchema.getType()).isEqualTo(Type.RECORD);
        assertThat(avroSchema.getFields()).hasSize(1);
        final var valueSchema = avroSchema.getField(OutputFieldType.VALUE.name).schema();
        assertThat(valueSchema.getType()).isEqualTo(Type.ARRAY);
        assertThat(valueSchema.getElementType().getType()).isEqualTo(Type.STRING);
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

        assertThat(avroSchema).isNotNull();
        assertThat(avroSchema.getType()).isEqualTo(Type.RECORD);
        assertThat(avroSchema.getFields()).hasSize(1);
        assertThat(avroSchema.getField("value").schema().getType()).isEqualTo(Type.STRING);
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

        assertThat(avroSchema).isNotNull();
        assertThat(avroSchema.getType()).isEqualTo(Type.RECORD);
        assertThat(avroSchema).isNotNull();
        assertThat(avroSchema.getType()).isEqualTo(Type.RECORD);
        assertThat(avroSchema.getFields())
            .extracting(Field::name, f -> f.schema().getType())
            .containsExactly(
                tuple(OutputFieldType.KEY.name, Type.STRING),
                tuple(OutputFieldType.VALUE.name, Type.STRING),
                tuple(OutputFieldType.TIMESTAMP.name, Type.LONG),
                tuple(OutputFieldType.OFFSET.name, Type.LONG),
                tuple(OutputFieldType.HEADERS.name, Type.MAP)
            );
        assertThat(avroSchema.getField(OutputFieldType.HEADERS.name).schema().getValueType().getType())
            .isEqualTo(Type.STRING);
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

        assertThat(avroSchema).isNotNull();
        assertThat(avroSchema.getType()).isEqualTo(Type.RECORD);
        assertThat(avroSchema.getFields())
            .extracting(Field::name, f -> f.schema().getType())
            .containsExactly(
                tuple(OutputFieldType.KEY.name, Type.STRING),
                tuple(OutputFieldType.VALUE.name, Type.STRING),
                tuple(OutputFieldType.TIMESTAMP.name, Type.LONG),
                tuple(OutputFieldType.OFFSET.name, Type.LONG),
                tuple(OutputFieldType.HEADERS.name, Type.NULL)
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

        assertThatThrownBy(() -> schemaBuilder.buildSchema(sinkRecordWithoutKeySchema))
            .isInstanceOf(DataException.class)
            .hasMessage("Record key without schema");

        final var sinkRecordWithoutRecordSchema =
                new SinkRecord(
                        "some-topic", 1,
                        Schema.STRING_SCHEMA, "some-key",
                        null, "some-value",
                        100L, 1000L,
                        TimestampType.CREATE_TIME);

        assertThatThrownBy(() -> schemaBuilder.buildSchema(sinkRecordWithoutRecordSchema))
            .isInstanceOf(DataException.class)
            .hasMessage("Record value without schema");
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

        assertThatThrownBy(() -> schemaBuilder.buildSchema(sinkRecordWithHeadersWithoutSchema))
            .isInstanceOf(DataException.class);

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

        assertThatThrownBy(() -> schemaBuilder.buildSchema(sinkRecordWithHeadersWithDiffSchema))
            .isInstanceOf(DataException.class);
    }
}
