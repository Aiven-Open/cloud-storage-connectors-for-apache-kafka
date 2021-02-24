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

import java.util.Collection;
import java.util.Objects;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.OutputField;

import io.confluent.connect.avro.AvroData;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Build schema for output fields based on SinkRecord values
 * For multiple output fields schema looks like this:
 * {
 *    "type" "record", "fields": [
 *       {"name": "key", type="RecordKeySchema"},
 *       {"name": "offset", type="long"},
 *       {"name": "timestamp", type="long"},
 *       {"name": "headers", type="map"},
 *       {"name": "value", type="RecordValueSchema"},
 *    ]
 * }
 */
class ParquetSchemaBuilder {

    private final Logger logger = LoggerFactory.getLogger(ParquetSchemaBuilder.class);

    private final Collection<OutputField> fields;

    private final AvroData avroData;

    ParquetSchemaBuilder(final Collection<OutputField> fields,
                         final AvroData avroData) {
        this.fields = fields;
        this.avroData = avroData;
    }

    public Schema buildSchema(final SinkRecord record) {
        Objects.requireNonNull(record, "record");
        if (Objects.isNull(record.keySchema())) {
            throw new DataException("Record key without schema");
        }
        if (Objects.isNull(record.valueSchema())) {
            throw new DataException("Record value without schema");
        }
        logger.debug("Create schema for record");
        logger.debug("Record Key Schema {}", record.keySchema());
        logger.debug("Record Value Schema {}", record.valueSchema());
        return avroSchemaFor(record);
    }

    private Schema headersSchema(final SinkRecord record) {
        if (record.headers().isEmpty()) {
            return SchemaBuilder.builder().nullType();
        }
        org.apache.kafka.connect.data.Schema headerSchema = null;
        for (final var h : record.headers()) {
            if (Objects.isNull(h.schema())) {
                throw new DataException("Header " + h + " without schema");
            }
            if (Objects.isNull(headerSchema)) {
                headerSchema = h.schema();
            } else if (headerSchema.type() != h.schema().type()) {
                throw new DataException("Header schema " + h.schema()
                        + " is not the same as " + headerSchema);
            }
        }
        return SchemaBuilder.map().values(avroData.fromConnectSchema(headerSchema));
    }

    private Schema avroSchemaFor(final SinkRecord record) {
        final var schemaFields =
                SchemaBuilder
                        .builder("io.aiven.parquet.output.schema")
                        .record("connector_records")
                        .fields();
        for (final var f : fields) {
            final var schema = outputFieldSchema(f, record);
            schemaFields.name(f.getFieldType().name).type(schema).noDefault();
        }
        return schemaFields.endRecord();
    }

    private Schema outputFieldSchema(final OutputField field, final SinkRecord record) {
        switch (field.getFieldType()) {
            case KEY:
                return avroData.fromConnectSchema(record.keySchema());
            case OFFSET:
            case TIMESTAMP:
                return SchemaBuilder.builder().longType();
            case VALUE:
                return avroData.fromConnectSchema(record.valueSchema());
            case HEADERS:
                return headersSchema(record);
            default:
                throw new ConnectException("Unknown field type " + field);
        }
    }

}
