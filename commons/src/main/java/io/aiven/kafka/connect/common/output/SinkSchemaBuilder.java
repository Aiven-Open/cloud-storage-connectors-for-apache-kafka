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

package io.aiven.kafka.connect.common.output;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
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
public abstract class SinkSchemaBuilder {

    private final Logger logger = LoggerFactory.getLogger(SinkSchemaBuilder.class);

    private final Collection<OutputField> fields;

    private final AvroData avroData;

    private final boolean envelopeEnabled;

    public SinkSchemaBuilder(final Collection<OutputField> fields,
                      final AvroData avroData,
                      final boolean envelopeEnabled) {
        this.fields = fields;
        this.avroData = avroData;
        this.envelopeEnabled = envelopeEnabled;
    }

    public SinkSchemaBuilder(final Collection<OutputField> fields,
                                final AvroData avroData) {
        this.fields = fields;
        this.avroData = avroData;
        this.envelopeEnabled = true;
    }

    protected abstract String getNamespace();

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

    protected Schema avroSchemaFor(final SinkRecord record) {
        if (envelopeEnabled) {
            final SchemaBuilder.FieldAssembler<Schema> schemaFields =
                SchemaBuilder
                    .builder(getNamespace())
                    .record("connector_records")
                    .fields();
            for (final OutputField f : fields) {
                final Schema schema = outputFieldSchema(f, record);
                schemaFields.name(f.getFieldType().name).type(schema).noDefault();
            }
            return schemaFields.endRecord();
        } else {
            return tryUnwrapEnvelope(record);
        }
    }

    private Schema tryUnwrapEnvelope(final SinkRecord record) {
        // envelope can be disabled only in case of single field
        final OutputField field = getFields().iterator().next();

        final Schema schema = outputFieldSchema(field, record);
        if (schema.getType() == Schema.Type.MAP) {
            @SuppressWarnings("unchecked") final Map<String, Object> value =
                (Map<String, Object>) record.value();
            final SchemaBuilder.FieldAssembler<Schema> schemaFields =
                SchemaBuilder
                    .builder(getNamespace())
                    .record("connector_records")
                    .fields();
            for (final Map.Entry<String, Object> entry : value.entrySet()) {
                schemaFields.name(entry.getKey()).type(schema.getValueType()).noDefault();
            }
            return schemaFields.endRecord();
        } else if (schema.getType() == Schema.Type.RECORD) {
            return getAvroData().fromConnectSchema(record.valueSchema());
        } else {
            return SchemaBuilder
                .builder(getNamespace())
                .record("connector_records")
                .fields()
                .name(field.getFieldType().name)
                .type(schema)
                .noDefault()
                .endRecord();
        }
    }

    private Schema headersSchema(final SinkRecord record) {
        if (record.headers().isEmpty()) {
            return SchemaBuilder.builder().nullType();
        }
        org.apache.kafka.connect.data.Schema headerSchema = null;
        for (final Header h : record.headers()) {
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

    protected Schema outputFieldSchema(final OutputField field, final SinkRecord record) {
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

    public Collection<OutputField> getFields() {
        return fields;
    }

    public AvroData getAvroData() {
        return avroData;
    }

    public boolean isEnvelopeEnabled() {
        return envelopeEnabled;
    }
}
