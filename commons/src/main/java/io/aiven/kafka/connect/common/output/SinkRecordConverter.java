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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldType;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final public class SinkRecordConverter {

    private final Logger logger = LoggerFactory.getLogger(SinkRecordConverter.class);

    private final Collection<OutputField> fields;

    private final AvroData avroData;

    private final boolean envelopeEnabled;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "stores mutable fields and avroData")
    public SinkRecordConverter(final Collection<OutputField> fields, final AvroData avroData,
            final boolean envelopeEnabled) {
        this.fields = fields;
        this.avroData = avroData;
        this.envelopeEnabled = envelopeEnabled;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "stores mutable fields and avroData")
    public SinkRecordConverter(final Collection<OutputField> fields, final AvroData avroData) {
        this.fields = fields;
        this.avroData = avroData;
        this.envelopeEnabled = true;
    }

    public GenericRecord convert(final SinkRecord record, final Schema schema) {
        logger.debug("Convert record {} for schema {}", record, schema);
        return createRecord(schema, record);
    }

    private GenericRecord createRecord(final Schema schema, final SinkRecord record) {
        if (envelopeEnabled) {
            final var avroRecord = new GenericData.Record(schema);
            for (final var f : fields) {
                final var fieldValue = getRecordValueFor(f.getFieldType(), record);
                avroRecord.put(f.getFieldType().name, fieldValue);
            }
            return avroRecord;
        } else {
            return tryUnwrapEnvelope(schema, record);
        }
    }

    private GenericData.Record tryUnwrapEnvelope(final Schema schema, final SinkRecord record) {
        // envelope can be disabled only in case of single field
        final OutputField field = fields.iterator().next();
        final var fieldValue = getRecordValueFor(field.getFieldType(), record);
        final Schema.Type originalValueSchemaType = avroData.fromConnectSchema(record.valueSchema()).getType();
        if (originalValueSchemaType == Schema.Type.MAP) {
            @SuppressWarnings("unchecked")
            final Set<Map.Entry<String, Object>> entries = ((Map<String, Object>) fieldValue).entrySet();
            final var avroRecord = new GenericData.Record(schema);
            for (final Map.Entry<String, Object> entry : entries) {
                avroRecord.put(entry.getKey(), entry.getValue());
            }
            return avroRecord;
        } else if (originalValueSchemaType == Schema.Type.RECORD) {
            return (GenericData.Record) fieldValue;
        } else {
            final var avroRecord = new GenericData.Record(schema);
            avroRecord.put(field.getFieldType().name, fieldValue);
            return avroRecord;
        }
    }

    private Object getRecordValueFor(final OutputFieldType fieldType, final SinkRecord record) {
        switch (fieldType) {
            case KEY :
                return fromConnectData(record.keySchema(), record.key());
            case VALUE :
                return fromConnectData(record.valueSchema(), record.value());
            case OFFSET :
                return record.kafkaOffset();
            case TIMESTAMP :
                return record.timestamp();
            case HEADERS :
                final var headers = new HashMap<String, Object>();
                for (final var h : record.headers()) {
                    final var key = h.key();
                    final var value = fromConnectData(h.schema(), h.value());
                    headers.put(key, value);
                }
                return headers;
            default :
                throw new ConnectException("Unsupported output field: " + fieldType);
        }
    }

    private Object fromConnectData(final org.apache.kafka.connect.data.Schema schema, final Object value) {
        final var avroDataValue = avroData.fromConnectData(schema, value);
        if (avroDataValue instanceof NonRecordContainer) {
            return ((NonRecordContainer) avroDataValue).getValue();
        } else {
            return avroDataValue;
        }
    }

}
