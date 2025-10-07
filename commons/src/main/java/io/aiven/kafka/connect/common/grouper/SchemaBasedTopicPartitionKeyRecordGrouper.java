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

package io.aiven.kafka.connect.common.grouper;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.templating.Template;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class SchemaBasedTopicPartitionKeyRecordGrouper extends TopicPartitionKeyRecordGrouper {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaBasedTopicPartitionKeyRecordGrouper.class);

    private final SchemaBasedRotator schemaBasedRotator = new SchemaBasedRotator();

    SchemaBasedTopicPartitionKeyRecordGrouper(final Template filenameTemplate, final Integer maxRecordsPerFile,
            final TimestampSource tsSource) {
        super(filenameTemplate, maxRecordsPerFile, tsSource);
    }

    @Override
    protected String resolveRecordKeyFor(final SinkRecord record) {
        LOG.debug("resolveRecordKeyFor()");
        if (schemaBasedRotator.rotate(record)) {
            return generateNewRecordKey(record);
        } else {
            return super.resolveRecordKeyFor(record);
        }
    }

    @Override
    public void clear() {
        schemaBasedRotator.clear();
        super.clear();
    }

    private static final class SchemaBasedRotator implements Rotator<SinkRecord> {
        private static final Logger LOG = LoggerFactory.getLogger(SchemaBasedRotator.class);
        private final Map<TopicPartitionKey, KeyValueSchema> keyValueSchemas = new HashMap<>();

        @Override
        public boolean rotate(final SinkRecord record) {
            if (Objects.isNull(record.valueSchema()) || Objects.isNull(record.keySchema())) {
                throw new SchemaProjectorException("Record must have schemas for key and value");
            }
            final var key = recordKey(record);
            final var tpk = new TopicPartitionKey(new TopicPartition(record.topic(), record.kafkaPartition()), key);
            final var keyValueVersion = keyValueSchemas.computeIfAbsent(tpk,
                    ignored -> {LOG.debug("Creating new KeyValueSchema"); return new KeyValueSchema(record.keySchema(), record.valueSchema());});

            final var schemaChanged = !keyValueVersion.keySchema.equals(record.keySchema())
                    || !keyValueVersion.valueSchema.equals(record.valueSchema());
            if (schemaChanged) {
                keyValueSchemas.put(tpk, new KeyValueSchema(record.keySchema(), record.valueSchema()));
            }
            return schemaChanged;
        }

        private String recordKey(final SinkRecord record) {
            final String key;
            if (record.key() == null) {
                key = "null";
            } else if (record.keySchema().type() == Schema.Type.STRING) {
                key = (String) record.key();
            } else {
                key = record.key().toString();
            }
            return key;
        }

        private static class KeyValueSchema {

            final Schema keySchema;

            final Schema valueSchema;

            KeyValueSchema(final Schema keySchema, final Schema valueSchema) {
                this.keySchema = keySchema;
                this.valueSchema = valueSchema;
            }

            @Override
            public boolean equals(final Object other) {
                if (this == other) {
                    return true;
                }
                if (other == null || getClass() != other.getClass()) {
                    return false;
                }
                final var that = (KeyValueSchema) other;
                return keySchema.equals(that.keySchema) && valueSchema.equals(that.valueSchema);
            }

            @Override
            public int hashCode() {
                return Objects.hash(keySchema, valueSchema);
            }
        }

        void clear() {
            keyValueSchemas.clear();
        }

    }

}
