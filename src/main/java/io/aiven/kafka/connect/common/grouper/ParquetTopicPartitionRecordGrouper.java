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

class ParquetTopicPartitionRecordGrouper extends TopicPartitionRecordGrouper {

    private final SchemaBasedRotator schemaBasedRotator = new SchemaBasedRotator();

    ParquetTopicPartitionRecordGrouper(final Template filenameTemplate,
                                       final Integer maxRecordsPerFile,
                                       final TimestampSource tsSource) {
        super(filenameTemplate, maxRecordsPerFile, tsSource);
    }

    @Override
    protected String resolveRecordKeyFor(final SinkRecord record) {
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

        private final Map<TopicPartition, KeyValueSchema> keyValueSchemas = new HashMap<>();

        @Override
        public boolean rotate(final SinkRecord record) {
            if (Objects.isNull(record.valueSchema()) || Objects.isNull(record.keySchema())) {
                throw new SchemaProjectorException("Record must have schemas for key and value");
            }
            final var tp = new TopicPartition(record.topic(), record.kafkaPartition());
            final var keyValueVersion =
                    keyValueSchemas.computeIfAbsent(tp, ignored -> new KeyValueSchema(
                            record.keySchema(),
                            record.valueSchema()));
            final var schemaChanged =
                    !keyValueVersion.keySchema.equals(record.keySchema())
                            || !keyValueVersion.valueSchema.equals(record.valueSchema());
            if (schemaChanged) {
                keyValueSchemas.put(tp,
                        new KeyValueSchema(record.keySchema(), record.valueSchema())
                );
            }
            return schemaChanged;
        }

        private static class KeyValueSchema {

            final Schema keySchema;

            final Schema valueSchema;

            KeyValueSchema(final Schema keySchema, final Schema valueSchema) {
                this.keySchema = keySchema;
                this.valueSchema = valueSchema;
            }

            @Override
            public boolean equals(final Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                final var that = (KeyValueSchema) o;
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
