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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.FilenameTemplateVariable;
import io.aiven.kafka.connect.common.config.StableTimeFormatter;
import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.common.templating.VariableTemplatePart.Parameter;

public class TopicPartitionKeyRecordGrouper implements RecordGrouper {

    private final Template filenameTemplate;

    private final Map<TopicPartitionKey, PartitionOffset> currentHeadRecords = new HashMap<>();

    private final Map<String, SinkRecordsBatch> fileBuffers = new HashMap<>();

    private final StableTimeFormatter timeFormatter;

    private final Rotator<SinkRecordsBatch> rotator;

    TopicPartitionKeyRecordGrouper(final Template filenameTemplate, final Integer maxRecordsPerFile,
            final TimestampSource tsSource) {
        Objects.requireNonNull(filenameTemplate, "filenameTemplate cannot be null");
        Objects.requireNonNull(tsSource, "tsSource cannot be null");
        this.filenameTemplate = filenameTemplate;

        this.timeFormatter = new StableTimeFormatter(tsSource);

        this.rotator = buffer -> {
            final var unlimited = maxRecordsPerFile == null;
            if (unlimited) {
                return false;
            } else {
                return buffer == null || buffer.getNumberOfRecords() >= maxRecordsPerFile;
            }
        };
    }

    @Override
    public void put(final SinkRecord record) {
        Objects.requireNonNull(record, "record cannot be null");
        final String recordKey = resolveRecordKeyFor(record);
        fileBuffers.computeIfAbsent(recordKey, ignored -> new SinkRecordsBatch(recordKey)).addSinkRecord(record);
    }

    protected String resolveRecordKeyFor(final SinkRecord record) {
        final var key = recordKey(record);

        final TopicPartitionKey tpk = new TopicPartitionKey(new TopicPartition(record.topic(), record.kafkaPartition()),
                key);
        final PartitionOffset currentHeadRecord = currentHeadRecords.computeIfAbsent(tpk,
                ignored -> new PartitionOffset(record.kafkaPartition(), record.kafkaOffset()));
        String objectKey = generateObjectKey(tpk, currentHeadRecord, record);
        if (rotator.rotate(fileBuffers.get(objectKey))) {
            // Create new file using this record as the head record.
            objectKey = generateNewRecordKey(record);
        }
        return objectKey;
    }

    private String recordKey(final SinkRecord record) {
        final String key;
        if (record.key() == null) {
            key = "null";
        } else if (record.keySchema() != null && record.keySchema().type() == Schema.Type.STRING) {
            key = (String) record.key();
        } else {
            key = record.key().toString();
        }
        return key;
    }

    public String generateObjectKey(final TopicPartitionKey tpk, final PartitionOffset headRecord,
            final SinkRecord currentRecord) {
        final Function<Parameter, String> setKafkaOffset = usePaddingParameter -> usePaddingParameter.asBoolean()
                ? String.format("%020d", headRecord.getOffset())
                : Long.toString(headRecord.getOffset());
        final Function<Parameter, String> setKafkaPartition = usePaddingParameter -> usePaddingParameter.asBoolean()
                ? String.format("%010d", headRecord.getPartition())
                : Long.toString(headRecord.getPartition());

        return filenameTemplate.instance()
                .bindVariable(FilenameTemplateVariable.TOPIC.name, tpk.topicPartition::topic)
                .bindVariable(FilenameTemplateVariable.PARTITION.name, setKafkaPartition)
                .bindVariable(FilenameTemplateVariable.KEY.name, tpk::getKey)
                .bindVariable(FilenameTemplateVariable.START_OFFSET.name, setKafkaOffset)
                .bindVariable(FilenameTemplateVariable.TIMESTAMP.name, timeFormatter.apply(currentRecord))
                .render();
    }

    protected String generateNewRecordKey(final SinkRecord record) {
        final var key = recordKey(record);
        final var tpk = new TopicPartitionKey(new TopicPartition(record.topic(), record.kafkaPartition()), key);
        currentHeadRecords.put(tpk, new PartitionOffset(record.kafkaPartition(), record.kafkaOffset()));
        return generateObjectKey(tpk, new PartitionOffset(record.kafkaPartition(), record.kafkaOffset()), record);
    }

    @Override
    public void clear() {
        currentHeadRecords.clear();
        fileBuffers.clear();
    }

    @Override
    public void clearProcessedRecords(final String identifier, final List<SinkRecord> records) {
        final SinkRecordsBatch grouperRecord = fileBuffers.getOrDefault(identifier, null);
        if (Objects.isNull(grouperRecord)) {
            return;
        }
        grouperRecord.removeSinkRecords(records);
    }

    @Override
    public Map<String, List<SinkRecord>> records() {
        return Collections.unmodifiableMap(fileBuffers.values()
                .stream()
                .collect(Collectors.toMap(SinkRecordsBatch::getFilename, SinkRecordsBatch::getSinkRecords)));
    }

    public static class TopicPartitionKey {
        final TopicPartition topicPartition;
        final String key;

        TopicPartitionKey(final TopicPartition topicPartition, final String key) {
            this.topicPartition = topicPartition;
            this.key = key;
        }

        public String getKey() {
            return key;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            final TopicPartitionKey that = (TopicPartitionKey) other;
            return Objects.equals(topicPartition, that.topicPartition) && Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicPartition, key);
        }
    }
}
