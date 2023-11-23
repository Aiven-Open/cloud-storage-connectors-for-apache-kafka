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

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.FilenameTemplateVariable;
import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.common.templating.VariableTemplatePart.Parameter;

/**
 * A {@link RecordGrouper} that groups records by topic and partition.
 *
 * <p>
 * The class requires a filename template with {@code topic}, {@code partition}, and {@code start_offset} variables
 * declared.
 *
 * <p>
 * The class supports limited and unlimited number of records in files.
 */
class TopicPartitionRecordGrouper implements RecordGrouper {

    private static final Map<String, DateTimeFormatter> TIMESTAMP_FORMATTERS = Map.of("yyyy",
            DateTimeFormatter.ofPattern("yyyy"), "MM", DateTimeFormatter.ofPattern("MM"), "dd",
            DateTimeFormatter.ofPattern("dd"), "HH", DateTimeFormatter.ofPattern("HH"));

    private final Template filenameTemplate;

    private final Map<TopicPartition, SinkRecord> currentHeadRecords = new HashMap<>();

    private final Map<String, List<SinkRecord>> fileBuffers = new HashMap<>();

    private final Function<SinkRecord, Function<Parameter, String>> setTimestampBasedOnRecord;

    private final Rotator<List<SinkRecord>> rotator;

    /**
     * A constructor.
     *
     * @param filenameTemplate
     *            the filename template.
     * @param maxRecordsPerFile
     *            the maximum number of records per file ({@code null} for unlimited).
     * @param tsSource
     *            timestamp sources
     */
    TopicPartitionRecordGrouper(final Template filenameTemplate, final Integer maxRecordsPerFile,
            final TimestampSource tsSource) {
        Objects.requireNonNull(filenameTemplate, "filenameTemplate cannot be null");
        Objects.requireNonNull(tsSource, "tsSource cannot be null");
        this.filenameTemplate = filenameTemplate;

        this.setTimestampBasedOnRecord = record -> parameter -> tsSource.time(record)
                .format(TIMESTAMP_FORMATTERS.get(parameter.getValue()));

        this.rotator = buffer -> {
            final var unlimited = maxRecordsPerFile == null;
            if (unlimited) {
                return false;
            } else {
                return buffer == null || buffer.size() >= maxRecordsPerFile;
            }
        };
    }

    @Override
    public void put(final SinkRecord record) {
        Objects.requireNonNull(record, "record cannot be null");
        final String recordKey = resolveRecordKeyFor(record);
        fileBuffers.computeIfAbsent(recordKey, ignored -> new ArrayList<>()).add(record);
    }

    protected String resolveRecordKeyFor(final SinkRecord record) {
        final TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
        final SinkRecord currentHeadRecord = currentHeadRecords.computeIfAbsent(topicPartition, ignored -> record);
        String recordKey = generateRecordKey(topicPartition, currentHeadRecord, record);
        if (rotator.rotate(fileBuffers.get(recordKey))) {
            // Create new file using this record as the head record.
            recordKey = generateNewRecordKey(record);
        }
        return recordKey;
    }

    private String generateRecordKey(final TopicPartition topicPartition, final SinkRecord headRecord,
            final SinkRecord currentRecord) {
        final Function<Parameter, String> setKafkaOffset = usePaddingParameter -> usePaddingParameter.asBoolean()
                ? String.format("%020d", headRecord.kafkaOffset())
                : Long.toString(headRecord.kafkaOffset());
        final Function<Parameter, String> setKafkaPartition = usePaddingParameter -> usePaddingParameter.asBoolean()
                ? String.format("%010d", headRecord.kafkaPartition())
                : Long.toString(headRecord.kafkaPartition());

        return filenameTemplate.instance()
                .bindVariable(FilenameTemplateVariable.TOPIC.name, topicPartition::topic)
                .bindVariable(FilenameTemplateVariable.PARTITION.name, setKafkaPartition)
                .bindVariable(FilenameTemplateVariable.START_OFFSET.name, setKafkaOffset)
                .bindVariable(FilenameTemplateVariable.TIMESTAMP.name, setTimestampBasedOnRecord.apply(currentRecord))
                .render();
    }

    protected String generateNewRecordKey(final SinkRecord record) {
        final TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
        currentHeadRecords.put(topicPartition, record);
        return generateRecordKey(topicPartition, record, record);
    }

    @Override
    public void clear() {
        currentHeadRecords.clear();
        fileBuffers.clear();
    }

    @Override
    public Map<String, List<SinkRecord>> records() {
        return Collections.unmodifiableMap(fileBuffers);
    }

}
