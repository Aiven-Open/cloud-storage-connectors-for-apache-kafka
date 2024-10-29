/*
 * Copyright 2020 Aiven Oy
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.FilenameTemplateVariable;
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.common.templating.VariableTemplatePart.Parameter;

/**
 * A {@link RecordGrouper} that groups records by topic, parition and key.
 *
 * <p>
 * The class requires a filename template with {@code key} variable declared and supports optional {@code topic} and
 * {@code partition} variables declared.
 *
 * <p>
 * The class supports one record per file.
 */
public final class KeyAndTopicPartitionRecordGrouper implements RecordGrouper {

    private final Template filenameTemplate;

    // One record pre file, but use List here for the compatibility with the interface.
    private final Map<String, List<SinkRecord>> fileBuffers = new HashMap<>();

    /**
     * A constructor.
     *
     * @param filenameTemplate
     *            the filename template.
     */
    public KeyAndTopicPartitionRecordGrouper(final Template filenameTemplate) {
        this.filenameTemplate = Objects.requireNonNull(filenameTemplate, "filenameTemplate cannot be null");
    }

    @Override
    public void put(final SinkRecord record) {
        Objects.requireNonNull(record, "records cannot be null");

        final String recordKey = generateRecordKey(record);

        final List<SinkRecord> records = fileBuffers.computeIfAbsent(recordKey, ignored -> new ArrayList<>(1));
        // one record per file
        records.clear();
        records.add(record);
    }

    private String generateRecordKey(final SinkRecord record) {
        final Supplier<String> setKey = () -> {
            if (record.key() == null) {
                return "null";
            } else if (record.keySchema() != null && record.keySchema().type() == Schema.Type.STRING) {
                return (String) record.key();
            } else {
                return record.key().toString();
            }
        };

        final Function<Parameter, String> setKafkaPartition = usePaddingParameter -> usePaddingParameter.asBoolean()
                ? String.format("%010d", record.kafkaPartition())
                : Long.toString(record.kafkaPartition());

        final TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());

        return filenameTemplate.instance()
                .bindVariable(FilenameTemplateVariable.KEY.name, setKey)
                .bindVariable(FilenameTemplateVariable.TOPIC.name, topicPartition::topic)
                .bindVariable(FilenameTemplateVariable.PARTITION.name, setKafkaPartition)
                .render();
    }

    @Override
    public void clear() {
        fileBuffers.clear();
    }

    @Override
    public void clearProcessedRecords(final String identifier, final List<SinkRecord> records) {
        // One entry per file, so the entire file can be removed to reduce memory overhead.
        fileBuffers.remove(identifier);
    }

    @Override
    public Map<String, List<SinkRecord>> records() {
        return Collections.unmodifiableMap(fileBuffers);
    }

}
