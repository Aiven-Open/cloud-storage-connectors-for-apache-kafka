/*
 * Copyright 2024 Aiven Oy
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

package io.aiven.kafka.connect.s3.source.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;

public class S3SourceRecord {
    private final Map<String, Object> partitionMap;
    private final long recordNumber;
    private final String topic;
    private final Integer topicPartition;
    private final SchemaAndValue keyData;

    private final SchemaAndValue valueData;

    private final String objectKey;

    public S3SourceRecord(final Map<String, Object> partitionMap, final long recordNumber,
            final String topic, final Integer topicPartition, final String objectKey, final SchemaAndValue keyData,
            final SchemaAndValue valueData) {
        this.partitionMap = new HashMap<>(partitionMap);
        this.recordNumber = recordNumber;
        this.topic = topic;
        this.topicPartition = topicPartition;
        this.keyData = keyData;
        this.valueData = valueData;
        this.objectKey = objectKey;
    }

    public Map<String, Object> getPartitionMap() {
        return Collections.unmodifiableMap(partitionMap);
    }

    public long getRecordNumber() {
        return recordNumber;
    }

    public String getTopic() {
        return topic;
    }

    public Integer partition() {
        return topicPartition;
    }

    public String getObjectKey() {
        return objectKey;
    }

    public SchemaAndValue getKey() {
        return new SchemaAndValue(keyData.schema(), keyData.value());
    }

    public SchemaAndValue getValue() {
        return new SchemaAndValue(valueData.schema(), valueData.value());
    }

    public SourceRecord getSourceRecord(OffsetManager offsetManager) {
        Map<String, Object> offsetMap = offsetManager.setCurrentOffsets(getPartitionMap(), getObjectKey(), getRecordNumber());
        return new SourceRecord(getPartitionMap(), offsetMap, topic, partition(), keyData.schema(),
                keyData.value(), valueData.schema(), valueData.value());
    }
}
