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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;

public class AivenS3SourceRecord {
    private final Map<String, Object> partitionMap;
    private final Map<String, Object> offsetMap;
    private final String toTopic;
    private final int topicPartition;
    private final byte[] recordKey;
    private final byte[] recordValue;

    public AivenS3SourceRecord(final Map<String, Object> partitionMap, final Map<String, Object> offsetMap,
            final String toTopic, final int topicPartition, final byte[] recordKey, final byte[] recordValue) {
        this.partitionMap = new HashMap<>(partitionMap);
        this.offsetMap = new HashMap<>(offsetMap);

        this.toTopic = toTopic;
        this.topicPartition = topicPartition;
        this.recordKey = Arrays.copyOf(recordKey, recordKey.length);
        this.recordValue = Arrays.copyOf(recordValue, recordValue.length);
    }

    public Map<String, Object> getPartitionMap() {
        return Collections.unmodifiableMap(partitionMap);
    }

    public Map<String, Object> getOffsetMap() {
        return Collections.unmodifiableMap(offsetMap);
    }

    public String getToTopic() {
        return toTopic;
    }

    public int partition() {
        return topicPartition;
    }

    public byte[] key() {
        return recordKey.clone();
    }

    public byte[] value() {
        return recordValue.clone();
    }

    public SourceRecord getSourceRecord(final String topic, final Optional<SchemaAndValue> keyData,
            final SchemaAndValue schemaAndValue) {
        return new SourceRecord(getPartitionMap(), getOffsetMap(), topic, partition(),
                keyData.map(SchemaAndValue::schema).orElse(null), keyData.map(SchemaAndValue::value).orElse(null),
                schemaAndValue.schema(), schemaAndValue.value());
    }
}
