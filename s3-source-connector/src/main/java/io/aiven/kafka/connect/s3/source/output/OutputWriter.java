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

package io.aiven.kafka.connect.s3.source.output;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.utils.OffsetManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface OutputWriter {

    Logger LOGGER = LoggerFactory.getLogger(AvroWriter.class);

    void configureValueConverter(Map<String, String> config, S3SourceConfig s3SourceConfig);

    @SuppressWarnings("PMD.ExcessiveParameterList")
    void handleValueData(Optional<byte[]> optionalKeyBytes, InputStream inputStream, String topic,
            List<ConsumerRecord<byte[], byte[]>> consumerRecordList, S3SourceConfig s3SourceConfig, int topicPartition,
            long startOffset, OffsetManager offsetManager, Map<Map<String, Object>, Long> currentOffsets,
            Map<String, Object> partitionMap);

    default ConsumerRecord<byte[], byte[]> getConsumerRecord(final Optional<byte[]> key, final byte[] value,
            final String topic, final int topicPartition, final OffsetManager offsetManager,
            final Map<Map<String, Object>, Long> currentOffsets, final long startOffset,
            final Map<String, Object> partitionMap) {

        long currentOffset;

        if (offsetManager.getOffsets().containsKey(partitionMap)) {
            currentOffset = offsetManager.incrementAndUpdateOffsetMap(partitionMap);
        } else {
            currentOffset = currentOffsets.getOrDefault(partitionMap, startOffset);
            currentOffsets.put(partitionMap, currentOffset + 1);
        }

        return new ConsumerRecord<>(topic, topicPartition, currentOffset, key.orElse(null), value);
    }
}
