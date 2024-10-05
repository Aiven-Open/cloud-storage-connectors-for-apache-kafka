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

import static io.aiven.kafka.connect.s3.source.S3SourceTask.BUCKET;
import static io.aiven.kafka.connect.s3.source.S3SourceTask.PARTITION;
import static io.aiven.kafka.connect.s3.source.S3SourceTask.TOPIC;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.utils.OffsetManager;

public interface OutputWriter {

    void configureValueConverter(Map<String, String> config, S3SourceConfig s3SourceConfig);
    void handleValueData(Optional<byte[]> optionalKeyBytes, InputStream inputStream, String topic,
            List<ConsumerRecord<byte[], byte[]>> consumerRecordList, S3SourceConfig s3SourceConfig, int topicPartition,
            long startOffset, OffsetManager offsetManager, Map<Map<String, Object>, Long> currentOffsets)
            throws IOException;

    default ConsumerRecord<byte[], byte[]> getConsumerRecord(final Optional<byte[]> key, final byte[] value,
            final String bucketName, final String topic, final int topicPartition, final OffsetManager offsetManager,
            final Map<Map<String, Object>, Long> currentOffsets, final long startOffset) {
        final Map<String, Object> partitionMap = new HashMap<>();
        partitionMap.put(BUCKET, bucketName);
        partitionMap.put(TOPIC, topic);
        partitionMap.put(PARTITION, topicPartition);

        long currentOffset;

        if (offsetManager.getOffsets().containsKey(partitionMap)) {
            currentOffset = offsetManager.getIncrementedOffsetForPartition(partitionMap);
        } else {
            currentOffset = currentOffsets.getOrDefault(partitionMap, startOffset);
        }

        final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(topic, topicPartition, currentOffset,
                key.orElse(null), value);

        offsetManager.updateOffset(partitionMap, currentOffset);

        return record;
    }
}
