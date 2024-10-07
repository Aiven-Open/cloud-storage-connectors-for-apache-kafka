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
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.SCHEMA_REGISTRY_URL;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.VALUE_SERIALIZER;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.utils.OffsetManager;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface OutputWriter {

    Logger LOGGER = LoggerFactory.getLogger(AvroWriter.class);

    void configureValueConverter(Map<String, String> config, S3SourceConfig s3SourceConfig);
    void handleValueData(Optional<byte[]> optionalKeyBytes, InputStream inputStream, String topic,
            List<ConsumerRecord<byte[], byte[]>> consumerRecordList, S3SourceConfig s3SourceConfig, int topicPartition,
            long startOffset, OffsetManager offsetManager, Map<Map<String, Object>, Long> currentOffsets);

    default ConsumerRecord<byte[], byte[]> getConsumerRecord(final Optional<byte[]> key, final byte[] value,
            final String bucketName, final String topic, final int topicPartition, final OffsetManager offsetManager,
            final Map<Map<String, Object>, Long> currentOffsets, final long startOffset) {
        final Map<String, Object> partitionMap = new HashMap<>();
        partitionMap.put(BUCKET, bucketName);
        partitionMap.put(TOPIC, topic);
        partitionMap.put(PARTITION, topicPartition);

        long currentOffset;

        if (offsetManager.getOffsets().containsKey(partitionMap)) {
            currentOffset = offsetManager.incrementAndUpdateOffsetMap(partitionMap);
        } else {
            currentOffset = currentOffsets.getOrDefault(partitionMap, startOffset);
            currentOffsets.put(partitionMap, currentOffset + 1);
        }

        return new ConsumerRecord<>(topic, topicPartition, currentOffset, key.orElse(null), value);
    }

    default byte[] serializeAvroRecordToBytes(final List<GenericRecord> avroRecords, final String topic,
            final S3SourceConfig s3SourceConfig) {
        final Map<String, String> config = Collections.singletonMap(SCHEMA_REGISTRY_URL,
                s3SourceConfig.getString(SCHEMA_REGISTRY_URL));

        try (KafkaAvroSerializer avroSerializer = (KafkaAvroSerializer) s3SourceConfig.getClass(VALUE_SERIALIZER)
                .getDeclaredConstructor()
                .newInstance(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            avroSerializer.configure(config, false);
            for (final GenericRecord avroRecord : avroRecords) {
                out.write(avroSerializer.serialize(topic, avroRecord));
            }
            return out.toByteArray();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException
                | IOException e) {
            LOGGER.error("Error in reading s3 object stream for topic " + topic + " with error : " + e.getMessage());
        }
        return new byte[0];
    }
}
