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

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.SCHEMA_REGISTRY_URL;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.VALUE_SERIALIZER;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
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

final public class OutputUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(OutputUtils.class);

    private OutputUtils() {
        // hidden
    }

    static byte[] serializeAvroRecordToBytes(final List<GenericRecord> avroRecords, final String topic,
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

    @SuppressWarnings("PMD.ExcessiveParameterList")
    static void buildConsumerRecordList(final Optional<byte[]> optionalKeyBytes, final String topic,
            final List<ConsumerRecord<byte[], byte[]>> consumerRecordList, final S3SourceConfig s3SourceConfig,
            final int topicPartition, final long startOffset, final OffsetManager offsetManager,
            final Map<Map<String, Object>, Long> currentOffsets, final List<GenericRecord> records,
            final Map<String, Object> partitionMap) {
        for (final GenericRecord record : records) {
            final byte[] valueBytes = OutputUtils.serializeAvroRecordToBytes(Collections.singletonList(record), topic,
                    s3SourceConfig);
            if (valueBytes.length > 0) {
                consumerRecordList.add(getConsumerRecord(optionalKeyBytes, valueBytes, topic, topicPartition,
                        offsetManager, currentOffsets, startOffset, partitionMap));
            }
        }
    }

    static ConsumerRecord<byte[], byte[]> getConsumerRecord(final Optional<byte[]> key, final byte[] value,
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
