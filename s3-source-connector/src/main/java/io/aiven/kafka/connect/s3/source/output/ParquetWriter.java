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

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.utils.OffsetManager;
import io.aiven.kafka.connect.s3.source.utils.ParquetUtils;

import org.apache.avro.generic.GenericRecord;

public class ParquetWriter implements OutputWriter {

    private final String bucketName;

    public ParquetWriter(final String bucketName) {
        this.bucketName = bucketName;
    }
    @Override
    public void configureValueConverter(final Map<String, String> config, final S3SourceConfig s3SourceConfig) {
        config.put(SCHEMA_REGISTRY_URL, s3SourceConfig.getString(SCHEMA_REGISTRY_URL));
    }

    @Override
    public void handleValueData(final Optional<byte[]> optionalKeyBytes, final InputStream inputStream,
            final String topic, final List<ConsumerRecord<byte[], byte[]>> consumerRecordList,
            final S3SourceConfig s3SourceConfig, final int topicPartition, final long startOffset,
            final OffsetManager offsetManager, final Map<Map<String, Object>, Long> currentOffsets) throws IOException {
        final List<GenericRecord> records = ParquetUtils.getRecords(inputStream, topic);
        for (final GenericRecord record : records) {
            final byte[] valueBytes = serializeAvroRecordToBytes(Collections.singletonList(record), topic,
                    s3SourceConfig);
            consumerRecordList.add(getConsumerRecord(optionalKeyBytes, valueBytes, this.bucketName, topic,
                    topicPartition, offsetManager, currentOffsets, startOffset));
        }
    }
}
