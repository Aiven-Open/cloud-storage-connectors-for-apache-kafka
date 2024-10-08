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

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.SCHEMAS_ENABLE;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.utils.OffsetManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonWriter implements OutputWriter {

    final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configureValueConverter(final Map<String, String> config, final S3SourceConfig s3SourceConfig) {
        config.put(SCHEMAS_ENABLE, "false");
    }

    @Override
    @SuppressWarnings("PMD.ExcessiveParameterList")
    public void handleValueData(final Optional<byte[]> optionalKeyBytes, final InputStream inputStream,
            final String topic, final List<ConsumerRecord<byte[], byte[]>> consumerRecordList,
            final S3SourceConfig s3SourceConfig, final int topicPartition, final long startOffset,
            final OffsetManager offsetManager, final Map<Map<String, Object>, Long> currentOffsets,
            final Map<String, Object> partitionMap) {
        final byte[] valueBytes = serializeJsonData(inputStream);
        if (valueBytes.length > 0) {
            consumerRecordList.add(OutputUtils.getConsumerRecord(optionalKeyBytes, valueBytes, topic, topicPartition,
                    offsetManager, currentOffsets, startOffset, partitionMap));
        }
    }

    private byte[] serializeJsonData(final InputStream inputStream) {
        final JsonNode jsonNode;
        try {
            jsonNode = objectMapper.readTree(inputStream);
            return objectMapper.writeValueAsBytes(jsonNode);
        } catch (IOException e) {
            LOGGER.error("Error in reading s3 object stream " + e.getMessage());
        }
        return new byte[0];
    }
}
