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

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.MAX_MESSAGE_BYTES_SIZE;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteArrayTransformer implements Transformer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ByteArrayTransformer.class);

    @Override
    public void configureValueConverter(final Map<String, String> config, final S3SourceConfig s3SourceConfig) {
        // For byte array transformations, no explicit converter is configured.
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    @Override
    public List<Object> getRecords(final InputStream inputStream, final String topic, final int topicPartition,
            final S3SourceConfig s3SourceConfig) {

        final int maxMessageBytesSize = s3SourceConfig.getInt(MAX_MESSAGE_BYTES_SIZE);
        final byte[] buffer = new byte[maxMessageBytesSize];
        int bytesRead;

        final List<Object> chunks = new ArrayList<>();
        try {
            bytesRead = inputStream.read(buffer);
            while (bytesRead != -1) {
                final byte[] chunk = new byte[bytesRead];
                System.arraycopy(buffer, 0, chunk, 0, bytesRead);
                chunks.add(chunk);
                bytesRead = inputStream.read(buffer);
            }
        } catch (IOException e) {
            LOGGER.error("Error reading from input stream: {}", e.getMessage(), e);
        }

        return chunks;
    }

    @Override
    public byte[] getValueBytes(final Object record, final String topic, final S3SourceConfig s3SourceConfig) {
        return (byte[]) record;
    }
}
