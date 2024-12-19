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

package io.aiven.kafka.connect.common.source.input;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.SchemaAndValue;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.function.IOSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteArrayTransformer extends Transformer<byte[]> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ByteArrayTransformer.class);

    private static final int MAX_BUFFER_SIZE = 4096;

    @Override
    public void configureValueConverter(final Map<String, String> config, final AbstractConfig sourceConfig) {
        // For byte array transformations, ByteArrayConverter is the converter which is the default config.
    }

    @Override
    public StreamSpliterator<byte[]> createSpliterator(final IOSupplier<InputStream> inputStreamIOSupplier,
            final String topic, final int topicPartition, final AbstractConfig sourceConfig) {
        return new StreamSpliterator<byte[]>(LOGGER, inputStreamIOSupplier) {
            @Override
            protected InputStream inputOpened(final InputStream input) {
                return input;
            }

            @Override
            protected void doClose() {
                // nothing to do.
            }

            @Override
            protected boolean doAdvance(final Consumer<? super byte[]> action) {
                final byte[] buffer = new byte[MAX_BUFFER_SIZE];
                try {
                    final int bytesRead = IOUtils.read(inputStream, buffer);
                    if (bytesRead == 0) {
                        return false;
                    }
                    if (bytesRead < MAX_BUFFER_SIZE) {
                        action.accept(Arrays.copyOf(buffer, bytesRead));
                    } else {
                        action.accept(buffer);
                    }
                    return true;
                } catch (IOException e) {
                    LOGGER.error("Error trying to advance inputStream: {}", e.getMessage(), e);
                    return false;
                }
            }
        };
    }

    @Override
    public SchemaAndValue getValueData(final byte[] record, final String topic, final AbstractConfig sourceConfig) {
        return new SchemaAndValue(null, record);
    }

    @Override
    public SchemaAndValue getKeyData(final Object cloudStorageKey, final String topic,
            final AbstractConfig sourceConfig) {
        return new SchemaAndValue(null, ((String) cloudStorageKey).getBytes(StandardCharsets.UTF_8));
    }
}
