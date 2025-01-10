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

import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.function.IOSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteArrayTransformer extends Transformer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ByteArrayTransformer.class);

    @Override
    public void configureValueConverter(final Map<String, String> config, final SourceCommonConfig sourceConfig) {
        // For byte array transformations, ByteArrayConverter is the converter which is the default config.

    }

    @Override
    public StreamSpliterator createSpliterator(final IOSupplier<InputStream> inputStreamIOSupplier, final String topic,
            final int topicPartition, final SourceCommonConfig sourceConfig) {
        // The max buffer size for the byte array the default is 4096 if not set by the user.
        final int maxBufferSize = sourceConfig.getByteArrayTransformerMaxBufferSize();
        return new StreamSpliterator(LOGGER, inputStreamIOSupplier) {
            @Override
            protected InputStream inputOpened(final InputStream input) {
                return input;
            }

            @Override
            protected void doClose() {
                // nothing to do.
            }

            @Override
            protected boolean doAdvance(final Consumer<? super SchemaAndValue> action) {
                final byte[] buffer = new byte[maxBufferSize];
                try {
                    final int bytesRead = IOUtils.read(inputStream, buffer);
                    if (bytesRead == 0) {
                        return false;
                    }
                    if (bytesRead < maxBufferSize) {
                        action.accept(new SchemaAndValue(null, Arrays.copyOf(buffer, bytesRead)));
                    } else {
                        action.accept(new SchemaAndValue(null, buffer));
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
    public SchemaAndValue getKeyData(final Object cloudStorageKey, final String topic,
            final SourceCommonConfig sourceConfig) {
        return new SchemaAndValue(null, ((String) cloudStorageKey).getBytes(StandardCharsets.UTF_8));
    }
}
