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

package io.aiven.kafka.connect.s3.source.input;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.config.AbstractConfig;

import org.apache.commons.io.function.IOSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteArrayTransformer implements Transformer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ByteArrayTransformer.class);

    @Override
    public void configureValueConverter(final Map<String, String> config, final AbstractConfig sourceConfig) {
        // For byte array transformations, ByteArrayConverter is the converter which is the default config.
    }

    @Override
    public Stream<Object> getRecords(final IOSupplier<InputStream> inputStreamIOSupplier, final String topic,
            final int topicPartition, final AbstractConfig sourceConfig) {

        // Create a Stream that processes each chunk lazily
        return StreamSupport.stream(new Spliterators.AbstractSpliterator<>(Long.MAX_VALUE, Spliterator.ORDERED) {
            final byte[] buffer = new byte[4096];

            @Override
            public boolean tryAdvance(final java.util.function.Consumer<? super Object> action) {
                try (InputStream inputStream = inputStreamIOSupplier.get()) {
                    final int bytesRead = inputStream.read(buffer);
                    if (bytesRead == -1) {
                        return false;
                    }
                    final byte[] chunk = new byte[bytesRead];
                    System.arraycopy(buffer, 0, chunk, 0, bytesRead);
                    action.accept(chunk);
                    return true;
                } catch (IOException e) {
                    LOGGER.error("Error trying to advance byte stream: {}", e.getMessage(), e);
                    return false;
                }
            }
        }, false);
    }

    @Override
    public byte[] getValueBytes(final Object record, final String topic, final AbstractConfig sourceConfig) {
        return (byte[]) record;
    }
}
