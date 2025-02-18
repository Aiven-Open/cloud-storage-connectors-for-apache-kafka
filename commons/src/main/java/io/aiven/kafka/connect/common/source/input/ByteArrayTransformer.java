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
import java.util.function.Consumer;

import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.task.Context;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.function.IOSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ByteArrayTransformer chunks an entire object into a maximum size specified by the
 * {@link io.aiven.kafka.connect.common.config.TransformerFragment#TRANSFORMER_MAX_BUFFER_SIZE} configuration option.
 * <p>
 * If the configuration option specifies a buffer that is smaller than the length of the input stream, the record will
 * be split into multiple parts. When this happens the transformer makes no guarantees for only once delivery or
 * delivery order as those are dependant upon the Kafka producer and remote consumer configurations. This class will
 * produce the blocks in order and on restart will send any blocks that were not acknowledged by Kafka.
 * </p>
 */
public class ByteArrayTransformer extends Transformer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ByteArrayTransformer.class);

    @Override
    public StreamSpliterator createSpliterator(final IOSupplier<InputStream> inputStreamIOSupplier,
            final long streamLength, final Context<?> context, final SourceCommonConfig sourceConfig) {
        if (streamLength == 0) {
            LOGGER.warn(
                    "Object sent for processing has an invalid streamLength of {}, object is empty returning an empty spliterator.",
                    streamLength);
            return emptySpliterator(inputStreamIOSupplier);
        }
        // The max buffer size for the byte array the default is 4096 if not set by the user.
        final int maxBufferSize = sourceConfig.getTransformerMaxBufferSize();
        return new StreamSpliterator(LOGGER, inputStreamIOSupplier) {

            @Override
            protected void inputOpened(final InputStream input) {

            }

            @Override
            protected void doClose() {
                // nothing to do.
            }

            @Override
            protected boolean doAdvance(final Consumer<? super SchemaAndValue> action) {

                try {
                    final byte[] buffer = new byte[maxBufferSize];
                    final byte[] chunk = Arrays.copyOf(buffer, IOUtils.read(inputStream, buffer));
                    if (chunk.length > 0) {
                        action.accept(new SchemaAndValue(null, chunk));
                        return true;
                    }

                    return false;
                } catch (IOException e) {
                    LOGGER.error("Error trying to advance inputStream: {}", e.getMessage(), e);
                    return false;
                }
            }
        };
    }

    /**
     * This method returns an empty spliterator when an empty input stream is supplied to be split
     *
     * @param inputStreamIOSupplier
     *            The empty input stream that was supplied
     * @return an Empty spliterator to return to the calling method.
     */
    private static StreamSpliterator emptySpliterator(final IOSupplier<InputStream> inputStreamIOSupplier) {
        return new StreamSpliterator(LOGGER, inputStreamIOSupplier) {
            @Override
            protected boolean doAdvance(final Consumer<? super SchemaAndValue> action) {
                return false;
            }

            @Override
            protected void doClose() {
                // nothing to do
            }

            @Override
            protected void inputOpened(final InputStream input) {
            }

        };
    }

    @Override
    public SchemaAndValue getKeyData(final Object cloudStorageKey, final String topic,
            final SourceCommonConfig sourceConfig) {
        return new SchemaAndValue(null, ((String) cloudStorageKey).getBytes(StandardCharsets.UTF_8));
    }
}
