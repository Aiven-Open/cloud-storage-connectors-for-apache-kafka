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
import java.util.Arrays;
import java.util.function.Consumer;

import io.aiven.kafka.connect.common.OffsetManager;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.function.IOSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A transformer that  reads plain bytes from the input stream.
 */
public class ByteArrayTransformer extends Transformer {
    /** The logger for this transformer */
    private static final Logger LOGGER = LoggerFactory.getLogger(ByteArrayTransformer.class);
    /** The maximum record size this transform will read.  If more data is sent then the record is ignored. */
    private static final int MAX_BUFFER_SIZE = 4096;

    @Override
    public Schema getKeySchema() {
        return null;
    }

    @Override
    public StreamSpliterator createSpliterator(final IOSupplier<InputStream> inputStreamIOSupplier, final OffsetManager.OffsetManagerEntry<?> offsetManagerEntry, final AbstractConfig sourceConfig) {
        return new StreamSpliterator(LOGGER, inputStreamIOSupplier, offsetManagerEntry) {
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
                final byte[] buffer = new byte[MAX_BUFFER_SIZE];
                try {
                    final int bytesRead = IOUtils.read(inputStream, buffer);
                    if (bytesRead == 0) {
                        return false;
                    }
                    if (bytesRead < MAX_BUFFER_SIZE) {
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
}
