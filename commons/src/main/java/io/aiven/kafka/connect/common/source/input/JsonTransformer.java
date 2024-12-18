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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;

import io.aiven.kafka.connect.common.OffsetManager;

import org.apache.commons.io.function.IOSupplier;
import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transformer to read the input file line by line and process each line as a Json object.
 */
public class JsonTransformer extends Transformer {
    /** The json converter to read with */
    private final JsonConverter jsonConverter;
    /** The logger for this transform */
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonTransformer.class);

    /**
     * Constructs a Json transform with the specified converter.
     *
     * @param jsonConverter
     *            the Json converter to read with.
     */
    JsonTransformer(final JsonConverter jsonConverter) {
        super();
        this.jsonConverter = jsonConverter;
    }

    @Override
    public Schema getKeySchema() {
        return null;
    }

    @Override
    public StreamSpliterator createSpliterator(final IOSupplier<InputStream> inputStreamIOSupplier,
            final OffsetManager.OffsetManagerEntry<?> offsetManagerEntry, final AbstractConfig sourceConfig) {
        return new StreamSpliterator(LOGGER, inputStreamIOSupplier, offsetManagerEntry) {
            BufferedReader reader;

            @Override
            protected InputStream inputOpened(final InputStream input) throws IOException {
                reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
                return input;
            }

            @Override
            public void doClose() {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        LOGGER.error("Error closing reader: {}", e.getMessage(), e);
                    }
                }
            }

            @Override
            public boolean doAdvance(final Consumer<? super SchemaAndValue> action) {
                String line = null;
                try {
                    // remove blank and empty lines.
                    while (StringUtils.isBlank(line)) {
                        line = reader.readLine();
                        if (line == null) {
                            // end of file
                            return false;
                        }
                    }
                    line = line.trim();
                    action.accept(jsonConverter.toConnectData(offsetManagerEntry.getTopic(),
                            line.getBytes(StandardCharsets.UTF_8)));
                    return true;
                } catch (IOException e) {
                    LOGGER.error("Error reading input stream: {}", e.getMessage(), e);
                    return false;
                }
            }
        };
    }
}
