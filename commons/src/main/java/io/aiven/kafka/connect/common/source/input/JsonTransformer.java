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

import static io.aiven.kafka.connect.common.config.SchemaRegistryFragment.SCHEMAS_ENABLE;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.kafka.common.config.AbstractConfig;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.function.IOSupplier;
import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonTransformer extends Transformer<JsonNode> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonTransformer.class);

    final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configureValueConverter(final Map<String, String> config, final AbstractConfig sourceConfig) {
        config.put(SCHEMAS_ENABLE, "false");
    }

    @Override
    public StreamSpliterator<JsonNode> createSpliterator(final IOSupplier<InputStream> inputStreamIOSupplier,
            final String topic, final int topicPartition, final AbstractConfig sourceConfig) {
        final StreamSpliterator<JsonNode> spliterator = new StreamSpliterator<JsonNode>(LOGGER, inputStreamIOSupplier) {
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
            public boolean doAdvance(final Consumer<? super JsonNode> action) {
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
                    try {
                        action.accept(objectMapper.readTree(line)); // Parse the JSON
                    } catch (IOException e) {
                        LOGGER.error("Error parsing JSON record: {}", e.getMessage(), e);
                        return false;
                    }
                    return true;
                } catch (IOException e) {
                    LOGGER.error("Error reading input stream: {}", e.getMessage(), e);
                    return false;
                }
            }
        };

        return spliterator;
    }

    @Override
    public byte[] getValueBytes(final JsonNode record, final String topic, final AbstractConfig sourceConfig) {
        try {
            return objectMapper.writeValueAsBytes(record);
        } catch (JsonProcessingException e) {
            LOGGER.error("Failed to serialize record to JSON bytes. Error: {}", e.getMessage(), e);
            return new byte[0];
        }
    }
}
