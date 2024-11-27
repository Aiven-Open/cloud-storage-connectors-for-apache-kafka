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

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.SCHEMAS_ENABLE;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.config.AbstractConfig;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.function.IOSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonTransformer implements Transformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonTransformer.class);

    final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configureValueConverter(final Map<String, String> config, final AbstractConfig sourceConfig) {
        config.put(SCHEMAS_ENABLE, "false");
    }

    @Override
    public Stream<Object> getRecords(final IOSupplier<InputStream> inputStreamIOSupplier, final String topic,
            final int topicPartition, final AbstractConfig sourceConfig) {
        return readJsonRecordsAsStream(inputStreamIOSupplier);
    }

    @Override
    public byte[] getValueBytes(final Object record, final String topic, final AbstractConfig sourceConfig) {
        try {
            return objectMapper.writeValueAsBytes(record);
        } catch (JsonProcessingException e) {
            LOGGER.error("Failed to serialize record to JSON bytes. Error: {}", e.getMessage(), e);
            return new byte[0];
        }
    }

    private Stream<Object> readJsonRecordsAsStream(final IOSupplier<InputStream> inputStreamIOSupplier) {
        // Use a Stream that lazily processes each line as a JSON object
        CustomSpliterator customSpliteratorParam;
        try {
            customSpliteratorParam = new CustomSpliterator(inputStreamIOSupplier);
        } catch (IOException e) {
            LOGGER.error("Error creating Json transformer CustomSpliterator: {}", e.getMessage(), e);
            return Stream.empty();
        }
        return StreamSupport.stream(customSpliteratorParam, false).onClose(() -> {
            try {
                customSpliteratorParam.reader.close(); // Ensure the reader is closed after streaming
            } catch (IOException e) {
                LOGGER.error("Error closing BufferedReader: {}", e.getMessage(), e);
            }
        });
    }

    /*
     * This CustomSpliterator class is created so that BufferedReader instantiation is not closed before the all the
     * records from stream is closed. With this now, we have a onclose method declared in parent declaration.
     */
    final class CustomSpliterator extends Spliterators.AbstractSpliterator<Object> {
        BufferedReader reader;
        String line;
        CustomSpliterator(final IOSupplier<InputStream> inputStreamIOSupplier) throws IOException {
            super(Long.MAX_VALUE, Spliterator.ORDERED | Spliterator.NONNULL);
            reader = new BufferedReader(new InputStreamReader(inputStreamIOSupplier.get(), StandardCharsets.UTF_8));
        }

        @Override
        public boolean tryAdvance(final java.util.function.Consumer<? super Object> action) {
            try {
                if (line == null) {
                    line = reader.readLine();
                }
                while (line != null) {
                    line = line.trim();
                    if (!line.isEmpty()) {
                        try {
                            final JsonNode jsonNode = objectMapper.readTree(line); // Parse the JSON
                            // line
                            action.accept(jsonNode); // Provide the parsed JSON node to the stream
                        } catch (IOException e) {
                            LOGGER.error("Error parsing JSON record: {}", e.getMessage(), e);
                        }
                        line = null; // NOPMD
                        return true;
                    }
                    line = reader.readLine();
                }
                return false; // End of file
            } catch (IOException e) {
                LOGGER.error("Error reading S3 object stream: {}", e.getMessage(), e);
                return false;
            }
        }
    }
}
