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
import java.util.HashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;

import org.apache.commons.io.function.IOSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonTransformer implements Transformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonTransformer.class);

    private static final JsonConverter JSON_CONVERTER = new JsonConverter();

    private boolean isConfigured;

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
    public SchemaAndValue getValueData(final Object record, final String topic, final AbstractConfig sourceConfig) {
        ensureJsonConverterConfigured();
        return JSON_CONVERTER.toConnectData(topic, (byte[]) record);
    }

    @Override
    public SchemaAndValue getKeyData(final Object record, final String topic, final AbstractConfig sourceConfig) {
        return new SchemaAndValue(null, ((String) record).getBytes(StandardCharsets.UTF_8));
    }

    private void ensureJsonConverterConfigured() {
        // This ensures configuration only if not already configured
        if (!isConfigured) {
            final Map<String, String> config = new HashMap<>();
            config.put(SCHEMAS_ENABLE, "false");
            JSON_CONVERTER.configure(config, false);
            isConfigured = true;
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
                        action.accept(line.getBytes(StandardCharsets.UTF_8));
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
