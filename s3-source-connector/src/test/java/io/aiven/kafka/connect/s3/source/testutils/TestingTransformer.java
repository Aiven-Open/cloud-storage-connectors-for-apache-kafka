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

package io.aiven.kafka.connect.s3.source.testutils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.common.config.AbstractConfig;

import io.aiven.kafka.connect.common.source.input.Transformer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.function.IOSupplier;

/**
 * Helper class to transform bytes to the same bytes with "TESTING: " prefixed.
 */
public class TestingTransformer implements Transformer { // NOPMD not test class but a utility
    @Override
    public void configureValueConverter(final Map<String, String> config, final AbstractConfig sourceConfig) {
        config.put("TestingTransformer", "Operational");
    }

    @Override
    public Stream<Object> getRecords(final IOSupplier<InputStream> inputStreamIOSupplier, final String topic,
            final int topicPartition, final AbstractConfig sourceConfig, final long skipRecords) {
        try (InputStream input = inputStreamIOSupplier.get();
                InputStreamReader reader = new InputStreamReader(input, StandardCharsets.UTF_8)) {
            final List<String> lines = IOUtils.readLines(reader);
            final List<Object> result = new ArrayList<>();
            for (int i = 0; i < lines.size(); i++) {
                if (i >= skipRecords) {
                    result.add(("TESTING: " + lines.get(i)).getBytes(StandardCharsets.UTF_8));
                }
            }
            return result.stream();
        } catch (IOException e) {
            throw new RuntimeException(e); // NOPMD allow RuntimeExeption
        }
    }

    @Override
    public byte[] getValueBytes(final Object record, final String topic, final AbstractConfig sourceConfig) {
        return (byte[]) record;
    }
}
