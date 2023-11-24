/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.kafka.connect.gcs.config;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigValue;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.gcs.GcsSinkConfig;

import org.junit.jupiter.api.Test;

/**
 * Tests {@link GcsSinkConfig}'s config definition.
 */
final class GcsSinkConfigValidationTest {

    @Test
    void recommendedValuesForCompression() {
        final Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put("file.compression.type", "unknown");

        final ConfigValue configValue = GcsSinkConfig.configDef()
                .validate(connectorProps)
                .stream()
                .filter(x -> "file.compression.type".equals(x.name()))
                .findFirst()
                .get();
        assertIterableEquals(CompressionType.names(), configValue.recommendedValues());
    }

    @Test
    void recommendedValuesForFields() {
        final Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put("format.output.fields", "unknown");

        final ConfigValue configValue = GcsSinkConfig.configDef()
                .validate(connectorProps)
                .stream()
                .filter(x -> "format.output.fields".equals(x.name()))
                .findFirst()
                .get();
        assertIterableEquals(OutputFieldType.names(), configValue.recommendedValues());
    }
}
