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

package io.aiven.kafka.connect.common.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

public class SchemaRegistryFragmentTest {// NOPMD

    @Test
    void validateCorrectBufferSizeIsAccepted() {
        final int bufferSize = 50;
        final ConfigDef configDef = SchemaRegistryFragment.update(new ConfigDef());
        final Map<String, Object> props = new HashMap<>();
        props.put(SchemaRegistryFragment.BYTE_ARRAY_TRANSFORMER_MAX_BUFFER_SIZE, bufferSize);

        final SchemaRegistryFragment schemaReg = new SchemaRegistryFragment(new AbstractConfig(configDef, props));
        assertThat(schemaReg.getByteArrayTransformerMaxBufferSize()).isEqualTo(bufferSize);
    }

    @Test
    void validateInvalidBufferSizeThrowsConfigException() {
        final ConfigDef configDef = SchemaRegistryFragment.update(new ConfigDef());
        final Map<String, Object> props = new HashMap<>();
        props.put(SchemaRegistryFragment.BYTE_ARRAY_TRANSFORMER_MAX_BUFFER_SIZE, 0);

        assertThatThrownBy(() -> new SchemaRegistryFragment(new AbstractConfig(configDef, props)))
                .isInstanceOf(ConfigException.class);
    }

}
