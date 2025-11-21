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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TransformerFragmentTest {

    @Test
    void validateCorrectBufferSizeIsAccepted() {
        final int bufferSize = 50;
        final ConfigDef configDef = TransformerFragment.update(new ConfigDef());
        final Map<String, Object> props = new HashMap<>();
        props.put(TransformerFragment.TRANSFORMER_MAX_BUFFER_SIZE, bufferSize);

        final TransformerFragment schemaReg = new TransformerFragment(
                FragmentDataAccess.from(new AbstractConfig(configDef, props)));
        assertThat(schemaReg.getTransformerMaxBufferSize()).isEqualTo(bufferSize);
    }

    @ParameterizedTest
    @CsvSource({
            "21474836471,Invalid value 21474836471 for configuration transformer.max.buffer.size: Not a number of type INT",
            "-1,Invalid value -1 for configuration transformer.max.buffer.size: Value must be at least 1 B",
            "MAX,Invalid value MAX for configuration transformer.max.buffer.size: Not a number of type INT",
            "0,Invalid value 0 for configuration transformer.max.buffer.size: Value must be at least 1 B",
            "-9000,Invalid value -9000 for configuration transformer.max.buffer.size: Value must be at least 1 B",
            "MTA=,Invalid value MTA= for configuration transformer.max.buffer.size: Not a number of type INT" })
    void validateInvalidBufferSizeThrowsConfigException(final String value, final String expectedMessage) {
        final ConfigDef configDef = TransformerFragment.update(new ConfigDef());
        final Map<String, Object> props = new HashMap<>();

        props.put(TransformerFragment.TRANSFORMER_MAX_BUFFER_SIZE, value);
        assertThatThrownBy(() -> new TransformerFragment(FragmentDataAccess.from(new AbstractConfig(configDef, props))))
                .isInstanceOf(ConfigException.class)
                .hasMessage(expectedMessage);
    }

}
