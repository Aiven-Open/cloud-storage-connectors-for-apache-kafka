/*
 * Copyright 2021 Aiven Oy
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

package io.aiven.kafka.connect.common.config.validators;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

class FilenameTemplateValidatorTest {
    private static final String TEST_CONFIG_NAME = "TEST_CONFIG";
    private final FilenameTemplateValidator validator = new FilenameTemplateValidator(TEST_CONFIG_NAME);

    @Test
    void validateVariableWithInvalidParameterName() {
        final String value = "{{topic}}-{{partition:qwe=true}}-{{start_offset}}";
        final String message = "Invalid value {{topic}}-{{partition:qwe=true}}-{{start_offset}} for configuration TEST_CONFIG: unsupported set of template variables parameters, supported sets are: partition:padding=true|false,start_offset:padding=true|false,timestamp:unit=yyyy|MM|dd|HH";

        assertThatThrownBy(() -> validator.ensureValid(TEST_CONFIG_NAME, value)).isInstanceOf(ConfigException.class)
                .hasMessage(message);
    }
}
