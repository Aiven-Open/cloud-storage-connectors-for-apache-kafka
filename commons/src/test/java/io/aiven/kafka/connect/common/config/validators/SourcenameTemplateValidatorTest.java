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
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;

import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.source.task.DistributionType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SourcenameTemplateValidatorTest {
    private static final String TEST_CONFIG_NAME = "TEST_CONFIG";

    @ParameterizedTest
    @CsvSource({ "{{topic}}-{{partition}}-{{start_offset}}.avro,partition", "*-{{partition}}.parquet,partition",
            "*.gz,object_hash", "'([a-zA-Z0-9_-]{3,5}.gz',object_hash" })
    void validateBasicSourceNamesPass(final String sourceName, final String distributionType) {
        assertThatCode(() -> new SourcenameTemplateValidator(DistributionType.forName(distributionType))
                .ensureValid(TEST_CONFIG_NAME, sourceName)).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @CsvSource({
            "{{topic}}-{{start_offset}}.avro, partition,'Invalid value {{topic}}-{{start_offset}}.avro for configuration TEST_CONFIG: Partition distribution type requires parameter {{partition}} in the file.name.template'",
            "*.parquet, partition,'Invalid value *.parquet for configuration TEST_CONFIG: Partition distribution type requires parameter {{partition}} in the file.name.template'",
            "' ', object_hash,'Invalid value must not be empty or not set for configuration TEST_CONFIG'" })
    void validateVariableWithInvalidParameterName(final String sourceName, final String distributionType,
            final String message) {

        assertThatThrownBy(() -> new SourcenameTemplateValidator(DistributionType.forName(distributionType))
                .ensureValid(TEST_CONFIG_NAME, sourceName)).isInstanceOf(ConfigException.class).hasMessage(message);
    }

    @ParameterizedTest
    @CsvSource({
            "{{topic}}-{{long}}.avro,partition,'Invalid value {{topic}}-{{long}}.avro for configuration TEST_CONFIG: unsupported template variable used ({{long}}), supported values are: {{partition}}, {{start_offset}}, {{timestamp}}, {{topic}}'",
            "{{filename}}-{{partition}}.parquet,partition,'Invalid value {{filename}}-{{partition}}.parquet for configuration TEST_CONFIG: unsupported template variable used ({{filename}}), supported values are: {{partition}}, {{start_offset}}, {{timestamp}}, {{topic}}'", })
    void validateCorrectExceptionsOnInvalidTemplates(final String sourceName, final String distributionType,
            final String message) {

        assertThatThrownBy(() -> new SourcenameTemplateValidator(DistributionType.forName(distributionType))
                .ensureValid(TEST_CONFIG_NAME, sourceName)).isInstanceOf(ConfigException.class).hasMessage(message);
    }
}
