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

package io.aiven.kafka.connect.common.config;

import static io.aiven.kafka.connect.common.config.AivenCommonConfig.addOutputFieldsFormatConfigGroup;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

class AivenCommonConfigTest {

    private SinkCommonConfig.SinkCommonConfigDef getBaseConfigDefinition() {
        final SinkCommonConfig.SinkCommonConfigDef definition = new SinkCommonConfig.SinkCommonConfigDef(null,
                CompressionType.NONE);
        addOutputFieldsFormatConfigGroup(definition, OutputFieldType.VALUE);

        return definition;
    }

    @Test
    void avroOutputFormatFilename() {
        final Map<String, String> properties = Map.of("format.output.fields", "key,value", "format.output.type",
                "avro");
        final SinkCommonConfig.SinkCommonConfigDef definition = getBaseConfigDefinition();

        final AivenCommonConfig config = new AivenCommonConfig(definition, properties);
        assertThat(config.getFilename()).isEqualTo("{{topic}}-{{partition}}-{{start_offset}}.avro");
    }

    @Test
    void avroOutputFormatFilenameGzipCompression() {
        final Map<String, String> properties = Map.of("format.output.fields", "key,value", "format.output.type", "avro",
                "file.compression.type", "gzip");
        final SinkCommonConfig.SinkCommonConfigDef definition = getBaseConfigDefinition();

        final AivenCommonConfig config = new AivenCommonConfig(definition, properties);
        assertThat(config.getFilename()).isEqualTo("{{topic}}-{{partition}}-{{start_offset}}.avro.gz");
    }

    @Test
    void defaultOutputFormatFilename() {
        final Map<String, String> properties = Map.of("format.output.fields", "key,value");
        final SinkCommonConfig.SinkCommonConfigDef definition = getBaseConfigDefinition();

        final AivenCommonConfig config = new AivenCommonConfig(definition, properties);
        assertThat(config.getFilename()).isEqualTo("{{topic}}-{{partition}}-{{start_offset}}");
    }

    @Test
    void invalidEnvelopeConfiguration() {
        final Map<String, String> properties = Map.of("format.output.fields", "key,value", "format.output.envelope",
                "false");

        final SinkCommonConfig.SinkCommonConfigDef definition = new SinkCommonConfig.SinkCommonConfigDef(
                OutputFieldType.VALUE, CompressionType.NONE);

        assertThatThrownBy(() -> new AivenCommonConfig(definition, properties)).isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "When format.output.envelope is false, format.output.fields must contain only one field");
    }

    @Test
    void invalidMaxRecordsForKeyBasedGrouper() {
        final SinkCommonConfig.SinkCommonConfigDef definition = getBaseConfigDefinition();

        final Map<String, String> propertiesWithKey = Map.of("file.name.template", "{{key}}.gz", "file.max.records",
                "10");

        assertThatThrownBy(() -> new AivenCommonConfig(definition, propertiesWithKey))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "When file.name.template is {{key}}.gz, " + "file.max.records must be either 1 or not set");

        final Map<String, String> propertiesWithTopicPartitionKey = Map.of("file.name.template",
                "{{topic}}-{{partition}}-{{key}}.gz", "file.max.records", "10");

        // reset definition as some config settings were previously sent before the exception was thrown,
        // This will cause a duplicate config exception instead of testing the desied behaviour
        final SinkCommonConfig.SinkCommonConfigDef definitionWithKey = getBaseConfigDefinition();

        assertThatThrownBy(() -> new AivenCommonConfig(definitionWithKey, propertiesWithTopicPartitionKey))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("When file.name.template is {{topic}}-{{partition}}-{{key}}.gz, "
                        + "file.max.records must be either 1 or not set");

        final Map<String, String> propertiesWithoutKey = Map.of("file.max.records", "10");
        final SinkCommonConfig.SinkCommonConfigDef definitionWithoutKey = getBaseConfigDefinition();
        final AivenCommonConfig config = new AivenCommonConfig(definitionWithoutKey, propertiesWithoutKey);
        assertThat(config.getFilename()).isEqualTo("{{topic}}-{{partition}}-{{start_offset}}");
        assertThat(config.getMaxRecordsPerFile()).isEqualTo(10);
    }
}
