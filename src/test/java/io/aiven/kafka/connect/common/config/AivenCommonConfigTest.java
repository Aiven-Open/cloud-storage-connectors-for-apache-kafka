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

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import static io.aiven.kafka.connect.common.config.AivenCommonConfig.addOutputFieldsFormatConfigGroup;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AivenCommonConfigTest {

    private ConfigDef getBaseConfigDefinition() {
        final ConfigDef definition = new ConfigDef();
        addOutputFieldsFormatConfigGroup(definition, OutputFieldType.VALUE);

        definition.define(AivenCommonConfig.FILE_NAME_TEMPLATE_CONFIG,
            ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
            "File name template", "File", 1, ConfigDef.Width.LONG,
            "FILE_NAME_TEMPLATE_CONFIG");
        definition.define(AivenCommonConfig.FILE_COMPRESSION_TYPE_CONFIG,
            ConfigDef.Type.STRING, CompressionType.NONE.name, ConfigDef.Importance.MEDIUM,
            "File compression", "File", 2, ConfigDef.Width.NONE,
            "FILE_COMPRESSION_TYPE_CONFIG");

        return definition;
    }

    @Test
    void avroOutputFormatFilename() {
        final Map<String, String> properties = Map.of(
            "format.output.fields", "key,value",
            "format.output.type", "avro"
        );
        final ConfigDef definition = getBaseConfigDefinition();

        final AivenCommonConfig config = new AivenCommonConfig(definition, properties);
        assertThat(config.getFilename()).isEqualTo("{{topic}}-{{partition}}-{{start_offset}}.avro");
    }

    @Test
    void avroOutputFormatFilenameGzipCompression() {
        final Map<String, String> properties = Map.of(
            "format.output.fields", "key,value",
            "format.output.type", "avro",
            "file.compression.type", "gzip"
        );
        final ConfigDef definition = getBaseConfigDefinition();

        final AivenCommonConfig config = new AivenCommonConfig(definition, properties);
        assertThat(config.getFilename()).isEqualTo("{{topic}}-{{partition}}-{{start_offset}}.avro.gz");
    }

    @Test
    void defaultOutputFormatFilename() {
        final Map<String, String> properties = Map.of(
            "format.output.fields", "key,value"
        );
        final ConfigDef definition = getBaseConfigDefinition();

        final AivenCommonConfig config = new AivenCommonConfig(definition, properties);
        assertThat(config.getFilename()).isEqualTo("{{topic}}-{{partition}}-{{start_offset}}");
    }

    @Test
    void invalidEnvelopeConfiguration() {
        final Map<String, String> properties = Map.of(
            "format.output.fields", "key,value",
            "format.output.envelope", "false"
        );

        final ConfigDef definition = new ConfigDef();
        addOutputFieldsFormatConfigGroup(definition, OutputFieldType.VALUE);


        assertThatThrownBy(() -> new AivenCommonConfig(definition, properties))
            .isInstanceOf(ConfigException.class)
            .hasMessage("When format.output.envelope is false, format.output.fields must contain only one field");
    }
}
