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

package io.aiven.kafka.connect.azure.sink.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

import io.aiven.kafka.connect.azure.sink.AzureBlobSinkConfig;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.common.templating.VariableTemplatePart;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests {@link AzureBlobSinkConfig} class.
 */
final class AzureSinkConfigTest {

    static final String TEMPLATE_VARIABLES = "topic,partition,start_offset,timestamp; "
            + "topic,partition,key,start_offset,timestamp; key; key,topic,partition";

    @ParameterizedTest
    @ValueSource(strings = { "", "{{topic}}", "{{partition}}", "{{start_offset}}", "{{topic}}-{{partition}}",
            "{{topic}}-{{start_offset}}", "{{partition}}-{{start_offset}}",
            "{{topic}}-{{partition}}-{{start_offset}}-{{unknown}}" })
    void incorrectFilenameTemplates(final String template) {
        final Map<String, String> properties = Map.of("file.name.template", template, "azure.storage.container.name",
                "some-container", "azure.storage.connection.string", "somestring");

        final ConfigValue configValue = AzureBlobSinkConfig.configDef()
                .validate(properties)
                .stream()
                .filter(x -> FileNameFragment.FILE_NAME_TEMPLATE_CONFIG.equals(x.name()))
                .findFirst()
                .orElseThrow();
        assertThat(configValue.errorMessages()).isNotEmpty();

        assertThatThrownBy(() -> new AzureBlobSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessageContaining("for configuration file.name.template: ");
    }

    @Test
    void acceptMultipleParametersWithTheSameName() {
        final Map<String, String> properties = Map.of("file.name.template",
                "{{topic}}-{{timestamp:unit=yyyy}}-" + "{{timestamp:unit=MM}}-{{timestamp:unit=dd}}"
                        + "-{{partition:padding=true}}-{{start_offset:padding=true}}.gz",
                "azure.storage.container.name", "asdasd", "azure.storage.connection.string", "test");

        assertConfigDefValidationPasses(properties);

        final Template template = new AzureBlobSinkConfig(properties).getFilenameTemplate();
        final String fileName = template.instance()
                .bindVariable("topic", () -> "a")
                .bindVariable("timestamp", VariableTemplatePart.Parameter::getValue)
                .bindVariable("partition", () -> "p")
                .bindVariable("start_offset", VariableTemplatePart.Parameter::getValue)
                .render();
        assertThat(fileName).isEqualTo("a-yyyy-MM-dd-p-true.gz");
    }

    @Test
    void requiredConfigurations() {
        final Map<String, String> properties = Map.of();

        final String[] expectedErrorMessage = {
                "Missing required configuration \"azure.storage.container.name\" which has no default value.",
                "Missing required configuration \"azure.storage.connection.string\" which has no default value.",
                "Invalid value null for configuration azure.storage.container.name: names must be from 3 through 63 characters long." };

        assertValidationContainsMessage(properties, "azure.storage.container.name", expectedErrorMessage);

        assertThatThrownBy(() -> new AzureBlobSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "Missing required configuration \"azure.storage.connection.string\" which has no default value.");
    }

    @Test
    void emptyAzureContainerName() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "");

        final var expectedErrorMessage = "Missing required configuration \"azure.storage.connection.string\" which has no default value.";
        final var expectedErrorMessage2 = "Invalid value  for configuration azure.storage.container.name: names must be from 3 through 63 characters long.";

        assertValidationContainsMessage(properties, "azure.storage.container.name", expectedErrorMessage,
                expectedErrorMessage2);

        assertThatThrownBy(() -> new AzureBlobSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessageContainingAll(expectedErrorMessage);
    }

    @Test
    void correctMinimalConfig() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test");

        assertConfigDefValidationPasses(properties);

        final AzureBlobSinkConfig config = new AzureBlobSinkConfig(properties);
        assertThat(config.getContainerName()).isEqualTo("test-container");
        assertThat(config.getCompressionType()).isEqualTo(CompressionType.NONE);
        assertThat(config.getPrefix()).isEmpty();
        assertThat(config.getFilenameTemplate()
                .instance()
                .bindVariable("topic", () -> "a")
                .bindVariable("partition", () -> "b")
                .bindVariable("start_offset", () -> "c")
                .render()).isEqualTo("a-b-c");
        assertThat(config.getOutputFields())
                .containsExactly(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.BASE64));
        assertThat(config.getFormatType()).isEqualTo(FormatType.forName("csv"));
        assertThat(config.getAzureRetryBackoffInitialDelay())
                .hasMillis(AzureBlobSinkConfig.AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_DEFAULT);
        assertThat(config.getAzureRetryBackoffMaxDelay())
                .hasMillis(AzureBlobSinkConfig.AZURE_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT);
        assertThat(config.getAzureRetryBackoffMaxAttempts())
                .isEqualTo(AzureBlobSinkConfig.AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_DEFAULT);
    }

    @Test
    void customRetryPolicySettings() {
        final var properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "kafka.retry.backoff.ms", "1000",
                "azure.retry.backoff.initial.delay.ms", "2000", "azure.retry.backoff.max.delay.ms", "3000",
                "azure.retry.backoff.max.attempts", "100");
        final var config = new AzureBlobSinkConfig(properties);
        assertThat(config.getKafkaRetryBackoffMs()).isEqualTo(1000);
        assertThat(config.getAzureRetryBackoffInitialDelay()).isEqualTo(Duration.ofMillis(2000));
        assertThat(config.getAzureRetryBackoffMaxDelay()).isEqualTo(Duration.ofMillis(3000));
        assertThat(config.getAzureRetryBackoffMaxAttempts()).isEqualTo(100);
    }

    @ParameterizedTest
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void correctFullConfig(final String compression) {
        final Map<String, String> properties = new HashMap<>();
        properties.put("azure.storage.connection.string", "test-connection");
        properties.put("azure.storage.container.name", "test-container");
        properties.put("file.compression.type", compression);
        properties.put("file.name.prefix", "test-prefix");
        properties.put("file.name.template", "{{topic}}-{{partition}}-{{start_offset}}-{{timestamp:unit=yyyy}}.gz");
        properties.put("file.max.records", "42");
        properties.put("format.output.fields", "key,value,offset,timestamp");
        properties.put("format.output.fields.value.encoding", "base64");

        assertConfigDefValidationPasses(properties);

        final AzureBlobSinkConfig config = new AzureBlobSinkConfig(properties);
        assertThatNoException().isThrownBy(config::getConnectionString);
        assertThat(config.getContainerName()).isEqualTo("test-container");
        assertThat(config.getCompressionType()).isEqualTo(CompressionType.forName(compression));
        assertThat(config.getMaxRecordsPerFile()).isEqualTo(42);
        assertThat(config.getPrefix()).isEqualTo("test-prefix");
        assertThat(config.getFilenameTemplate()
                .instance()
                .bindVariable("topic", () -> "a")
                .bindVariable("partition", () -> "b")
                .bindVariable("start_offset", () -> "c")
                .bindVariable("timestamp", () -> "d")
                .render()).isEqualTo("a-b-c-d.gz");
        assertThat(config.getOutputFields()).containsExactly(
                new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.BASE64),
                new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE));

        assertThat(config.getFilenameTimezone()).isEqualTo(ZoneOffset.UTC);
        assertThat(config.getFilenameTimestampSource()).isInstanceOf(TimestampSource.WallclockTimestampSource.class);

    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void supportedCompression(final String compression) {
        final Map<String, String> properties = new HashMap<>();
        properties.put("azure.storage.container.name", "test-container");
        properties.put("azure.storage.connection.string", "test");

        if (compression != null) {
            properties.put("file.compression.type", compression);
        }

        assertConfigDefValidationPasses(properties);

        final AzureBlobSinkConfig config = new AzureBlobSinkConfig(properties);
        final CompressionType expectedCompressionType = compression == null
                ? CompressionType.NONE
                : CompressionType.forName(compression);
        assertThat(config.getCompressionType()).isEqualTo(expectedCompressionType);
    }

    @Test
    void unsupportedCompressionType() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.compression.type", "unsupported");

        final var expectedErrorMessage = "Invalid value unsupported for configuration file.compression.type: "
                + "String must be one of (case insensitive): ZSTD, GZIP, NONE, SNAPPY";

        final var configValue = AzureBlobSinkConfig.configDef().validateAll(properties).get("file.compression.type");
        assertThat(configValue.recommendedValues()).containsExactly("none", "gzip", "snappy", "zstd");

        assertThatThrownBy(() -> new AzureBlobSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void emptyOutputField() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "format.output.fields", "");

        final var expectedErrorMessage = "Invalid value [] for configuration format.output.fields: cannot be empty";

        final var configValue = assertValidationContainsMessage(properties, "format.output.fields",
                expectedErrorMessage);
        assertThat(configValue.recommendedValues()).containsExactly("key", "value", "offset", "timestamp", "headers");

        assertThatThrownBy(() -> new AzureBlobSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void unsupportedOutputField() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "format.output.fields",
                "key,value,offset,timestamp,headers,unsupported");

        final var expectedErrorMessage = "Invalid value [key, value, offset, timestamp, headers, unsupported] "
                + "for configuration format.output.fields: "
                + "supported values are (case insensitive): key, value, offset, timestamp, headers";

        final var configValue = assertValidationContainsMessage(properties, "format.output.fields",
                expectedErrorMessage);
        assertThat(configValue.recommendedValues()).containsExactly("key", "value", "offset", "timestamp", "headers");

        assertThatThrownBy(() -> new AzureBlobSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void connectorName() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "name", "test-connector");

        assertConfigDefValidationPasses(properties);

        final AzureBlobSinkConfig config = new AzureBlobSinkConfig(properties);
        assertThat(config.getConnectorName()).isEqualTo("test-connector");
    }

    @Test
    @Disabled("need validation of entire fname not just the prefix.")
    void fileNamePrefixTooLong() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("azure.storage.container.name", "test-container");
        final String longString = Stream.generate(() -> "a").limit(1025).collect(Collectors.joining());
        properties.put("file.name.prefix", longString);

        final var expectedErrorMessage = "Invalid value " + longString
                + " for configuration azure.storage.container.name: " + "cannot be longer than 1024 characters";

        assertValidationContainsMessage(properties, "file.name.prefix", expectedErrorMessage);

        assertThatThrownBy(() -> new AzureBlobSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void maxRecordsPerFileNotSet() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test");

        assertConfigDefValidationPasses(properties);

        final AzureBlobSinkConfig config = new AzureBlobSinkConfig(properties);
        assertThat(config.getMaxRecordsPerFile()).isZero();
    }

    @Test
    void maxRecordsPerFileSetCorrect() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.max.records", "42");

        assertConfigDefValidationPasses(properties);

        final AzureBlobSinkConfig config = new AzureBlobSinkConfig(properties);
        assertThat(config.getMaxRecordsPerFile()).isEqualTo(42);
    }

    @Test
    void maxRecordsPerFileSetIncorrect() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "file.max.records", "-42", AzureBlobSinkConfig.AZURE_STORAGE_CONNECTION_STRING_CONFIG, "test");

        final var expectedErrorMessage = "Invalid value -42 for configuration file.max.records: Value must be at least 0";

        assertValidationContainsMessage(properties, "file.max.records", expectedErrorMessage);

        assertThatThrownBy(() -> new AzureBlobSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void filenameTemplateNotSet(final String compression) {
        final Map<String, String> properties = new HashMap<>();
        properties.put("azure.storage.container.name", "test-container");
        properties.put("azure.storage.connection.string", "test");
        if (compression != null) {
            properties.put("file.compression.type", compression);
        }

        assertConfigDefValidationPasses(properties);

        final CompressionType compressionType = compression == null
                ? CompressionType.NONE
                : CompressionType.forName(compression);
        final String expected = "a-b-c" + compressionType.extension();

        final AzureBlobSinkConfig config = new AzureBlobSinkConfig(properties);
        final String actual = config.getFilenameTemplate()
                .instance()
                .bindVariable("topic", () -> "a")
                .bindVariable("partition", () -> "b")
                .bindVariable("start_offset", () -> "c")
                .render();
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void topicPartitionOffsetFilenameTemplateVariablesOrder1() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template",
                "{{topic}}-{{partition}}-{{start_offset}}");

        assertConfigDefValidationPasses(properties);

        final AzureBlobSinkConfig config = new AzureBlobSinkConfig(properties);
        final String actual = config.getFilenameTemplate()
                .instance()
                .bindVariable("topic", () -> "a")
                .bindVariable("partition", () -> "b")
                .bindVariable("start_offset", () -> "c")
                .render();
        assertThat(actual).isEqualTo("a-b-c");
    }

    @Test
    void topicPartitionOffsetFilenameTemplateVariablesOrder2() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template",
                "{{start_offset}}-{{partition}}-{{topic}}");

        assertConfigDefValidationPasses(properties);

        final AzureBlobSinkConfig config = new AzureBlobSinkConfig(properties);
        final String actual = config.getFilenameTemplate()
                .instance()
                .bindVariable("topic", () -> "a")
                .bindVariable("partition", () -> "b")
                .bindVariable("start_offset", () -> "c")
                .render();
        assertThat(actual).isEqualTo("c-b-a");
    }

    @Test
    void acceptFilenameTemplateVariablesParameters() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template",
                "{{start_offset:padding=true}}-{{partition}}-{{topic}}");

        assertConfigDefValidationPasses(properties);
        final AzureBlobSinkConfig config = new AzureBlobSinkConfig(properties);
        final String actual = config.getFilenameTemplate()
                .instance()
                .bindVariable("topic", () -> "a")
                .bindVariable("partition", () -> "b")
                .bindVariable("start_offset", parameter -> {
                    assertThat(parameter.getName()).isEqualTo("padding");
                    assertThat(parameter.asBoolean()).isTrue();
                    return "c";
                })
                .render();
        assertThat(actual).isEqualTo("c-b-a");
    }

    @Test
    void keyFilenameTemplateVariable() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template", "{{key}}");

        assertConfigDefValidationPasses(properties);

        final AzureBlobSinkConfig config = new AzureBlobSinkConfig(properties);
        final String actual = config.getFilenameTemplate().instance().bindVariable("key", () -> "a").render();
        assertThat(actual).isEqualTo("a");
    }

    @Test
    void emptyFilenameTemplate() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template", "");

        final var expectedErrorMessage = "Invalid value  for configuration file.name.template: RecordGrouper requires that the template [] has variables defined. Supported variables are: "
                + TEMPLATE_VARIABLES + ".";

        assertValidationContainsMessage(properties, "file.name.template", expectedErrorMessage);

    }

    @Test
    void filenameTemplateUnknownVariable() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template",
                "{{ aaa }}{{ topic }}{{ partition }}{{ start_offset }}");

        final String errorPfx = "Invalid value {{ aaa }}{{ topic }}{{ partition }}{{ start_offset }} "
                + "for configuration file.name.template: ";

        final var expectedErrorMessage1 = errorPfx + "unsupported template variable used ({{aaa}}), "
                + "supported values are: {{key}}, {{partition}}, {{start_offset}}, {{timestamp}}, {{topic}}.";

        final var expectedErrorMessage2 = errorPfx + "unsupported set of template variables, supported sets are: "
                + TEMPLATE_VARIABLES + ".";

        assertValidationContainsMessage(properties, "file.name.template", expectedErrorMessage1, expectedErrorMessage2);

        assertThatThrownBy(() -> new AzureBlobSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessageContaining(expectedErrorMessage1, expectedErrorMessage2);
    }

    @Test
    void filenameTemplateNoTopic() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template", "{{ partition }}{{ start_offset }}");

        final var expectedErrorMessage = "Invalid value {{ partition }}{{ start_offset }} for configuration file.name.template: "
                + "unsupported set of template variables, supported sets are: " + TEMPLATE_VARIABLES + ".";

        assertValidationContainsMessage(properties, "file.name.template", expectedErrorMessage);
    }

    @Test
    void wrongVariableParameterValue() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template",
                "{{start_offset:padding=FALSE}}-{{partition}}-{{topic}}");

        final var expectedErrorMessage = "Invalid value {{start_offset:padding=FALSE}}-{{partition}}-{{topic}} "
                + "for configuration file.name.template: FALSE is not a valid value for parameter padding, supported values are: true|false.";

        assertValidationContainsMessage(properties, "file.name.template", expectedErrorMessage);

        assertThatThrownBy(() -> new AzureBlobSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessageContaining(expectedErrorMessage);
    }

    @Test
    void variableWithoutRequiredParameterValue() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template",
                "{{start_offset}}-{{partition}}-{{topic}}-{{timestamp}}");

        final var expectedErrorMessage = "Invalid value {{start_offset}}-{{partition}}-{{topic}}-{{timestamp}} "
                + "for configuration file.name.template: parameter unit is required for the the variable timestamp, "
                + "supported values are: yyyy|MM|dd|HH.";

        assertValidationContainsMessage(properties, "file.name.template", expectedErrorMessage);

        assertThatThrownBy(() -> new AzureBlobSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessageContaining(expectedErrorMessage);
    }

    @Test
    void wrongVariableWithoutParameter() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template",
                "{{start_offset:}}-{{partition}}-{{topic}}");

        final var expectedErrorMessage = "Invalid value {{start_offset:}}-{{partition}}-{{topic}} "
                + "for configuration file.name.template: Wrong variable with parameter definition";

        assertValidationContainsMessage(properties, "file.name.template", expectedErrorMessage);

        assertThatThrownBy(() -> new AzureBlobSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void noVariableWithParameter() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template",
                "{{:padding=true}}-{{partition}}-{{topic}}");

        final var expectedErrorMessage = "Invalid value {{:padding=true}}-{{partition}}-{{topic}} "
                + "for configuration file.name.template: Variable name hasn't been set for template: {{:padding=true}}-{{partition}}-{{topic}}";

        assertValidationContainsMessage(properties, "file.name.template", expectedErrorMessage);

        assertThatThrownBy(() -> new AzureBlobSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void wrongVariableWithoutParameterValue() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template",
                "{{start_offset:padding=}}-{{partition}}-{{topic}}");

        final var expectedErrorMessage = "Invalid value {{start_offset:padding=}}-{{partition}}-{{topic}} "
                + "for configuration file.name.template: Parameter value for variable `start_offset` and parameter `padding` has not been set";

        assertValidationContainsMessage(properties, "file.name.template", expectedErrorMessage);

        assertThatThrownBy(() -> new AzureBlobSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void wrongVariableWithoutParameterName() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template",
                "{{start_offset:=true}}-{{partition}}-{{topic}}");

        final var expectedErrorMessage = "Invalid value {{start_offset:=true}}-{{partition}}-{{topic}} "
                + "for configuration file.name.template: Parameter name for variable `start_offset` has not been set";

        assertThatThrownBy(() -> new AzureBlobSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void filenameTemplateNoPartition() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template", "{{ topic }}{{ start_offset }}");

        final var expectedErrorMessage = "Invalid value {{ topic }}{{ start_offset }} for configuration file.name.template: "
                + "unsupported set of template variables, supported sets are: " + TEMPLATE_VARIABLES + ".";

        assertValidationContainsMessage(properties, "file.name.template", expectedErrorMessage);

        assertThatThrownBy(() -> new AzureBlobSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessageContaining(expectedErrorMessage);
    }

    @Test
    void filenameTemplateNoStartOffset() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template", "{{ topic }}{{ partition }}");

        final var expectedErrorMessage = "Invalid value {{ topic }}{{ partition }} for configuration file.name.template: "
                + "unsupported set of template variables, supported sets are: " + TEMPLATE_VARIABLES + ".";

        assertValidationContainsMessage(properties, "file.name.template", expectedErrorMessage);

        assertThatThrownBy(() -> new AzureBlobSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessageContaining(expectedErrorMessage);
    }

    @Test
    void keyFilenameTemplateAndLimitedRecordsPerFileNotSet() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template", "{{key}}");

        assertConfigDefValidationPasses(properties);
        assertThatNoException().isThrownBy(() -> new AzureBlobSinkConfig(properties));
    }

    @Test
    void keyFilenameTemplateAndLimitedRecordsPerFile1() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template", "{{key}}", "file.max.records", "1");

        assertConfigDefValidationPasses(properties);
        assertThatNoException().isThrownBy(() -> new AzureBlobSinkConfig(properties));
    }

    @Test
    void keyFilenameTemplateAndLimitedRecordsPerFileMoreThan1() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template", "{{key}}", "file.max.records", "42");

        final String expectedErrorMessage = "Invalid value 42 for configuration file.max.records: When file.name.template is {{key}}, file.max.records must be either 1 or not set.";

        assertValidationContainsMessage(properties, "file.max.records", expectedErrorMessage);
    }

    @Test
    void correctShortFilenameTimezone() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.timestamp.timezone", "CET");

        assertConfigDefValidationPasses(properties);

        final AzureBlobSinkConfig sinkConfig = new AzureBlobSinkConfig(properties);
        assertThat(sinkConfig.getFilenameTimezone()).isEqualTo(ZoneId.of("CET"));
    }

    @Test
    void correctLongFilenameTimezone() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.timestamp.timezone", "Europe/Berlin");

        assertConfigDefValidationPasses(properties);

        final AzureBlobSinkConfig sinkConfig = new AzureBlobSinkConfig(properties);
        assertThat(sinkConfig.getFilenameTimezone()).isEqualTo(ZoneId.of("Europe/Berlin"));
    }

    @Test
    void wrongFilenameTimestampSource() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.timestamp.timezone", "Europe/Berlin",
                "file.name.timestamp.source", "UNKNOWN_TIMESTAMP_SOURCE");

        final var expectedErrorMessage = "Invalid value UNKNOWN_TIMESTAMP_SOURCE for configuration "
                + "file.name.timestamp.source: String must be one of (case insensitive): EVENT, WALLCLOCK";

        assertThatThrownBy(() -> new AzureBlobSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void correctFilenameTimestampSource() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.timestamp.timezone", "Europe/Berlin",
                "file.name.timestamp.source", "wallclock");

        assertConfigDefValidationPasses(properties);

        final AzureBlobSinkConfig sinkConfig = new AzureBlobSinkConfig(properties);
        assertThat(sinkConfig.getFilenameTimestampSource().getClass())
                .isEqualTo(TimestampSource.WallclockTimestampSource.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { "jsonl", "json", "csv" })
    void supportedFormatTypeConfig(final String formatType) {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "format.output.type", formatType);

        assertConfigDefValidationPasses(properties);

        final AzureBlobSinkConfig sinkConfig = new AzureBlobSinkConfig(properties);
        final FormatType expectedFormatType = FormatType.forName(formatType);

        assertThat(sinkConfig.getFormatType()).isEqualTo(expectedFormatType);
    }

    @Test
    void wrongFormatTypeConfig() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "format.output.type", "unknown");

        final var expectedErrorMessage = "Invalid value unknown for configuration format.output.type: "
                + "String must be one of (case insensitive): PARQUET, CSV, JSON, AVRO, JSONL";

        final var configValue = assertValidationContainsMessage(properties, "format.output.type", expectedErrorMessage);

        assertThat(configValue.recommendedValues()).containsExactly("avro", "csv", "json", "jsonl", "parquet");

        assertThatThrownBy(() -> new AzureBlobSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @ParameterizedTest
    @ValueSource(strings = { "{{key}}", "{{topic}}/{{partition}}/{{key}}" })
    void notSupportedFileMaxRecords(final String fileNameTemplate) {
        final Map<String, String> properties = new HashMap<>();
        FileNameFragment.setter(properties).template(fileNameTemplate).maxRecordsPerFile(2);
        properties.put(AzureBlobSinkConfig.AZURE_STORAGE_CONTAINER_NAME_CONFIG, "any-container");
        properties.put(AzureBlobSinkConfig.AZURE_STORAGE_CONNECTION_STRING_CONFIG, "test");

        final String expectedErrorMessage = String.format(
                "Invalid value 2 for configuration file.max.records: When file.name.template is %s, file.max.records must be either 1 or not set.",
                fileNameTemplate);

        assertValidationContainsMessage(properties, "file.max.records", expectedErrorMessage);
    }

    private void assertConfigDefValidationPasses(final Map<String, String> properties) {
        for (final ConfigValue configValue : AzureBlobSinkConfig.configDef().validate(properties)) {
            assertThat(configValue.errorMessages()).isEmpty();
        }
    }

    private ConfigValue assertValidationContainsMessage(final Map<String, String> properties,
            final String configuration, final String... expectedErrorMessages) {

        final List<String> errorMsgs = new ArrayList<>();
        final List<ConfigValue> configValues = AzureBlobSinkConfig.configDef().validate(properties);
        configValues.stream().map(ConfigValue::errorMessages).forEach(errorMsgs::addAll);
        assertThat(errorMsgs).containsExactlyInAnyOrder(expectedErrorMessages);

        final Optional<ConfigValue> result = configValues.stream()
                .filter(cv -> cv.name().equals(configuration))
                .findAny();
        assertThat(result).withFailMessage("Config value not found").isNotEmpty();
        return result.get();
    }
}
