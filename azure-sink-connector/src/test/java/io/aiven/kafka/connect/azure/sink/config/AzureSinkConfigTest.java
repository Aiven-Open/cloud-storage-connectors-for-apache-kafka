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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

import io.aiven.kafka.connect.azure.sink.AzureBlobSinkConfig;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.common.templating.VariableTemplatePart;

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
                "some-container");

        final ConfigValue configValue = AzureBlobSinkConfig.configDef()
                .validate(properties)
                .stream()
                .filter(x -> AzureBlobSinkConfig.FILE_NAME_TEMPLATE_CONFIG.equals(x.name()))
                .findFirst()
                .get();
        assertFalse(configValue.errorMessages().isEmpty());

        final var throwable = assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties));
        assertTrue(throwable.getMessage().startsWith("Invalid value "));
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
        assertEquals("a-yyyy-MM-dd-p-true.gz", fileName);
    }

    @Test
    void requiredConfigurations() {
        final Map<String, String> properties = Map.of();

        final var expectedErrorMessage = "Missing required configuration \"azure.storage.container.name\" which has no default value.";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "azure.storage.container.name",
                expectedErrorMessage);
        final Throwable throwable = assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties));
        assertEquals(expectedErrorMessage, throwable.getMessage());
    }

    @Test
    void emptyAzureContainerName() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "");

        final var expectedErrorMessage = "Invalid value  for configuration azure.storage.container.name: String must be non-empty";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "azure.storage.container.name",
                expectedErrorMessage);

        final Throwable throwable = assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties));
        assertEquals(expectedErrorMessage, throwable.getMessage());
    }

    @Test
    void correctMinimalConfig() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test");

        assertConfigDefValidationPasses(properties);

        final AzureBlobSinkConfig config = new AzureBlobSinkConfig(properties);
        assertEquals("test-container", config.getContainerName());
        assertEquals(CompressionType.NONE, config.getCompressionType());
        assertEquals("", config.getPrefix());
        assertEquals("a-b-c",
                config.getFilenameTemplate()
                        .instance()
                        .bindVariable("topic", () -> "a")
                        .bindVariable("partition", () -> "b")
                        .bindVariable("start_offset", () -> "c")
                        .render());
        assertIterableEquals(
                Collections.singleton(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.BASE64)),
                config.getOutputFields());
        assertEquals(FormatType.forName("csv"), config.getFormatType());
        assertEquals(Duration.ofMillis(AzureBlobSinkConfig.AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_DEFAULT),
                config.getAzureRetryBackoffInitialDelay());
        assertEquals(Duration.ofMillis(AzureBlobSinkConfig.AZURE_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT),
                config.getAzureRetryBackoffMaxDelay());
        assertEquals(AzureBlobSinkConfig.AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_DEFAULT,
                config.getAzureRetryBackoffMaxAttempts());
    }

    @Test
    void customRetryPolicySettings() {
        final var properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "kafka.retry.backoff.ms", "1000",
                "azure.retry.backoff.initial.delay.ms", "2000", "azure.retry.backoff.max.delay.ms", "3000",
                "azure.retry.backoff.max.attempts", "100");
        final var config = new AzureBlobSinkConfig(properties);
        assertEquals(1000, config.getKafkaRetryBackoffMs());
        assertEquals(Duration.ofMillis(2000), config.getAzureRetryBackoffInitialDelay());
        assertEquals(Duration.ofMillis(3000), config.getAzureRetryBackoffMaxDelay());
        assertEquals(100, config.getAzureRetryBackoffMaxAttempts());
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
        assertDoesNotThrow(config::getConnectionString);
        assertEquals("test-container", config.getContainerName());
        assertEquals(CompressionType.forName(compression), config.getCompressionType());
        assertEquals(42, config.getMaxRecordsPerFile());
        assertEquals("test-prefix", config.getPrefix());
        assertEquals("a-b-c-d.gz",
                config.getFilenameTemplate()
                        .instance()
                        .bindVariable("topic", () -> "a")
                        .bindVariable("partition", () -> "b")
                        .bindVariable("start_offset", () -> "c")
                        .bindVariable("timestamp", () -> "d")
                        .render());
        assertIterableEquals(
                Arrays.asList(new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
                        new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.BASE64),
                        new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
                        new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE)),
                config.getOutputFields());

        assertEquals(ZoneOffset.UTC, config.getFilenameTimezone());
        assertEquals(TimestampSource.WallclockTimestampSource.class, config.getFilenameTimestampSource().getClass());

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
        assertEquals(expectedCompressionType, config.getCompressionType());
    }

    @Test
    void unsupportedCompressionType() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.compression.type", "unsupported");

        final var expectedErrorMessage = "Invalid value unsupported for configuration file.compression.type: "
                + "supported values are: 'none', 'gzip', 'snappy', 'zstd'";

        final var configValue = expectErrorMessageForConfigurationInConfigDefValidation(properties,
                "file.compression.type", expectedErrorMessage);
        assertIterableEquals(List.of("none", "gzip", "snappy", "zstd"), configValue.recommendedValues());

        final Throwable throwable = assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties));
        assertEquals(expectedErrorMessage, throwable.getMessage());
    }

    @Test
    void emptyOutputField() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "format.output.fields", "");

        final var expectedErrorMessage = "Invalid value [] for configuration format.output.fields: cannot be empty";

        final var configValue = expectErrorMessageForConfigurationInConfigDefValidation(properties,
                "format.output.fields", expectedErrorMessage);
        assertIterableEquals(List.of("key", "value", "offset", "timestamp", "headers"),
                configValue.recommendedValues());

        final Throwable throwable = assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties));
        assertEquals(expectedErrorMessage, throwable.getMessage());
    }

    @Test
    void unsupportedOutputField() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "format.output.fields",
                "key,value,offset,timestamp,headers,unsupported");

        final var expectedErrorMessage = "Invalid value [key, value, offset, timestamp, headers, unsupported] "
                + "for configuration format.output.fields: "
                + "supported values are: 'key', 'value', 'offset', 'timestamp', 'headers'";

        final var configValue = expectErrorMessageForConfigurationInConfigDefValidation(properties,
                "format.output.fields", expectedErrorMessage);
        assertIterableEquals(List.of("key", "value", "offset", "timestamp", "headers"),
                configValue.recommendedValues());

        final Throwable throwable = assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties));
        assertEquals(expectedErrorMessage, throwable.getMessage());
    }

    @Test
    void connectorName() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "name", "test-connector");

        assertConfigDefValidationPasses(properties);

        final AzureBlobSinkConfig config = new AzureBlobSinkConfig(properties);
        assertEquals("test-connector", config.getConnectorName());
    }

    @Test
    void fileNamePrefixTooLong() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("azure.storage.container.name", "test-container");
        final String longString = Stream.generate(() -> "a").limit(1025).collect(Collectors.joining());
        properties.put("file.name.prefix", longString);

        final var expectedErrorMessage = "Invalid value " + longString
                + " for configuration azure.storage.container.name: " + "cannot be longer than 1024 characters";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.prefix", expectedErrorMessage);

        final Throwable throwable = assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties));
        assertEquals(expectedErrorMessage, throwable.getMessage());
    }

    @Test
    void maxRecordsPerFileNotSet() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test");

        assertConfigDefValidationPasses(properties);

        final AzureBlobSinkConfig config = new AzureBlobSinkConfig(properties);
        assertEquals(0, config.getMaxRecordsPerFile());
    }

    @Test
    void maxRecordsPerFileSetCorrect() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.max.records", "42");

        assertConfigDefValidationPasses(properties);

        final AzureBlobSinkConfig config = new AzureBlobSinkConfig(properties);
        assertEquals(42, config.getMaxRecordsPerFile());
    }

    @Test
    void maxRecordsPerFileSetIncorrect() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "file.max.records", "-42");

        final var expectedErrorMessage = "Invalid value -42 for configuration file.max.records: "
                + "must be a non-negative integer number";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.max.records", expectedErrorMessage);

        final Throwable throwable = assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties));
        assertEquals(expectedErrorMessage, throwable.getMessage());
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
        assertEquals(expected, actual);
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
        assertEquals("a-b-c", actual);
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
        assertEquals("c-b-a", actual);
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
                    assertEquals("padding", parameter.getName());
                    assertTrue(parameter.asBoolean());
                    return "c";
                })
                .render();
        assertEquals("c-b-a", actual);
    }

    @Test
    void keyFilenameTemplateVariable() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template", "{{key}}");

        assertConfigDefValidationPasses(properties);

        final AzureBlobSinkConfig config = new AzureBlobSinkConfig(properties);
        final String actual = config.getFilenameTemplate().instance().bindVariable("key", () -> "a").render();
        assertEquals("a", actual);
    }

    @Test
    void emptyFilenameTemplate() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template", "");

        final var expectedErrorMessage = "Invalid value  for configuration file.name.template: "
                + "unsupported set of template variables, supported sets are: " + TEMPLATE_VARIABLES;

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.template", expectedErrorMessage);

        final Throwable throwable = assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties));
        assertEquals(expectedErrorMessage, throwable.getMessage());
    }

    @Test
    void filenameTemplateUnknownVariable() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template",
                "{{ aaa }}{{ topic }}{{ partition }}{{ start_offset }}");

        final var expectedErrorMessage = "Invalid value {{ aaa }}{{ topic }}{{ partition }}{{ start_offset }} "
                + "for configuration file.name.template: unsupported set of template variables, "
                + "supported sets are: " + TEMPLATE_VARIABLES;

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.template", expectedErrorMessage);

        final Throwable throwable = assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties));
        assertEquals(expectedErrorMessage, throwable.getMessage());
    }

    @Test
    void filenameTemplateNoTopic() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template", "{{ partition }}{{ start_offset }}");

        final var expectedErrorMessage = "Invalid value {{ partition }}{{ start_offset }} for configuration file.name.template: "
                + "unsupported set of template variables, supported sets are: " + TEMPLATE_VARIABLES;

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.template", expectedErrorMessage);

        final Throwable throwable = assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties));
        assertEquals(expectedErrorMessage, throwable.getMessage());
    }

    @Test
    void wrongVariableParameterValue() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template",
                "{{start_offset:padding=FALSE}}-{{partition}}-{{topic}}");

        final var expectedErrorMessage = "Invalid value {{start_offset:padding=FALSE}}-{{partition}}-{{topic}} "
                + "for configuration file.name.template: unsupported set of template variables parameters, "
                + "supported sets are: "
                + "partition:padding=true|false,start_offset:padding=true|false,timestamp:unit=yyyy|MM|dd|HH";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.template", expectedErrorMessage);

        final Throwable throwable = assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties));
        assertEquals(expectedErrorMessage, throwable.getMessage());
    }

    @Test
    void variableWithoutRequiredParameterValue() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template",
                "{{start_offset}}-{{partition}}-{{topic}}-{{timestamp}}");

        final var expectedErrorMessage = "Invalid value {{start_offset}}-{{partition}}-{{topic}}-{{timestamp}} "
                + "for configuration file.name.template: parameter unit is required for the the variable timestamp, "
                + "supported values are: yyyy|MM|dd|HH";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.template", expectedErrorMessage);

        final Throwable throwable = assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties));
        assertEquals(expectedErrorMessage, throwable.getMessage());
    }

    @Test
    void wrongVariableWithoutParameter() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template",
                "{{start_offset:}}-{{partition}}-{{topic}}");

        final var expectedErrorMessage = "Invalid value {{start_offset:}}-{{partition}}-{{topic}} "
                + "for configuration file.name.template: Wrong variable with parameter definition";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.template", expectedErrorMessage);

        final Throwable throwable = assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties));
        assertEquals(expectedErrorMessage, throwable.getMessage());
    }

    @Test
    void noVariableWithParameter() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template",
                "{{:padding=true}}-{{partition}}-{{topic}}");

        final var expectedErrorMessage = "Invalid value {{:padding=true}}-{{partition}}-{{topic}} "
                + "for configuration file.name.template: Variable name hasn't been set for template: {{:padding=true}}-{{partition}}-{{topic}}";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.template", expectedErrorMessage);

        final Throwable throwable = assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties));
        assertEquals(expectedErrorMessage, throwable.getMessage());
    }

    @Test
    void wrongVariableWithoutParameterValue() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template",
                "{{start_offset:padding=}}-{{partition}}-{{topic}}");

        final var expectedErrorMessage = "Invalid value {{start_offset:padding=}}-{{partition}}-{{topic}} "
                + "for configuration file.name.template: Parameter value for variable `start_offset` and parameter `padding` has not been set";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.template", expectedErrorMessage);

        final Throwable throwable = assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties));
        assertEquals(expectedErrorMessage, throwable.getMessage());
    }

    @Test
    void wrongVariableWithoutParameterName() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template",
                "{{start_offset:=true}}-{{partition}}-{{topic}}");

        final var expectedErrorMessage = "Invalid value {{start_offset:=true}}-{{partition}}-{{topic}} "
                + "for configuration file.name.template: Parameter name for variable `start_offset` has not been set";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.template", expectedErrorMessage);

        final Throwable throwable = assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties));
        assertEquals(expectedErrorMessage, throwable.getMessage());
    }

    @Test
    void filenameTemplateNoPartition() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template", "{{ topic }}{{ start_offset }}");

        final var expectedErrorMessage = "Invalid value {{ topic }}{{ start_offset }} for configuration file.name.template: "
                + "unsupported set of template variables, supported sets are: " + TEMPLATE_VARIABLES;

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.template", expectedErrorMessage);

        final Throwable throwable = assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties));
        assertEquals(expectedErrorMessage, throwable.getMessage());
    }

    @Test
    void filenameTemplateNoStartOffset() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template", "{{ topic }}{{ partition }}");

        final var expectedErrorMessage = "Invalid value {{ topic }}{{ partition }} for configuration file.name.template: "
                + "unsupported set of template variables, supported sets are: " + TEMPLATE_VARIABLES;

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.template", expectedErrorMessage);

        final Throwable throwable = assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties));
        assertEquals(expectedErrorMessage, throwable.getMessage());
    }

    @Test
    void keyFilenameTemplateAndLimitedRecordsPerFileNotSet() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template", "{{key}}");

        assertConfigDefValidationPasses(properties);
        assertDoesNotThrow(() -> new AzureBlobSinkConfig(properties));
    }

    @Test
    void keyFilenameTemplateAndLimitedRecordsPerFile1() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template", "{{key}}", "file.max.records", "1");

        assertConfigDefValidationPasses(properties);
        assertDoesNotThrow(() -> new AzureBlobSinkConfig(properties));
    }

    @Test
    void keyFilenameTemplateAndLimitedRecordsPerFileMoreThan1() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.template", "{{key}}", "file.max.records", "42");

        // Should pass here, because ConfigDef validation doesn't check interdependencies.
        assertConfigDefValidationPasses(properties);

        final Throwable throwable = assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties));
        assertEquals("When file.name.template is {{key}}, file.max.records must be either 1 or not set",
                throwable.getMessage());
    }

    @Test
    void correctShortFilenameTimezone() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.timestamp.timezone", "CET");

        assertConfigDefValidationPasses(properties);

        final AzureBlobSinkConfig sinkConfig = new AzureBlobSinkConfig(properties);
        assertEquals(ZoneId.of("CET"), sinkConfig.getFilenameTimezone());
    }

    @Test
    void correctLongFilenameTimezone() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.timestamp.timezone", "Europe/Berlin");

        assertConfigDefValidationPasses(properties);

        final AzureBlobSinkConfig sinkConfig = new AzureBlobSinkConfig(properties);
        assertEquals(ZoneId.of("Europe/Berlin"), sinkConfig.getFilenameTimezone());
    }

    @Test
    void wrongFilenameTimestampSource() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.timestamp.timezone", "Europe/Berlin",
                "file.name.timestamp.source", "UNKNOWN_TIMESTAMP_SOURCE");

        final var expectedErrorMessage = "Invalid value UNKNOWN_TIMESTAMP_SOURCE for configuration "
                + "file.name.timestamp.source: Unknown timestamp source: UNKNOWN_TIMESTAMP_SOURCE";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.timestamp.source",
                expectedErrorMessage);

        final Throwable throwable = assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties));
        assertEquals(expectedErrorMessage, throwable.getMessage());
    }

    @Test
    void correctFilenameTimestampSource() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "file.name.timestamp.timezone", "Europe/Berlin",
                "file.name.timestamp.source", "wallclock");

        assertConfigDefValidationPasses(properties);

        final AzureBlobSinkConfig sinkConfig = new AzureBlobSinkConfig(properties);
        assertEquals(TimestampSource.WallclockTimestampSource.class,
                sinkConfig.getFilenameTimestampSource().getClass());
    }

    @ParameterizedTest
    @ValueSource(strings = { "jsonl", "json", "csv" })
    void supportedFormatTypeConfig(final String formatType) {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "format.output.type", formatType);

        assertConfigDefValidationPasses(properties);

        final AzureBlobSinkConfig sinkConfig = new AzureBlobSinkConfig(properties);
        final FormatType expectedFormatType = FormatType.forName(formatType);

        assertEquals(expectedFormatType, sinkConfig.getFormatType());
    }

    @Test
    void wrongFormatTypeConfig() {
        final Map<String, String> properties = Map.of("azure.storage.container.name", "test-container",
                "azure.storage.connection.string", "test", "format.output.type", "unknown");

        final var expectedErrorMessage = "Invalid value unknown for configuration format.output.type: "
                + "supported values are: 'avro', 'csv', 'json', 'jsonl', 'parquet'";

        final var configValue = expectErrorMessageForConfigurationInConfigDefValidation(properties,
                "format.output.type", expectedErrorMessage);
        assertIterableEquals(List.of("avro", "csv", "json", "jsonl", "parquet"), configValue.recommendedValues());

        final Throwable throwable = assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties));
        assertEquals(expectedErrorMessage, throwable.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = { "{{key}}", "{{topic}}/{{partition}}/{{key}}" })
    void notSupportedFileMaxRecords(final String fileNameTemplate) {
        final Map<String, String> properties = Map.of(AzureBlobSinkConfig.FILE_NAME_TEMPLATE_CONFIG, fileNameTemplate,
                AzureBlobSinkConfig.FILE_MAX_RECORDS, "2", AzureBlobSinkConfig.AZURE_STORAGE_CONTAINER_NAME_CONFIG,
                "any_container");
        assertThrows(ConfigException.class, () -> new AzureBlobSinkConfig(properties), String.format(
                "When file.name.template is %s, file.max.records must be either 1 or not set", fileNameTemplate));
    }

    private void assertConfigDefValidationPasses(final Map<String, String> properties) {
        for (final ConfigValue configValue : AzureBlobSinkConfig.configDef().validate(properties)) {
            assertTrue(configValue.errorMessages().isEmpty());
        }
    }

    private ConfigValue expectErrorMessageForConfigurationInConfigDefValidation(final Map<String, String> properties,
            final String configuration, final String expectedErrorMessage) {
        ConfigValue result = null;
        for (final ConfigValue configValue : AzureBlobSinkConfig.configDef().validate(properties)) {
            if (configValue.name().equals(configuration)) {
                assertIterableEquals(List.of(expectedErrorMessage), configValue.errorMessages());
                result = configValue;
            } else {
                assertTrue(configValue.errorMessages().isEmpty());
            }
        }
        assertNotNull(result, "Not found");
        return result;
    }
}
