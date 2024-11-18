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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.common.templating.VariableTemplatePart;
import io.aiven.kafka.connect.gcs.GcsSinkConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests {@link GcsSinkConfig} class.
 */
final class GcsSinkConfigTest {

    static final String TEMPLATE_VARIABLES = "topic,partition,start_offset,timestamp; "
            + "topic,partition,key,start_offset,timestamp; key; key,topic,partition";

    @ParameterizedTest
    @ValueSource(strings = { "", "{{topic}}", "{{partition}}", "{{start_offset}}", "{{topic}}-{{partition}}",
            "{{topic}}-{{start_offset}}", "{{partition}}-{{start_offset}}",
            "{{topic}}-{{partition}}-{{start_offset}}-{{unknown}}" })
    void incorrectFilenameTemplates(final String template) {
        final Map<String, String> properties = Map.of("file.name.template", template, "gcs.bucket.name", "some-bucket");

        final ConfigValue configValue = GcsSinkConfig.configDef()
                .validate(properties)
                .stream()
                .filter(x -> GcsSinkConfig.FILE_NAME_TEMPLATE_CONFIG.equals(x.name()))
                .findFirst()
                .orElseThrow();
        assertThat(configValue.errorMessages()).isNotEmpty();

        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessageStartingWith("Invalid value ");
    }

    @Test
    void acceptMultipleParametersWithTheSameName() {
        final Map<String, String> properties = Map
                .of("file.name.template",
                        "{{topic}}-{{timestamp:unit=yyyy}}-" + "{{timestamp:unit=MM}}-{{timestamp:unit=dd}}"
                                + "-{{partition:padding=true}}-{{start_offset:padding=true}}.gz",
                        "gcs.bucket.name", "asdasd");

        assertConfigDefValidationPasses(properties);

        final Template template = new GcsSinkConfig(properties).getFilenameTemplate();
        final String fileName = template.instance()
                .bindVariable("topic", () -> "a")
                .bindVariable("timestamp", VariableTemplatePart.Parameter::getValue)
                .bindVariable("partition", () -> "p")
                .bindVariable("start_offset", VariableTemplatePart.Parameter::getValue)
                .render();
        assertThat(fileName).isEqualTo("a-yyyy-MM-dd-p-true.gz");
    }

    @Test
    void correctlySupportDeprecatedYyyyUppercase() {
        final Map<String, String> properties = Map.of("file.name.template",
                "{{topic}}-" + "{{timestamp:unit=YYYY}}-{{timestamp:unit=yyyy}}-"
                        + "{{ timestamp:unit=YYYY }}-{{ timestamp:unit=yyyy }}"
                        + "-{{partition}}-{{start_offset:padding=true}}.gz",
                "gcs.bucket.name", "asdasd");

        assertConfigDefValidationPasses(properties);

        final Template template = new GcsSinkConfig(properties).getFilenameTemplate();
        final String fileName = template.instance()
                .bindVariable("topic", () -> "_")
                .bindVariable("timestamp", VariableTemplatePart.Parameter::getValue)
                .bindVariable("partition", () -> "_")
                .bindVariable("start_offset", () -> "_")
                .render();
        assertThat(fileName).isEqualTo("_-yyyy-yyyy-yyyy-yyyy-_-_.gz");
    }

    @Test
    void requiredConfigurations() {
        final Map<String, String> properties = Map.of();

        final var expectedErrorMessage = "Missing required configuration \"gcs.bucket.name\" which has no default value.";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "gcs.bucket.name", expectedErrorMessage);
        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void emptyGcsBucketName() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "");

        final var expectedErrorMessage = "Invalid value  for configuration gcs.bucket.name: String must be non-empty";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "gcs.bucket.name", expectedErrorMessage);

        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void correctMinimalConfig() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket");

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        assertThat(config.getBucketName()).isEqualTo("test-bucket");
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
        assertThat(config.getGcsRetryBackoffInitialDelay())
                .hasMillis(GcsSinkConfig.GCS_RETRY_BACKOFF_INITIAL_DELAY_MS_DEFAULT);
        assertThat(config.getGcsRetryBackoffMaxDelay()).hasMillis(GcsSinkConfig.GCS_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT);
        assertThat(config.getGcsRetryBackoffTotalTimeout())
                .hasMillis(GcsSinkConfig.GCS_RETRY_BACKOFF_TOTAL_TIMEOUT_MS_DEFAULT);
        assertThat(config.getGcsRetryBackoffMaxAttempts())
                .isEqualTo(GcsSinkConfig.GCS_RETRY_BACKOFF_MAX_ATTEMPTS_DEFAULT);
        assertThat(config.getGcsRetryBackoffDelayMultiplier())
                .isEqualTo(GcsSinkConfig.GCS_RETRY_BACKOFF_DELAY_MULTIPLIER_DEFAULT);
    }

    @Test
    void customRetryPolicySettings() {
        final var properties = Map.of("gcs.bucket.name", "test-bucket", "kafka.retry.backoff.ms", "1000",
                "gcs.retry.backoff.initial.delay.ms", "2000", "gcs.retry.backoff.max.delay.ms", "3000",
                "gcs.retry.backoff.delay.multiplier", "2.0", "gcs.retry.backoff.total.timeout.ms", "4000",
                "gcs.retry.backoff.max.attempts", "100");
        final var config = new GcsSinkConfig(properties);
        assertThat(config.getKafkaRetryBackoffMs()).isEqualTo(1000);
        assertThat(config.getGcsRetryBackoffInitialDelay()).hasMillis(2000);
        assertThat(config.getGcsRetryBackoffMaxDelay()).hasMillis(3000);
        assertThat(config.getGcsRetryBackoffTotalTimeout()).hasMillis(4000);
        assertThat(config.getGcsRetryBackoffDelayMultiplier()).isEqualTo(2.0D);
        assertThat(config.getGcsRetryBackoffMaxAttempts()).isEqualTo(100);
    }

    @Test
    void wrongRetryPolicySettings() {
        final var initialDelayProp = Map.of("gcs.bucket.name", "test-bucket", "gcs.retry.backoff.initial.delay.ms",
                "-1");
        assertThatThrownBy(() -> new GcsSinkConfig(initialDelayProp)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value -1 for configuration gcs.retry.backoff.initial.delay.ms: "
                        + "Value must be at least 0");

        final var maxDelayProp = Map.of("gcs.bucket.name", "test-bucket", "gcs.retry.backoff.max.delay.ms", "-1");
        assertThatThrownBy(() -> new GcsSinkConfig(maxDelayProp)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value -1 for configuration gcs.retry.backoff.max.delay.ms: "
                        + "Value must be at least 0");

        final var delayMultiplayerProp = Map.of("gcs.bucket.name", "test-bucket", "gcs.retry.backoff.delay.multiplier",
                "-1", "gcs.retry.backoff.total.timeout.ms", "-1", "gcs.retry.backoff.max.attempts", "-1");
        assertThatThrownBy(() -> new GcsSinkConfig(delayMultiplayerProp)).isInstanceOf(ConfigException.class)
                .hasMessage(
                        "Invalid value -1.0 for configuration gcs.retry.backoff.delay.multiplier: Value must be at least 1.0");

        final var maxAttemptsProp = Map.of("gcs.bucket.name", "test-bucket", "gcs.retry.backoff.max.attempts", "-1");
        assertThatThrownBy(() -> new GcsSinkConfig(maxAttemptsProp)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value -1 for configuration gcs.retry.backoff.max.attempts: "
                        + "Value must be at least 0");

        final var totalTimeoutProp = Map.of("gcs.bucket.name", "test-bucket", "gcs.retry.backoff.total.timeout.ms",
                "-1");
        assertThatThrownBy(() -> new GcsSinkConfig(totalTimeoutProp)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value -1 for configuration gcs.retry.backoff.total.timeout.ms: "
                        + "Value must be at least 0");

        final var tooBigTotalTimeoutProp = Map.of("gcs.bucket.name", "test-bucket",
                "gcs.retry.backoff.total.timeout.ms", String.valueOf(TimeUnit.HOURS.toMillis(25)));

        assertThatThrownBy(() -> new GcsSinkConfig(tooBigTotalTimeoutProp)).isInstanceOf(ConfigException.class)
                .hasMessage(
                        "Invalid value 90000000 for configuration gcs.retry.backoff.total.timeout.ms: Value must be no more than 86400000 (24 hours)");
    }

    @ParameterizedTest
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void correctFullConfig(final String compression) {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.credentials.path",
                Thread.currentThread().getContextClassLoader().getResource("test_gcs_credentials.json").getPath());
        properties.put("gcs.bucket.name", "test-bucket");
        properties.put("file.compression.type", compression);
        properties.put("file.name.prefix", "test-prefix");
        properties.put("file.name.template", "{{topic}}-{{partition}}-{{start_offset}}-{{timestamp:unit=yyyy}}.gz");
        properties.put("file.max.records", "42");
        properties.put("format.output.fields", "key,value,offset,timestamp");
        properties.put("format.output.fields.value.encoding", "base64");

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        assertThatNoException().isThrownBy(config::getCredentials);
        assertThat(config.getBucketName()).isEqualTo("test-bucket");
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
        properties.put("gcs.bucket.name", "test-bucket");
        if (compression != null) {
            properties.put("file.compression.type", compression);
        }

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        final CompressionType expectedCompressionType = compression == null
                ? CompressionType.NONE
                : CompressionType.forName(compression);
        assertThat(config.getCompressionType()).isEqualTo(expectedCompressionType);
    }

    @Test
    void unsupportedCompressionType() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.compression.type",
                "unsupported");

        final var expectedErrorMessage = "Invalid value unsupported for configuration file.compression.type: "
                + "supported values are: 'none', 'gzip', 'snappy', 'zstd'";

        final var configValue = expectErrorMessageForConfigurationInConfigDefValidation(properties,
                "file.compression.type", expectedErrorMessage);
        assertThat(configValue.recommendedValues()).containsExactly("none", "gzip", "snappy", "zstd");

        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void emptyOutputField() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "format.output.fields", "");

        final var expectedErrorMessage = "Invalid value [] for configuration format.output.fields: cannot be empty";

        final var configValue = expectErrorMessageForConfigurationInConfigDefValidation(properties,
                "format.output.fields", expectedErrorMessage);
        assertThat(configValue.recommendedValues()).containsExactly("key", "value", "offset", "timestamp", "headers");

        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void unsupportedOutputField() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "format.output.fields",
                "key,value,offset,timestamp,headers,unsupported");

        final var expectedErrorMessage = "Invalid value [key, value, offset, timestamp, headers, unsupported] "
                + "for configuration format.output.fields: "
                + "supported values are: 'key', 'value', 'offset', 'timestamp', 'headers'";

        final var configValue = expectErrorMessageForConfigurationInConfigDefValidation(properties,
                "format.output.fields", expectedErrorMessage);
        assertThat(configValue.recommendedValues()).containsExactly("key", "value", "offset", "timestamp", "headers");

        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void connectorName() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "name", "test-connector");

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        assertThat(config.getConnectorName()).isEqualTo("test-connector");
    }

    @Test
    void fileNamePrefixTooLong() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        final String longString = Stream.generate(() -> "a").limit(1025).collect(Collectors.joining());
        properties.put("file.name.prefix", longString);

        final var expectedErrorMessage = "Invalid value " + longString + " for configuration gcs.bucket.name: "
                + "cannot be longer than 1024 characters";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.prefix", expectedErrorMessage);

        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void fileNamePrefixProhibitedPrefix() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.prefix",
                ".well-known/acme-challenge/something");

        final var expectedErrorMessage = "Invalid value .well-known/acme-challenge/something for configuration gcs.bucket.name: "
                + "cannot start with '.well-known/acme-challenge'";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.prefix", expectedErrorMessage);

        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void maxRecordsPerFileNotSet() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket");

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        assertThat(config.getMaxRecordsPerFile()).isZero();
    }

    @Test
    void maxRecordsPerFileSetCorrect() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.max.records", "42");

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        assertThat(config.getMaxRecordsPerFile()).isEqualTo(42);
    }

    @Test
    void maxRecordsPerFileSetIncorrect() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.max.records", "-42");

        final var expectedErrorMessage = "Invalid value -42 for configuration file.max.records: "
                + "must be a non-negative integer number";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.max.records", expectedErrorMessage);

        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void filenameTemplateNotSet(final String compression) {
        final Map<String, String> properties = new HashMap<>();
        properties.put("gcs.bucket.name", "test-bucket");
        if (compression != null) {
            properties.put("file.compression.type", compression);
        }

        assertConfigDefValidationPasses(properties);

        final CompressionType compressionType = compression == null
                ? CompressionType.NONE
                : CompressionType.forName(compression);
        final String expected = "a-b-c" + compressionType.extension();

        final GcsSinkConfig config = new GcsSinkConfig(properties);
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
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.template",
                "{{topic}}-{{partition}}-{{start_offset}}");

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig config = new GcsSinkConfig(properties);
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
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.template",
                "{{start_offset}}-{{partition}}-{{topic}}");

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig config = new GcsSinkConfig(properties);
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
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.template",
                "{{start_offset:padding=true}}-{{partition}}-{{topic}}");

        assertConfigDefValidationPasses(properties);
        final GcsSinkConfig config = new GcsSinkConfig(properties);
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
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.template",
                "{{key}}");

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig config = new GcsSinkConfig(properties);
        final String actual = config.getFilenameTemplate().instance().bindVariable("key", () -> "a").render();
        assertThat(actual).isEqualTo("a");
    }

    @Test
    void emptyFilenameTemplate() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.template", "");

        final var expectedErrorMessage = "Invalid value  for configuration file.name.template: "
                + "unsupported set of template variables, supported sets are: " + TEMPLATE_VARIABLES;

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.template", expectedErrorMessage);

        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void filenameTemplateUnknownVariable() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.template",
                "{{ aaa }}{{ topic }}{{ partition }}{{ start_offset }}");

        final var expectedErrorMessage = "Invalid value {{ aaa }}{{ topic }}{{ partition }}{{ start_offset }} "
                + "for configuration file.name.template: unsupported set of template variables, "
                + "supported sets are: " + TEMPLATE_VARIABLES;

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.template", expectedErrorMessage);

        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void filenameTemplateNoTopic() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.template",
                "{{ partition }}{{ start_offset }}");

        final var expectedErrorMessage = "Invalid value {{ partition }}{{ start_offset }} for configuration file.name.template: "
                + "unsupported set of template variables, supported sets are: " + TEMPLATE_VARIABLES;

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.template", expectedErrorMessage);

        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void wrongVariableParameterValue() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.template",
                "{{start_offset:padding=FALSE}}-{{partition}}-{{topic}}");

        final var expectedErrorMessage = "Invalid value {{start_offset:padding=FALSE}}-{{partition}}-{{topic}} "
                + "for configuration file.name.template: " + "unsupported set of template variables parameters, "
                + "supported sets are: "
                + "partition:padding=true|false,start_offset:padding=true|false,timestamp:unit=yyyy|MM|dd|HH";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.template", expectedErrorMessage);

        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void variableWithoutRequiredParameterValue() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.template",
                "{{start_offset}}-{{partition}}-{{topic}}-{{timestamp}}");

        final var expectedErrorMessage = "Invalid value {{start_offset}}-{{partition}}-{{topic}}-{{timestamp}} "
                + "for configuration file.name.template: "
                + "parameter unit is required for the the variable timestamp, " + "supported values are: yyyy|MM|dd|HH";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.template", expectedErrorMessage);

        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void wrongVariableWithoutParameter() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.template",
                "{{start_offset:}}-{{partition}}-{{topic}}");

        final var expectedErrorMessage = "Invalid value {{start_offset:}}-{{partition}}-{{topic}} "
                + "for configuration file.name.template: " + "Wrong variable with parameter definition";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.template", expectedErrorMessage);

        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void noVariableWithParameter() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.template",
                "{{:padding=true}}-{{partition}}-{{topic}}");

        final var expectedErrorMessage = "Invalid value {{:padding=true}}-{{partition}}-{{topic}} "
                + "for configuration file.name.template: "
                + "Variable name hasn't been set for template: {{:padding=true}}-{{partition}}-{{topic}}";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.template", expectedErrorMessage);

        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void wrongVariableWithoutParameterValue() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.template",
                "{{start_offset:padding=}}-{{partition}}-{{topic}}");

        final var expectedErrorMessage = "Invalid value {{start_offset:padding=}}-{{partition}}-{{topic}} "
                + "for configuration file.name.template: "
                + "Parameter value for variable `start_offset` and parameter `padding` has not been set";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.template", expectedErrorMessage);

        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void wrongVariableWithoutParameterName() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.template",
                "{{start_offset:=true}}-{{partition}}-{{topic}}");

        final var expectedErrorMessage = "Invalid value {{start_offset:=true}}-{{partition}}-{{topic}} "
                + "for configuration file.name.template: "
                + "Parameter name for variable `start_offset` has not been set";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.template", expectedErrorMessage);

        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void filenameTemplateNoPartition() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.template",
                "{{ topic }}{{ start_offset }}");

        final var expectedErrorMessage = "Invalid value {{ topic }}{{ start_offset }} for configuration file.name.template: "
                + "unsupported set of template variables, supported sets are: " + TEMPLATE_VARIABLES;

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.template", expectedErrorMessage);

        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void filenameTemplateNoStartOffset() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.template",
                "{{ topic }}{{ partition }}");

        final var expectedErrorMessage = "Invalid value {{ topic }}{{ partition }} for configuration file.name.template: "
                + "unsupported set of template variables, supported sets are: " + TEMPLATE_VARIABLES;

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.template", expectedErrorMessage);

        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void keyFilenameTemplateAndLimitedRecordsPerFileNotSet() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.template",
                "{{key}}");

        assertConfigDefValidationPasses(properties);
        assertThatNoException().isThrownBy(() -> new GcsSinkConfig(properties));
    }

    @Test
    void keyFilenameTemplateAndLimitedRecordsPerFile1() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.template", "{{key}}",
                "file.max.records", "1");

        assertConfigDefValidationPasses(properties);
        assertThatNoException().isThrownBy(() -> new GcsSinkConfig(properties));
    }

    @Test
    void keyFilenameTemplateAndLimitedRecordsPerFileMoreThan1() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.template", "{{key}}",
                "file.max.records", "42");

        // Should pass here, because ConfigDef validation doesn't check interdependencies.
        assertConfigDefValidationPasses(properties);

        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage("When file.name.template is {{key}}, file.max.records must be either 1 or not set");
    }

    @Test
    void correctShortFilenameTimezone() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.timestamp.timezone",
                "CET");

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig sinkConfig = new GcsSinkConfig(properties);
        assertThat(sinkConfig.getFilenameTimezone()).isEqualTo(ZoneId.of("CET"));
    }

    @Test
    void correctLongFilenameTimezone() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.timestamp.timezone",
                "Europe/Berlin");

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig sinkConfig = new GcsSinkConfig(properties);
        assertThat(sinkConfig.getFilenameTimezone()).isEqualTo(ZoneId.of("Europe/Berlin"));
    }

    @Test
    void wrongFilenameTimestampSource() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.timestamp.timezone",
                "Europe/Berlin", "file.name.timestamp.source", "UNKNOWN_TIMESTAMP_SOURCE");

        final var expectedErrorMessage = "Invalid value UNKNOWN_TIMESTAMP_SOURCE for configuration "
                + "file.name.timestamp.source: Unknown timestamp source: UNKNOWN_TIMESTAMP_SOURCE";

        expectErrorMessageForConfigurationInConfigDefValidation(properties, "file.name.timestamp.source",
                expectedErrorMessage);

        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void correctFilenameTimestampSource() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "file.name.timestamp.timezone",
                "Europe/Berlin", "file.name.timestamp.source", "wallclock");

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig sinkConfig = new GcsSinkConfig(properties);
        assertThat(sinkConfig.getFilenameTimestampSource())
                .isInstanceOf(TimestampSource.WallclockTimestampSource.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { "jsonl", "json", "csv" })
    void supportedFormatTypeConfig(final String formatType) {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "format.output.type",
                formatType);

        assertConfigDefValidationPasses(properties);

        final GcsSinkConfig sinkConfig = new GcsSinkConfig(properties);
        final FormatType expectedFormatType = FormatType.forName(formatType);

        assertThat(sinkConfig.getFormatType()).isEqualTo(expectedFormatType);
    }

    @Test
    void wrongFormatTypeConfig() {
        final Map<String, String> properties = Map.of("gcs.bucket.name", "test-bucket", "format.output.type",
                "unknown");

        final var expectedErrorMessage = "Invalid value unknown for configuration format.output.type: "
                + "supported values are: 'avro', 'csv', 'json', 'jsonl', 'parquet'";

        final var configValue = expectErrorMessageForConfigurationInConfigDefValidation(properties,
                "format.output.type", expectedErrorMessage);
        assertThat(configValue.recommendedValues()).containsExactly("avro", "csv", "json", "jsonl", "parquet");

        assertThatThrownBy(() -> new GcsSinkConfig(properties)).isInstanceOf(ConfigException.class)
                .hasMessage(expectedErrorMessage);
    }

    @ParameterizedTest
    @ValueSource(strings = { "{{key}}", "{{topic}}/{{partition}}/{{key}}" })
    void notSupportedFileMaxRecords(final String fileNameTemplate) {
        final Map<String, String> properties = Map.of(GcsSinkConfig.FILE_NAME_TEMPLATE_CONFIG, fileNameTemplate,
                GcsSinkConfig.FILE_MAX_RECORDS, "2", GcsSinkConfig.GCS_BUCKET_NAME_CONFIG, "any_bucket");
        assertThatThrownBy(() -> new GcsSinkConfig(properties))
                .withFailMessage(
                        String.format("When file.name.template is %s, file.max.records must be either 1 or not set",
                                fileNameTemplate))
                .isInstanceOf(ConfigException.class);
    }

    private void assertConfigDefValidationPasses(final Map<String, String> properties) {
        for (final ConfigValue configValue : GcsSinkConfig.configDef().validate(properties)) {
            assertThat(configValue.errorMessages()).isEmpty();
        }
    }

    private ConfigValue expectErrorMessageForConfigurationInConfigDefValidation(final Map<String, String> properties,
            final String configuration, final String expectedErrorMessage) {
        ConfigValue result = null;
        for (final ConfigValue configValue : GcsSinkConfig.configDef().validate(properties)) {
            if (configValue.name().equals(configuration)) {
                assertThat(configValue.errorMessages()).containsExactly(expectedErrorMessage);
                result = configValue;
            } else {
                assertThat(configValue.errorMessages()).isEmpty();
            }
        }
        assertThat(result).withFailMessage("Not found").isNotNull();
        return result;
    }
}
