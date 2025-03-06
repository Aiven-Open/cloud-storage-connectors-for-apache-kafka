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

package io.aiven.kafka.connect.azure.sink;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.config.AivenCommonConfig;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FixedSetRecommender;
import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.config.validators.FilenameTemplateValidator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AzureBlobSinkConfig extends AivenCommonConfig {
    private static final Logger LOG = LoggerFactory.getLogger(AzureBlobSinkConfig.class);
    private static final String USER_AGENT_HEADER_FORMAT = "Azure Blob Sink/%s (GPN: Aiven;)";
    public static final String USER_AGENT_HEADER_VALUE = String.format(USER_AGENT_HEADER_FORMAT, Version.VERSION);
    private static final String GROUP_AZURE = "Azure";
    public static final String AZURE_STORAGE_CONNECTION_STRING_CONFIG = "azure.storage.connection.string";
    public static final String AZURE_STORAGE_CONTAINER_NAME_CONFIG = "azure.storage.container.name";
    public static final String AZURE_USER_AGENT = "azure.user.agent";
    private static final String GROUP_FILE = "File";
    public static final String FILE_NAME_PREFIX_CONFIG = "file.name.prefix";
    public static final String FILE_NAME_TEMPLATE_CONFIG = "file.name.template";
    public static final String FILE_COMPRESSION_TYPE_CONFIG = "file.compression.type";
    public static final String FILE_MAX_RECORDS = "file.max.records";
    public static final String FILE_NAME_TIMESTAMP_TIMEZONE = "file.name.timestamp.timezone";
    public static final String FILE_NAME_TIMESTAMP_SOURCE = "file.name.timestamp.source";

    public static final String FORMAT_OUTPUT_FIELDS_CONFIG = "format.output.fields";
    public static final String FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG = "format.output.fields.value.encoding";

    private static final String GROUP_AZURE_RETRY_BACKOFF_POLICY = "Azure retry backoff policy";

    public static final String AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG = "azure.retry.backoff.initial.delay.ms";
    public static final String AZURE_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG = "azure.retry.backoff.max.delay.ms";
    public static final String AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG = "azure.retry.backoff.max.attempts";

    public static final long AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_DEFAULT = 1_000L;
    public static final long AZURE_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT = 32_000L;
    public static final int AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_DEFAULT = 6;

    public static final String NAME_CONFIG = "name";

    public static ConfigDef configDef() {
        final ConfigDef configDef = new ConfigDef();
        addAzureConfigGroup(configDef);
        addFileConfigGroup(configDef);
        OutputFormatFragment.update(configDef, OutputFieldType.VALUE);
        addAzureRetryPolicies(configDef);
        addUserAgentConfig(configDef);
        return configDef;
    }

    private static void addUserAgentConfig(final ConfigDef configDef) {
        configDef.define(AZURE_USER_AGENT, ConfigDef.Type.STRING, USER_AGENT_HEADER_VALUE, ConfigDef.Importance.LOW,
                "A custom user agent used while contacting Azure");
    }

    private static void addAzureConfigGroup(final ConfigDef configDef) {
        int azureGroupCounter = 0;
        configDef.define(AZURE_STORAGE_CONNECTION_STRING_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                "Azure Storage connection string.", GROUP_AZURE, azureGroupCounter++, ConfigDef.Width.NONE,
                AZURE_STORAGE_CONNECTION_STRING_CONFIG);

        configDef.define(AZURE_STORAGE_CONTAINER_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH,
                "The Azure Blob container name to store output files in.", GROUP_AZURE, azureGroupCounter++, // NOPMD
                ConfigDef.Width.NONE, AZURE_STORAGE_CONTAINER_NAME_CONFIG);
    }

    private static void addAzureRetryPolicies(final ConfigDef configDef) {
        int retryPolicyGroupCounter = 0;
        configDef.define(AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG, ConfigDef.Type.LONG,
                AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_DEFAULT, ConfigDef.Range.atLeast(0L), ConfigDef.Importance.MEDIUM,
                "Initial retry delay in milliseconds. The default value is "
                        + AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_DEFAULT,
                GROUP_AZURE_RETRY_BACKOFF_POLICY, retryPolicyGroupCounter++, ConfigDef.Width.NONE,
                AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG);
        configDef.define(AZURE_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG, ConfigDef.Type.LONG,
                AZURE_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT, ConfigDef.Range.atLeast(0L), ConfigDef.Importance.MEDIUM,
                "Maximum retry delay in milliseconds. The default value is " + AZURE_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT,
                GROUP_AZURE_RETRY_BACKOFF_POLICY, retryPolicyGroupCounter++, ConfigDef.Width.NONE,
                AZURE_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG);
        configDef.define(AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG, ConfigDef.Type.INT,
                AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_DEFAULT, ConfigDef.Range.atLeast(0L), ConfigDef.Importance.MEDIUM,
                "Retry max attempts. The default value is " + AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_DEFAULT,
                GROUP_AZURE_RETRY_BACKOFF_POLICY, retryPolicyGroupCounter++, ConfigDef.Width.NONE, // NOPMD
                                                                                                   // retryPolicyGroupCounter
                                                                                                   // updated value
                                                                                                   // never
                                                                                                   // used
                AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG);
    }

    private static void addFileConfigGroup(final ConfigDef configDef) {
        int fileGroupCounter = 0;
        configDef.define(FILE_NAME_PREFIX_CONFIG, ConfigDef.Type.STRING, "", new ConfigDef.Validator() {
            @Override
            public void ensureValid(final String name, final Object value) {
                assert value instanceof String;
                final String valueStr = (String) value;
                if (valueStr.length() > 1024) { // NOPMD avoid literal
                    throw new ConfigException(AZURE_STORAGE_CONTAINER_NAME_CONFIG, value,
                            "cannot be longer than 1024 characters");
                }
            }
        }, ConfigDef.Importance.MEDIUM, "The prefix to be added to the name of each file put on Azure Blob.",
                GROUP_FILE, fileGroupCounter++, ConfigDef.Width.NONE, FILE_NAME_PREFIX_CONFIG);

        configDef.define(FILE_NAME_TEMPLATE_CONFIG, ConfigDef.Type.STRING, null,
                new FilenameTemplateValidator(FILE_NAME_TEMPLATE_CONFIG), ConfigDef.Importance.MEDIUM,
                "The template for file names on Azure Blob. "
                        + "Supports `{{ variable }}` placeholders for substituting variables. "
                        + "Currently supported variables are `topic`, `partition`, and `start_offset` "
                        + "(the offset of the first record in the file). "
                        + "Only some combinations of variables are valid, which currently are:\n"
                        + "- `topic`, `partition`, `start_offset`.",
                GROUP_FILE, fileGroupCounter++, ConfigDef.Width.LONG, FILE_NAME_TEMPLATE_CONFIG);

        final String supportedCompressionTypes = CompressionType.names()
                .stream()
                .map(f -> "'" + f + "'")
                .collect(Collectors.joining(", "));
        configDef.define(FILE_COMPRESSION_TYPE_CONFIG, ConfigDef.Type.STRING, CompressionType.NONE.name,
                new ConfigDef.Validator() {
                    @Override
                    public void ensureValid(final String name, final Object value) {
                        assert value instanceof String;
                        final String valueStr = (String) value;
                        if (!CompressionType.names().contains(valueStr)) {
                            throw new ConfigException(FILE_COMPRESSION_TYPE_CONFIG, valueStr,
                                    "supported values are: " + supportedCompressionTypes);
                        }
                    }
                }, ConfigDef.Importance.MEDIUM,
                "The compression type used for files put on Azure Blob. " + "The supported values are: "
                        + supportedCompressionTypes + ".",
                GROUP_FILE, fileGroupCounter++, ConfigDef.Width.NONE, FILE_COMPRESSION_TYPE_CONFIG,
                FixedSetRecommender.ofSupportedValues(CompressionType.names()));

        configDef.define(FILE_MAX_RECORDS, ConfigDef.Type.INT, 0, new ConfigDef.Validator() {
            @Override
            public void ensureValid(final String name, final Object value) {
                assert value instanceof Integer;
                if ((Integer) value < 0) {
                    throw new ConfigException(FILE_MAX_RECORDS, value, "must be a non-negative integer number");
                }
            }
        }, ConfigDef.Importance.MEDIUM,
                "The maximum number of records to put in a single file. " + "Must be a non-negative integer number. "
                        + "0 is interpreted as \"unlimited\", which is the default.",
                GROUP_FILE, fileGroupCounter++, ConfigDef.Width.SHORT, FILE_MAX_RECORDS);

        configDef.define(FILE_NAME_TIMESTAMP_TIMEZONE, ConfigDef.Type.STRING, ZoneOffset.UTC.toString(),
                new ConfigDef.Validator() {
                    @Override
                    public void ensureValid(final String name, final Object value) {
                        try {
                            ZoneId.of(value.toString());
                        } catch (final Exception e) { // NOPMD broad exception catched
                            throw new ConfigException(FILE_NAME_TIMESTAMP_TIMEZONE, value, e.getMessage());
                        }
                    }
                }, ConfigDef.Importance.LOW,
                "Specifies the timezone in which the dates and time for the timestamp variable will be treated. "
                        + "Use standard shot and long names. Default is UTC",
                GROUP_FILE, fileGroupCounter++, ConfigDef.Width.SHORT, FILE_NAME_TIMESTAMP_TIMEZONE);

        configDef.define(FILE_NAME_TIMESTAMP_SOURCE, ConfigDef.Type.STRING, TimestampSource.Type.WALLCLOCK.name(),
                new ConfigDef.Validator() {
                    @Override
                    public void ensureValid(final String name, final Object value) {
                        try {
                            TimestampSource.Type.of(value.toString());
                        } catch (final Exception e) { // NOPMD broad exception catched
                            throw new ConfigException(FILE_NAME_TIMESTAMP_SOURCE, value, e.getMessage());
                        }
                    }
                }, ConfigDef.Importance.LOW, "Specifies the timestamp variable source. Default is wall-clock.",
                GROUP_FILE, fileGroupCounter, ConfigDef.Width.SHORT, FILE_NAME_TIMESTAMP_SOURCE);
    }

    public AzureBlobSinkConfig(final Map<String, String> properties) {
        super(configDef(), handleDeprecatedYyyyUppercase(properties));
        validate();
    }

    static Map<String, String> handleDeprecatedYyyyUppercase(final Map<String, String> properties) {
        if (properties.containsKey(FILE_NAME_TEMPLATE_CONFIG)) {
            final var result = new HashMap<>(properties);

            String template = properties.get(FILE_NAME_TEMPLATE_CONFIG);
            final String originalTemplate = template;

            final var unitYyyyPattern = Pattern.compile("\\{\\{\\s*timestamp\\s*:\\s*unit\\s*=\\s*YYYY\\s*}}");
            template = unitYyyyPattern.matcher(template)
                    .replaceAll(matchResult -> matchResult.group().replace("YYYY", "yyyy"));

            if (!template.equals(originalTemplate)) {
                LOG.warn(
                        "{{timestamp:unit=YYYY}} is no longer supported, "
                                + "please use {{timestamp:unit=yyyy}} instead. " + "It was automatically replaced: {}",
                        template);
            }

            result.put(FILE_NAME_TEMPLATE_CONFIG, template);

            return result;
        } else {
            return properties;
        }
    }

    private void validate() {
        final String connectionString = getString(AZURE_STORAGE_CONNECTION_STRING_CONFIG);

        if (connectionString == null) {
            throw new ConfigException(
                    String.format("The configuration %s cannot be null.", AZURE_STORAGE_CONNECTION_STRING_CONFIG));
        }
    }

    public String getConnectionString() {
        return getString(AZURE_STORAGE_CONNECTION_STRING_CONFIG);
    }

    public String getContainerName() {
        return getString(AZURE_STORAGE_CONTAINER_NAME_CONFIG);
    }

    @Override
    public CompressionType getCompressionType() {
        return CompressionType.forName(getString(FILE_COMPRESSION_TYPE_CONFIG));
    }

    @Override
    public List<OutputField> getOutputFields() {
        final List<OutputField> result = new ArrayList<>();
        for (final String outputFieldTypeStr : getList(FORMAT_OUTPUT_FIELDS_CONFIG)) {
            final OutputFieldType fieldType = OutputFieldType.forName(outputFieldTypeStr);
            final OutputFieldEncodingType encodingType;
            if (fieldType == OutputFieldType.VALUE) {
                encodingType = OutputFieldEncodingType.forName(getString(FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG));
            } else {
                encodingType = OutputFieldEncodingType.NONE;
            }
            result.add(new OutputField(fieldType, encodingType));
        }
        return result;
    }

    public String getPrefix() {
        return getString(FILE_NAME_PREFIX_CONFIG);
    }

    public String getConnectorName() {
        return originalsStrings().get(NAME_CONFIG);
    }

    public int getAzureRetryBackoffMaxAttempts() {
        return getInt(AZURE_RETRY_BACKOFF_MAX_ATTEMPTS_CONFIG);
    }

    public Duration getAzureRetryBackoffInitialDelay() {
        return Duration.ofMillis(getLong(AZURE_RETRY_BACKOFF_INITIAL_DELAY_MS_CONFIG));
    }

    public Duration getAzureRetryBackoffMaxDelay() {
        return Duration.ofMillis(getLong(AZURE_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG));
    }

    public String getUserAgent() {
        return getString(AZURE_USER_AGENT);
    }
}
