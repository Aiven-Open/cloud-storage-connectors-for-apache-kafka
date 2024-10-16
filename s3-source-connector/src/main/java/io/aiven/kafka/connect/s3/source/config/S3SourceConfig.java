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

package io.aiven.kafka.connect.s3.source.config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.config.validators.NonEmptyPassword;
import io.aiven.kafka.connect.common.config.validators.UrlValidator;
import io.aiven.kafka.connect.s3.source.output.InputFormat;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.internal.BucketNameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({ "PMD.TooManyMethods", "PMD.GodClass", "PMD.ExcessiveImports" })
final public class S3SourceConfig extends AbstractConfig {

    public static final Logger LOGGER = LoggerFactory.getLogger(S3SourceConfig.class);

    public static final String AWS_S3_PREFIX_CONFIG = "aws.s3.prefix";
    public static final String AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG = "aws.s3.backoff.delay.ms";
    public static final String AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG = "aws.s3.backoff.max.delay.ms";
    public static final String AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG = "aws.s3.backoff.max.retries";
    public static final String AWS_S3_REGION_CONFIG = "aws.s3.region";
    public static final String AWS_S3_ENDPOINT_CONFIG = "aws.s3.endpoint";
    public static final String AWS_STS_ROLE_ARN = "aws.sts.role.arn";
    public static final String AWS_STS_ROLE_EXTERNAL_ID = "aws.sts.role.external.id";
    public static final String AWS_STS_ROLE_SESSION_NAME = "aws.sts.role.session.name";
    public static final String AWS_STS_ROLE_SESSION_DURATION = "aws.sts.role.session.duration";
    public static final String AWS_STS_CONFIG_ENDPOINT = "aws.sts.config.endpoint";
    private static final String GROUP_AWS = "AWS";
    private static final String GROUP_AWS_STS = "AWS_STS";
    private static final String GROUP_OTHER = "OTHER_CFG";
    private static final String GROUP_OFFSET_TOPIC = "OFFSET_TOPIC";
    private static final String GROUP_S3_RETRY_BACKOFF_POLICY = "S3 retry backoff policy";

    // Default values from AWS SDK, since they are hidden
    public static final int AWS_S3_RETRY_BACKOFF_DELAY_MS_DEFAULT = 100;
    public static final int AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT = 20_000;
    public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
    public static final String VALUE_CONVERTER_SCHEMA_REGISTRY_URL = "value.converter.schema.registry.url";
    public static final String VALUE_SERIALIZER = "value.serializer";
    public static final String AWS_ACCESS_KEY_ID_CONFIG = "aws.access.key.id";
    public static final String AWS_SECRET_ACCESS_KEY_CONFIG = "aws.secret.access.key";
    public static final String AWS_CREDENTIALS_PROVIDER_CONFIG = "aws.credentials.provider";
    public static final String AWS_CREDENTIAL_PROVIDER_DEFAULT = "com.amazonaws.auth.DefaultAWSCredentialsProviderChain";
    public static final String AWS_S3_BUCKET_NAME_CONFIG = "aws.s3.bucket.name";
    public static final String AWS_S3_SSE_ALGORITHM_CONFIG = "aws.s3.sse.algorithm";
    public static final String TARGET_TOPIC_PARTITIONS = "topic.partitions";
    public static final String TARGET_TOPICS = "topics";
    public static final String FETCH_PAGE_SIZE = "aws.s3.fetch.page.size";
    public static final String MAX_POLL_RECORDS = "max.poll.records";
    public static final String MAX_MESSAGE_BYTES_SIZE = "max.message.bytes";
    public static final String KEY_CONVERTER = "key.converter";
    public static final String VALUE_CONVERTER = "value.converter";
    public static final int S3_RETRY_BACKOFF_MAX_RETRIES_DEFAULT = 3;
    public static final String INPUT_FORMAT_KEY = "input.format";
    public static final String SCHEMAS_ENABLE = "schemas.enable";

    public S3SourceConfig(final Map<String, String> properties) {
        super(configDef(), preprocessProperties(properties));
        validate(); // NOPMD ConstructorCallsOverridableMethod getStsRole is called
    }

    static Map<String, String> preprocessProperties(final Map<String, String> properties) {
        // Add other preprocessings when needed here. Mind the order.
        return handleDeprecatedYyyyUppercase(properties);
    }

    private static Map<String, String> handleDeprecatedYyyyUppercase(final Map<String, String> properties) {
        if (!properties.containsKey(AWS_S3_PREFIX_CONFIG)) {
            return properties;
        }

        final var result = new HashMap<>(properties);
        for (final var prop : List.of(AWS_S3_PREFIX_CONFIG)) {
            if (properties.containsKey(prop)) {
                String template = properties.get(prop);
                final String originalTemplate = template;

                final var unitYyyyPattern = Pattern.compile("\\{\\{\\s*timestamp\\s*:\\s*unit\\s*=\\s*YYYY\\s*}}");
                template = unitYyyyPattern.matcher(template)
                        .replaceAll(matchResult -> matchResult.group().replace("YYYY", "yyyy"));

                if (!template.equals(originalTemplate)) {
                    LOGGER.warn("{{timestamp:unit=YYYY}} is no longer supported, "
                            + "please use {{timestamp:unit=yyyy}} instead. " + "It was automatically replaced: {}",
                            template);
                }

                result.put(prop, template);
            }
        }
        return result;
    }

    public static ConfigDef configDef() {
        final var configDef = new S3SourceConfigDef();
        addSchemaRegistryGroup(configDef);
        addOffsetStorageConfig(configDef);
        addAwsStsConfigGroup(configDef);
        addAwsConfigGroup(configDef);
        addDeprecatedConfiguration(configDef);
        addS3RetryPolicies(configDef);
        addOtherConfig(configDef);
        return configDef;
    }

    private static void addSchemaRegistryGroup(final ConfigDef configDef) {
        int srCounter = 0;
        configDef.define(SCHEMA_REGISTRY_URL, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "SCHEMA REGISTRY URL", GROUP_OTHER, srCounter++, ConfigDef.Width.NONE,
                SCHEMA_REGISTRY_URL);
        configDef.define(VALUE_CONVERTER_SCHEMA_REGISTRY_URL, ConfigDef.Type.STRING, null,
                new ConfigDef.NonEmptyString(), ConfigDef.Importance.MEDIUM, "SCHEMA REGISTRY URL", GROUP_OTHER,
                srCounter++, ConfigDef.Width.NONE, VALUE_CONVERTER_SCHEMA_REGISTRY_URL);
        configDef.define(INPUT_FORMAT_KEY, ConfigDef.Type.STRING, InputFormat.BYTES.getValue(),
                new ConfigDef.NonEmptyString(), ConfigDef.Importance.MEDIUM, "Output format avro/json/parquet/bytes",
                GROUP_OTHER, srCounter++, // NOPMD
                ConfigDef.Width.NONE, INPUT_FORMAT_KEY);

        configDef.define(VALUE_SERIALIZER, ConfigDef.Type.CLASS, null, ConfigDef.Importance.MEDIUM, "Value serializer",
                GROUP_OTHER, srCounter++, // NOPMD
                // UnusedAssignment
                ConfigDef.Width.NONE, VALUE_SERIALIZER);
    }

    private static void addOtherConfig(final S3SourceConfigDef configDef) {
        int awsOtherGroupCounter = 0;
        configDef.define(FETCH_PAGE_SIZE, ConfigDef.Type.INT, 10, ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.MEDIUM, "Fetch page size", GROUP_OTHER, awsOtherGroupCounter++, // NOPMD
                                                                                                     // UnusedAssignment
                ConfigDef.Width.NONE, FETCH_PAGE_SIZE);
        configDef.define(MAX_POLL_RECORDS, ConfigDef.Type.INT, 5, ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.MEDIUM, "Max poll records", GROUP_OTHER, awsOtherGroupCounter++, // NOPMD
                                                                                                      // UnusedAssignment
                ConfigDef.Width.NONE, MAX_POLL_RECORDS);
        configDef.define(KEY_CONVERTER, ConfigDef.Type.CLASS, "org.apache.kafka.connect.converters.ByteArrayConverter",
                ConfigDef.Importance.MEDIUM, "Key converter", GROUP_OTHER, awsOtherGroupCounter++, // NOPMD
                // UnusedAssignment
                ConfigDef.Width.NONE, KEY_CONVERTER);
        configDef.define(VALUE_CONVERTER, ConfigDef.Type.CLASS,
                "org.apache.kafka.connect.converters.ByteArrayConverter", ConfigDef.Importance.MEDIUM,
                "Value converter", GROUP_OTHER, awsOtherGroupCounter++, // NOPMD
                // UnusedAssignment
                ConfigDef.Width.NONE, VALUE_CONVERTER);
        configDef.define(MAX_MESSAGE_BYTES_SIZE, ConfigDef.Type.INT, 1_048_588, ConfigDef.Importance.MEDIUM,
                "The largest record batch size allowed by Kafka config max.message.bytes", GROUP_OTHER,
                awsOtherGroupCounter++, // NOPMD
                // UnusedAssignment
                ConfigDef.Width.NONE, MAX_MESSAGE_BYTES_SIZE);
    }

    private static void addAwsStsConfigGroup(final ConfigDef configDef) {
        int awsStsGroupCounter = 0;
        configDef.define(AWS_STS_ROLE_ARN, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "AWS STS Role", GROUP_AWS_STS, awsStsGroupCounter++, // NOPMD
                                                                                                  // UnusedAssignment
                ConfigDef.Width.NONE, AWS_STS_ROLE_ARN);

        configDef.define(AWS_STS_ROLE_SESSION_NAME, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "AWS STS Session name", GROUP_AWS_STS, awsStsGroupCounter++, // NOPMD
                // UnusedAssignment
                ConfigDef.Width.NONE, AWS_STS_ROLE_SESSION_NAME);

        configDef.define(AWS_STS_ROLE_SESSION_DURATION, ConfigDef.Type.INT, 3600,
                ConfigDef.Range.between(AwsStsRole.MIN_SESSION_DURATION, AwsStsRole.MAX_SESSION_DURATION),
                ConfigDef.Importance.MEDIUM, "AWS STS Session duration", GROUP_AWS_STS, awsStsGroupCounter++, // NOPMD
                // UnusedAssignment
                ConfigDef.Width.NONE, AWS_STS_ROLE_SESSION_DURATION);

        configDef.define(AWS_STS_ROLE_EXTERNAL_ID, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "AWS STS External Id", GROUP_AWS_STS, awsStsGroupCounter++, // NOPMD
                                                                                                         // UnusedAssignment
                ConfigDef.Width.NONE, AWS_STS_ROLE_EXTERNAL_ID);

        configDef.define(AWS_STS_CONFIG_ENDPOINT, ConfigDef.Type.STRING, AwsStsEndpointConfig.AWS_STS_GLOBAL_ENDPOINT,
                new ConfigDef.NonEmptyString(), ConfigDef.Importance.MEDIUM, "AWS STS Config Endpoint", GROUP_AWS_STS,
                awsStsGroupCounter++, // NOPMD UnusedAssignment
                ConfigDef.Width.NONE, AWS_STS_CONFIG_ENDPOINT);
    }

    private static void addS3RetryPolicies(final ConfigDef configDef) {
        var retryPolicyGroupCounter = 0;
        configDef.define(AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG, ConfigDef.Type.LONG,
                AWS_S3_RETRY_BACKOFF_DELAY_MS_DEFAULT, ConfigDef.Range.atLeast(1L), ConfigDef.Importance.MEDIUM,
                "S3 default base sleep time for non-throttled exceptions in milliseconds. " + "Default is "
                        + AWS_S3_RETRY_BACKOFF_DELAY_MS_DEFAULT + ".",
                GROUP_S3_RETRY_BACKOFF_POLICY, retryPolicyGroupCounter++, // NOPMD UnusedAssignment
                ConfigDef.Width.NONE, AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG);
        configDef.define(AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG, ConfigDef.Type.LONG,
                AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT, ConfigDef.Range.atLeast(1L), ConfigDef.Importance.MEDIUM,
                "S3 maximum back-off time before retrying a request in milliseconds. " + "Default is "
                        + AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT + ".",
                GROUP_S3_RETRY_BACKOFF_POLICY, retryPolicyGroupCounter++, // NOPMD UnusedAssignment
                ConfigDef.Width.NONE, AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG);
        configDef.define(AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG, ConfigDef.Type.INT,
                S3_RETRY_BACKOFF_MAX_RETRIES_DEFAULT, ConfigDef.Range.between(1L, 30), ConfigDef.Importance.MEDIUM,
                "Maximum retry limit " + "(if the value is greater than 30, "
                        + "there can be integer overflow issues during delay calculation). " + "Default is "
                        + S3_RETRY_BACKOFF_MAX_RETRIES_DEFAULT + ".",
                GROUP_S3_RETRY_BACKOFF_POLICY, retryPolicyGroupCounter++, // NOPMD UnusedAssignment
                ConfigDef.Width.NONE, AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG);
    }

    private static void addOffsetStorageConfig(final ConfigDef configDef) {
        configDef.define(TARGET_TOPIC_PARTITIONS, ConfigDef.Type.STRING, "0", new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "eg : 0,1", GROUP_OFFSET_TOPIC, 0, ConfigDef.Width.NONE,
                TARGET_TOPIC_PARTITIONS);
        configDef.define(TARGET_TOPICS, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "eg : connect-storage-offsets", GROUP_OFFSET_TOPIC, 0,
                ConfigDef.Width.NONE, TARGET_TOPICS);
    }

    private static void addDeprecatedConfiguration(final ConfigDef configDef) {
        configDef.define(AWS_S3_PREFIX_CONFIG, ConfigDef.Type.STRING, "prefix", new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM,
                "[Deprecated] Use `file.name.template` instead. Prefix for stored objects, e.g. cluster-1/", GROUP_AWS,
                0, ConfigDef.Width.NONE, AWS_S3_PREFIX_CONFIG);
    }

    private static void addAwsConfigGroup(final ConfigDef configDef) {
        int awsGroupCounter = 0;

        configDef.define(AWS_ACCESS_KEY_ID_CONFIG, ConfigDef.Type.PASSWORD, null, new NonEmptyPassword(),
                ConfigDef.Importance.MEDIUM, "AWS Access Key ID", GROUP_AWS, awsGroupCounter++, ConfigDef.Width.NONE,
                AWS_ACCESS_KEY_ID_CONFIG);

        configDef.define(AWS_SECRET_ACCESS_KEY_CONFIG, ConfigDef.Type.PASSWORD, null, new NonEmptyPassword(),
                ConfigDef.Importance.MEDIUM, "AWS Secret Access Key", GROUP_AWS, awsGroupCounter++,
                ConfigDef.Width.NONE, AWS_SECRET_ACCESS_KEY_CONFIG);

        configDef.define(AWS_CREDENTIALS_PROVIDER_CONFIG, ConfigDef.Type.CLASS, AWS_CREDENTIAL_PROVIDER_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                "When you initialize a new " + "service client without supplying any arguments, "
                        + "the AWS SDK for Java attempts to find temporary "
                        + "credentials by using the default credential " + "provider chain implemented by the "
                        + "DefaultAWSCredentialsProviderChain class.",

                GROUP_AWS, awsGroupCounter++, ConfigDef.Width.NONE, AWS_CREDENTIALS_PROVIDER_CONFIG);

        configDef.define(AWS_S3_BUCKET_NAME_CONFIG, ConfigDef.Type.STRING, null, new BucketNameValidator(),
                ConfigDef.Importance.MEDIUM, "AWS S3 Bucket name", GROUP_AWS, awsGroupCounter++, ConfigDef.Width.NONE,
                AWS_S3_BUCKET_NAME_CONFIG);

        // AWS S3 Server Side Encryption Algorithm configuration
        // Example values: 'AES256' for S3-managed keys, 'aws:kms' for AWS KMS-managed keys
        configDef.define(AWS_S3_SSE_ALGORITHM_CONFIG, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM,
                "AWS S3 Server Side Encryption Algorithm. Example values: 'AES256', 'aws:kms'.", GROUP_AWS,
                awsGroupCounter++, ConfigDef.Width.NONE, AWS_S3_SSE_ALGORITHM_CONFIG);

        configDef.define(AWS_S3_ENDPOINT_CONFIG, ConfigDef.Type.STRING, null, new UrlValidator(),
                ConfigDef.Importance.LOW, "Explicit AWS S3 Endpoint Address, mainly for testing", GROUP_AWS,
                awsGroupCounter++, ConfigDef.Width.NONE, AWS_S3_ENDPOINT_CONFIG);

        configDef.define(AWS_S3_REGION_CONFIG, ConfigDef.Type.STRING, null, new AwsRegionValidator(),
                ConfigDef.Importance.MEDIUM, "AWS S3 Region, e.g. us-east-1", GROUP_AWS, awsGroupCounter++, // NOPMD
                                                                                                            // UnusedAssignment
                ConfigDef.Width.NONE, AWS_S3_REGION_CONFIG);
    }

    protected static class AwsRegionValidator implements ConfigDef.Validator {
        private static final String SUPPORTED_AWS_REGIONS = Arrays.stream(Regions.values())
                .map(Regions::getName)
                .collect(Collectors.joining(", "));

        @Override
        public void ensureValid(final String name, final Object value) {
            if (Objects.nonNull(value)) {
                final String valueStr = (String) value;
                final Region region = RegionUtils.getRegion(valueStr);
                if (!RegionUtils.getRegions().contains(region)) {
                    throw new ConfigException(name, valueStr, "supported values are: " + SUPPORTED_AWS_REGIONS);
                }
            }
        }
    }

    private static class BucketNameValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            try {
                if (value != null) {
                    BucketNameUtils.validateBucketName((String) value);
                }
            } catch (final IllegalArgumentException e) {
                throw new ConfigException("Illegal bucket name: " + e.getMessage());
            }
        }
    }

    private void validate() {
        LOGGER.debug("Validating config.");
    }

    public long getS3RetryBackoffDelayMs() {
        return getLong(AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG);
    }

    public long getS3RetryBackoffMaxDelayMs() {
        return getLong(AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG);
    }

    public int getS3RetryBackoffMaxRetries() {
        return getInt(AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG);
    }

    public String getAwsS3BucketName() {
        return getString(AWS_S3_BUCKET_NAME_CONFIG);
    }

    public InputFormat getOutputFormat() {
        return InputFormat.valueOf(getString(INPUT_FORMAT_KEY).toUpperCase(Locale.ROOT));
    }

    Region getAwsS3Region() {
        // we have priority of properties if old one not set or both old and new one set
        // the new property value will be selected
        if (Objects.nonNull(getString(AWS_S3_REGION_CONFIG))) {
            return RegionUtils.getRegion(getString(AWS_S3_REGION_CONFIG));
        } else {
            return RegionUtils.getRegion(Regions.US_EAST_1.getName());
        }
    }

    String getAwsS3EndPoint() {
        return getString(AWS_S3_ENDPOINT_CONFIG);
    }

    boolean hasAwsStsRole() {
        return getStsRole().isValid();
    }

    AwsStsRole getStsRole() {
        return new AwsStsRole(getString(AWS_STS_ROLE_ARN), getString(AWS_STS_ROLE_EXTERNAL_ID),
                getString(AWS_STS_ROLE_SESSION_NAME), getInt(AWS_STS_ROLE_SESSION_DURATION));
    }

    boolean hasStsEndpointConfig() {
        return getStsEndpointConfig().isValid();
    }

    AwsStsEndpointConfig getStsEndpointConfig() {
        return new AwsStsEndpointConfig(getString(AWS_STS_CONFIG_ENDPOINT), getString(AWS_S3_REGION_CONFIG));
    }

    AwsAccessSecret getAwsCredentials() {
        return new AwsAccessSecret(getPassword(AWS_ACCESS_KEY_ID_CONFIG), getPassword(AWS_SECRET_ACCESS_KEY_CONFIG));
    }

    AWSCredentialsProvider getCustomCredentialsProvider() {
        return getConfiguredInstance(AWS_CREDENTIALS_PROVIDER_CONFIG, AWSCredentialsProvider.class);
    }

    String getTargetTopics() {
        return getString(TARGET_TOPICS);
    }

    String getTargetTopicPartitions() {
        return getString(TARGET_TOPIC_PARTITIONS);
    }

    String getSchemaRegistryUrl() {
        return getString(SCHEMA_REGISTRY_URL);
    }
}
