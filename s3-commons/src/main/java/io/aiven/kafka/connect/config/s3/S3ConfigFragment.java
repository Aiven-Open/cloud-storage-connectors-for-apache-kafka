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

package io.aiven.kafka.connect.config.s3;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

import io.aiven.commons.collections.Scale;
import io.aiven.kafka.connect.common.config.AbstractFragmentSetter;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.ConfigFragment;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.FragmentDataAccess;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.config.validators.NonEmptyPassword;
import io.aiven.kafka.connect.common.config.validators.PredicateGatedValidator;
import io.aiven.kafka.connect.common.config.validators.ScaleValidator;
import io.aiven.kafka.connect.common.config.validators.TimeScaleValidator;
import io.aiven.kafka.connect.common.config.validators.TimeZoneValidator;
import io.aiven.kafka.connect.common.config.validators.TimestampSourceValidator;
import io.aiven.kafka.connect.common.config.validators.UrlValidator;
import io.aiven.kafka.connect.common.config.validators.UsageLoggingValidator;
import io.aiven.kafka.connect.iam.AwsStsEndpointConfig;
import io.aiven.kafka.connect.iam.AwsStsRole;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.internal.BucketUtils;

/**
 * The configuration fragment that defines the S3 specific characteristics.
 */
@SuppressWarnings({ "PMD.ExcessivePublicCount", "PMD.TooManyMethods", "PMD.ExcessiveImports", "PMD.GodClass" })
public final class S3ConfigFragment extends ConfigFragment {
    public enum DelayType {
        STANDARD, EXPONENTIAL
    }

    public static final int DEFAULT_PART_SIZE = (int) Scale.MiB.asBytes(5);
    private static final Logger LOGGER = LoggerFactory.getLogger(S3ConfigFragment.class);
    @Deprecated
    public static final String OUTPUT_COMPRESSION = "output_compression";

    @Deprecated
    public static final String OUTPUT_FIELDS = "output_fields";
    @Deprecated
    public static final String TIMESTAMP_TIMEZONE = "timestamp.timezone";
    @Deprecated
    public static final String TIMESTAMP_SOURCE = "timestamp.source";
    @Deprecated
    public static final String OUTPUT_FIELD_NAME_KEY = "key";
    @Deprecated
    public static final String OUTPUT_FIELD_NAME_OFFSET = "offset";
    @Deprecated
    public static final String OUTPUT_FIELD_NAME_TIMESTAMP = "timestamp";
    @Deprecated
    public static final String OUTPUT_FIELD_NAME_VALUE = "value";
    @Deprecated
    public static final String OUTPUT_FIELD_NAME_HEADERS = "headers";

    @Deprecated
    public static final String AWS_ACCESS_KEY_ID = "aws_access_key_id";
    @Deprecated
    public static final String AWS_SECRET_ACCESS_KEY = "aws_secret_access_key";
    @Deprecated
    public static final String AWS_S3_BUCKET = "aws_s3_bucket";
    @Deprecated
    public static final String AWS_S3_ENDPOINT = "aws_s3_endpoint";
    @Deprecated
    public static final String AWS_S3_REGION = "aws_s3_region";
    @Deprecated
    public static final String AWS_S3_PREFIX = "aws_s3_prefix";
    // FIXME since we support so far both old style and new style of property names
    // Importance was set to medium,
    // as soon we will migrate to new values it must be set to HIGH
    // same for default value
    public static final String AWS_ACCESS_KEY_ID_CONFIG = "aws.access.key.id";
    public static final String AWS_SECRET_ACCESS_KEY_CONFIG = "aws.secret.access.key";
    public static final String AWS_CREDENTIALS_PROVIDER_CONFIG = "aws.credentials.provider";
    /**
     * not used in codebase
     *
     * @deprecated to be removed
     */
    @Deprecated
    public static final String AWS_CREDENTIAL_PROVIDER_DEFAULT = "com.amazonaws.auth.DefaultAWSCredentialsProviderChain";
    public static final String AWS_S3_BUCKET_NAME_CONFIG = "aws.s3.bucket.name";
    public static final String AWS_S3_SSE_ALGORITHM_CONFIG = "aws.s3.sse.algorithm";
    public static final String AWS_S3_ENDPOINT_CONFIG = "aws.s3.endpoint";
    public static final String AWS_S3_REGION_CONFIG = "aws.s3.region";
    public static final String AWS_S3_PART_SIZE = "aws.s3.part.size.bytes";
    @Deprecated
    public static final String AWS_S3_PREFIX_CONFIG = "aws.s3.prefix";
    public static final String AWS_STS_ROLE_ARN = "aws.sts.role.arn";
    public static final String AWS_STS_ROLE_EXTERNAL_ID = "aws.sts.role.external.id";
    public static final String AWS_STS_ROLE_SESSION_NAME = "aws.sts.role.session.name";
    public static final String AWS_STS_ROLE_SESSION_DURATION = "aws.sts.role.session.duration";
    public static final String AWS_STS_CONFIG_ENDPOINT = "aws.sts.config.endpoint";

    public static final String AWS_S3_RETRY_BACKOFF_TYPE_CONFIG = "aws.s3.backoff.type";
    public static final String AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG = "aws.s3.backoff.delay.ms";
    public static final String AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG = "aws.s3.backoff.max.delay.ms";
    public static final String AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG = "aws.s3.backoff.max.retries";

    public static final String FETCH_PAGE_SIZE = "aws.s3.fetch.page.size";
    /** @deprecated use SourceConfigFragment.RING_BUFFER_SIZE */
    @Deprecated
    public static final String AWS_S3_FETCH_BUFFER_SIZE = "aws.s3.fetch.buffer.size";

    private static final String GROUP_AWS = "AWS";
    private static final String GROUP_AWS_STS = "AWS STS";

    private static final String GROUP_S3_RETRY_BACKOFF_POLICY = "S3 retry backoff policy";

    // Default values from AWS SDK, since they are hidden
    public static final int AWS_S3_RETRY_BACKOFF_DELAY_MS_DEFAULT = 100;
    public static final int AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT = 20_000;
    // Comment in AWS SDK for max retries:
    // Maximum retry limit. Avoids integer overflow issues.
    //
    // NOTE: If the value is greater than 30, there can be integer overflow
    // issues during delay calculation.
    // in other words we can't use values greater than 30
    public static final int S3_RETRY_BACKOFF_MAX_RETRIES_DEFAULT = 3;

    /**
     * Constructor.
     *
     * @param dataAccess
     *            the configuration to resolve requests against.
     */
    public S3ConfigFragment(final FragmentDataAccess dataAccess) {
        super(dataAccess);
    }

    /**
     * Adds the configuration options for compression to the configuration definition.
     *
     * @param configDef
     *            the Configuration definition.
     * @return the update configuration definition
     */
    public static ConfigDef update(final ConfigDef configDef, final boolean isSink) {
        addAwsConfigGroup(configDef, isSink);
        addAwsStsConfigGroup(configDef);
        addS3RetryPolicies(configDef);
        modifyFilenamePrefixDefinition(configDef);
        return configDef;
    }

    public static Setter setter(final Map<String, String> configData) {
        return new Setter(configData);
    }

    private static void modifyFilenamePrefixDefinition(final ConfigDef configDef) {
        final ConfigDef.ConfigKey oldKey = configDef.configKeys().get(FileNameFragment.FILE_NAME_PREFIX_CONFIG);
        if (oldKey != null) {
            configDef.configKeys().remove(FileNameFragment.FILE_NAME_PREFIX_CONFIG);
            configDef.define(oldKey.name, oldKey.type, null,
                    ConfigDef.CompositeValidator.of(new ConfigDef.NonEmptyString(), oldKey.validator),
                    oldKey.importance, oldKey.documentation, oldKey.group, oldKey.orderInGroup, oldKey.width,
                    oldKey.displayName);
        }
    }

    /**
     * Set the options with default values from AWS SDK, since they are hidden
     *
     * @param configDef
     *            the configuration definition to update.
     */
    static void addS3RetryPolicies(final ConfigDef configDef) {
        var retryPolicyGroupCounter = 0;
        configDef.define(AWS_S3_RETRY_BACKOFF_TYPE_CONFIG, ConfigDef.Type.STRING, DelayType.EXPONENTIAL.name(),
                ConfigDef.CaseInsensitiveValidString
                        .in(Arrays.stream(DelayType.values()).map(Object::toString).toArray(String[]::new)),
                ConfigDef.Importance.MEDIUM, "Back off type. Default is " + DelayType.EXPONENTIAL.name() + ".",
                GROUP_S3_RETRY_BACKOFF_POLICY, ++retryPolicyGroupCounter, ConfigDef.Width.NONE,
                AWS_S3_RETRY_BACKOFF_TYPE_CONFIG);

        configDef.define(AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG, ConfigDef.Type.LONG,
                AWS_S3_RETRY_BACKOFF_DELAY_MS_DEFAULT, TimeScaleValidator.atLeast(1), ConfigDef.Importance.MEDIUM,
                String.format(
                        "S3 default base sleep time for non-throttled exceptions in milliseconds. Applies only if %s is %s. Default is %s.",
                        AWS_S3_RETRY_BACKOFF_TYPE_CONFIG, DelayType.EXPONENTIAL, AWS_S3_RETRY_BACKOFF_DELAY_MS_DEFAULT),
                GROUP_S3_RETRY_BACKOFF_POLICY, ++retryPolicyGroupCounter, ConfigDef.Width.NONE,
                AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG);
        configDef.define(AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG, ConfigDef.Type.LONG,
                AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT, TimeScaleValidator.atLeast(1), ConfigDef.Importance.MEDIUM,
                String.format(
                        "S3 maximum back-off time before retrying a request in milliseconds. Applies only if %s is %s. Default is %s.",
                        AWS_S3_RETRY_BACKOFF_TYPE_CONFIG, DelayType.EXPONENTIAL,
                        AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT),
                GROUP_S3_RETRY_BACKOFF_POLICY, ++retryPolicyGroupCounter, ConfigDef.Width.NONE,
                AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG);
        // Comment in AWS SDK for max retries:
        // Maximum retry limit. Avoids integer overflow issues.
        //
        // NOTE: If the value is greater than 30, there can be integer overflow
        // issues during delay calculation.
        // in other words we can't use values greater than 30
        configDef.define(AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG, ConfigDef.Type.INT,
                S3_RETRY_BACKOFF_MAX_RETRIES_DEFAULT, ConfigDef.Range.between(1L, 30), ConfigDef.Importance.MEDIUM,
                String.format("Maximum retry limit "
                        + "(if the value is greater than 30, there can be integer overflow issues during delay calculation). "
                        + " Applies only if %s is %s. Default is %s.", AWS_S3_RETRY_BACKOFF_TYPE_CONFIG,
                        DelayType.EXPONENTIAL, S3_RETRY_BACKOFF_MAX_RETRIES_DEFAULT),
                GROUP_S3_RETRY_BACKOFF_POLICY, ++retryPolicyGroupCounter, ConfigDef.Width.NONE,
                AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG);
    }

    static void addAwsConfigGroup(final ConfigDef configDef, final boolean isSink) {
        int awsGroupCounter = 0;

        configDef.define(AWS_ACCESS_KEY_ID_CONFIG, ConfigDef.Type.PASSWORD, null, new NonEmptyPassword(),
                ConfigDef.Importance.MEDIUM, "AWS Access Key ID", GROUP_AWS, ++awsGroupCounter, ConfigDef.Width.NONE,
                AWS_ACCESS_KEY_ID_CONFIG);

        configDef.define(AWS_SECRET_ACCESS_KEY_CONFIG, ConfigDef.Type.PASSWORD, null, new NonEmptyPassword(),
                ConfigDef.Importance.MEDIUM, "AWS Secret Access Key", GROUP_AWS, ++awsGroupCounter,
                ConfigDef.Width.NONE, AWS_SECRET_ACCESS_KEY_CONFIG);

        configDef.define(AWS_CREDENTIALS_PROVIDER_CONFIG, ConfigDef.Type.CLASS, null, ConfigDef.Importance.MEDIUM,
                "When you initialize a new service client without supplying any arguments, "
                        + "the AWS SDK for Java attempts to find temporary "
                        + "credentials by using the default credential provider chain.",
                GROUP_AWS, ++awsGroupCounter, ConfigDef.Width.NONE, AWS_CREDENTIALS_PROVIDER_CONFIG);

        configDef.define(AWS_S3_BUCKET_NAME_CONFIG, ConfigDef.Type.STRING, null, new BucketNameValidator(),
                ConfigDef.Importance.MEDIUM, "AWS S3 Bucket name", GROUP_AWS, ++awsGroupCounter, ConfigDef.Width.NONE,
                AWS_S3_BUCKET_NAME_CONFIG);

        // AWS S3 Server Side Encryption Algorithm configuration
        // Example values: 'AES256' for S3-managed keys, 'aws:kms' for AWS KMS-managed keys
        configDef.define(AWS_S3_SSE_ALGORITHM_CONFIG, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM,
                "AWS S3 Server Side Encryption Algorithm. Example values: 'AES256', 'aws:kms'.", GROUP_AWS,
                ++awsGroupCounter, ConfigDef.Width.NONE, AWS_S3_SSE_ALGORITHM_CONFIG);

        configDef.define(AWS_S3_ENDPOINT_CONFIG, ConfigDef.Type.STRING, null, new UrlValidator(),
                ConfigDef.Importance.LOW, "Explicit AWS S3 Endpoint Address, mainly for testing", GROUP_AWS,
                ++awsGroupCounter, ConfigDef.Width.NONE, AWS_S3_ENDPOINT_CONFIG);

        configDef.define(AWS_S3_REGION_CONFIG, ConfigDef.Type.STRING, null, new AwsRegionValidator(),
                ConfigDef.Importance.MEDIUM, "AWS S3 Region, e.g. us-east-1", GROUP_AWS, ++awsGroupCounter,
                ConfigDef.Width.NONE, AWS_S3_REGION_CONFIG);

        configDef.define(FETCH_PAGE_SIZE, ConfigDef.Type.INT, 10, ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.MEDIUM, "AWS S3 Fetch page size", GROUP_AWS, ++awsGroupCounter,
                ConfigDef.Width.NONE, FETCH_PAGE_SIZE);

        if (isSink) {
            configDef.define(AWS_S3_PART_SIZE, ConfigDef.Type.LONG, DEFAULT_PART_SIZE,
                    ScaleValidator.between(Scale.MiB.asBytes(1), Integer.MAX_VALUE, Scale.IEC),
                    ConfigDef.Importance.MEDIUM,
                    "The Part Size in S3 Multi-part Uploads in bytes. Maximum is "
                            + Scale.scaleOf(Integer.MAX_VALUE, Scale.IEC) + " and default is "
                            + Scale.size(S3ConfigFragment.DEFAULT_PART_SIZE, Scale.IEC),
                    GROUP_AWS, ++awsGroupCounter, ConfigDef.Width.NONE, S3ConfigFragment.AWS_S3_PART_SIZE);
        }

        // deprecated options
        addDeprecatedConfiguration(configDef, awsGroupCounter, isSink);

    }

    private static String deprecatedDescription(final String deprecatedKey, final ConfigDef.ConfigKey validKey) {
        return String.format("%s property is deprecated, use %s. %s", deprecatedKey, validKey.name,
                validKey.documentation);
    }

    private static int deprecation(final int counter, final ConfigDef configDef, final String deprecatedKey,
            final String validKey) {
        final int result = counter + 1;
        final ConfigDef.ConfigKey key = configDef.configKeys().get(validKey);
        final String description = deprecatedDescription(deprecatedKey, key);
        configDef.define(deprecatedKey, key.type(), null,
                new UsageLoggingValidator(key.validator, (n, v) -> logDeprecated(LOGGER, deprecatedKey, validKey)),
                key.importance, description, GROUP_AWS, result, key.width, deprecatedKey);
        return result;
    }

    static void addDeprecatedConfiguration(final ConfigDef configDef, final int awsGroupCounter, final boolean isSink) {
        int counter = awsGroupCounter;
        if (isSink) {
            // Output fields in different group
            final ConfigDef.ConfigKey key = configDef.configKeys()
                    .get(OutputFormatFragment.FORMAT_OUTPUT_FIELDS_CONFIG);
            final String description = deprecatedDescription(OUTPUT_FIELDS, key);
            configDef.define(OUTPUT_FIELDS, key.type(), null,
                    new UsageLoggingValidator(OutputFormatFragment.OUTPUT_FIELDS_VALIDATOR,
                            (name, value) -> logDeprecated(LOGGER, name,
                                    OutputFormatFragment.FORMAT_OUTPUT_FIELDS_CONFIG)),
                    key.importance, description, OutputFormatFragment.GROUP_NAME, 50, key.width, OUTPUT_FIELDS);
        } else {
            // allow null values for deprecated value.
            final ConfigDef.ConfigKey key = configDef.configKeys().get(SourceConfigFragment.RING_BUFFER_SIZE);
            final String description = deprecatedDescription(AWS_S3_FETCH_BUFFER_SIZE, key);
            configDef.define(AWS_S3_FETCH_BUFFER_SIZE, key.type(), null, new UsageLoggingValidator((n, v) -> {
            }, (n, v) -> logDeprecated(LOGGER, AWS_S3_FETCH_BUFFER_SIZE, SourceConfigFragment.RING_BUFFER_SIZE)),
                    key.importance, description, GROUP_AWS, ++counter, key.width, AWS_S3_FETCH_BUFFER_SIZE);
        }
        counter = deprecation(counter, configDef, AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY_ID_CONFIG);
        counter = deprecation(counter, configDef, AWS_SECRET_ACCESS_KEY, AWS_SECRET_ACCESS_KEY_CONFIG);
        counter = deprecation(counter, configDef, AWS_S3_BUCKET, AWS_S3_BUCKET_NAME_CONFIG);
        counter = deprecation(counter, configDef, AWS_S3_ENDPOINT, AWS_S3_ENDPOINT_CONFIG);
        counter = deprecation(counter, configDef, AWS_S3_REGION, AWS_S3_REGION_CONFIG);
        if (isSink) {
            counter = deprecation(counter, configDef, AWS_S3_PREFIX, FileNameFragment.FILE_NAME_PREFIX_CONFIG);
            deprecation(counter, configDef, AWS_S3_PREFIX_CONFIG, FileNameFragment.FILE_NAME_PREFIX_CONFIG);
        } else {
            counter = deprecation(counter, configDef, AWS_S3_PREFIX, FileNameFragment.FILE_PATH_PREFIX_TEMPLATE_CONFIG);
            deprecation(counter, configDef, AWS_S3_PREFIX_CONFIG, FileNameFragment.FILE_PATH_PREFIX_TEMPLATE_CONFIG);
        }

        // deprecations in different groups

        configDef.define(OUTPUT_COMPRESSION, ConfigDef.Type.STRING, null,
                new UsageLoggingValidator(
                        new PredicateGatedValidator(Objects::nonNull, FileNameFragment.COMPRESSION_TYPE_VALIDATOR),
                        (name, value) -> logDeprecated(LOGGER, name, FileNameFragment.FILE_COMPRESSION_TYPE_CONFIG)),
                ConfigDef.Importance.MEDIUM, "Output compression.", FileNameFragment.GROUP_NAME, 50,
                ConfigDef.Width.SHORT, OUTPUT_COMPRESSION);

        configDef.define(TIMESTAMP_TIMEZONE, ConfigDef.Type.STRING, null, new UsageLoggingValidator(
                new PredicateGatedValidator(Objects::nonNull, new TimeZoneValidator()),
                (n, v) -> logDeprecated(LOGGER, TIMESTAMP_TIMEZONE, FileNameFragment.FILE_NAME_TIMESTAMP_TIMEZONE)),
                ConfigDef.Importance.LOW,
                "Specifies the timezone in which the dates and time for the timestamp variable will be treated. "
                        + "Use standard shot and long names. Default is UTC",
                FileNameFragment.GROUP_NAME, 51, ConfigDef.Width.SHORT, TIMESTAMP_TIMEZONE);

        configDef.define(TIMESTAMP_SOURCE, ConfigDef.Type.STRING, null,
                new UsageLoggingValidator(new PredicateGatedValidator(Objects::nonNull, new TimestampSourceValidator()),
                        (n, v) -> logDeprecated(LOGGER, TIMESTAMP_SOURCE, FileNameFragment.FILE_NAME_TIMESTAMP_SOURCE)),
                ConfigDef.Importance.LOW, "Specifies the the timestamp variable source. Default is wall-clock.",
                FileNameFragment.GROUP_NAME, 52, ConfigDef.Width.SHORT, TIMESTAMP_SOURCE);

    }

    static void addAwsStsConfigGroup(final ConfigDef configDef) {
        int awsStsGroupCounter = 0;
        configDef.define(AWS_STS_ROLE_ARN, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "AWS STS Role", GROUP_AWS_STS, ++awsStsGroupCounter, ConfigDef.Width.NONE,
                AWS_STS_ROLE_ARN);

        configDef.define(AWS_STS_ROLE_SESSION_NAME, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "AWS STS Session name", GROUP_AWS_STS, ++awsStsGroupCounter,
                ConfigDef.Width.NONE, AWS_STS_ROLE_SESSION_NAME);

        configDef.define(AWS_STS_ROLE_SESSION_DURATION, ConfigDef.Type.INT, 3600,
                ConfigDef.Range.between(AwsStsRole.MIN_SESSION_DURATION, AwsStsRole.MAX_SESSION_DURATION),
                ConfigDef.Importance.MEDIUM, "AWS STS Session duration", GROUP_AWS_STS, ++awsStsGroupCounter,
                ConfigDef.Width.NONE, AWS_STS_ROLE_SESSION_DURATION);

        configDef.define(AWS_STS_ROLE_EXTERNAL_ID, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "AWS STS External Id", GROUP_AWS_STS, ++awsStsGroupCounter,
                ConfigDef.Width.NONE, AWS_STS_ROLE_EXTERNAL_ID);

        configDef.define(AWS_STS_CONFIG_ENDPOINT, ConfigDef.Type.STRING, AwsStsEndpointConfig.AWS_STS_GLOBAL_ENDPOINT,
                new ConfigDef.NonEmptyString(), ConfigDef.Importance.MEDIUM, "AWS STS Config Endpoint", GROUP_AWS_STS,
                ++awsStsGroupCounter, ConfigDef.Width.NONE, AWS_STS_CONFIG_ENDPOINT);
    }

    /**
     * Validate the various variables do not conflict.
     *
     * @param configMap
     *            the distribution type for the validator
     */
    @Override
    public void validate(final Map<String, ConfigValue> configMap) {
        validateCredentials(configMap);
        validateBucket(configMap);
    }

    public void validateCredentials(final Map<String, ConfigValue> configMap) {
        final AwsStsRole awsStsRole = getStsRole();

        if (awsStsRole.isValid()) {
            final AwsStsEndpointConfig stsEndpointConfig = getStsEndpointConfig();
            if (!stsEndpointConfig.isValid()
                    && !AwsStsEndpointConfig.AWS_STS_GLOBAL_ENDPOINT.equals(stsEndpointConfig.getServiceEndpoint())) {
                if (StringUtils.isEmpty(getString(AWS_S3_REGION_CONFIG))) {
                    registerIssue(configMap, AWS_S3_REGION_CONFIG, getString(AWS_S3_REGION_CONFIG), String.format(
                            "%s should be specified together with %s", AWS_S3_REGION_CONFIG, AWS_STS_CONFIG_ENDPOINT));
                } else {
                    registerIssue(configMap, AWS_STS_CONFIG_ENDPOINT, getString(AWS_STS_CONFIG_ENDPOINT), String.format(
                            "%s should be specified together with %s", AWS_S3_REGION_CONFIG, AWS_STS_CONFIG_ENDPOINT));
                }
            }
        } else {
            final AwsBasicCredentials awsCredentialsV2 = getAwsCredentialsV2();
            if (awsCredentialsV2 == null) {
                LOGGER.info(
                        "Connector uses {} as credential Provider, "
                                + "when configuration for {{}, {}} OR {{}, {}} are absent",
                        AWS_CREDENTIALS_PROVIDER_CONFIG, AWS_ACCESS_KEY_ID_CONFIG, AWS_SECRET_ACCESS_KEY_CONFIG,
                        AWS_STS_ROLE_ARN, AWS_STS_ROLE_SESSION_NAME);
            }
        }
    }

    public void validateBucket(final Map<String, ConfigValue> configMap) {
        if (Objects.isNull(getString(AWS_S3_BUCKET_NAME_CONFIG)) && Objects.isNull(getString(AWS_S3_BUCKET))) {
            registerIssue(configMap, AWS_S3_BUCKET_NAME_CONFIG, getString(AWS_S3_BUCKET_NAME_CONFIG),
                    AWS_S3_BUCKET_NAME_CONFIG + " should be specified");
        }
    }

    // Custom Validators
    protected static class AwsRegionValidator implements ConfigDef.Validator {
        private static final String SUPPORTED_AWS_REGIONS = Region.regions()
                .stream()
                .map(Region::id)
                .collect(Collectors.joining(", "));

        @Override
        public void ensureValid(final String name, final Object value) {
            if (Objects.nonNull(value)) {
                final String valueStr = (String) value;
                if (Region.regions().stream().noneMatch(r -> r.id().equals(valueStr))) {
                    throw new ConfigException(name, valueStr, "See documentation for list of valid regions.");
                }
            }
        }

        @Override
        public String toString() {
            return SUPPORTED_AWS_REGIONS;
        }

    }

    private static class BucketNameValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            try {
                if (value != null) {
                    BucketUtils.isValidDnsBucketName((String) value, true);
                }
            } catch (final IllegalArgumentException e) {
                throw new ConfigException("Illegal bucket name: " + e.getMessage());
            }
        }

        @Override
        public String toString() {
            return "Bucket name may not be null, may contain only the characters A-Z, a-z, 0-9, '-', '.', '_' must be between 3 and 63 characters long, must not be formatted as an IP Address, must not contain uppercase characters or white space, "
                    + "must not end with a period or a dash nor contains two adjacent periods, must not contain dashes next to periods nor begin with a dash";
        }
    }

    public AwsStsRole getStsRole() {
        return new AwsStsRole(getString(AWS_STS_ROLE_ARN), getString(AWS_STS_ROLE_EXTERNAL_ID),
                getString(AWS_STS_ROLE_SESSION_NAME), getInt(AWS_STS_ROLE_SESSION_DURATION));
    }

    public boolean hasAwsStsRole() {
        return getStsRole().isValid();
    }

    public boolean hasStsEndpointConfig() {
        return getStsEndpointConfig().isValid();
    }

    public AwsStsEndpointConfig getStsEndpointConfig() {
        return new AwsStsEndpointConfig(getString(AWS_STS_CONFIG_ENDPOINT), getString(AWS_S3_REGION_CONFIG));
    }

    public AwsBasicCredentials getAwsCredentialsV2() {
        if (Objects.nonNull(getPassword(AWS_ACCESS_KEY_ID_CONFIG))
                && Objects.nonNull(getPassword(AWS_SECRET_ACCESS_KEY_CONFIG))) {

            return AwsBasicCredentials.create(getPassword(AWS_ACCESS_KEY_ID_CONFIG).value(),
                    getPassword(AWS_SECRET_ACCESS_KEY_CONFIG).value());
        } else if (Objects.nonNull(getPassword(AWS_ACCESS_KEY_ID))
                && Objects.nonNull(getPassword(AWS_SECRET_ACCESS_KEY))) {
            LOGGER.warn("Config options {} and {} are not supported for this Connector", AWS_ACCESS_KEY_ID,
                    AWS_SECRET_ACCESS_KEY);
        }
        return null;
    }

    public String getAwsS3EndPoint() {
        return Objects.nonNull(getString(AWS_S3_ENDPOINT_CONFIG))
                ? getString(AWS_S3_ENDPOINT_CONFIG)
                : getString(AWS_S3_ENDPOINT);
    }

    public Region getAwsS3RegionV2() {
        if (Objects.nonNull(getString(AWS_S3_REGION_CONFIG))) {
            return Region.of(getString(AWS_S3_REGION_CONFIG));
        }
        return Region.of(Region.US_EAST_1.id());
    }

    public String getAwsS3BucketName() {
        return getString(AWS_S3_BUCKET_NAME_CONFIG);
    }

    public String getServerSideEncryptionAlgorithmName() {
        return getString(AWS_S3_SSE_ALGORITHM_CONFIG);
    }

    public int getAwsS3PartSize() {
        return getLong(AWS_S3_PART_SIZE).intValue();
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

    public DelayType getDelayType() {
        return DelayType.valueOf(getString(AWS_S3_RETRY_BACKOFF_TYPE_CONFIG).toUpperCase(Locale.ROOT));
    }

    /**
     * Gets the Aws Credentials provider.
     *
     * @return the Aws Credentials provider.
     */
    public AwsCredentialsProvider getCustomCredentialsProviderV2() {
        final AwsCredentialsProvider result = getConfiguredInstance(AWS_CREDENTIALS_PROVIDER_CONFIG,
                AwsCredentialsProvider.class);
        return result != null ? result : DefaultCredentialsProvider.builder().build();
    }

    public int getFetchPageSize() {
        return getInt(FETCH_PAGE_SIZE);
    }

    /**
     * Copies deprecated key data to valid key data if valid key data is not set and deprecated key data is valid.
     *
     * @param properties
     *            the map of properties.
     * @param deprecatedKey
     *            the deprecated key
     * @param depFilter
     *            if {@code true} this the deprecated key value is valid.
     * @param validKey
     *            the valid key.
     * @param mayOverwrite
     *            if {@code true} the valid key data may be overwritten.
     * @return
     */
    private static boolean moveData(final Map<String, String> properties, final String deprecatedKey,
            final Predicate<String> depFilter, final String validKey, final Predicate<String> mayOverwrite) {
        // is the deprecatedKey data valid ?
        if (properties.containsKey(deprecatedKey) && depFilter.test(properties.get(deprecatedKey))) {
            // should we overwrite the validKey ?
            if (!properties.containsKey(validKey) || mayOverwrite.test(properties.get(validKey))) {
                logDeprecated(LOGGER, deprecatedKey, "value of %s is being placed into %s", deprecatedKey, validKey);
                properties.put(validKey, properties.get(deprecatedKey));
                return true;
            } else {
                logDeprecated(LOGGER, deprecatedKey, validKey);
            }
        }
        return false;
    }

    /**
     * Handle moving deprecated values.
     *
     * @param properties
     *            the properties to update.
     * @return the updated properties.
     */
    public static Map<String, String> handleDeprecatedOptions(final Map<String, String> properties,
            final CompressionType defaultCompression) {
        // we need to have the old OUTPUT_COMPRESSION take priority over the new FILE_COMPRESSION_TYPE_CONFIG
        moveData(properties, OUTPUT_COMPRESSION, Objects::nonNull, FileNameFragment.FILE_COMPRESSION_TYPE_CONFIG,
                t -> t == null || defaultCompression.name().equalsIgnoreCase(t));

        FileNameFragment.replaceYyyyUppercase(properties, AWS_S3_PREFIX_CONFIG, AWS_S3_PREFIX);

        moveData(properties, AWS_S3_FETCH_BUFFER_SIZE, x -> true, SourceConfigFragment.RING_BUFFER_SIZE, x -> true);

        // ensure that FileNameFragment.FILE_NAME_PREFIX_CONFIG contains the prefix if it is set.
        // order of precedence: FileNameFragment.FILE_NAME_PREFIX_CONFIG, AWS_S3_PREFIX_CONFIG, AWS_S3_PREFIX
        if (!moveData(properties, AWS_S3_PREFIX_CONFIG, Objects::nonNull, FileNameFragment.FILE_NAME_PREFIX_CONFIG,
                x -> x != null && x.isBlank())) {
            moveData(properties, AWS_S3_PREFIX, Objects::nonNull, FileNameFragment.FILE_NAME_PREFIX_CONFIG,
                    x -> x != null && x.isBlank());
        }

        moveData(properties, OUTPUT_FIELDS, Objects::nonNull, OutputFormatFragment.FORMAT_OUTPUT_FIELDS_CONFIG,
                Objects::isNull);

        moveData(properties, TIMESTAMP_TIMEZONE, Objects::nonNull, FileNameFragment.FILE_NAME_TIMESTAMP_TIMEZONE,
                Objects::isNull);

        moveData(properties, TIMESTAMP_SOURCE, Objects::nonNull, FileNameFragment.FILE_NAME_TIMESTAMP_SOURCE,
                Objects::isNull);

        moveData(properties, AWS_ACCESS_KEY_ID, Objects::nonNull, AWS_ACCESS_KEY_ID_CONFIG, Objects::isNull);
        moveData(properties, AWS_SECRET_ACCESS_KEY, Objects::nonNull, AWS_SECRET_ACCESS_KEY_CONFIG, Objects::isNull);
        moveData(properties, AWS_S3_BUCKET, Objects::nonNull, AWS_S3_BUCKET_NAME_CONFIG, Objects::isNull);
        moveData(properties, AWS_S3_ENDPOINT, Objects::nonNull, AWS_S3_ENDPOINT_CONFIG, Objects::isNull);
        moveData(properties, AWS_S3_REGION, Objects::nonNull, AWS_S3_REGION_CONFIG, Objects::isNull);

        return properties;
    }

    /**
     * A setter for the S3ConfigFragment.
     */
    public final static class Setter extends AbstractFragmentSetter<Setter> {

        private Setter(final Map<String, String> data) {
            super(data);
        }

        public Setter accessKeyId(final String accessKeyId) {
            return setValue(AWS_ACCESS_KEY_ID_CONFIG, accessKeyId);
        }

        public Setter accessKeySecret(final String accessKeySecret) {
            return setValue(AWS_SECRET_ACCESS_KEY_CONFIG, accessKeySecret);
        }

        public Setter bucketName(final String bucketName) {
            return setValue(AWS_S3_BUCKET_NAME_CONFIG, bucketName);
        }

        public Setter credentialsProvider(final Class<? extends AwsCredentialsProvider> credentialsProvider) {
            return setValue(AWS_CREDENTIALS_PROVIDER_CONFIG, credentialsProvider.getCanonicalName());
        }

        public Setter credentialsProvider(final String credentialsProvider) {
            return setValue(AWS_CREDENTIALS_PROVIDER_CONFIG, credentialsProvider);
        }

        public Setter endpoint(final URI endpoint) {
            return setValue(AWS_S3_ENDPOINT_CONFIG, endpoint);
        }

        public Setter endpoint(final String endpoint) {
            return setValue(AWS_S3_ENDPOINT_CONFIG, endpoint);
        }

        public Setter fetchPageSize(final int fetchPageSize) {
            return setValue(FETCH_PAGE_SIZE, fetchPageSize);
        }

        public Setter partSize(final long partSize) {
            return setValue(AWS_S3_PART_SIZE, partSize);
        }

        public Setter prefix(final String prefix) {
            return setValue(AWS_S3_PREFIX_CONFIG, prefix);
        }

        public Setter region(final String region) {
            return setValue(AWS_S3_REGION_CONFIG, region);
        }

        public Setter region(final Region region) {
            return setValue(AWS_S3_REGION_CONFIG, region.toString());
        }

        public Setter retryBackoffDelay(final Duration duration) {
            return setValue(AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG, duration.toMillis());
        }

        public Setter retryBackoffMaxDelay(final Duration duration) {
            return setValue(AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG, duration.toMillis());
        }

        public Setter retryBackoffMaxRetries(final int retries) {
            return setValue(AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG, retries);
        }

        public Setter sseAlgorithm(final String sseAlgorithm) {
            return setValue(AWS_S3_SSE_ALGORITHM_CONFIG, sseAlgorithm);
        }

        public Setter stsEndpoint(final String stsEndpoint) {
            return setValue(AWS_STS_CONFIG_ENDPOINT, stsEndpoint);
        }

        public Setter stsRoleArn(final String stsRoleArn) {
            return setValue(AWS_STS_ROLE_ARN, stsRoleArn);
        }

        public Setter stsRoleExternalId(final String stsRoleExternalId) {
            return setValue(AWS_STS_ROLE_EXTERNAL_ID, stsRoleExternalId);
        }

        public Setter stsRoleSessionName(final String stsRoleSessionName) {
            return setValue(AWS_STS_ROLE_SESSION_NAME, stsRoleSessionName);
        }

        public Setter stsRoleSessionDuration(final Duration stsRoleSessionDuration) {
            return setValue(AWS_STS_ROLE_SESSION_DURATION, stsRoleSessionDuration.toSeconds());
        }
    }
}
