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

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.config.ConfigFragment;
import io.aiven.kafka.connect.common.config.validators.FileCompressionTypeValidator;
import io.aiven.kafka.connect.common.config.validators.NonEmptyPassword;
import io.aiven.kafka.connect.common.config.validators.OutputFieldsValidator;
import io.aiven.kafka.connect.common.config.validators.UrlValidator;
import io.aiven.kafka.connect.iam.AwsStsEndpointConfig;
import io.aiven.kafka.connect.iam.AwsStsRole;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.internal.BucketNameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * The configuration fragment that defines the S3 specific characteristics.
 */
@SuppressWarnings({ "PMD.TooManyMethods", "PMD.ExcessiveImports", "PMD.TooManyStaticImports", "PMD.GodClass" })
public final class S3ConfigFragment extends ConfigFragment {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3ConfigFragment.class);
    @Deprecated
    public static final String OUTPUT_COMPRESSION = "output_compression";
    @Deprecated
    public static final String OUTPUT_COMPRESSION_TYPE_GZIP = "gzip";
    @Deprecated
    public static final String OUTPUT_COMPRESSION_TYPE_NONE = "none";

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
    public static final String AWS_CREDENTIAL_PROVIDER_DEFAULT = "com.amazonaws.auth.DefaultAWSCredentialsProviderChain";
    public static final String AWS_S3_BUCKET_NAME_CONFIG = "aws.s3.bucket.name";
    public static final String AWS_S3_SSE_ALGORITHM_CONFIG = "aws.s3.sse.algorithm";
    public static final String AWS_S3_ENDPOINT_CONFIG = "aws.s3.endpoint";
    public static final String AWS_S3_REGION_CONFIG = "aws.s3.region";
    public static final String AWS_S3_PART_SIZE = "aws.s3.part.size.bytes";

    public static final String AWS_S3_PREFIX_CONFIG = "aws.s3.prefix";
    public static final String AWS_STS_ROLE_ARN = "aws.sts.role.arn";
    public static final String AWS_STS_ROLE_EXTERNAL_ID = "aws.sts.role.external.id";
    public static final String AWS_STS_ROLE_SESSION_NAME = "aws.sts.role.session.name";
    public static final String AWS_STS_ROLE_SESSION_DURATION = "aws.sts.role.session.duration";
    public static final String AWS_STS_CONFIG_ENDPOINT = "aws.sts.config.endpoint";

    public static final String AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG = "aws.s3.backoff.delay.ms";
    public static final String AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG = "aws.s3.backoff.max.delay.ms";
    public static final String AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG = "aws.s3.backoff.max.retries";

    public static final String FETCH_PAGE_SIZE = "aws.s3.fetch.page.size";

    private static final String GROUP_AWS = "AWS";
    private static final String GROUP_AWS_STS = "AWS STS";

    private static final String GROUP_S3_RETRY_BACKOFF_POLICY = "S3 retry backoff policy";

    public static final int DEFAULT_PART_SIZE = 5 * 1024 * 1024;

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
     * @param cfg
     *            the configuration to resolve requests against.
     */
    public S3ConfigFragment(final AbstractConfig cfg) {
        super(cfg);
    }

    /**
     * Adds the configuration options for compression to the configuration definition.
     *
     * @param configDef
     *            the Configuration definition.
     * @return the update configuration definition
     */
    public static ConfigDef update(final ConfigDef configDef) {
        addAwsConfigGroup(configDef);
        addAwsStsConfigGroup(configDef);
        addDeprecatedConfiguration(configDef);
        addS3RetryPolicies(configDef);
        return configDef;
    }

    static void addS3RetryPolicies(final ConfigDef configDef) {
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

    static void addAwsConfigGroup(final ConfigDef configDef) {
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
                ConfigDef.Importance.MEDIUM, "AWS S3 Region, e.g. us-east-1", GROUP_AWS, awsGroupCounter++,
                ConfigDef.Width.NONE, AWS_S3_REGION_CONFIG);

        configDef.define(AWS_S3_PREFIX_CONFIG, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "Prefix for stored objects, e.g. cluster-1/", GROUP_AWS, awsGroupCounter++,
                ConfigDef.Width.NONE, AWS_S3_PREFIX_CONFIG);

        configDef.define(FETCH_PAGE_SIZE, ConfigDef.Type.INT, 10, ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.MEDIUM, "AWS S3 Fetch page size", GROUP_AWS, awsGroupCounter++, // NOPMD
                // UnusedAssignment
                ConfigDef.Width.NONE, FETCH_PAGE_SIZE);
    }

    static void addAwsStsConfigGroup(final ConfigDef configDef) {
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

    static void addDeprecatedConfiguration(final ConfigDef configDef) {

        configDef.define(AWS_ACCESS_KEY_ID, ConfigDef.Type.PASSWORD, null, new NonEmptyPassword() {
            @Override
            public void ensureValid(final String name, final Object value) {
                LOGGER.info(AWS_ACCESS_KEY_ID + " property is deprecated please read documentation for the new name");
                super.ensureValid(name, value);
            }
        }, ConfigDef.Importance.MEDIUM, "AWS Access Key ID");

        configDef.define(AWS_SECRET_ACCESS_KEY, ConfigDef.Type.PASSWORD, null, new NonEmptyPassword() {
            @Override
            public void ensureValid(final String name, final Object value) {
                LOGGER.info(
                        AWS_SECRET_ACCESS_KEY + " property is deprecated please read documentation for the new name");
                super.ensureValid(name, value);
            }
        }, ConfigDef.Importance.MEDIUM, "AWS Secret Access Key");

        configDef.define(AWS_S3_BUCKET, ConfigDef.Type.STRING, null, new BucketNameValidator() {
            @Override
            public void ensureValid(final String name, final Object object) {
                LOGGER.info(AWS_S3_BUCKET + " property is deprecated please read documentation for the new name");
                super.ensureValid(name, object);
            }
        }, ConfigDef.Importance.MEDIUM, "AWS S3 Bucket name");

        configDef.define(AWS_S3_ENDPOINT, ConfigDef.Type.STRING, null, new UrlValidator() {
            @Override
            public void ensureValid(final String name, final Object object) {
                LOGGER.info(AWS_S3_ENDPOINT + " property is deprecated please read documentation for the new name");
                super.ensureValid(name, object);
            }
        }, ConfigDef.Importance.LOW, "Explicit AWS S3 Endpoint Address, mainly for testing");

        configDef.define(AWS_S3_REGION, ConfigDef.Type.STRING, null, new AwsRegionValidator() {
            @Override
            public void ensureValid(final String name, final Object object) {
                LOGGER.info(AWS_S3_REGION + " property is deprecated please read documentation for the new name");
                super.ensureValid(name, object);
            }
        }, ConfigDef.Importance.MEDIUM, "AWS S3 Region, e.g. us-east-1");

        configDef.define(AWS_S3_PREFIX, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString() {
            @Override
            public void ensureValid(final String name, final Object object) {
                LOGGER.info(AWS_S3_PREFIX + " property is deprecated please read documentation for the new name");
                super.ensureValid(name, object);
            }
        }, ConfigDef.Importance.MEDIUM, "Prefix for stored objects, e.g. cluster-1/");

        configDef.define(OUTPUT_FIELDS, ConfigDef.Type.LIST, null, new OutputFieldsValidator() {
            @Override
            public void ensureValid(final String name, final Object value) {
                LOGGER.info(OUTPUT_FIELDS + " property is deprecated please read documentation for the new name");
                super.ensureValid(name, value);
            }
        }, ConfigDef.Importance.MEDIUM,
                "Output fields. A comma separated list of one or more: " + OUTPUT_FIELD_NAME_KEY + ", "
                        + OUTPUT_FIELD_NAME_OFFSET + ", " + OUTPUT_FIELD_NAME_TIMESTAMP + ", " + OUTPUT_FIELD_NAME_VALUE
                        + ", " + OUTPUT_FIELD_NAME_HEADERS);

        configDef.define(OUTPUT_COMPRESSION, ConfigDef.Type.STRING, null, new FileCompressionTypeValidator() {
            @Override
            public void ensureValid(final String name, final Object value) {
                LOGGER.info(OUTPUT_COMPRESSION + " property is deprecated please read documentation for the new name");
                super.ensureValid(name, value);
            }
        }, ConfigDef.Importance.MEDIUM, "Output compression. Valid values are: " + OUTPUT_COMPRESSION_TYPE_GZIP
                + " and " + OUTPUT_COMPRESSION_TYPE_NONE);
    }

    @Override
    public void validate() {
        validateCredentials();
        validateBucket();
    }

    public void validateCredentials() {
        final AwsStsRole awsStsRole = getStsRole();

        if (awsStsRole.isValid()) {
            final AwsStsEndpointConfig stsEndpointConfig = getStsEndpointConfig();
            if (!stsEndpointConfig.isValid()
                    && !AwsStsEndpointConfig.AWS_STS_GLOBAL_ENDPOINT.equals(stsEndpointConfig.getServiceEndpoint())) {
                throw new ConfigException(String.format("%s should be specified together with %s", AWS_S3_REGION_CONFIG,
                        AWS_STS_CONFIG_ENDPOINT));
            }
        } else {
            final BasicAWSCredentials awsCredentials = getAwsCredentials();
            final AwsBasicCredentials awsCredentialsV2 = getAwsCredentialsV2();
            if (awsCredentials == null && awsCredentialsV2 == null) {
                LOGGER.info(
                        "Connector use {} as credential Provider, "
                                + "when configuration for {{}, {}} OR {{}, {}} are absent",
                        AWS_CREDENTIALS_PROVIDER_CONFIG, AWS_ACCESS_KEY_ID_CONFIG, AWS_SECRET_ACCESS_KEY_CONFIG,
                        AWS_STS_ROLE_ARN, AWS_STS_ROLE_SESSION_NAME);
            }
        }
    }

    public void validateBucket() {
        if (Objects.isNull(cfg.getString(AWS_S3_BUCKET_NAME_CONFIG)) && Objects.isNull(cfg.getString(AWS_S3_BUCKET))) {
            throw new ConfigException(String.format("Neither %s nor %s properties have been set",
                    AWS_S3_BUCKET_NAME_CONFIG, AWS_S3_BUCKET));
        }
    }

    // Custom Validators
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

    public AwsStsRole getStsRole() {
        return new AwsStsRole(cfg.getString(AWS_STS_ROLE_ARN), cfg.getString(AWS_STS_ROLE_EXTERNAL_ID),
                cfg.getString(AWS_STS_ROLE_SESSION_NAME), cfg.getInt(AWS_STS_ROLE_SESSION_DURATION));
    }

    public boolean hasAwsStsRole() {
        return getStsRole().isValid();
    }

    public boolean hasStsEndpointConfig() {
        return getStsEndpointConfig().isValid();
    }

    public AwsStsEndpointConfig getStsEndpointConfig() {
        return new AwsStsEndpointConfig(cfg.getString(AWS_STS_CONFIG_ENDPOINT), cfg.getString(AWS_S3_REGION_CONFIG));
    }

    @Deprecated
    public AwsClientBuilder.EndpointConfiguration getAwsEndpointConfiguration() {
        final AwsStsEndpointConfig config = getStsEndpointConfig();
        return new AwsClientBuilder.EndpointConfiguration(config.getServiceEndpoint(), config.getSigningRegion());
    }

    @Deprecated
    public BasicAWSCredentials getAwsCredentials() {
        if (Objects.nonNull(cfg.getPassword(AWS_ACCESS_KEY_ID_CONFIG))
                && Objects.nonNull(cfg.getPassword(AWS_SECRET_ACCESS_KEY_CONFIG))) {

            return new BasicAWSCredentials(cfg.getPassword(AWS_ACCESS_KEY_ID_CONFIG).value(),
                    cfg.getPassword(AWS_SECRET_ACCESS_KEY_CONFIG).value());
        } else if (Objects.nonNull(cfg.getPassword(AWS_ACCESS_KEY_ID))
                && Objects.nonNull(cfg.getPassword(AWS_SECRET_ACCESS_KEY))) {
            LOGGER.warn("Config options {} and {} are deprecated", AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY);
            return new BasicAWSCredentials(cfg.getPassword(AWS_ACCESS_KEY_ID).value(),
                    cfg.getPassword(AWS_SECRET_ACCESS_KEY).value());
        }
        return null;
    }

    public AwsBasicCredentials getAwsCredentialsV2() {
        if (Objects.nonNull(cfg.getPassword(AWS_ACCESS_KEY_ID_CONFIG))
                && Objects.nonNull(cfg.getPassword(AWS_SECRET_ACCESS_KEY_CONFIG))) {

            return AwsBasicCredentials.create(cfg.getPassword(AWS_ACCESS_KEY_ID_CONFIG).value(),
                    cfg.getPassword(AWS_SECRET_ACCESS_KEY_CONFIG).value());
        } else if (Objects.nonNull(cfg.getPassword(AWS_ACCESS_KEY_ID))
                && Objects.nonNull(cfg.getPassword(AWS_SECRET_ACCESS_KEY))) {
            LOGGER.warn("Config options {} and {} are not supported for this Connector", AWS_ACCESS_KEY_ID,
                    AWS_SECRET_ACCESS_KEY);
        }
        return null;
    }

    public String getAwsS3EndPoint() {
        return Objects.nonNull(cfg.getString(AWS_S3_ENDPOINT_CONFIG))
                ? cfg.getString(AWS_S3_ENDPOINT_CONFIG)
                : cfg.getString(AWS_S3_ENDPOINT);
    }
    @Deprecated
    public Region getAwsS3Region() {
        // we have priority of properties if old one not set or both old and new one set
        // the new property value will be selected
        if (Objects.nonNull(cfg.getString(AWS_S3_REGION_CONFIG))) {
            return RegionUtils.getRegion(cfg.getString(AWS_S3_REGION_CONFIG));
        } else if (Objects.nonNull(cfg.getString(AWS_S3_REGION))) {
            return RegionUtils.getRegion(cfg.getString(AWS_S3_REGION));
        } else {
            return RegionUtils.getRegion(Regions.US_EAST_1.getName());
        }
    }

    public software.amazon.awssdk.regions.Region getAwsS3RegionV2() {
        // we have priority of properties if old one not set or both old and new one set
        // the new property value will be selected
        if (Objects.nonNull(cfg.getString(AWS_S3_REGION_CONFIG))) {
            return software.amazon.awssdk.regions.Region.of(cfg.getString(AWS_S3_REGION_CONFIG));
        } else if (Objects.nonNull(cfg.getString(AWS_S3_REGION))) {
            return software.amazon.awssdk.regions.Region.of(cfg.getString(AWS_S3_REGION));
        } else {
            return software.amazon.awssdk.regions.Region.of(Regions.US_EAST_1.getName());
        }
    }

    public String getAwsS3BucketName() {
        return Objects.nonNull(cfg.getString(AWS_S3_BUCKET_NAME_CONFIG))
                ? cfg.getString(AWS_S3_BUCKET_NAME_CONFIG)
                : cfg.getString(AWS_S3_BUCKET);
    }

    public String getServerSideEncryptionAlgorithmName() {
        return cfg.getString(AWS_S3_SSE_ALGORITHM_CONFIG);
    }

    public String getAwsS3Prefix() {
        return Objects.nonNull(cfg.getString(AWS_S3_PREFIX_CONFIG))
                ? cfg.getString(AWS_S3_PREFIX_CONFIG)
                : cfg.getString(AWS_S3_PREFIX);
    }

    public int getAwsS3PartSize() {
        return cfg.getInt(AWS_S3_PART_SIZE);
    }

    public long getS3RetryBackoffDelayMs() {
        return cfg.getLong(AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG);
    }

    public long getS3RetryBackoffMaxDelayMs() {
        return cfg.getLong(AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG);
    }

    public int getS3RetryBackoffMaxRetries() {
        return cfg.getInt(AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG);
    }

    public AWSCredentialsProvider getCustomCredentialsProvider() {
        return cfg.getConfiguredInstance(AWS_CREDENTIALS_PROVIDER_CONFIG, AWSCredentialsProvider.class);
    }

    public AwsCredentialsProvider getCustomCredentialsProviderV2() {
        return cfg.getConfiguredInstance(AWS_CREDENTIALS_PROVIDER_CONFIG, AwsCredentialsProvider.class);
    }

    public int getFetchPageSize() {
        return cfg.getInt(FETCH_PAGE_SIZE);
    }

}
