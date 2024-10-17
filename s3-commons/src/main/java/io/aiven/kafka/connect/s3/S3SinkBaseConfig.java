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

package io.aiven.kafka.connect.s3;

import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_ACCESS_KEY_ID;
import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_ACCESS_KEY_ID_CONFIG;
import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_CREDENTIALS_PROVIDER_CONFIG;
import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_S3_BUCKET;
import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_S3_BUCKET_NAME_CONFIG;
import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_S3_ENDPOINT;
import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_S3_ENDPOINT_CONFIG;
import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_S3_PART_SIZE;
import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_S3_PREFIX;
import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_S3_PREFIX_CONFIG;
import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_S3_REGION;
import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_S3_REGION_CONFIG;
import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG;
import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG;
import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG;
import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_S3_SSE_ALGORITHM_CONFIG;
import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_SECRET_ACCESS_KEY;
import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_SECRET_ACCESS_KEY_CONFIG;
import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_STS_CONFIG_ENDPOINT;
import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_STS_ROLE_ARN;
import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_STS_ROLE_EXTERNAL_ID;
import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_STS_ROLE_SESSION_DURATION;
import static io.aiven.kafka.connect.s3.S3CommonConfig.AWS_STS_ROLE_SESSION_NAME;
import static io.aiven.kafka.connect.s3.S3CommonConfig.handleDeprecatedYyyyUppercase;

import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.connect.common.config.SinkCommonConfig;
import io.aiven.kafka.connect.iam.AwsStsEndpointConfig;
import io.aiven.kafka.connect.iam.AwsStsRole;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
@SuppressWarnings({ "PMD.ExcessiveImports", "PMD.TooManyStaticImports" })
public class S3SinkBaseConfig extends SinkCommonConfig {
    public static final Logger LOGGER = LoggerFactory.getLogger(S3SinkBaseConfig.class);

    protected S3SinkBaseConfig(ConfigDef definition, Map<String, String> originals) { // NOPMD UnusedAssignment
        super(definition, handleDeprecatedYyyyUppercase(originals));
    }

    protected static void addDeprecatedConfiguration(final ConfigDef configDef) {
        S3CommonConfig.addDeprecatedConfiguration(configDef);
    }

    protected static void addAwsStsConfigGroup(final ConfigDef configDef) {
        S3CommonConfig.addAwsStsConfigGroup(configDef);
    }

    protected static void addAwsConfigGroup(final ConfigDef configDef) {
        S3CommonConfig.addAwsConfigGroup(configDef);
    }
    protected static void addS3RetryPolicies(final ConfigDef configDef) {
        S3CommonConfig.addS3RetryPolicies(configDef);
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

    public AwsClientBuilder.EndpointConfiguration getAwsEndpointConfiguration() {
        final AwsStsEndpointConfig config = getStsEndpointConfig();
        return new AwsClientBuilder.EndpointConfiguration(config.getServiceEndpoint(), config.getSigningRegion());
    }

    public BasicAWSCredentials getAwsCredentials() {
        if (Objects.nonNull(getPassword(AWS_ACCESS_KEY_ID_CONFIG))
                && Objects.nonNull(getPassword(AWS_SECRET_ACCESS_KEY_CONFIG))) {

            return new BasicAWSCredentials(getPassword(AWS_ACCESS_KEY_ID_CONFIG).value(),
                    getPassword(AWS_SECRET_ACCESS_KEY_CONFIG).value());
        } else if (Objects.nonNull(getPassword(AWS_ACCESS_KEY_ID))
                && Objects.nonNull(getPassword(AWS_SECRET_ACCESS_KEY))) {
            LOGGER.warn("Config options {} and {} are deprecated", AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY);
            return new BasicAWSCredentials(getPassword(AWS_ACCESS_KEY_ID).value(),
                    getPassword(AWS_SECRET_ACCESS_KEY).value());
        }
        return null;
    }

    public String getAwsS3EndPoint() {
        return Objects.nonNull(getString(AWS_S3_ENDPOINT_CONFIG))
                ? getString(AWS_S3_ENDPOINT_CONFIG)
                : getString(AWS_S3_ENDPOINT);
    }

    public Region getAwsS3Region() {
        // we have priority of properties if old one not set or both old and new one set
        // the new property value will be selected
        if (Objects.nonNull(getString(AWS_S3_REGION_CONFIG))) {
            return RegionUtils.getRegion(getString(AWS_S3_REGION_CONFIG));
        } else if (Objects.nonNull(getString(AWS_S3_REGION))) {
            return RegionUtils.getRegion(getString(AWS_S3_REGION));
        } else {
            return RegionUtils.getRegion(Regions.US_EAST_1.getName());
        }
    }

    public String getAwsS3BucketName() {
        return Objects.nonNull(getString(AWS_S3_BUCKET_NAME_CONFIG))
                ? getString(AWS_S3_BUCKET_NAME_CONFIG)
                : getString(AWS_S3_BUCKET);
    }

    public String getServerSideEncryptionAlgorithmName() {
        return getString(AWS_S3_SSE_ALGORITHM_CONFIG);
    }

    public String getAwsS3Prefix() {
        return Objects.nonNull(getString(AWS_S3_PREFIX_CONFIG))
                ? getString(AWS_S3_PREFIX_CONFIG)
                : getString(AWS_S3_PREFIX);
    }

    public int getAwsS3PartSize() {
        return getInt(AWS_S3_PART_SIZE);
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

    public AWSCredentialsProvider getCustomCredentialsProvider() {
        return getConfiguredInstance(AWS_CREDENTIALS_PROVIDER_CONFIG, AWSCredentialsProvider.class);
    }

}
