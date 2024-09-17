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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({ "PMD.TooManyMethods", "PMD.GodClass", "PMD.ExcessiveImports" })
final public class S3SourceConfig extends AbstractConfig {

    public static final Logger LOGGER = LoggerFactory.getLogger(S3SourceConfig.class);

    @Deprecated
    public static final String AWS_ACCESS_KEY_ID = "aws_access_key_id";

    public static final String AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG = "aws.s3.backoff.delay.ms";

    public static final String AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG = "aws.s3.backoff.max.delay.ms";

    public static final String AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG = "aws.s3.backoff.max.retries";

    public static final String AWS_S3_REGION_CONFIG = "aws.s3.region";

    public static final String AWS_S3_ENDPOINT_CONFIG = "aws.s3.endpoint";

    @Deprecated
    public static final String AWS_S3_ENDPOINT = "aws_s3_endpoint";

    @Deprecated
    public static final String AWS_S3_REGION = "aws_s3_region";

    public static final String AWS_STS_ROLE_ARN = "aws.sts.role.arn";

    public static final String AWS_STS_ROLE_EXTERNAL_ID = "aws.sts.role.external.id";

    public static final String AWS_STS_ROLE_SESSION_NAME = "aws.sts.role.session.name";
    public static final String AWS_STS_ROLE_SESSION_DURATION = "aws.sts.role.session.duration";
    public static final String AWS_STS_CONFIG_ENDPOINT = "aws.sts.config.endpoint";

    @Deprecated
    public static final String AWS_SECRET_ACCESS_KEY = "aws_secret_access_key";

    public static final String AWS_ACCESS_KEY_ID_CONFIG = "aws.access.key.id";
    public static final String AWS_SECRET_ACCESS_KEY_CONFIG = "aws.secret.access.key";

    public static final String AWS_CREDENTIALS_PROVIDER_CONFIG = "aws.credentials.provider";

    public S3SourceConfig(final Map<String, String> properties) {
        super(configDef(), preprocessProperties(properties));
        validate(); // NOPMD ConstructorCallsOverridableMethod getStsRole is called
    }

    static Map<String, String> preprocessProperties(final Map<String, String> properties) {
        LOGGER.info("preprocessProperties " + properties);
        return Collections.emptyMap();
    }

    public static ConfigDef configDef() {
        return new S3SourceConfigDef();
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

    public String getAwsS3EndPoint() {
        return Objects.nonNull(getString(AWS_S3_ENDPOINT_CONFIG))
                ? getString(AWS_S3_ENDPOINT_CONFIG)
                : getString(AWS_S3_ENDPOINT);
    }

    public boolean hasAwsStsRole() {
        return getStsRole().isValid();
    }

    public AwsStsRole getStsRole() {
        return new AwsStsRole(getString(AWS_STS_ROLE_ARN), getString(AWS_STS_ROLE_EXTERNAL_ID),
                getString(AWS_STS_ROLE_SESSION_NAME), getInt(AWS_STS_ROLE_SESSION_DURATION));
    }

    public boolean hasStsEndpointConfig() {
        return getStsEndpointConfig().isValid();
    }

    public AwsStsEndpointConfig getStsEndpointConfig() {
        return new AwsStsEndpointConfig(getString(AWS_STS_CONFIG_ENDPOINT), getString(AWS_S3_REGION_CONFIG));
    }

    public AwsAccessSecret getAwsCredentials() {
        return getNewAwsCredentials().isValid() ? getNewAwsCredentials() : getOldAwsCredentials();
    }

    public AwsAccessSecret getNewAwsCredentials() {
        return new AwsAccessSecret(getPassword(AWS_ACCESS_KEY_ID_CONFIG), getPassword(AWS_SECRET_ACCESS_KEY_CONFIG));
    }

    public AwsAccessSecret getOldAwsCredentials() {
        return new AwsAccessSecret(getPassword(AWS_ACCESS_KEY_ID), getPassword(AWS_SECRET_ACCESS_KEY));
    }

    public AWSCredentialsProvider getCustomCredentialsProvider() {
        return getConfiguredInstance(AWS_CREDENTIALS_PROVIDER_CONFIG, AWSCredentialsProvider.class);
    }
}
