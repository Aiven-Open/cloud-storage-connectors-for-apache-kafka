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

import java.util.Map;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.config.s3.S3CommonConfig;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;
import io.aiven.kafka.connect.iam.AwsCredentialProviderFactory;
import io.aiven.kafka.connect.iam.AwsStsEndpointConfig;
import io.aiven.kafka.connect.iam.AwsStsRole;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

final public class S3SourceConfig extends SourceCommonConfig implements S3CommonConfig {

    private final S3ConfigFragment s3ConfigFragment;
    private final AwsCredentialProviderFactory awsCredentialsProviderFactory;

    public S3SourceConfig(final Map<String, String> properties) {
        super(new S3SourceConfigDef(), properties);
        s3ConfigFragment = new S3ConfigFragment(dataAccess);
        awsCredentialsProviderFactory = new AwsCredentialProviderFactory();
    }

    public AwsStsRole getStsRole() {
        return s3ConfigFragment.getStsRole();
    }

    public boolean hasAwsStsRole() {
        return s3ConfigFragment.hasAwsStsRole();
    }

    public boolean hasStsEndpointConfig() {
        return s3ConfigFragment.hasStsEndpointConfig();
    }

    public AwsStsEndpointConfig getStsEndpointConfig() {
        return s3ConfigFragment.getStsEndpointConfig();
    }

    public AwsBasicCredentials getAwsCredentials() {
        return s3ConfigFragment.getAwsCredentialsV2();
    }

    @Override
    public String getAwsS3EndPoint() {
        return s3ConfigFragment.getAwsS3EndPoint();
    }

    @Override
    public S3ConfigFragment.DelayType getDelayType() {
        return s3ConfigFragment.getDelayType();
    }

    @Override
    public Region getAwsS3Region() {
        return s3ConfigFragment.getAwsS3RegionV2();
    }

    public String getAwsS3BucketName() {
        return s3ConfigFragment.getAwsS3BucketName();
    }

    public String getServerSideEncryptionAlgorithmName() {
        return s3ConfigFragment.getServerSideEncryptionAlgorithmName();
    }

    public int getAwsS3PartSize() {
        return s3ConfigFragment.getAwsS3PartSize();
    }

    @Override
    public long getS3RetryBackoffDelayMs() {
        return s3ConfigFragment.getS3RetryBackoffDelayMs();
    }

    @Override
    public long getS3RetryBackoffMaxDelayMs() {
        return s3ConfigFragment.getS3RetryBackoffMaxDelayMs();
    }

    @Override
    public int getS3RetryBackoffMaxRetries() {
        return s3ConfigFragment.getS3RetryBackoffMaxRetries();
    }

    public int getFetchPageSize() {
        return s3ConfigFragment.getFetchPageSize();
    }

    @Override
    public AwsCredentialsProvider getAwsV2Provider() {
        return awsCredentialsProviderFactory.getAwsV2Provider(s3ConfigFragment);
    }

}
