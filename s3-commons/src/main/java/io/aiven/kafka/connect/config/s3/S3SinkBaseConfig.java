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

import static io.aiven.kafka.connect.config.s3.S3CommonConfig.handleDeprecatedYyyyUppercase;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.connect.common.config.SinkCommonConfig;
import io.aiven.kafka.connect.iam.AwsStsEndpointConfig;
import io.aiven.kafka.connect.iam.AwsStsRole;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;
@SuppressWarnings({ "PMD.ExcessiveImports", "PMD.TooManyStaticImports" })
public class S3SinkBaseConfig extends SinkCommonConfig {
    private final S3ConfigFragment s3ConfigFragment;

    protected S3SinkBaseConfig(ConfigDef definition, Map<String, String> originals) { // NOPMD UnusedAssignment
        super(definition, handleDeprecatedYyyyUppercase(originals));
        s3ConfigFragment = new S3ConfigFragment(this);
        validate();
    }

    private void validate() {
        s3ConfigFragment.validate();
    }

    /**
     *
     */
    @Deprecated
    protected static void addDeprecatedConfiguration(final ConfigDef configDef) {

    }

    /**
     *
     */
    @Deprecated
    protected static void addAwsStsConfigGroup(final ConfigDef configDef) {

    }

    /**
     *
     */
    @Deprecated
    protected static void addAwsConfigGroup(final ConfigDef configDef) {

    }

    /**
     *
     */
    @Deprecated
    protected static void addS3RetryPolicies(final ConfigDef configDef) {

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

    public AwsClientBuilder.EndpointConfiguration getAwsEndpointConfiguration() {
        return s3ConfigFragment.getAwsEndpointConfiguration();
    }

    public BasicAWSCredentials getAwsCredentials() {
        return s3ConfigFragment.getAwsCredentials();
    }

    public String getAwsS3EndPoint() {
        return s3ConfigFragment.getAwsS3EndPoint();
    }

    public Region getAwsS3Region() {
        return s3ConfigFragment.getAwsS3RegionV2();
    }

    public String getAwsS3BucketName() {
        return s3ConfigFragment.getAwsS3BucketName();
    }

    public String getServerSideEncryptionAlgorithmName() {
        return s3ConfigFragment.getServerSideEncryptionAlgorithmName();
    }

    public String getAwsS3Prefix() {
        return s3ConfigFragment.getAwsS3Prefix();
    }

    public int getAwsS3PartSize() {
        return s3ConfigFragment.getAwsS3PartSize();
    }

    public long getS3RetryBackoffDelayMs() {
        return s3ConfigFragment.getS3RetryBackoffDelayMs();
    }

    public long getS3RetryBackoffMaxDelayMs() {
        return s3ConfigFragment.getS3RetryBackoffMaxDelayMs();
    }

    public int getS3RetryBackoffMaxRetries() {
        return s3ConfigFragment.getS3RetryBackoffMaxRetries();
    }

    public AWSCredentialsProvider getCustomCredentialsProvider() {
        return s3ConfigFragment.getCustomCredentialsProvider();
    }
}
