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

import static io.aiven.kafka.connect.config.s3.S3CommonConfig.handleDeprecatedYyyyUppercase;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.common.config.SchemaRegistryFragment;
import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;
import io.aiven.kafka.connect.iam.AwsStsEndpointConfig;
import io.aiven.kafka.connect.iam.AwsStsRole;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;

final public class S3SourceConfig extends SourceCommonConfig {

    public static final Logger LOGGER = LoggerFactory.getLogger(S3SourceConfig.class);

    private final S3ConfigFragment s3ConfigFragment;
    private final FileNameFragment s3FileNameFragment;
    public S3SourceConfig(final Map<String, String> properties) {
        super(configDef(), handleDeprecatedYyyyUppercase(properties));
        s3ConfigFragment = new S3ConfigFragment(this);
        s3FileNameFragment = new FileNameFragment(this);
        validate(); // NOPMD ConstructorCallsOverridableMethod getStsRole is called
    }

    public static ConfigDef configDef() {

        final var configDef = new S3SourceConfigDef();
        S3ConfigFragment.update(configDef);
        SourceConfigFragment.update(configDef);
        FileNameFragment.update(configDef);
        SchemaRegistryFragment.update(configDef);
        OutputFormatFragment.update(configDef, OutputFieldType.VALUE);

        return configDef;
    }

    private void validate() {

        // s3ConfigFragment is validated in this method as it is created here.
        // Other Fragments created in the ConfigDef are validated in the parent classes their instances are created in.
        // e.g. SourceConfigFragment, FileNameFragment, SchemaRegistryFragment and OutputFormatFragment are all
        // validated in SourceCommonConfig.
        s3ConfigFragment.validate();
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

    public S3ConfigFragment getS3ConfigFragment() {
        return s3ConfigFragment;
    }

    public FileNameFragment getS3FileNameFragment() {
        return s3FileNameFragment;
    }

}
