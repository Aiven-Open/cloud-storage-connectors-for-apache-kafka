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

package io.aiven.kafka.connect.iam;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.common.Configurable;

import io.aiven.kafka.connect.config.s3.S3ConfigDefDefaults;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.regions.Regions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests the Credential provider factory generation of V1 credentials
 */
final class AwsCredentialProviderFactoryTest {
    private AwsCredentialProviderFactory factory;
    private Map<String, String> props;

    @BeforeEach
    public void setUp() {
        factory = new AwsCredentialProviderFactory();
        props = S3ConfigDefDefaults.defaultProperties();
    }

    @Test
    void createsStsCredentialProviderIfSpecified() {
        S3ConfigFragment.setter(props)
                .accessKeyId("blah-blah-blah")
                .accessKeySecret("blah-blah-blah")
                .stsRoleArn("arn:aws:iam::12345678910:role/S3SinkTask")
                .stsRoleSessionName("SESSION_NAME")
                .region(Regions.US_EAST_1.getName())
                .stsEndpoint("https://sts.us-east-1.amazonaws.com");

        final AwsCredentialTestingConfig config = new AwsCredentialTestingConfig(props);

        final var credentialProvider = factory.getProvider(config.getS3ConfigFragment());
        assertThat(credentialProvider).isInstanceOf(STSAssumeRoleSessionCredentialsProvider.class);
    }

    @Test
    void createStaticCredentialProviderByDefault() {
        S3ConfigFragment.setter(props).accessKeyId("blah-blah-blah").accessKeySecret("blah-blah-blah");

        final AwsCredentialTestingConfig config = new AwsCredentialTestingConfig(props);

        final var credentialProvider = factory.getProvider(config.getS3ConfigFragment());
        assertThat(credentialProvider).isInstanceOf(AWSStaticCredentialsProvider.class);
    }

    @Test
    void createDefaultCredentialsWhenNoCredentialsSpecified() {
        final AwsCredentialTestingConfig config = new AwsCredentialTestingConfig(props);

        final var credentialProvider = factory.getProvider(config.getS3ConfigFragment());
        assertThat(credentialProvider).isInstanceOf(DefaultAWSCredentialsProviderChain.class);
    }

    @Test
    void customCredentialProviderTest() {
        S3ConfigFragment.setter(props).credentialsProvider(DummyCredentialsProvider.class.getName());
        final AwsCredentialTestingConfig config = new AwsCredentialTestingConfig(props);

        final var credentialProvider = factory.getProvider(config.getS3ConfigFragment());
        assertThat(credentialProvider).isInstanceOf(DummyCredentialsProvider.class);
        assertThat(((DummyCredentialsProvider) credentialProvider).configured).isTrue();
    }

    /**
     * A custom V1 credential provider for testing.
     */
    public static class DummyCredentialsProvider implements AWSCredentialsProvider, Configurable {
        boolean configured;

        @Override
        public AWSCredentials getCredentials() {
            return null;
        }

        @Override
        public void refresh() {

        }

        @Override
        public void configure(final Map<String, ?> map) {
            configured = true;
        }
    }

}
