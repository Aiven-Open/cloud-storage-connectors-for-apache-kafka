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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

/**
 * Tests the Credential provider factory generation of V2 credentials
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
                .region(Region.US_EAST_1)
                .stsEndpoint("https://sts.us-east-1.amazonaws.com");

        final AwsCredentialTestingConfig config = new AwsCredentialTestingConfig(props);

        final var credentialProvider = factory.getAwsV2Provider(config.getS3ConfigFragment());
        assertThat(credentialProvider).isInstanceOf(StsAssumeRoleCredentialsProvider.class);
    }

    @Test
    void createStaticCredentialProviderByDefault() {
        S3ConfigFragment.setter(props).accessKeyId("blah-blah-blah").accessKeySecret("blah-blah-blah");

        final AwsCredentialTestingConfig config = new AwsCredentialTestingConfig(props);

        final var credentialProvider = factory.getAwsV2Provider(config.getS3ConfigFragment());
        assertThat(credentialProvider).isInstanceOf(StaticCredentialsProvider.class);
    }

    @Test
    void createDefaultCredentialsWhenNoCredentialsSpecified() {
        final AwsCredentialTestingConfig config = new AwsCredentialTestingConfig(props);

        final var credentialProvider = factory.getAwsV2Provider(config.getS3ConfigFragment());
        assertThat(credentialProvider).isInstanceOf(AwsCredentialsProvider.class);
    }

    @Test
    void customCredentialProviderTest() {
        S3ConfigFragment.setter(props).credentialsProvider(DummyCredentialsProvider.class.getName());
        final AwsCredentialTestingConfig config = new AwsCredentialTestingConfig(props);

        final var credentialProvider = factory.getAwsV2Provider(config.getS3ConfigFragment());
        assertThat(credentialProvider).isInstanceOf(DummyCredentialsProvider.class);
        assertThat(((DummyCredentialsProvider) credentialProvider).configured).isTrue();
    }

    /**
     * A custom V2 credential provider for testing.
     */
    public static class DummyCredentialsProvider implements AwsCredentialsProvider, Configurable {
        boolean configured;

        @Override
        public void configure(final Map<String, ?> map) {
            configured = true;
        }

        @Override
        public AwsCredentials resolveCredentials() {
            return null;
        }
    }

}
