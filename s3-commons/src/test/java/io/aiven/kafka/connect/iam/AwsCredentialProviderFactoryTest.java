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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.Configurable;

import io.aiven.kafka.connect.config.s3.S3ConfigFragment;
import io.aiven.kafka.connect.tools.AwsCredentialBaseConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

/**
 * Tests the Credential provider factory generation of V1 credentials
 */
final class AwsCredentialProviderFactoryTest {
    private AwsCredentialProviderFactory factory;
    private Map<String, String> props;

    @BeforeEach
    public void setUp() {
        factory = new AwsCredentialProviderFactory();
        props = new HashMap<>();
        props.put(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG, "any-bucket");
    }

    @Test
    void createsStsCredentialProviderIfSpecified() {

        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_STS_ROLE_ARN, "arn:aws:iam::12345678910:role/S3SinkTask");
        props.put(S3ConfigFragment.AWS_STS_ROLE_SESSION_NAME, "SESSION_NAME");
        props.put(S3ConfigFragment.AWS_S3_REGION_CONFIG, Region.US_EAST_1.id());
        props.put(S3ConfigFragment.AWS_STS_CONFIG_ENDPOINT, "https://sts.us-east-1.amazonaws.com");

        final var config = new AwsCredentialBaseConfig(props);

        final var credentialProvider = factory.getAwsV2Provider(new S3ConfigFragment(config));
        assertThat(credentialProvider).isInstanceOf(StsAssumeRoleCredentialsProvider.class);
    }

    @Test
    void createStaticCredentialProviderByDefault() {
        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");

        final var config = new AwsCredentialBaseConfig(props);

        final var credentialProvider = factory.getAwsV2Provider(new S3ConfigFragment(config));
        assertThat(credentialProvider).isInstanceOf(StaticCredentialsProvider.class);
    }

    @Test
    void createDefaultCredentialsWhenNoCredentialsSpecified() {
        final var config = new AwsCredentialBaseConfig(props);

        final var credentialProvider = factory.getAwsV2Provider(new S3ConfigFragment(config));
        assertThat(credentialProvider).isInstanceOf(DefaultCredentialsProvider.class);
    }

    @Test
    void customCredentialProviderTest() {
        props.put(S3ConfigFragment.AWS_CREDENTIALS_PROVIDER_CONFIG, DummyCredentialsProvider.class.getName());
        final var config = new AwsCredentialBaseConfig(props);

        final var credentialProvider = factory.getAwsV2Provider(new S3ConfigFragment(config));
        assertThat(credentialProvider).isInstanceOf(DummyCredentialsProvider.class);
        assertThat(((DummyCredentialsProvider) credentialProvider).configured).isTrue();
    }

    /**
     * A custom V1 credential provider for testing.
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

        @Override
        public Class<AwsCredentialsIdentity> identityType() {
            return AwsCredentialsProvider.super.identityType();
        }

        @Override
        public CompletableFuture<AwsCredentialsIdentity> resolveIdentity(final ResolveIdentityRequest request) {
            return AwsCredentialsProvider.super.resolveIdentity(request);
        }
    }

}
