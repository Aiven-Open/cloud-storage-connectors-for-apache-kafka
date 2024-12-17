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

import java.net.URI;
import java.util.Objects;

import io.aiven.kafka.connect.iam.AwsCredentialProviderFactory;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

public class S3ClientFactory {

    private final AwsCredentialProviderFactory credentialFactory = new AwsCredentialProviderFactory();

    public S3Client createAmazonS3Client(final S3SourceConfig config) {

        // EndpointConfiguration is no longer used in SDK 2.X
        // TODO Review back off strategy
        // final BackoffStrategy backoffStrategy =
        // BackoffStrategy.exponentialDelayWithoutJitter(Duration.ofMillis(Math.toIntExact(config.getS3RetryBackoffDelayMs())),
        // Duration.ofMillis(Math.toIntExact(config.getS3RetryBackoffMaxDelayMs())));

        final ClientOverrideConfiguration clientOverrideConfiguration = ClientOverrideConfiguration.builder()
                .retryStrategy(RetryMode.STANDARD)
                .build();
        if (Objects.isNull(config.getAwsS3EndPoint())) {
            return S3Client.builder()
                    .overrideConfiguration(clientOverrideConfiguration)
                    .region(config.getAwsS3Region())
                    .credentialsProvider(credentialFactory.getAwsV2Provider(config.getS3ConfigFragment()))
                    .build();
        } else {
            // TODO This is definitely used for testing but not sure if customers use it.
            return S3Client.builder()
                    .overrideConfiguration(clientOverrideConfiguration)
                    .region(config.getAwsS3Region())
                    .credentialsProvider(credentialFactory.getAwsV2Provider(config.getS3ConfigFragment()))
                    .endpointOverride(URI.create(config.getAwsS3EndPoint()))
                    .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                    .build();
        }

    }

}
