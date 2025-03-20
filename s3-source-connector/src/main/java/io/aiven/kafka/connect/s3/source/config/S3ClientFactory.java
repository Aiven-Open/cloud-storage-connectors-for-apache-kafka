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
import java.time.Duration;
import java.util.Objects;
import java.util.Random;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.retries.api.internal.backoff.ExponentialDelayWithJitter;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

public class S3ClientFactory {

    public S3Client createAmazonS3Client(final S3SourceConfig config) {

        final ExponentialDelayWithJitter backoffStrategy = new ExponentialDelayWithJitter(Random::new,
                Duration.ofMillis(Math.toIntExact(config.getS3RetryBackoffDelayMs())),
                Duration.ofMillis(Math.toIntExact(config.getS3RetryBackoffMaxDelayMs())));

        final ClientOverrideConfiguration clientOverrideConfiguration = ClientOverrideConfiguration.builder()
                .retryStrategy(RetryMode.STANDARD)
                .build();
        if (Objects.isNull(config.getAwsS3EndPoint())) {
            return S3Client.builder()
                    .overrideConfiguration(clientOverrideConfiguration)
                    .overrideConfiguration(o -> o.retryStrategy(
                            r -> r.backoffStrategy(backoffStrategy).maxAttempts(config.getS3RetryBackoffMaxRetries())))
                    .region(config.getAwsS3Region())
                    .credentialsProvider(config.getAwsV2Provider())
                    .build();
        } else {
            // TODO This is definitely used for testing but not sure if customers use it.
            return S3Client.builder()
                    .overrideConfiguration(clientOverrideConfiguration)
                    .region(config.getAwsS3Region())
                    .credentialsProvider(config.getAwsV2Provider())
                    .endpointOverride(URI.create(config.getAwsS3EndPoint()))
                    .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                    .build();
        }

    }

}
