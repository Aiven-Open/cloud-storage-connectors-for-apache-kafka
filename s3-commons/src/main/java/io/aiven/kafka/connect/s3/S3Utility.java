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

import java.util.Objects;

import io.aiven.kafka.connect.iam.AwsCredentialProviderFactory;

import com.amazonaws.PredefinedClientConfigurations;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class S3Utility {

    private final AwsCredentialProviderFactory credentialFactory = new AwsCredentialProviderFactory();

    public AmazonS3 createAmazonS3Client(final S3BaseConfig config) {
        final var awsEndpointConfig = newEndpointConfiguration(config);
        final var clientConfig = PredefinedClientConfigurations.defaultConfig()
                .withRetryPolicy(new RetryPolicy(PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
                        new PredefinedBackoffStrategies.FullJitterBackoffStrategy(
                                Math.toIntExact(config.getS3RetryBackoffDelayMs()),
                                Math.toIntExact(config.getS3RetryBackoffMaxDelayMs())),
                        config.getS3RetryBackoffMaxRetries(), false));
        final var s3ClientBuilder = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialFactory.getProvider(config))
                .withClientConfiguration(clientConfig);
        if (Objects.isNull(awsEndpointConfig)) {
            s3ClientBuilder.withRegion(config.getAwsS3Region().getName());
        } else {
            s3ClientBuilder.withEndpointConfiguration(awsEndpointConfig).withPathStyleAccessEnabled(true);
        }
        return s3ClientBuilder.build();
    }

    private AwsClientBuilder.EndpointConfiguration newEndpointConfiguration(final S3BaseConfig config) {
        if (Objects.isNull(config.getAwsS3EndPoint())) {
            return null;
        }
        return new AwsClientBuilder.EndpointConfiguration(config.getAwsS3EndPoint(), config.getAwsS3Region().getName());
    }
}
