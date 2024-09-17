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

package io.aiven.kafka.connect.s3.source;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import io.aiven.kafka.connect.s3.source.config.AwsCredentialProviderFactory;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.PredefinedClientConfigurations;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * S3SourceTask is a Kafka Connect SourceTask implementation that reads from source-s3 buckets and generates Kafka
 * Connect records.
 */
public class S3SourceTask extends SourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(AivenKafkaConnectS3SourceConnector.class);

    private S3SourceConfig config;

    private AmazonS3 s3Client;

    AwsCredentialProviderFactory credentialFactory = new AwsCredentialProviderFactory();

    @SuppressWarnings("PMD.UnnecessaryConstructor") // required by Connect
    public S3SourceTask() {
        super();
    }

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(final Map<String, String> props) {
        LOGGER.info("S3 Source task started.");
        Objects.requireNonNull(props, "props hasn't been set");
        config = new S3SourceConfig(props);

        s3Client = createAmazonS3Client(config);
        LOGGER.info("S3 client initialized " + s3Client.getBucketLocation(""));
        // prepareReaderFromOffsetStorageReader();
    }

    private AmazonS3 createAmazonS3Client(final S3SourceConfig config) {
        final var awsEndpointConfig = newEndpointConfiguration(this.config);
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

    private AwsClientBuilder.EndpointConfiguration newEndpointConfiguration(final S3SourceConfig config) {
        if (Objects.isNull(config.getAwsS3EndPoint())) {
            return null;
        }
        return new AwsClientBuilder.EndpointConfiguration(config.getAwsS3EndPoint(), config.getAwsS3Region().getName());
    }

    @Override
    public List<SourceRecord> poll() {
        LOGGER.info("Using S3 client and poll " + s3Client.getBucketLocation(""));
        return Collections.emptyList();
    }

    @Override
    public void stop() {
    }
}
