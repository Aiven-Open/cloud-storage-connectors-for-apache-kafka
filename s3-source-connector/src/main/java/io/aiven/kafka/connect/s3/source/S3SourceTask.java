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

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.MAX_POLL_RECORDS;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.TOPICS_KEY;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.TOPIC_PARTITIONS_KEY;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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
import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * S3SourceTask is a Kafka Connect SourceTask implementation that reads from source-s3 buckets and generates Kafka
 * Connect records.
 */
public class S3SourceTask extends SourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(AivenKafkaConnectS3SourceConnector.class);

    private S3SourceConfig s3SourceConfig;

    private Map<S3Partition, S3Offset> offsets;

    private AmazonS3 s3Client;

    Iterator<S3SourceRecord> sourceRecordIterator;

    private final AtomicBoolean stopped = new AtomicBoolean();

    private final static long S_3_POLL_INTERVAL = 10_000L;

    private final static long ERROR_BACKOFF = 1000L;

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
        s3SourceConfig = new S3SourceConfig(props);

        s3Client = createAmazonS3Client(s3SourceConfig);
        LOGGER.info("S3 client initialized " + s3Client.getBucketLocation(""));
        prepareReaderFromOffsetStorageReader();
    }

    private void prepareReaderFromOffsetStorageReader() {
        final String s3Prefix = s3SourceConfig.getString("aws.s3.prefix");
        final String s3Bucket = s3SourceConfig.getString("aws.s3.bucket");

        final Set<Integer> partitionList = getPartitions();
        final Set<String> topics = getTopics();

        // map to s3 partitions
        final List<S3Partition> s3Partitions = partitionList.stream()
                .flatMap(p -> topics.stream().map(t -> S3Partition.from(s3Bucket, s3Prefix, t, p)))
                .collect(toList());

        // get partition offsets
        final Map<Map<String, Object>, Map<String, Object>> offsetMap = context.offsetStorageReader()
                .offsets(s3Partitions.stream().map(S3Partition::asMap).collect(toList()));

        if (offsets == null) {
            offsets = offsetMap.entrySet()
                    .stream()
                    .filter(e -> e.getValue() != null)
                    .collect(
                            toMap(entry -> S3Partition.from(entry.getKey()), entry -> S3Offset.from(entry.getValue())));
        }

        LOGGER.info("{} reading from S3 with offsets {}", s3SourceConfig.getString("name"), offsets);

        final byte[] valueDelimiter = Optional.ofNullable(s3SourceConfig.getString("value.delimiter"))
                .map(Object::toString)
                .orElse("\n")
                .getBytes(parseEncoding(s3SourceConfig, "value.encoding"));

        final Optional<byte[]> keyDelimiter = Optional.ofNullable(s3SourceConfig.getString("key.delimiter"))
                .map(Object::toString)
                .map(s -> s.getBytes(parseEncoding(s3SourceConfig, "key.encoding")));

        sourceRecordIterator = new S3FilesReader(s3SourceConfig, s3Client, s3Bucket, s3Prefix, offsets,
                new DelimitedRecordReader(valueDelimiter, keyDelimiter)).readAll();
    }

    private Set<Integer> getPartitions() {
        final String partitionString = s3SourceConfig.getString(TOPIC_PARTITIONS_KEY);
        if (Objects.nonNull(partitionString)) {
            return Arrays.stream(partitionString.split(",")).map(Integer::parseInt).collect(Collectors.toSet());
        } else {
            throw new IllegalStateException("Partition list is not configured.");
        }
    }

    private Set<String> getTopics() {
        final String topicString = s3SourceConfig.getString(TOPICS_KEY);
        if (Objects.nonNull(topicString)) {
            return Arrays.stream(topicString.split(",")).collect(Collectors.toSet());
        } else {
            throw new IllegalStateException("Topics list is not configured.");
        }
    }

    private Charset parseEncoding(final S3SourceConfig s3SourceConfig, final String key) {
        return Optional.ofNullable(s3SourceConfig.getString(key))
                .map(Object::toString)
                .map(Charset::forName)
                .orElse(StandardCharsets.UTF_8);
    }

    private AmazonS3 createAmazonS3Client(final S3SourceConfig config) {
        final var awsEndpointConfig = newEndpointConfiguration(this.s3SourceConfig);
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
    public List<SourceRecord> poll() throws InterruptedException {
        // read up to the configured poll size
        final List<SourceRecord> results = new ArrayList<>(s3SourceConfig.getInt(MAX_POLL_RECORDS));

        if (stopped.get()) {
            return results;
        }

        // AWS errors will happen. Nothing to do about it but sleep and try again.
        while (!stopped.get()) {
            try {
                return getSourceRecords(results);
            } catch (AmazonS3Exception e) {
                if (e.isRetryable()) {
                    LOGGER.warn("Retryable error while polling. Will sleep and try again.", e);
                    Thread.sleep(ERROR_BACKOFF);
                    prepareReaderFromOffsetStorageReader();
                } else {
                    // die
                    throw e;
                }
            }
        }
        return results;
    }

    private List<SourceRecord> getSourceRecords(final List<SourceRecord> results) throws InterruptedException {
        while (!sourceRecordIterator.hasNext() && !stopped.get()) {
            LOGGER.debug("Blocking until new S3 files are available.");
            // sleep and block here until new files are available
            Thread.sleep(S_3_POLL_INTERVAL);
            prepareReaderFromOffsetStorageReader();
        }

        if (stopped.get()) {
            return results;
        }

        for (int i = 0; sourceRecordIterator.hasNext() && i < s3SourceConfig.getInt(MAX_POLL_RECORDS)
                && !stopped.get(); i++) {
            final S3SourceRecord record = sourceRecordIterator.next();
            LOGGER.info(record.offset() + record.getToTopic() + record.partition());
        }

        LOGGER.debug("{} returning {} records.", s3SourceConfig.getString("name"), results.size());
        return results;
    }

    @Override
    public void stop() {
    }
}
