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

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.AWS_S3_BUCKET_NAME_CONFIG;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.AWS_S3_PREFIX_CONFIG;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.MAX_POLL_RECORDS;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.TARGET_TOPICS;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.TARGET_TOPIC_PARTITIONS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

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

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;

import io.aiven.kafka.connect.s3.source.config.S3ClientFactory;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.services.s3.AmazonS3;
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

    Iterator<S3SourceRecord> sourceRecordIterator;

    private Optional<Converter> keyConverter;
    private Converter valueConverter;

    private final AtomicBoolean stopped = new AtomicBoolean();

    private final static long S_3_POLL_INTERVAL_MS = 10_000L;

    private final static long ERROR_BACKOFF = 1000L;

    final S3ClientFactory s3ClientFactory = new S3ClientFactory();
    private AmazonS3 s3Client;

    @SuppressWarnings("PMD.UnnecessaryConstructor") // required by Connect
    public S3SourceTask() {
        super();
    }

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Deprecated
    @Override
    public void start(final Map<String, String> props) {
        LOGGER.info("S3 Source task started.");
        Objects.requireNonNull(props, "props hasn't been set");
        s3SourceConfig = new S3SourceConfig(props);

        try {
            keyConverter = Optional.of((Converter) s3SourceConfig.getClass("key.converter").newInstance());
            valueConverter = (Converter) s3SourceConfig.getClass("value.converter").newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new ConnectException("Connect converters could not be instantiated.", e);
        }

        this.s3Client = s3ClientFactory.createAmazonS3Client(s3SourceConfig);

        LOGGER.info("S3 client initialized ");
        prepareReaderFromOffsetStorageReader();
    }

    @Deprecated
    private void prepareReaderFromOffsetStorageReader() {
        final String s3Prefix = s3SourceConfig.getString(AWS_S3_PREFIX_CONFIG);
        final String s3Bucket = s3SourceConfig.getString(AWS_S3_BUCKET_NAME_CONFIG);

        final Set<Integer> offsetStorageTopicPartitions = getTargetTopicPartitions();
        final Set<String> targetTopics = getTargetTopics();

        // map to s3 partitions
        final List<S3Partition> s3Partitions = offsetStorageTopicPartitions.stream()
                .flatMap(p -> targetTopics.stream().map(t -> S3Partition.from(s3Bucket, s3Prefix, t, p)))
                .collect(toList());

        // get partition offsets
        final List<Map<String, Object>> partitions = s3Partitions.stream().map(S3Partition::asMap).collect(toList());
        final Map<Map<String, Object>, Map<String, Object>> offsetMap = context.offsetStorageReader()
                .offsets(partitions);

        LOGGER.info("offsetMap : " + offsetMap);
        LOGGER.info("offsetMap entry set : " + offsetMap.entrySet());

        if (offsets == null) {
            offsets = offsetMap.entrySet()
                    .stream()
                    .filter(e -> e.getValue() != null)
                    .collect(
                            toMap(entry -> S3Partition.from(entry.getKey()), entry -> S3Offset.from(entry.getValue())));
        }
        LOGGER.info("Storage offsets : " + offsets);
        sourceRecordIterator = new S3SourceRecordIterator(s3SourceConfig, s3Client, s3Bucket, s3Prefix, offsets);
    }

    private Set<Integer> getTargetTopicPartitions() {
        final String partitionString = s3SourceConfig.getString(TARGET_TOPIC_PARTITIONS);
        if (Objects.nonNull(partitionString)) {
            return Arrays.stream(partitionString.split(",")).map(Integer::parseInt).collect(Collectors.toSet());
        } else {
            throw new IllegalStateException("Offset storage topics partition list is not configured.");
        }
    }

    private Set<String> getTargetTopics() {
        final String topicString = s3SourceConfig.getString(TARGET_TOPICS);
        if (Objects.nonNull(topicString)) {
            return Arrays.stream(topicString.split(",")).collect(Collectors.toSet());
        } else {
            throw new IllegalStateException("Offset storage topics list is not configured.");
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        final List<SourceRecord> results = new ArrayList<>(s3SourceConfig.getInt(MAX_POLL_RECORDS));

        if (stopped.get()) {
            return results;
        }

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
            Thread.sleep(S_3_POLL_INTERVAL_MS);
            prepareReaderFromOffsetStorageReader();
        }

        if (stopped.get()) {
            return results;
        }

        for (int i = 0; sourceRecordIterator.hasNext() && i < s3SourceConfig.getInt(MAX_POLL_RECORDS)
                && !stopped.get(); i++) {
            final S3SourceRecord record = sourceRecordIterator.next();
            LOGGER.info(record.offset() + record.getToTopic() + record.partition());
            final String topic = record.getToTopic();
            final Optional<SchemaAndValue> key = keyConverter.map(c -> c.toConnectData(topic, record.key()));
            final SchemaAndValue value = valueConverter.toConnectData(topic, record.value());
            results.add(new SourceRecord(record.file().asMap(), record.offset().asMap(), topic, record.partition(),
                    key.map(SchemaAndValue::schema).orElse(null), key.map(SchemaAndValue::value).orElse(null),
                    value.schema(), value.value()));
        }

        LOGGER.debug("{} records.", results.size());
        return results;
    }

    @Override
    public void stop() {
        this.stopped.set(true);
    }
}
