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

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

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

    private final AtomicBoolean stopped = new AtomicBoolean();

    private final static long S_3_POLL_INTERVAL_MS = 10_000L;

    private final static long ERROR_BACKOFF = 1000L;

    @SuppressWarnings("PMD.UnnecessaryConstructor") // required by Connect
    public S3SourceTask() {
        super();
    }

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void start(final Map<String, String> props) {
        LOGGER.info("S3 Source task started.");
        Objects.requireNonNull(props, "props hasn't been set");
        s3SourceConfig = new S3SourceConfig(props);

        LOGGER.info("S3 client initialized ");
        prepareReaderFromOffsetStorageReader();
    }

    private void prepareReaderFromOffsetStorageReader() {
        final String s3Prefix = s3SourceConfig.getString("aws.s3.prefix");
        final String s3Bucket = s3SourceConfig.getString("aws.s3.bucket.name");

        final Set<Integer> partitionList = getPartitions();
        final Set<String> topics = getTopics();

        // map to s3 partitions
        final List<S3Partition> s3Partitions = partitionList.stream()
                .flatMap(p -> topics.stream().map(t -> S3Partition.from(s3Bucket, s3Prefix, t, p)))
                .collect(toList());

        // List<String> topicPartitions = Arrays.asList("1","2");
        // List<Map<String, String>> offsetPartitions = topicPartitions.stream().map(
        // tp -> {
        // HashMap<String, String> offsetInfo = new HashMap<>();
        // offsetInfo.put("source", tp);
        // offsetInfo.put("targetPrefix", "targetTopicPrefix");
        // return offsetInfo;
        // }
        // ).collect(Collectors.toList());
        // final Map<Map<String, String>, Map<String, Object>> offsetMap =
        // context.offsetStorageReader().offsets(offsetPartitions);

        // get partition offsets
        final List<Map<String, Object>> partitions = s3Partitions.stream().map(S3Partition::asMap).collect(toList());
        final Map<Map<String, Object>, Map<String, Object>> offsetMap = context.offsetStorageReader()
                .offsets(partitions);

        if (offsets == null) {
            offsets = offsetMap.entrySet()
                    .stream()
                    .filter(e -> e.getValue() != null)
                    .collect(
                            toMap(entry -> S3Partition.from(entry.getKey()), entry -> S3Offset.from(entry.getValue())));
        }

        final byte[] valueDelimiter = Optional.ofNullable(s3SourceConfig.getString("value.delimiter"))
                .map(Object::toString)
                .orElse("\n")
                .getBytes(parseEncoding(s3SourceConfig, "value.encoding"));

        final Optional<byte[]> keyDelimiter = Optional.ofNullable(s3SourceConfig.getString("key.delimiter"))
                .map(Object::toString)
                .map(s -> s.getBytes(parseEncoding(s3SourceConfig, "key.encoding")));

        sourceRecordIterator = new S3SourceRecordIterator(s3SourceConfig, s3Bucket, s3Prefix, offsets,
                new DelimitedRecordReader(valueDelimiter, keyDelimiter));
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
        }

        LOGGER.debug("{} returning {} records.", s3SourceConfig.getString("name"), results.size());
        return results;
    }

    @Override
    public void stop() {
        this.stopped.set(true);
    }
}
