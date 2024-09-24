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

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.FETCH_PAGE_SIZE;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.LOGGER;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;

public final class S3SourceRecordIterator implements Iterator<S3SourceRecord> {

    public static final Pattern DEFAULT_PATTERN = Pattern
            .compile("(?<topic>[^/]+?)-" + "(?<partition>\\d{5})-" + "(?<offset>\\d{12})" + "\\.(?<extension>[^.]+)$");
    private String currentKey;
    private Iterator<S3ObjectSummary> nextFileIterator;
    private Iterator<Optional<ConsumerRecord<byte[], byte[]>>> recordIterator = Collections.emptyIterator();

    private final Map<S3Partition, S3Offset> offsets;

    private final S3SourceConfig s3SourceConfig;
    private final String bucketName;
    private final String s3Prefix;
    private final AmazonS3 s3Client;

    public S3SourceRecordIterator(final S3SourceConfig s3SourceConfig, final AmazonS3 s3Client, final String bucketName,
            final String s3Prefix, final Map<S3Partition, S3Offset> offsets) {
        this.s3SourceConfig = s3SourceConfig;
        this.offsets = Optional.ofNullable(offsets).orElseGet(HashMap::new);
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.s3Prefix = s3Prefix;
        try {
            final List<S3ObjectSummary> chunks = fetchObjectSummaries(s3Client);
            nextFileIterator = chunks.iterator();
        } catch (IOException e) {
            throw new AmazonClientException("Failed to initialize S3 file reader", e);
        }
    }

    private List<S3ObjectSummary> fetchObjectSummaries(final AmazonS3 s3Client) throws IOException {
        final ObjectListing objectListing = s3Client.listObjects(new ListObjectsRequest().withBucketName(bucketName)
                // .withPrefix(s3Prefix)
                // .withMarker(s3SourceConfig.getString(START_MARKER_KEY))
                .withMaxKeys(s3SourceConfig.getInt(FETCH_PAGE_SIZE) * 2));

        return new ArrayList<>(objectListing.getObjectSummaries());
    }

    private void nextObject() {
        if (!nextFileIterator.hasNext()) {
            recordIterator = Collections.emptyIterator();
            return;
        }

        try {
            final S3ObjectSummary file = nextFileIterator.next();
            currentKey = file.getKey();
            recordIterator = createIteratorForCurrentFile();
        } catch (IOException e) {
            throw new AmazonClientException(e);
        }
    }

    private Iterator<Optional<ConsumerRecord<byte[], byte[]>>> createIteratorForCurrentFile() throws IOException {
        final S3Object s3Object = s3Client.getObject(bucketName, currentKey);
        try (InputStream content = getContent(s3Object)) {
            final Matcher matcher = DEFAULT_PATTERN.matcher(currentKey);
            String topic = null;
            int partition = 0;
            long startOffset = 0l;
            if (matcher.find()) {
                topic = matcher.group("topic");
                partition = Integer.parseInt(matcher.group("partition"));
                startOffset = Long.parseLong(matcher.group("offset"));
            }

            final String finalTopic = topic;
            final int finalPartition = partition;
            final long finalStartOffset = startOffset;
            return getIterator(content, finalTopic, finalPartition, finalStartOffset);
        }
    }

    private Iterator<Optional<ConsumerRecord<byte[], byte[]>>> getIterator(final InputStream content,
            final String finalTopic, final int finalPartition, final long finalStartOffset) {
        return new Iterator<>() {
            private Map<S3Partition, Long> currentOffsets = new HashMap<>();
            private Optional<ConsumerRecord<byte[], byte[]>> nextRecord = readNext();

            private Optional<ConsumerRecord<byte[], byte[]>> readNext() {
                try {
                    Optional<byte[]> key = Optional.empty();
                    if (currentKey != null) {
                        key = Optional.of(currentKey.getBytes(StandardCharsets.UTF_8));
                    }
                    final byte[] value = IOUtils.toByteArray(content);

                    if (value == null) {
                        if (key.isPresent()) {
                            throw new IllegalStateException("missing value for key!" + key);
                        }
                        return Optional.empty();
                    }

                    return getConsumerRecord(key, value);
                } catch (IOException e) {
                    throw new org.apache.kafka.connect.errors.ConnectException(
                            "Connect converters could not be instantiated.", e);
                }
            }

            private Optional<ConsumerRecord<byte[], byte[]>> getConsumerRecord(final Optional<byte[]> key,
                    final byte[] value) {
                final S3Partition s3Partition = S3Partition.from(bucketName, s3Prefix, finalTopic, finalPartition);

                long currentOffset;
                if (offsets.containsKey(s3Partition)) {
                    LOGGER.info("getConsumerRecord containsKey: " + offsets);
                    final S3Offset s3Offset = offsets.get(s3Partition);
                    currentOffset = s3Offset.getOffset() + 1;
                } else {
                    currentOffset = currentOffsets.getOrDefault(s3Partition, finalStartOffset);
                }
                LOGGER.info("currentOffset :" + currentOffset);
                final Optional<ConsumerRecord<byte[], byte[]>> record = Optional
                        .of(new ConsumerRecord<>(finalTopic, finalPartition, currentOffset, key.orElse(null), value));
                currentOffsets.put(s3Partition, currentOffset + 1);
                return record;
            }

            @Override
            public boolean hasNext() {
                // Check if there's another record
                return nextRecord.isPresent();
            }

            @Override
            public Optional<ConsumerRecord<byte[], byte[]>> next() {
                if (nextRecord.isEmpty()) {
                    throw new NoSuchElementException();
                }
                final Optional<ConsumerRecord<byte[], byte[]>> currentRecord = nextRecord;
                nextRecord = Optional.empty();
                return currentRecord;
            }
        };
    }

    private InputStream getContent(final S3Object object) throws IOException {
        return object.getObjectContent();
    }

    @Override
    public boolean hasNext() {
        while (!recordIterator.hasNext() && nextFileIterator.hasNext()) {
            nextObject();
        }
        return recordIterator.hasNext();
    }

    @Override
    public S3SourceRecord next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final ConsumerRecord<byte[], byte[]> record = recordIterator.next().get();
        return new S3SourceRecord(S3Partition.from(bucketName, s3Prefix, record.topic(), record.partition()),
                S3Offset.from(currentKey, record.offset()), record.topic(), record.partition(), record.key(),
                record.value());
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
