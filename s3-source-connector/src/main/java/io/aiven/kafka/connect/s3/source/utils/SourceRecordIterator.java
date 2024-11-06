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

package io.aiven.kafka.connect.s3.source.utils;

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.MAX_POLL_RECORDS;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.input.Transformer;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterator that processes S3 files and creates Kafka source records. Supports different output formats (Avro, JSON,
 * Parquet).
 */
public final class SourceRecordIterator implements Iterator<S3SourceRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceRecordIterator.class);
    public static final String PATTERN_TOPIC_KEY = "topicName";
    public static final String PATTERN_PARTITION_KEY = "partitionId";

    public static final Pattern FILE_DEFAULT_PATTERN = Pattern.compile("(?<topicName>[^/]+?)-"
            + "(?<partitionId>\\d{5})-" + "(?<uniqueId>[a-zA-Z0-9]+)" + "\\.(?<fileExtension>[^.]+)$"); // topic-00001.txt
    private String currentObjectKey;

    private final Iterator<S3ObjectSummary> s3ObjectSummaryIterator;
    private Iterator<S3SourceRecord> recordIterator = Collections.emptyIterator();

    private final OffsetManager offsetManager;

    private final S3SourceConfig s3SourceConfig;
    private final String bucketName;
    private final AmazonS3 s3Client;

    private final Transformer transformer;

    private final FileReader fileReader; // NOPMD

    public SourceRecordIterator(final S3SourceConfig s3SourceConfig, final AmazonS3 s3Client, final String bucketName,
            final OffsetManager offsetManager, final Transformer transformer, final FileReader fileReader) {
        this.s3SourceConfig = s3SourceConfig;
        this.offsetManager = offsetManager;
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.transformer = transformer;
        this.fileReader = fileReader;
        s3ObjectSummaryIterator = fileReader.fetchObjectSummaries(s3Client);
    }

    private void nextS3Object() {
        if (!s3ObjectSummaryIterator.hasNext()) {
            recordIterator = Collections.emptyIterator();
            return;
        }

        try {
            final S3ObjectSummary file = s3ObjectSummaryIterator.next();
            if (file != null) {
                currentObjectKey = file.getKey();
                recordIterator = createIteratorForCurrentFile();
            }
        } catch (IOException e) {
            throw new AmazonClientException(e);
        }
    }

    private Iterator<S3SourceRecord> createIteratorForCurrentFile() throws IOException {
        try (S3Object s3Object = s3Client.getObject(bucketName, currentObjectKey);
                S3ObjectInputStream inputStream = s3Object.getObjectContent()) {

            final Matcher fileMatcher = FILE_DEFAULT_PATTERN.matcher(currentObjectKey);
            String topicName;
            int defaultPartitionId;

            if (fileMatcher.find()) {
                topicName = fileMatcher.group(PATTERN_TOPIC_KEY);
                defaultPartitionId = Integer.parseInt(fileMatcher.group(PATTERN_PARTITION_KEY));
            } else {
                LOGGER.error("File naming doesn't match to any topic. {}", currentObjectKey);
                inputStream.abort();
                s3Object.close();
                return Collections.emptyIterator();
            }

            final long defaultStartOffsetId = 1L;

            final String finalTopic = topicName;
            final Map<String, Object> partitionMap = ConnectUtils.getPartitionMap(topicName, defaultPartitionId,
                    bucketName);

            return getObjectIterator(inputStream, finalTopic, defaultPartitionId, defaultStartOffsetId, transformer,
                    partitionMap);
        }
    }

    @SuppressWarnings("PMD.CognitiveComplexity")
    private Iterator<S3SourceRecord> getObjectIterator(final InputStream valueInputStream, final String topic,
            final int topicPartition, final long startOffset, final Transformer transformer,
            final Map<String, Object> partitionMap) {
        return new Iterator<>() {
            private final Iterator<S3SourceRecord> internalIterator = readNext().iterator();

            private List<S3SourceRecord> readNext() {
                final byte[] keyBytes = currentObjectKey.getBytes(StandardCharsets.UTF_8);
                final List<S3SourceRecord> sourceRecords = new ArrayList<>();

                int numOfProcessedRecs = 1;
                boolean checkOffsetMap = true;
                for (final Object record : transformer.getRecords(valueInputStream, topic, topicPartition,
                        s3SourceConfig)) {
                    if (offsetManager.shouldSkipRecord(partitionMap, currentObjectKey, numOfProcessedRecs)
                            && checkOffsetMap) {
                        numOfProcessedRecs++;
                        continue;
                    }

                    final byte[] valueBytes = transformer.getValueBytes(record, topic, s3SourceConfig);
                    checkOffsetMap = false;
                    sourceRecords.add(getSourceRecord(keyBytes, valueBytes, topic, topicPartition, offsetManager,
                            startOffset, partitionMap));
                    if (sourceRecords.size() >= s3SourceConfig.getInt(MAX_POLL_RECORDS)) {
                        break;
                    }

                    numOfProcessedRecs++;
                }
                return sourceRecords;
            }

            private S3SourceRecord getSourceRecord(final byte[] key, final byte[] value, final String topic,
                    final int topicPartition, final OffsetManager offsetManager, final long startOffset,
                    final Map<String, Object> partitionMap) {

                long currentOffset;

                if (offsetManager.getOffsets().containsKey(partitionMap)) {
                    LOGGER.info("***** offsetManager.getOffsets() ***** {}", offsetManager.getOffsets());
                    currentOffset = offsetManager.incrementAndUpdateOffsetMap(partitionMap, currentObjectKey,
                            startOffset);
                } else {
                    LOGGER.info("Into else block ...");
                    currentOffset = startOffset;
                    offsetManager.createNewOffsetMap(partitionMap, currentObjectKey, currentOffset);
                }

                final Map<String, Object> offsetMap = offsetManager.getOffsetValueMap(currentObjectKey, currentOffset);

                return new S3SourceRecord(partitionMap, offsetMap, topic, topicPartition, key, value, currentObjectKey);
            }

            @Override
            public boolean hasNext() {
                return internalIterator.hasNext();
            }

            @Override
            public S3SourceRecord next() {
                return internalIterator.next();
            }
        };
    }

    @Override
    public boolean hasNext() {
        return recordIterator.hasNext() || s3ObjectSummaryIterator.hasNext();
    }

    @Override
    public S3SourceRecord next() {
        if (!recordIterator.hasNext()) {
            nextS3Object();
        }

        if (!recordIterator.hasNext()) {
            // If there are still no records, return null or throw an exception
            return null; // Or throw new NoSuchElementException();
        }

        return recordIterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("This iterator is unmodifiable");
    }

}
