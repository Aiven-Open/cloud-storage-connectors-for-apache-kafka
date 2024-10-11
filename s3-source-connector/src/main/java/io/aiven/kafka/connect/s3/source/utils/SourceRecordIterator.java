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

import static io.aiven.kafka.connect.s3.source.S3SourceTask.OBJECT_KEY;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.output.OutputWriter;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterator that processes S3 files and creates Kafka source records. Supports different output formats (Avro, JSON,
 * Parquet).
 */
public final class SourceRecordIterator implements Iterator<List<AivenS3SourceRecord>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceRecordIterator.class);
    public static final String PATTERN_TOPIC_KEY = "topicName";
    public static final String PATTERN_PARTITION_KEY = "partitionId";
    public static final String OFFSET_KEY = "offset";

    public static final Pattern FILE_DEFAULT_PATTERN = Pattern
            .compile("(?<topicName>[^/]+?)-" + "(?<partitionId>\\d{5})" + "\\.(?<fileExtension>[^.]+)$"); // ex :
                                                                                                          // topic-00001.txt
    private String currentObjectKey;

    private Iterator<S3ObjectSummary> nextFileIterator;
    private Iterator<List<ConsumerRecord<byte[], byte[]>>> recordIterator = Collections.emptyIterator();

    private final OffsetManager offsetManager;

    private final S3SourceConfig s3SourceConfig;
    private final String bucketName;
    private final AmazonS3 s3Client;

    private final OutputWriter outputWriter;

    public SourceRecordIterator(final S3SourceConfig s3SourceConfig, final AmazonS3 s3Client, final String bucketName,
            final OffsetManager offsetManager, final OutputWriter outputWriter, final Set<String> failedObjectKeys) {
        this.s3SourceConfig = s3SourceConfig;
        this.offsetManager = offsetManager;
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.outputWriter = outputWriter;
        final FileReader fileReader = new FileReader(s3SourceConfig, bucketName, failedObjectKeys, offsetManager);
        try {
            final List<S3ObjectSummary> chunks = fileReader.fetchObjectSummaries(s3Client);
            nextFileIterator = chunks.iterator();
        } catch (IOException e) {
            throw new AmazonClientException("Failed to initialize S3 file reader", e);
        }
    }

    private void nextS3Object() {
        if (!nextFileIterator.hasNext()) {
            recordIterator = Collections.emptyIterator();
            return;
        }

        try {
            final S3ObjectSummary file = nextFileIterator.next();
            currentObjectKey = file.getKey();
            recordIterator = createIteratorForCurrentFile();
        } catch (IOException e) {
            throw new AmazonClientException(e);
        }
    }

    private Iterator<List<ConsumerRecord<byte[], byte[]>>> createIteratorForCurrentFile() throws IOException {
        try (S3Object s3Object = s3Client.getObject(bucketName, currentObjectKey);
                InputStream inputStream = s3Object.getObjectContent()) {
            String topicName;
            int defaultPartitionId = 0;
            final long defaultStartOffsetId = 0L;

            final Matcher fileMatcher = FILE_DEFAULT_PATTERN.matcher(currentObjectKey);
            if (fileMatcher.find()) {
                topicName = fileMatcher.group(PATTERN_TOPIC_KEY);
                defaultPartitionId = Integer.parseInt(fileMatcher.group(PATTERN_PARTITION_KEY));
            } else {
                topicName = offsetManager.getFirstConfiguredTopic(s3SourceConfig);
            }

            final String finalTopic = topicName;
            final Map<String, Object> partitionMap = ConnectUtils.getPartitionMap(topicName, defaultPartitionId,
                    bucketName);

            return getObjectIterator(inputStream, finalTopic, defaultPartitionId, defaultStartOffsetId, outputWriter,
                    partitionMap);
        }
    }

    @SuppressWarnings("PMD.CognitiveComplexity")
    private Iterator<List<ConsumerRecord<byte[], byte[]>>> getObjectIterator(final InputStream valueInputStream,
            final String topic, final int topicPartition, final long startOffset, final OutputWriter outputWriter,
            final Map<String, Object> partitionMap) {
        return new Iterator<>() {
            private Map<Map<String, Object>, Long> currentOffsets = new HashMap<>();
            private List<ConsumerRecord<byte[], byte[]>> nextRecord = readNext();

            private List<ConsumerRecord<byte[], byte[]>> readNext() {

                final Optional<byte[]> optionalKeyBytes = Optional.ofNullable(currentObjectKey)
                        .map(k -> k.getBytes(StandardCharsets.UTF_8));
                final List<ConsumerRecord<byte[], byte[]>> consumerRecordList = new ArrayList<>();

                for (final Object record : outputWriter.getRecords(valueInputStream, topic, topicPartition)) {
                    final byte[] valueBytes = outputWriter.getValueBytes(record, topic, s3SourceConfig);
                    consumerRecordList.add(getConsumerRecord(optionalKeyBytes, valueBytes, topic, topicPartition,
                            offsetManager, currentOffsets, startOffset, partitionMap));
                }
                return consumerRecordList;
            }

            private ConsumerRecord<byte[], byte[]> getConsumerRecord(final Optional<byte[]> key, final byte[] value,
                    final String topic, final int topicPartition, final OffsetManager offsetManager,
                    final Map<Map<String, Object>, Long> currentOffsets, final long startOffset,
                    final Map<String, Object> partitionMap) {

                long currentOffset;

                if (offsetManager.getOffsets().containsKey(partitionMap)) {
                    LOGGER.info("***** offsetManager.getOffsets() ***** " + offsetManager.getOffsets());
                    currentOffset = offsetManager.incrementAndUpdateOffsetMap(partitionMap);
                } else {
                    LOGGER.info("Into else block ...");
                    currentOffset = currentOffsets.getOrDefault(partitionMap, startOffset);
                    currentOffsets.put(partitionMap, currentOffset + 1);
                }

                return new ConsumerRecord<>(topic, topicPartition, currentOffset, key.orElse(null), value);
            }

            @Override
            public boolean hasNext() {
                return !nextRecord.isEmpty();
            }

            @Override
            public List<ConsumerRecord<byte[], byte[]>> next() {
                if (nextRecord.isEmpty()) {
                    LOGGER.error("May be error in reading s3 object " + currentObjectKey);
                    return Collections.emptyList();
                    // throw new NoSuchElementException();
                }
                final List<ConsumerRecord<byte[], byte[]>> currentRecord = nextRecord;
                nextRecord = Collections.emptyList();
                return currentRecord;
            }
        };
    }

    @Override
    public boolean hasNext() {
        return recordIterator.hasNext() || nextFileIterator.hasNext();
    }

    @Override
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    public List<AivenS3SourceRecord> next() {
        if (!recordIterator.hasNext()) {
            nextS3Object();
        }

        final List<ConsumerRecord<byte[], byte[]>> consumerRecordList = recordIterator.next();
        if (consumerRecordList.isEmpty()) {
            LOGGER.error("May be error in reading s3 object " + currentObjectKey);
            return Collections.emptyList();
            // throw new NoSuchElementException();
        }
        final List<AivenS3SourceRecord> aivenS3SourceRecordList = new ArrayList<>();

        AivenS3SourceRecord aivenS3SourceRecord;
        Map<String, Object> offsetMap;
        Map<String, Object> partitionMap;
        for (final ConsumerRecord<byte[], byte[]> currentRecord : consumerRecordList) {

            partitionMap = ConnectUtils.getPartitionMap(currentRecord.topic(), currentRecord.partition(), bucketName);

            // Create the offset map
            offsetMap = new HashMap<>();
            offsetMap.put(OFFSET_KEY, currentRecord.offset());
            offsetMap.put(OBJECT_KEY, currentObjectKey);

            aivenS3SourceRecord = new AivenS3SourceRecord(partitionMap, offsetMap, currentRecord.topic(),
                    currentRecord.partition(), currentRecord.key(), currentRecord.value(), currentObjectKey);

            aivenS3SourceRecordList.add(aivenS3SourceRecord);
        }

        return Collections.unmodifiableList(aivenS3SourceRecordList);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("This iterator is unmodifiable");
    }

}
