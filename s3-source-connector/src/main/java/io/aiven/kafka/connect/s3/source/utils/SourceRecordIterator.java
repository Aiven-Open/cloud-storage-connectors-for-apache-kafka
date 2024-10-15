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
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.output.OutputWriter;

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
public final class SourceRecordIterator implements Iterator<AivenS3SourceRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceRecordIterator.class);
    public static final String PATTERN_TOPIC_KEY = "topicName";
    public static final String PATTERN_PARTITION_KEY = "partitionId";
    public static final String OFFSET_KEY = "offset";

    public static final Pattern FILE_DEFAULT_PATTERN = Pattern.compile("(?<topicName>[^/]+?)-"
            + "(?<partitionId>\\d{5})-" + "(?<uniqueId>[a-zA-Z0-9]+)" + "\\.(?<fileExtension>[^.]+)$"); // topic-00001.txt
    private String currentObjectKey;

    private Iterator<S3ObjectSummary> nextFileIterator;
    private Iterator<ConsumerRecord<byte[], byte[]>> recordIterator = Collections.emptyIterator();

    private final OffsetManager offsetManager;

    private final S3SourceConfig s3SourceConfig;
    private final String bucketName;
    private final AmazonS3 s3Client;

    private final OutputWriter outputWriter;

    private final FileReader fileReader; // NOPMD

    public SourceRecordIterator(final S3SourceConfig s3SourceConfig, final AmazonS3 s3Client, final String bucketName,
            final OffsetManager offsetManager, final OutputWriter outputWriter, final FileReader fileReader) {
        this.s3SourceConfig = s3SourceConfig;
        this.offsetManager = offsetManager;
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.outputWriter = outputWriter;
        this.fileReader = fileReader;
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

    private Iterator<ConsumerRecord<byte[], byte[]>> createIteratorForCurrentFile() throws IOException {
        try (S3Object s3Object = s3Client.getObject(bucketName, currentObjectKey);
                S3ObjectInputStream inputStream = s3Object.getObjectContent()) {

            final Matcher fileMatcher = FILE_DEFAULT_PATTERN.matcher(currentObjectKey);
            String topicName;
            int defaultPartitionId;

            if (fileMatcher.find()) {
                topicName = fileMatcher.group(PATTERN_TOPIC_KEY);
                defaultPartitionId = Integer.parseInt(fileMatcher.group(PATTERN_PARTITION_KEY));
            } else {
                LOGGER.error("File naming doesn't match to any topic. " + currentObjectKey);
                inputStream.abort();
                s3Object.close();
                return Collections.emptyIterator();
            }

            final long defaultStartOffsetId = 1L;

            final String finalTopic = topicName;
            final Map<String, Object> partitionMap = ConnectUtils.getPartitionMap(topicName, defaultPartitionId,
                    bucketName);

            return getObjectIterator(inputStream, finalTopic, defaultPartitionId, defaultStartOffsetId, outputWriter,
                    partitionMap);
        }
    }

    @SuppressWarnings("PMD.CognitiveComplexity")
    private Iterator<ConsumerRecord<byte[], byte[]>> getObjectIterator(final InputStream valueInputStream,
            final String topic, final int topicPartition, final long startOffset, final OutputWriter outputWriter,
            final Map<String, Object> partitionMap) {
        return new Iterator<>() {
            private final Iterator<ConsumerRecord<byte[], byte[]>> internalIterator = readNext().iterator();

            private List<ConsumerRecord<byte[], byte[]>> readNext() {

                final Optional<byte[]> optionalKeyBytes = Optional.ofNullable(currentObjectKey)
                        .map(k -> k.getBytes(StandardCharsets.UTF_8));
                final List<ConsumerRecord<byte[], byte[]>> consumerRecordList = new ArrayList<>();

                int numOfProcessedRecs = 1;
                boolean checkOffsetMap = true;
                for (final Object record : outputWriter.getRecords(valueInputStream, topic, topicPartition,
                        s3SourceConfig)) {

                    if (offsetManager.getOffsets().containsKey(partitionMap) && checkOffsetMap) {
                        final Map<String, Object> offsetVal = offsetManager.getOffsets().get(partitionMap);
                        if (offsetVal.containsKey(offsetManager.getObjectMapKey(currentObjectKey))) {
                            final long offsetValue = (long) offsetVal
                                    .get(offsetManager.getObjectMapKey(currentObjectKey));
                            if (numOfProcessedRecs <= offsetValue) {
                                numOfProcessedRecs++;
                                continue;
                            }
                        }
                    }
                    final byte[] valueBytes = outputWriter.getValueBytes(record, topic, s3SourceConfig);
                    checkOffsetMap = false;
                    consumerRecordList.add(getConsumerRecord(optionalKeyBytes, valueBytes, topic, topicPartition,
                            offsetManager, startOffset, partitionMap));
                    if (consumerRecordList.size() >= s3SourceConfig.getInt(MAX_POLL_RECORDS)) {
                        break;
                    }

                    numOfProcessedRecs++;
                }
                return consumerRecordList;
            }

            private ConsumerRecord<byte[], byte[]> getConsumerRecord(final Optional<byte[]> key, final byte[] value,
                    final String topic, final int topicPartition, final OffsetManager offsetManager,
                    final long startOffset, final Map<String, Object> partitionMap) {

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

                return new ConsumerRecord<>(topic, topicPartition, currentOffset, key.orElse(null), value);
            }

            @Override
            public boolean hasNext() {
                return internalIterator.hasNext();
            }

            @Override
            public ConsumerRecord<byte[], byte[]> next() {
                return internalIterator.next();
            }
        };
    }

    @Override
    public boolean hasNext() {
        return recordIterator.hasNext() || nextFileIterator.hasNext();
    }

    @Override
    public AivenS3SourceRecord next() {
        if (!recordIterator.hasNext()) {
            nextS3Object();
        }

        if (!recordIterator.hasNext()) {
            // If there are still no records, return null or throw an exception
            return null; // Or throw new NoSuchElementException();
        }

        final ConsumerRecord<byte[], byte[]> consumerRecord = recordIterator.next();
        final Map<String, Object> partitionMap = ConnectUtils.getPartitionMap(consumerRecord.topic(),
                consumerRecord.partition(), bucketName);
        final Map<String, Object> offsetMap = offsetManager.getOffsetValueMap(currentObjectKey,
                consumerRecord.offset());

        return new AivenS3SourceRecord(partitionMap, offsetMap, consumerRecord.topic(), consumerRecord.partition(),
                consumerRecord.key(), consumerRecord.value(), currentObjectKey);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("This iterator is unmodifiable");
    }

}
