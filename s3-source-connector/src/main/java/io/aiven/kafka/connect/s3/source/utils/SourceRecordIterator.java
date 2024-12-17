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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.kafka.connect.common.source.input.ByteArrayTransformer;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.apache.commons.io.function.IOSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;

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
    public static final long BYTES_TRANSFORMATION_NUM_OF_RECS = 1L;
    private String currentObjectKey;

    private Iterator<String> objectListIterator;
    private Iterator<S3SourceRecord> recordIterator = Collections.emptyIterator();

    private final OffsetManager offsetManager;

    private final S3SourceConfig s3SourceConfig;
    private final String bucketName;

    private final Transformer transformer;
    // Once we decouple the S3Object from the Source Iterator we can change this to be the SourceApiClient
    // At which point it will work for al our integrations.
    private final AWSV2SourceClient sourceClient; // NOPMD

    public SourceRecordIterator(final S3SourceConfig s3SourceConfig, final OffsetManager offsetManager,
            final Transformer transformer, final AWSV2SourceClient sourceClient) {
        this.s3SourceConfig = s3SourceConfig;
        this.offsetManager = offsetManager;

        this.bucketName = s3SourceConfig.getAwsS3BucketName();
        this.transformer = transformer;
        this.sourceClient = sourceClient;
        objectListIterator = sourceClient.getListOfObjectKeys(null);
    }

    private void nextS3Object() {
        if (!objectListIterator.hasNext()) {
            // Start after the object Key we have just finished with.
            objectListIterator = sourceClient.getListOfObjectKeys(currentObjectKey);
            if (!objectListIterator.hasNext()) {
                recordIterator = Collections.emptyIterator();
                return;
            }
        }

        try {
            currentObjectKey = objectListIterator.next();
            if (currentObjectKey != null) {
                recordIterator = createIteratorForCurrentFile();
            }
        } catch (IOException e) {
            throw SdkException.create(e.getMessage(), e.getCause());
        }
    }

    private Iterator<S3SourceRecord> createIteratorForCurrentFile() throws IOException {

        final Matcher fileMatcher = FILE_DEFAULT_PATTERN.matcher(currentObjectKey);
        String topicName;
        int defaultPartitionId;

        if (fileMatcher.find()) {
            // TODO move this from the SourceRecordIterator so that we can decouple it from S3 and make it API agnostic

            final IOSupplier<InputStream> s3Object = sourceClient.getObject(currentObjectKey);
            topicName = fileMatcher.group(PATTERN_TOPIC_KEY);
            defaultPartitionId = Integer.parseInt(fileMatcher.group(PATTERN_PARTITION_KEY));

            final long defaultStartOffsetId = 1L;

            final String finalTopic = topicName;
            final Map<String, Object> partitionMap = ConnectUtils.getPartitionMap(topicName, defaultPartitionId,
                    bucketName);

            return getObjectIterator(s3Object, finalTopic, defaultPartitionId, defaultStartOffsetId, transformer,
                    partitionMap);

        } else {
            LOGGER.error("File naming doesn't match to any topic. {}", currentObjectKey);
            return Collections.emptyIterator();
        }
    }

    @SuppressWarnings("PMD.CognitiveComplexity")
    private Iterator<S3SourceRecord> getObjectIterator(final IOSupplier<InputStream> s3Object, final String topic,
            final int topicPartition, final long startOffset, final Transformer transformer,
            final Map<String, Object> partitionMap) {
        return new Iterator<>() {
            private final Iterator<S3SourceRecord> internalIterator = readNext().iterator();

            private List<S3SourceRecord> readNext() {

                final List<S3SourceRecord> sourceRecords = new ArrayList<>();

                final long numberOfRecsAlreadyProcessed = offsetManager.recordsProcessedForObjectKey(partitionMap,
                        currentObjectKey);

                // Optimizing without reading stream again.
                if (checkBytesTransformation(transformer, numberOfRecsAlreadyProcessed)) {
                    return sourceRecords;
                }

                try (Stream<Object> recordStream = transformer.getRecords(s3Object, topic, topicPartition,
                        s3SourceConfig, numberOfRecsAlreadyProcessed)) {

                    final Iterator<Object> recordIterator = recordStream.iterator();
                    while (recordIterator.hasNext()) {
                        final Object record = recordIterator.next();

                        sourceRecords.add(getSourceRecord(topic, topicPartition, offsetManager, startOffset,
                                partitionMap, transformer.getValueData(record, topic, s3SourceConfig),
                                transformer.getKeyData(currentObjectKey, topic, s3SourceConfig)));

                        // Break if we have reached the max records per poll
                        if (sourceRecords.size() >= s3SourceConfig.getMaxPollRecords()) {
                            break;
                        }
                    }
                }

                return sourceRecords;
            }

            // For bytes transformation, read whole file as 1 record
            private boolean checkBytesTransformation(final Transformer transformer,
                    final long numberOfRecsAlreadyProcessed) {
                return transformer instanceof ByteArrayTransformer
                        && numberOfRecsAlreadyProcessed == BYTES_TRANSFORMATION_NUM_OF_RECS;
            }

            private S3SourceRecord getSourceRecord(final String topic, final int topicPartition,
                    final OffsetManager offsetManager, final long startOffset, final Map<String, Object> partitionMap,
                    final SchemaAndValue valueData, final SchemaAndValue keyData) {

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

                return new S3SourceRecord(partitionMap, offsetMap, topic, topicPartition, currentObjectKey, keyData,
                        valueData);
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
        return recordIterator.hasNext() || objectListIterator.hasNext();
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
