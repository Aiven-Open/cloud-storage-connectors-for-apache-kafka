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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.aiven.kafka.connect.common.OffsetManager;
import io.aiven.kafka.connect.common.source.input.ByteArrayTransformer;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.collections.IteratorUtils;
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
    public static final long BYTES_TRANSFORMATION_NUM_OF_RECS = 1L;
    //private String currentObjectKey;

    private Iterator<S3ObjectSummary> s3ObjectSummaryIterator;
    private Iterator<S3SourceRecord> recordIterator = Collections.emptyIterator();

    private final OffsetManager<S3OffsetManagerEntry> offsetManager;

    private final S3SourceConfig s3SourceConfig;
    private final String bucketName;

    private S3OffsetManagerEntry offsetManagerEntry;

    private final Transformer transformer;
    // Once we decouple the S3Object from the Source Iterator we can change this to be the SourceApiClient
    // At which point it will work for al our integrations.
    private final AWSV2SourceClient sourceClient; // NOPMD

    public SourceRecordIterator(final S3SourceConfig s3SourceConfig, final OffsetManager<S3OffsetManagerEntry> offsetManager,
            final Transformer transformer, final AWSV2SourceClient sourceClient) {
        this.s3SourceConfig = s3SourceConfig;
        this.offsetManager = offsetManager;
        this.bucketName = s3SourceConfig.getAwsS3BucketName();
        this.transformer = transformer;
        this.sourceClient = sourceClient;
        s3ObjectSummaryIterator = sourceClient.getListOfObjectKeys(null);
        Iterator<Iterator<S3SourceRecord>> IteratorIterator = IteratorUtils.transformedIterator(s3ObjectSummaryIterator, this::createIteratorForS3Object);
    }

    private Iterator<S3SourceRecord> createIteratorForS3Object(S3ObjectSummary s3ObjectSummary)  {

        final Matcher fileMatcher = FILE_DEFAULT_PATTERN.matcher(s3ObjectSummary.getKey());


        if (fileMatcher.find()) {
            // TODO move this from the SourceRecordIterator so that we can decouple it from S3 and make it API agnostic
            try (S3Object s3Object = sourceClient.getObject(s3ObjectSummary.getKey())) {
                String topicName;
                int defaultPartitionId;
                offsetManagerEntry = new S3OffsetManagerEntry(bucketName, s3ObjectSummary.getKey(), fileMatcher.group(PATTERN_TOPIC_KEY),
                 Integer.parseInt(fileMatcher.group(PATTERN_PARTITION_KEY));

                return getObjectIterator(s3Object, offsetManagerEntry, transformer);
            }
        } else {
            LOGGER.error("File naming doesn't match to any topic. {}", currentObjectKey);
            return Collections.emptyIterator();
        }
    }



    private Iterator<S3SourceRecord> getObjectIterator(final S3Object s3Object, final String topic,
            final int partition, final long startOffset, final Transformer transformer) {
        return new Iterator<>() {
            private final Iterator<S3SourceRecord> internalIterator = readNext().iterator();

            // this entry will keep track of the current record.
            private S3OffsetManagerEntry s3OffsetManagerEntry;

            private List<S3SourceRecord> readNext() {

                final long numberOfRecsAlreadyProcessed = s3OffsetManagerEntry.getRecordCount();
                final List<S3SourceRecord> sourceRecords = new ArrayList<>();

                // Optimizing without reading stream again.
                if (checkBytesTransformation(transformer, s3OffsetManagerEntry.getRecordCount())) {
                    return sourceRecords;
                }

                final byte[] keyBytes = currentObjectKey.getBytes(StandardCharsets.UTF_8);
                try (Stream<Object> recordStream = transformer.getRecords(s3Object::getObjectContent, topic,
                        s3OffsetManagerEntry.getPartition(), s3SourceConfig, numberOfRecsAlreadyProcessed)) {
                    final Iterator<Object> recordIterator = recordStream.iterator();
                    while (recordIterator.hasNext()) {
                        final Object record = recordIterator.next();

                        final byte[] valueBytes = transformer.getValueBytes(record, topic, s3SourceConfig);

                        sourceRecords.add(new S3SourceRecord(s3OffsetManagerEntry, keyBytes, valueBytes));

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

            @Override
            public boolean hasNext() {
                if (s3OffsetManagerEntry == null) {
                    final S3OffsetManagerEntry keyEntry = new S3OffsetManagerEntry(bucketName, currentObjectKey, topic, partition);
                    s3OffsetManagerEntry = offsetManager.getEntry(keyEntry.getManagerKey(), keyEntry::fromProperties);
                }

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
        if (recordIterator.hasNext()) {
            return true;
        }
        while (s3ObjectSummaryIterator.hasNext()) {
            nextS3Object();

            recordIterator = s3ObjectSummaryIterator.
            return recordIterator.hasNext() || s3ObjectSummaryIterator.hasNext();
        }
        return false;
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
