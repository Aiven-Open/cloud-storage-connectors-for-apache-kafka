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
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.source.SourceTaskContext;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.output.BadDataException;
import io.aiven.kafka.connect.s3.source.output.Transformer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterator that processes S3 files and creates Kafka source records. Supports different output formats (Avro, JSON,
 * Parquet).
 */
public final class AivenS3SourceRecordIterator implements Iterator<AivenS3SourceRecord> {
    /** The logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(AivenS3SourceRecordIterator.class);
    /** The name for the topic pattern */
    private static final int PATTERN_TOPIC_GROUP = 1;
    /** The name for the partition pattern */
    private static final int PATTERN_PARTITION_GROUP = 2;
    /** the name of the offset value in the offset map */
    public static final String OFFSET_KEY = "offset";
    /** The pattern to extract topic name, partition id, and file extension from the processed file */
    // ^(.+)-(\d{5}).*(\.[^.]+)$
    public static final Pattern FILE_DEFAULT_PATTERN = Pattern.compile("(.+)-(\\d{5}).*(\\.[^.]+)$"); // ex :
                                                                                                      // topic-00001.txt
    /** The default partition id */
    public static final int DEFAULT_PARTITION = 0;
    /** The default offset value */
    public static final long DEFAULT_START_OFFSET_ID = 0L;
    /** The iterator of S3Objects */
    private final Iterator<S3Object> s3ObjectIterator;
    /** The Object offset manager */
    private OffsetManager offsetManager;
    /** The source configuration */
    private final S3SourceConfig s3SourceConfig;
    /** The bucket name to process */
    private final String bucketName;
    /** The transformer to transform the data */
    private final Transformer transformer;
    /** The iterator of iterators produced from the transformer output */
    private Iterator<Iterator<AivenS3SourceRecord>> innerIterator;
    /** The active iterator. This is retrieved from the innerIterator */
    private Iterator<AivenS3SourceRecord> outerIterator;
    /** The consumer to record S3 keys that could not be processed */
    private final Consumer<String> badS3ObjectConsumer;

    /**
     * A transforming iterator that converts {@link S3ObjectSummary}s into {@link AivenS3SourceRecord}s
     *
     * @param s3SourceConfig
     *            The Configuration for this source.
     * @param s3Client
     *            The client to communicate with S3.
     * @param bucketName
     *            the name of the bucket to read from.
     * @param context
     *            the source task context.
     * @param transformer
     *            the transformer to translate S3 string to the expected output byte[].
     * @param s3ObjectSummaryIterator
     *            An iterator of S3ObjectSummaries.
     * @param badS3ObjectConsumer
     *            a consumer for S3 key that represent objects that could not be processed by the transformer.
     */
    public AivenS3SourceRecordIterator(final S3SourceConfig s3SourceConfig, final AmazonS3 s3Client,
                                       final String bucketName, final SourceTaskContext context, final Transformer transformer,
                                       final Iterator<S3ObjectSummary> s3ObjectSummaryIterator, final Consumer<String> badS3ObjectConsumer) {
        this.s3SourceConfig = s3SourceConfig;
        this.bucketName = bucketName;
        this.transformer = transformer;
        this.s3ObjectIterator = new TransformIterator<>(s3ObjectSummaryIterator,
                (s) -> s3Client.getObject(bucketName, s.getKey()));
        this.innerIterator = new TransformIterator<>(s3ObjectIterator,
                this::convertS3ObjectToAvinS3SourceRecordIterator);
        this.outerIterator = Collections.emptyIterator();
        resetOffsetManager(context);
        this.badS3ObjectConsumer = badS3ObjectConsumer;
    }

    /**
     * Resets the offset manager to the offsets stored in the context
     *
     * @param context
     *            the Context to initialize the offset manager from.
     */
    public void resetOffsetManager(final SourceTaskContext context) {
        offsetManager = new OffsetManager(context, s3SourceConfig);
    }

    /**
     * Convers an S3Object into a AivenS2SourceRecord iterator.
     *
     * @param s3Object
     * @return
     */
    private Iterator<AivenS3SourceRecord> convertS3ObjectToAvinS3SourceRecordIterator(final S3Object s3Object) {
        final String topic;
        final int topicPartition;
        final byte[] key = s3Object.getKey() == null ? null : s3Object.getKey().getBytes(StandardCharsets.UTF_8);

        try {
            if (key == null) {
                return Collections.emptyIterator();
            }
            // extract the topic an partition information
            final Matcher fileMatcher = FILE_DEFAULT_PATTERN.matcher(s3Object.getKey());
            if (fileMatcher.find()) {
                topic = fileMatcher.group(PATTERN_TOPIC_GROUP);
                topicPartition = Integer.parseInt(fileMatcher.group(PATTERN_PARTITION_GROUP));
            } else {
                return Collections.emptyIterator();
            }

            // create the partition map.
            final Map<String, Object> partitionMap = ConnectUtils.getPartitionMap(topic, topicPartition, bucketName);

            final Long offsetValue = offsetManager.getOffset(partitionMap, s3Object.getKey());

            // predicate to filter out empty byte arrays and records that we have already read.
            Predicate<byte[]> recordFilter = new Predicate<byte[]>() {
                private int recordCounter = 0;
                @Override
                public boolean test(byte[] bytes) {
                    try {
                        if (bytes.length == 0) {
                            return false;
                        }
                        if (offsetValue != null) {
                            return recordCounter > offsetValue;
                        }
                        return true;
                    } finally {
                        recordCounter++;
                    }
                }
            };

            try (InputStream inputStream = s3Object.getObjectContent()) {
                // create a byte[] iterator and filter out empty byte[]
                Iterator<byte[]> data = new FilterIterator<>(
                        transformer.byteArrayIterator(inputStream, topic, s3SourceConfig), recordFilter);
                // iterator that converts the byte[] iterator to AivenS3SourceRecord iterator.
                return new TransformIterator<byte[], AivenS3SourceRecord>(data, value -> {
                    Map<String, Object> offsetMap = new HashMap<>();
                    offsetMap.put(OFFSET_KEY, offsetManager.incrementAndUpdateOffset(partitionMap, s3Object.getKey(),
                            DEFAULT_START_OFFSET_ID));

                    return new AivenS3SourceRecord(partitionMap, offsetMap, topic, topicPartition, key, value,
                            s3Object.getKey());
                });
            } catch (BadDataException e) {
                LOGGER.error("Error reading data for {}, adding to skip list.", s3Object.getKey(), e);
                badS3ObjectConsumer.accept(s3Object.getKey());
                return Collections.emptyIterator();
            }

        } catch (IOException e) {
            LOGGER.error("can not create consumer record for S3Object", e);
            return Collections.emptyIterator();
        }
    }

    @Override
    public boolean hasNext() {
        while (innerIterator.hasNext() && !outerIterator.hasNext()) {
            outerIterator = innerIterator.next();
        }
        return outerIterator.hasNext();
    }

    @Override
    public AivenS3SourceRecord next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return outerIterator.next();
    }

}
