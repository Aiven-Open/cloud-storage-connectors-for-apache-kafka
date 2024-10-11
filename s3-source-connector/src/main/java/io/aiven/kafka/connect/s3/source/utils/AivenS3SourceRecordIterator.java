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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.output.BadDataException;
import io.aiven.kafka.connect.s3.source.output.Transformer;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.aiven.kafka.connect.s3.source.S3SourceTask.*;

/**
 * Iterator that processes S3 files and creates Kafka source records. Supports different output formats (Avro, JSON,
 * Parquet).
 */
public final class AivenS3SourceRecordIterator implements Iterator<AivenS3SourceRecord>  {
    private static final Logger LOGGER = LoggerFactory.getLogger(AivenS3SourceRecordIterator.class);
    public static final String PATTERN_TOPIC_KEY = "topicName";
    public static final String PATTERN_PARTITION_KEY = "partitionId";
    public static final String OFFSET_KEY = "offset";

    public static final Pattern FILE_DEFAULT_PATTERN = Pattern
            .compile("(?<topicName>[^/]+?)-" + "(?<partitionId>\\d{5})" + "\\.(?<fileExtension>[^.]+)$"); // ex :

    public static final int PAGE_SIZE_FACTOR = 2;

    public static final int DEFAULT_PARTITION = 0;

    public static final long DEFAULT_START_OFFSET_ID = 0L;

    private final Iterator<S3Object> s3ObjectIterator;

    private OffsetManager offsetManager;

    private final S3SourceConfig s3SourceConfig;
    private final String bucketName;
    private final AmazonS3 s3Client;

    private final Transformer outputWriter;

    private Iterator<Iterator<AivenS3SourceRecord>> innerIterator;

    private Iterator<AivenS3SourceRecord> outerIterator;

    private final Consumer<String> badS3ObjectConsumer;

    public AivenS3SourceRecordIterator(final S3SourceConfig s3SourceConfig, final AmazonS3 s3Client, final String bucketName,
                                       final SourceTaskContext context, final Transformer outputWriter, final Set<String> failedObjectKeys,
                                       final Iterator<S3ObjectSummary> s3ObjectSummaryIterator,
                                       final Consumer<String> badS3ObjectConsumer) {
        this.s3SourceConfig = s3SourceConfig;
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.outputWriter = outputWriter;
        this.s3ObjectIterator = new TransformIterator<S3ObjectSummary, S3Object>(s3ObjectSummaryIterator, (s) ->  s3Client.getObject(bucketName, s.getKey()));
        this.innerIterator = new TransformIterator<S3Object, Iterator<AivenS3SourceRecord>>(s3ObjectIterator, this::convertS3ObjectToAvinS3SourceRecordIterator);
        this.outerIterator = Collections.emptyIterator();
        resetOffsetManager(context);
        this.badS3ObjectConsumer = badS3ObjectConsumer;
    }

    public void resetOffsetManager(final SourceTaskContext context) {
        offsetManager = new OffsetManager(context, s3SourceConfig);
    }
    private Iterator<AivenS3SourceRecord> convertS3ObjectToAvinS3SourceRecordIterator(final S3Object s3Object) {
        final String topic;
        final int topicPartition;
        final byte[] key = s3Object.getKey() == null ? null : s3Object.getKey().getBytes(StandardCharsets.UTF_8);

        try {
            if (key == null) {
                topic = offsetManager.getFirstConfiguredTopic(s3SourceConfig);
                topicPartition = DEFAULT_PARTITION;
            } else {
                final Matcher fileMatcher = FILE_DEFAULT_PATTERN.matcher(s3Object.getKey());
                if (fileMatcher.find()) {
                    topic = fileMatcher.group(PATTERN_TOPIC_KEY);
                    topicPartition = Integer.parseInt(fileMatcher.group(PATTERN_PARTITION_KEY));
                } else {
                    topic = offsetManager.getFirstConfiguredTopic(s3SourceConfig);
                    topicPartition = DEFAULT_PARTITION;
                }
            }

            final Map<String, Object> partitionMap = ConnectUtils.getPartitionMap(topic, topicPartition,
                    bucketName);

            Map<Map<String, Object>, Long> currentOffsets = new HashMap<>();

            try (InputStream inputStream = s3Object.getObjectContent()) {
                // create a byte[] iterator and filter out empty byte[]
                Iterator<byte[]> data = new FilterIterator<>(outputWriter.byteArrayIterator(inputStream, topic, s3SourceConfig), bytes -> bytes.length > 0);
                // convert byte[] to ConsumerRecord
                return new TransformIterator<byte[], AivenS3SourceRecord>(data,
                        value -> {
                            long currentOffset;
                            if (offsetManager.getOffsets().containsKey(partitionMap)) {
                                currentOffset = offsetManager.incrementAndUpdateOffsetMap(partitionMap);
                            } else {
                                currentOffset = currentOffsets.getOrDefault(partitionMap, DEFAULT_START_OFFSET_ID);
                                currentOffsets.put(partitionMap, currentOffset + 1);
                            }
                            Map<String, Object> aivenPartitionMap = new HashMap<>();
                            aivenPartitionMap.put(BUCKET, bucketName);
                            aivenPartitionMap.put(TOPIC, topic);
                            aivenPartitionMap.put(PARTITION, topicPartition);

                            // Create the offset map
                            Map<String, Object> offsetMap = new HashMap<>();
                            offsetMap.put(OFFSET_KEY, currentOffset);

                            return new AivenS3SourceRecord(partitionMap, offsetMap, topic,
                                    topicPartition, key, value, s3Object.getKey());


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
