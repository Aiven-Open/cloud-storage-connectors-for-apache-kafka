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

import static io.aiven.kafka.connect.s3.source.S3SourceTask.BUCKET;
import static io.aiven.kafka.connect.s3.source.S3SourceTask.PARTITION;
import static io.aiven.kafka.connect.s3.source.S3SourceTask.TOPIC;

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
import io.aiven.kafka.connect.s3.source.output.OutputWriter;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Iterator that processes S3 files and creates Kafka source records. Supports different output formats (Avro, JSON,
 * Parquet).
 */
@SuppressWarnings("PMD.ExcessiveImports")
public final class SourceRecordIterator implements Iterator<List<AivenS3SourceRecord>> {

    public static final Pattern DEFAULT_PATTERN = Pattern
            .compile("(?<topic>[^/]+?)-" + "(?<partition>\\d{5})-" + "(?<offset>\\d{12})" + "\\.(?<extension>[^.]+)$");
    public static final String PATTERN_TOPIC_KEY = "topic";
    public static final String PATTERN_PARTITION_KEY = "partition";
    public static final String OFFSET_KEY = "offset";
    private String currentKey;

    final ObjectMapper objectMapper = new ObjectMapper();
    private Iterator<S3ObjectSummary> nextFileIterator;
    private Iterator<List<ConsumerRecord<byte[], byte[]>>> recordIterator = Collections.emptyIterator();

    private final OffsetManager offsetManager;

    private final S3SourceConfig s3SourceConfig;
    private final String bucketName;
    private final AmazonS3 s3Client;

    private final FileReader fileReader;

    private final OutputWriter outputWriter;

    public SourceRecordIterator(final S3SourceConfig s3SourceConfig, final AmazonS3 s3Client, final String bucketName,
            final OffsetManager offsetManager, final OutputWriter outputWriter) {
        this.s3SourceConfig = s3SourceConfig;
        this.offsetManager = offsetManager;
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.outputWriter = outputWriter;
        this.fileReader = new FileReader(s3SourceConfig, bucketName);
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
            currentKey = file.getKey();
            recordIterator = createIteratorForCurrentFile();
        } catch (IOException e) {
            throw new AmazonClientException(e);
        }
    }

    private Iterator<List<ConsumerRecord<byte[], byte[]>>> createIteratorForCurrentFile() throws IOException {
        final S3Object s3Object = s3Client.getObject(bucketName, currentKey);
        try (InputStream inputStream = fileReader.getContent(s3Object)) {
            final Matcher matcher = DEFAULT_PATTERN.matcher(currentKey);
            String topic;
            int partition = 0;
            long startOffset = 0L;
            if (matcher.find()) {
                topic = matcher.group(PATTERN_TOPIC_KEY);
                partition = Integer.parseInt(matcher.group(PATTERN_PARTITION_KEY));
                startOffset = Long.parseLong(matcher.group(OFFSET_KEY));
            } else {
                topic = offsetManager.getFirstConfiguredTopic(s3SourceConfig);
            }

            final String finalTopic = topic;
            final int finalPartition = partition;
            final long finalStartOffset = startOffset;

            return getObjectIterator(inputStream, finalTopic, finalPartition, finalStartOffset, outputWriter);
        }
    }

    @SuppressWarnings("PMD.CognitiveComplexity")
    private Iterator<List<ConsumerRecord<byte[], byte[]>>> getObjectIterator(final InputStream valueInputStream,
            final String topic, final int topicPartition, final long startOffset, final OutputWriter outputWriter) {
        return new Iterator<>() {
            private Map<Map<String, Object>, Long> currentOffsets = new HashMap<>();
            private List<ConsumerRecord<byte[], byte[]>> nextRecord = readNext();

            private List<ConsumerRecord<byte[], byte[]>> readNext() {
                try {
                    final Optional<byte[]> optionalKeyBytes = Optional.ofNullable(currentKey)
                            .map(k -> k.getBytes(StandardCharsets.UTF_8));
                    final List<ConsumerRecord<byte[], byte[]>> consumerRecordList = new ArrayList<>();
                    outputWriter.handleValueData(optionalKeyBytes, valueInputStream, topic, consumerRecordList,
                            s3SourceConfig, topicPartition, startOffset, offsetManager, currentOffsets);

                    return consumerRecordList;

                } catch (IOException e) {
                    throw new org.apache.kafka.connect.errors.ConnectException(
                            "Connect converters could not be instantiated.", e);
                }
            }

            @Override
            public boolean hasNext() {
                return !nextRecord.isEmpty();
            }

            @Override
            public List<ConsumerRecord<byte[], byte[]>> next() {
                if (nextRecord.isEmpty()) {
                    throw new NoSuchElementException();
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
            throw new NoSuchElementException();
        }
        final List<AivenS3SourceRecord> aivenS3SourceRecordList = new ArrayList<>();

        AivenS3SourceRecord aivenS3SourceRecord;
        Map<String, Object> offsetMap;
        Map<String, Object> partitionMap;
        for (final ConsumerRecord<byte[], byte[]> currentRecord : consumerRecordList) {
            partitionMap = new HashMap<>();
            partitionMap.put(BUCKET, bucketName);
            partitionMap.put(TOPIC, currentRecord.topic());
            partitionMap.put(PARTITION, currentRecord.partition());

            // Create the offset map
            offsetMap = new HashMap<>();
            offsetMap.put(OFFSET_KEY, currentRecord.offset());

            aivenS3SourceRecord = new AivenS3SourceRecord(partitionMap, offsetMap, currentRecord.topic(),
                    currentRecord.partition(), currentRecord.key(), currentRecord.value());

            aivenS3SourceRecordList.add(aivenS3SourceRecord);
        }

        return aivenS3SourceRecordList;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
