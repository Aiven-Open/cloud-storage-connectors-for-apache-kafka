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
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.AVRO_OUTPUT_FORMAT;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.JSON_OUTPUT_FORMAT;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.OUTPUT_FORMAT;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.PARQUET_OUTPUT_FORMAT;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.SCHEMA_REGISTRY_URL;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.VALUE_SERIALIZER;

import java.io.ByteArrayOutputStream;
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
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

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

    public SourceRecordIterator(final S3SourceConfig s3SourceConfig, final AmazonS3 s3Client, final String bucketName,
            final OffsetManager offsetManager) {
        this.s3SourceConfig = s3SourceConfig;
        this.offsetManager = offsetManager;
        this.s3Client = s3Client;
        this.bucketName = bucketName;
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
        try (InputStream content = fileReader.getContent(s3Object)) {
            final Matcher matcher = DEFAULT_PATTERN.matcher(currentKey);
            String topic = null;
            int partition = 0;
            long startOffset = 0L;
            if (matcher.find()) {
                topic = matcher.group(PATTERN_TOPIC_KEY);
                partition = Integer.parseInt(matcher.group(PATTERN_PARTITION_KEY));
                startOffset = Long.parseLong(matcher.group(OFFSET_KEY));
            }

            final String finalTopic = topic;
            final int finalPartition = partition;
            final long finalStartOffset = startOffset;

            switch (s3SourceConfig.getString(OUTPUT_FORMAT)) {
                case AVRO_OUTPUT_FORMAT :
                    final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
                    DecoderFactory.get().binaryDecoder(content, null);
                    return getObjectIterator(content, finalTopic, finalPartition, finalStartOffset, datumReader,
                            AVRO_OUTPUT_FORMAT);
                case PARQUET_OUTPUT_FORMAT :
                    return getObjectIterator(content, finalTopic, finalPartition, finalStartOffset, null,
                            PARQUET_OUTPUT_FORMAT);
                case JSON_OUTPUT_FORMAT :
                    return getObjectIterator(content, finalTopic, finalPartition, finalStartOffset, null,
                            JSON_OUTPUT_FORMAT);
                default :
                    return getObjectIterator(content, finalTopic, finalPartition, finalStartOffset, null, "");
            }
        }
    }

    @SuppressWarnings("PMD.CognitiveComplexity")
    private Iterator<List<ConsumerRecord<byte[], byte[]>>> getObjectIterator(final InputStream inputStream,
            final String topic, final int topicPartition, final long startOffset,
            final DatumReader<GenericRecord> datumReader, final String fileFormat) {
        return new Iterator<>() {
            private Map<Map<String, Object>, Long> currentOffsets = new HashMap<>();
            private List<ConsumerRecord<byte[], byte[]>> nextRecord = readNext();

            private List<ConsumerRecord<byte[], byte[]>> readNext() {
                try {
                    final Optional<byte[]> key = Optional.ofNullable(currentKey)
                            .map(k -> k.getBytes(StandardCharsets.UTF_8));
                    final List<ConsumerRecord<byte[], byte[]>> consumerRecordList = new ArrayList<>();
                    handleValueData(key, consumerRecordList);

                    return consumerRecordList;

                } catch (IOException e) {
                    throw new org.apache.kafka.connect.errors.ConnectException(
                            "Connect converters could not be instantiated.", e);
                }
            }

            private void handleValueData(final Optional<byte[]> key,
                    final List<ConsumerRecord<byte[], byte[]>> consumerRecordList) throws IOException {
                switch (fileFormat) {
                    case PARQUET_OUTPUT_FORMAT : {
                        final List<GenericRecord> records = ParquetUtils.getRecords(inputStream, topic);
                        for (final GenericRecord record : records) {
                            final byte[] valueBytes = serializeAvroRecordToBytes(Collections.singletonList(record),
                                    topic);
                            consumerRecordList.add(getConsumerRecord(key, valueBytes));
                        }
                        break;
                    }
                    case AVRO_OUTPUT_FORMAT : {
                        final List<GenericRecord> records = readAvroRecords(inputStream, datumReader);
                        for (final GenericRecord record : records) {
                            final byte[] valueBytes = serializeAvroRecordToBytes(Collections.singletonList(record),
                                    topic);
                            consumerRecordList.add(getConsumerRecord(key, valueBytes));
                        }
                        break;
                    }
                    case JSON_OUTPUT_FORMAT :
                        consumerRecordList.add(getConsumerRecord(key, serializeJsonData(inputStream)));
                        break;
                    default :
                        consumerRecordList.add(getConsumerRecord(key, IOUtils.toByteArray(inputStream)));
                        break;
                }
            }

            private ConsumerRecord<byte[], byte[]> getConsumerRecord(final Optional<byte[]> key, final byte[] value) {
                final Map<String, Object> partitionMap = new HashMap<>();
                partitionMap.put(BUCKET, bucketName);
                partitionMap.put(TOPIC, topic);
                partitionMap.put(PARTITION, topicPartition);

                long currentOffset;

                if (offsetManager.getOffsets().containsKey(partitionMap)) {
                    currentOffset = offsetManager.getIncrementedOffsetForPartition(partitionMap);
                } else {
                    currentOffset = currentOffsets.getOrDefault(partitionMap, startOffset);
                }

                final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(topic, topicPartition, currentOffset,
                        key.orElse(null), value);

                offsetManager.updateOffset(partitionMap, currentOffset);

                return record;
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

    private List<GenericRecord> readAvroRecords(final InputStream content, final DatumReader<GenericRecord> datumReader)
            throws IOException {
        final List<GenericRecord> records = new ArrayList<>();
        try (SeekableByteArrayInput sin = new SeekableByteArrayInput(IOUtils.toByteArray(content))) {
            try (DataFileReader<GenericRecord> reader = new DataFileReader<>(sin, datumReader)) {
                reader.forEach(records::add);
            }
        }
        return records;
    }

    private byte[] serializeJsonData(final InputStream inputStream) throws IOException {
        final JsonNode jsonNode = objectMapper.readTree(inputStream);
        return objectMapper.writeValueAsBytes(jsonNode);
    }

    @Deprecated
    private byte[] serializeAvroRecordToBytes(final List<GenericRecord> avroRecords, final String topic)
            throws IOException {
        final Map<String, String> config = Collections.singletonMap(SCHEMA_REGISTRY_URL,
                s3SourceConfig.getString(SCHEMA_REGISTRY_URL));

        try (KafkaAvroSerializer avroSerializer = (KafkaAvroSerializer) s3SourceConfig.getClass(VALUE_SERIALIZER)
                .newInstance(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            avroSerializer.configure(config, false);
            for (final GenericRecord avroRecord : avroRecords) {
                out.write(avroSerializer.serialize(topic, avroRecord));
            }
            return out.toByteArray();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalStateException("Failed to initialize serializer", e);
        }
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
