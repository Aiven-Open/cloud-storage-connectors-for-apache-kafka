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

import static io.aiven.kafka.connect.s3.source.S3SourceTask.BUCKET;
import static io.aiven.kafka.connect.s3.source.S3SourceTask.PARTITION;
import static io.aiven.kafka.connect.s3.source.S3SourceTask.TOPIC;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.AVRO_OUTPUT_FORMAT;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.FETCH_PAGE_SIZE;
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
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
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
public final class SourceRecordIterator implements Iterator<S3SourceRecord> {

    public static final Pattern DEFAULT_PATTERN = Pattern
            .compile("(?<topic>[^/]+?)-" + "(?<partition>\\d{5})-" + "(?<offset>\\d{12})" + "\\.(?<extension>[^.]+)$");
    public static final String PATTERN_TOPIC_KEY = "topic";
    public static final String PATTERN_PARTITION_KEY = "partition";
    public static final String OFFSET_KEY = "offset";
    private String currentKey;

    final ObjectMapper objectMapper = new ObjectMapper();
    private Iterator<S3ObjectSummary> nextFileIterator;
    private Iterator<Optional<ConsumerRecord<byte[], byte[]>>> recordIterator = Collections.emptyIterator();

    // private final Map<Map<String, Object>, Map<String, Object>> offsets;

    private final OffsetManager offsetManager;

    private final S3SourceConfig s3SourceConfig;
    private final String bucketName;
    private final AmazonS3 s3Client;

    public SourceRecordIterator(final S3SourceConfig s3SourceConfig, final AmazonS3 s3Client, final String bucketName,
            final OffsetManager offsetManager) {
        this.s3SourceConfig = s3SourceConfig;
        // this.offsets = Optional.ofNullable(offsets).orElseGet(HashMap::new);
        this.offsetManager = offsetManager;
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        try {
            final List<S3ObjectSummary> chunks = fetchObjectSummaries(s3Client);
            nextFileIterator = chunks.iterator();
        } catch (IOException e) {
            throw new AmazonClientException("Failed to initialize S3 file reader", e);
        }
    }

    private List<S3ObjectSummary> fetchObjectSummaries(final AmazonS3 s3Client) throws IOException {
        final ObjectListing objectListing = s3Client.listObjects(new ListObjectsRequest().withBucketName(bucketName)
                .withMaxKeys(s3SourceConfig.getInt(FETCH_PAGE_SIZE) * 2));

        return new ArrayList<>(objectListing.getObjectSummaries());
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

    private Iterator<Optional<ConsumerRecord<byte[], byte[]>>> createIteratorForCurrentFile() throws IOException {
        final S3Object s3Object = s3Client.getObject(bucketName, currentKey);
        try (InputStream content = getContent(s3Object)) {
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

    private Iterator<Optional<ConsumerRecord<byte[], byte[]>>> getObjectIterator(final InputStream content,
            final String topic, final int topicPartition, final long startOffset,
            final DatumReader<GenericRecord> datumReader, final String fileFormat) {
        return new Iterator<>() {
            private Map<Map<String, Object>, Long> currentOffsets = new HashMap<>();
            private Optional<ConsumerRecord<byte[], byte[]>> nextRecord = readNext();

            private Optional<ConsumerRecord<byte[], byte[]>> readNext() {
                try {
                    final Optional<byte[]> key = Optional.ofNullable(currentKey)
                            .map(k -> k.getBytes(StandardCharsets.UTF_8));
                    final byte[] value = getValueBytes(fileFormat, content, datumReader, topic);

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
                final Map<String, Object> partitionMap = new HashMap<>();
                partitionMap.put(BUCKET, bucketName);
                partitionMap.put(TOPIC, topic);
                partitionMap.put(PARTITION, topicPartition);

                long currentOffset;

                if (offsetManager.getOffsets().containsKey(partitionMap)) {
                    final Map<String, Object> offsetMap = offsetManager.getOffsetForPartition(partitionMap);
                    currentOffset = (long) offsetMap.get(OFFSET_KEY) + 1;
                } else {
                    currentOffset = currentOffsets.getOrDefault(partitionMap, startOffset);
                }

                final Optional<ConsumerRecord<byte[], byte[]>> record = Optional
                        .of(new ConsumerRecord<>(topic, topicPartition, currentOffset, key.orElse(null), value));

                final Map<String, Object> newOffset = new HashMap<>();
                newOffset.put(OFFSET_KEY, currentOffset + 1);
                offsetManager.updateOffset(partitionMap, newOffset);

                return record;
            }

            @Override
            public boolean hasNext() {
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

    private byte[] getValueBytes(final String fileFormat, final InputStream content,
            final DatumReader<GenericRecord> datumReader, final String topicName) throws IOException {
        if (AVRO_OUTPUT_FORMAT.equals(fileFormat)) {
            return serializeAvroRecordToBytes(readAvroRecords(content, datumReader), topicName);
        } else if (JSON_OUTPUT_FORMAT.equals(fileFormat)) {
            return serializeJsonData(content);
        } else {
            return IOUtils.toByteArray(content);
        }
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

    private InputStream getContent(final S3Object object) {
        return object.getObjectContent();
    }

    // @Override
    // public boolean hasNext() {
    // while (!recordIterator.hasNext() && nextFileIterator.hasNext()) {
    // nextS3Object();
    // }
    // return recordIterator.hasNext();
    // }

    @Override
    public boolean hasNext() {
        return recordIterator.hasNext() || nextFileIterator.hasNext();
    }

    @Override
    public S3SourceRecord next() {
        if (!recordIterator.hasNext()) {
            nextS3Object();
        }

        final Optional<ConsumerRecord<byte[], byte[]>> consumerRecord = recordIterator.next();
        if (consumerRecord.isEmpty()) {
            throw new NoSuchElementException();
        }

        final ConsumerRecord<byte[], byte[]> currentRecord = consumerRecord.get();

        final Map<String, Object> partitionMap = new HashMap<>();
        partitionMap.put(BUCKET, bucketName);
        partitionMap.put(TOPIC, currentRecord.topic());
        partitionMap.put(PARTITION, currentRecord.partition());

        // Create the offset map
        final Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put(OFFSET_KEY, currentRecord.offset());

        return new S3SourceRecord(partitionMap, offsetMap, currentRecord.topic(), currentRecord.partition(),
                currentRecord.key(), currentRecord.value());
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
