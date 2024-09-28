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
import java.net.ConnectException;
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
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

@SuppressWarnings("PMD.ExcessiveImports")
public final class SourceRecordIterator implements Iterator<S3SourceRecord> {

    public static final Pattern DEFAULT_PATTERN = Pattern
            .compile("(?<topic>[^/]+?)-" + "(?<partition>\\d{5})-" + "(?<offset>\\d{12})" + "\\.(?<extension>[^.]+)$");
    private String currentKey;
    private Iterator<S3ObjectSummary> nextFileIterator;
    private Iterator<Optional<ConsumerRecord<byte[], byte[]>>> recordIterator = Collections.emptyIterator();

    private final Map<Map<String, Object>, Map<String, Object>> offsets;

    private final S3SourceConfig s3SourceConfig;
    private final String bucketName;
    private final AmazonS3 s3Client;

    public SourceRecordIterator(final S3SourceConfig s3SourceConfig, final AmazonS3 s3Client, final String bucketName,
            final Map<Map<String, Object>, Map<String, Object>> offsets) {
        this.s3SourceConfig = s3SourceConfig;
        this.offsets = Optional.ofNullable(offsets).orElseGet(HashMap::new);
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

    private void nextObject() {
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
            long startOffset = 0l;
            if (matcher.find()) {
                topic = matcher.group("topic");
                partition = Integer.parseInt(matcher.group("partition"));
                startOffset = Long.parseLong(matcher.group("offset"));
            }

            final String finalTopic = topic;
            final int finalPartition = partition;
            final long finalStartOffset = startOffset;
            switch (s3SourceConfig.getString(OUTPUT_FORMAT)) {
                case AVRO_OUTPUT_FORMAT :
                    final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
                    DecoderFactory.get().binaryDecoder(content, null);
                    return getIterator(content, finalTopic, finalPartition, finalStartOffset, datumReader,
                            AVRO_OUTPUT_FORMAT);
                case PARQUET_OUTPUT_FORMAT :
                    return getIterator(content, finalTopic, finalPartition, finalStartOffset, null,
                            PARQUET_OUTPUT_FORMAT);
                case JSON_OUTPUT_FORMAT :
                    return getIterator(content, finalTopic, finalPartition, finalStartOffset, null, JSON_OUTPUT_FORMAT);
                default :
                    return getIterator(content, finalTopic, finalPartition, finalStartOffset, null, "");
            }
        }
    }

    private Iterator<Optional<ConsumerRecord<byte[], byte[]>>> getIterator(final InputStream content,
            final String finalTopic, final int finalPartition, final long finalStartOffset,
            final DatumReader<GenericRecord> datumReader, final String fileFormat) {
        return new Iterator<>() {
            private Map<Map<String, Object>, Long> currentOffsets = new HashMap<>();
            private Optional<ConsumerRecord<byte[], byte[]>> nextRecord = readNext();

            private Optional<ConsumerRecord<byte[], byte[]>> readNext() {
                try {
                    Optional<byte[]> key = Optional.empty();
                    if (currentKey != null) {
                        key = Optional.of(currentKey.getBytes(StandardCharsets.UTF_8));
                    }
                    byte[] value;
                    value = getBytes(fileFormat, content, datumReader, finalTopic);

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

            // private Optional<ConsumerRecord<byte[], byte[]>> getConsumerRecord(final Optional<byte[]> key,
            // final byte[] value) {
            // final OffsetStoragePartitionKey offsetStoragePartitionKey = OffsetStoragePartitionKey
            // .fromPartitionMap(bucketName, finalTopic, finalPartition);
            //
            // long currentOffset;
            // if (offsets.containsKey(offsetStoragePartitionKey)) {
            // final OffsetStoragePartitionValue offsetStoragePartitionValue = offsets
            // .get(offsetStoragePartitionKey);
            // currentOffset = offsetStoragePartitionValue.getOffset() + 1;
            // } else {
            // currentOffset = currentOffsets.getOrDefault(offsetStoragePartitionKey, finalStartOffset);
            // }
            // final Optional<ConsumerRecord<byte[], byte[]>> record = Optional
            // .of(new ConsumerRecord<>(finalTopic, finalPartition, currentOffset, key.orElse(null), value));
            // currentOffsets.put(offsetStoragePartitionKey, currentOffset + 1);
            // return record;
            // }

            private Optional<ConsumerRecord<byte[], byte[]>> getConsumerRecord(final Optional<byte[]> key,
                    final byte[] value) {
                // Create a map to represent the partition information
                final Map<String, Object> partitionMap = new HashMap<>();
                partitionMap.put("bucket", bucketName);
                partitionMap.put("topic", finalTopic);
                partitionMap.put("partition", finalPartition);

                long currentOffset;

                // Check if the partition is present in the offsets map
                if (offsets.containsKey(partitionMap)) {
                    // Retrieve the offset map and extract the offset value
                    final Map<String, Object> offsetMap = offsets.get(partitionMap);
                    currentOffset = (long) offsetMap.get("offset") + 1; // Assuming "offset" is the key for the offset
                                                                        // value
                } else {
                    // If not present in offsets, check currentOffsets or use the finalStartOffset
                    currentOffset = currentOffsets.getOrDefault(partitionMap, finalStartOffset);
                }

                // Create the ConsumerRecord
                final Optional<ConsumerRecord<byte[], byte[]>> record = Optional
                        .of(new ConsumerRecord<>(finalTopic, finalPartition, currentOffset, key.orElse(null), value));

                // Update currentOffsets with the next offset
                currentOffsets.put(partitionMap, currentOffset + 1);

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

    private byte[] getBytes(final String fileFormat, final InputStream content,
            final DatumReader<GenericRecord> datumReader, final String topicName) throws IOException {
        byte[] value;
        if (fileFormat.equals(AVRO_OUTPUT_FORMAT)) {
            List<GenericRecord> items;
            try (SeekableInput sin = new SeekableByteArrayInput(IOUtils.toByteArray(content))) {
                try (DataFileReader<GenericRecord> reader = new DataFileReader<>(sin, datumReader)) {
                    items = new ArrayList<>();
                    reader.forEach(items::add);
                }
            }
            value = serializeAvroRecordToBytes(items, topicName);
        } else if (fileFormat.equals(JSON_OUTPUT_FORMAT)) {
            value = serializeJsonData(content);
        } else {
            value = IOUtils.toByteArray(content);
        }
        return value;
    }

    private byte[] serializeJsonData(final InputStream inputStream) throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode jsonNode = objectMapper.readTree(inputStream);
        return objectMapper.writeValueAsBytes(jsonNode);
    }

    @Deprecated
    private byte[] serializeAvroRecordToBytes(final List<GenericRecord> avroRecords, final String finalTopic)
            throws IOException {
        // Create a map to configure the Avro serializer
        final Map<String, String> config = new HashMap<>();
        config.put(SCHEMA_REGISTRY_URL, s3SourceConfig.getString(SCHEMA_REGISTRY_URL));

        try (KafkaAvroSerializer avroSerializer = (KafkaAvroSerializer) s3SourceConfig.getClass(VALUE_SERIALIZER)
                .newInstance()) {
            avroSerializer.configure(config, false); // `false` since this is for value serialization
            // Use a ByteArrayOutputStream to combine the serialized records
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            // Loop through each Avro record and serialize it
            for (final GenericRecord avroRecord : avroRecords) {
                final byte[] serializedRecord = avroSerializer.serialize(finalTopic, avroRecord);
                outputStream.write(serializedRecord);
            }

            // Convert the combined output stream to a byte array and return it
            return outputStream.toByteArray();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new ConnectException("Could not create instance of serializer.");
        }
    }

    private InputStream getContent(final S3Object object) {
        return object.getObjectContent();
    }

    @Override
    public boolean hasNext() {
        while (!recordIterator.hasNext() && nextFileIterator.hasNext()) {
            nextObject();
        }
        return recordIterator.hasNext();
    }

    @Override
    public S3SourceRecord next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final ConsumerRecord<byte[], byte[]> record = recordIterator.next().get();

        // Create the partition map
        final Map<String, Object> partitionMap = new HashMap<>();
        partitionMap.put("bucket", bucketName);
        partitionMap.put("topic", record.topic());
        partitionMap.put("partition", record.partition());

        // Create the offset map
        final Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put("offset", record.offset());

        return new S3SourceRecord(partitionMap, // Use the partition map
                offsetMap, // Use the offset map
                record.topic(), // Topic
                record.partition(), // Partition
                record.key(), // Record key
                record.value() // Record value
        );
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
