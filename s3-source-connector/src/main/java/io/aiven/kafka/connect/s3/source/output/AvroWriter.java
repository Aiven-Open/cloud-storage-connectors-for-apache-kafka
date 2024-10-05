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

package io.aiven.kafka.connect.s3.source.output;

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.SCHEMA_REGISTRY_URL;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.VALUE_SERIALIZER;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.utils.OffsetManager;

import com.amazonaws.util.IOUtils;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

public class AvroWriter implements OutputWriter {

    private final String bucketName;

    public AvroWriter(final String bucketName) {
        this.bucketName = bucketName;
    }

    @Override
    public void configureValueConverter(final Map<String, String> config, final S3SourceConfig s3SourceConfig) {
        config.put(SCHEMA_REGISTRY_URL, s3SourceConfig.getString(SCHEMA_REGISTRY_URL));
    }

    @Override
    public void handleValueData(final Optional<byte[]> optionalKeyBytes, final InputStream inputStream,
            final String topic, final List<ConsumerRecord<byte[], byte[]>> consumerRecordList,
            final S3SourceConfig s3SourceConfig, final int topicPartition, final long startOffset,
            final OffsetManager offsetManager, final Map<Map<String, Object>, Long> currentOffsets) throws IOException {
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DecoderFactory.get().binaryDecoder(inputStream, null);
        final List<GenericRecord> records = readAvroRecords(inputStream, datumReader);
        for (final GenericRecord record : records) {
            final byte[] valueBytes = serializeRecordToBytes(Collections.singletonList(record), topic, s3SourceConfig);
            consumerRecordList.add(getConsumerRecord(optionalKeyBytes, valueBytes, this.bucketName, topic,
                    topicPartition, offsetManager, currentOffsets, startOffset));
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

    @Deprecated
    private byte[] serializeRecordToBytes(final List<GenericRecord> avroRecords, final String topic,
            final S3SourceConfig s3SourceConfig) throws IOException {
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
}
