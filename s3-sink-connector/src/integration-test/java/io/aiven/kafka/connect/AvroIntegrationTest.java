/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.kafka.connect;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.connector.Connector;

import io.aiven.kafka.connect.common.config.CommonConfigFragment;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector;

import io.confluent.connect.avro.AvroConverter;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
final class AvroIntegrationTest extends AbstractIntegrationTest<String, GenericRecord> {

    private final Schema avroInputDataSchema = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"input_data\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}");

    private static Stream<Arguments> compressionAndCodecTestParameters() {
        return Stream.of(Arguments.of("bzip2", CompressionType.NONE), Arguments.of("deflate", CompressionType.NONE),
                Arguments.of("null", CompressionType.NONE), Arguments.of("snappy", CompressionType.GZIP), // single test
                                                                                                          // for codec
                                                                                                          // and
                                                                                                          // compression
                                                                                                          // when both
                                                                                                          // set.
                Arguments.of("zstandard", CompressionType.NONE));
    }

    @ParameterizedTest
    @MethodSource("compressionAndCodecTestParameters")
    void avroOutput(final String avroCodec, final CompressionType compression, final TestInfo testInfo)
            throws ExecutionException, InterruptedException, IOException {
        final var topicName = topicName(testInfo);
        final Map<String, String> connectorConfig = awsSpecificConfig(topicName);
        FileNameFragment.setter(connectorConfig).fileCompression(compression);
        OutputFormatFragment.setter(connectorConfig)
                .withFormatType(FormatType.AVRO)
                .withOutputFields(OutputFieldType.KEY, OutputFieldType.VALUE);
        connectorConfig.put("avro.codec", avroCodec);
        createConnector(connectorConfig);

        final int recordCountPerPartition = 10;
        produceRecords(recordCountPerPartition, topicName);

        waitForConnectToFinishProcessing();

        final List<String> expectedBlobs = Arrays.asList(getAvroBlobName(topicName, 0, 0, compression),
                getAvroBlobName(topicName, 1, 0, compression), getAvroBlobName(topicName, 2, 0, compression),
                getAvroBlobName(topicName, 3, 0, compression));

        for (final String blobName : expectedBlobs) {
            assertThat(testBucketAccessor.doesObjectExist(blobName)).isTrue();
        }

        final Map<String, List<GenericRecord>> blobContents = new HashMap<>();
        final Map<String, Schema> gcsOutputAvroSchemas = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final byte[] blobBytes = testBucketAccessor.readBytes(blobName, compression);
            try (SeekableInput sin = new SeekableByteArrayInput(blobBytes)) { // NOPMD AvoidInstantiatingObjectsInLoops
                final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(); // NOPMD
                                                                                                  // AvoidInstantiatingObjectsInLoops
                try (DataFileReader<GenericRecord> reader = new DataFileReader<>(sin, datumReader)) { // NOPMD
                                                                                                      // AvoidInstantiatingObjectsInLoops
                    final List<GenericRecord> items = new ArrayList<>(); // NOPMD AvoidInstantiatingObjectsInLoops
                    reader.forEach(items::add);
                    blobContents.put(blobName, items);
                    gcsOutputAvroSchemas.put(blobName, reader.getSchema());
                }
            }
        }

        int cnt = 0;
        for (int i = 0; i < recordCountPerPartition; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String blobName = getAvroBlobName(topicName, partition, 0, compression);
                final Schema gcsOutputAvroSchema = gcsOutputAvroSchemas.get(blobName);
                final GenericData.Record expectedRecord = new GenericData.Record(gcsOutputAvroSchema); // NOPMD
                                                                                                       // AvoidInstantiatingObjectsInLoops
                expectedRecord.put("key", new Utf8("key-" + cnt)); // NOPMD AvoidInstantiatingObjectsInLoops
                final GenericData.Record valueRecord = new GenericData.Record( // NOPMD
                                                                               // AvoidInstantiatingObjectsInLoops
                        gcsOutputAvroSchema.getField("value").schema());
                valueRecord.put("name", new Utf8("user-" + cnt)); // NOPMD AvoidInstantiatingObjectsInLoops
                expectedRecord.put("value", valueRecord);
                cnt += 1;

                final GenericRecord actualRecord = blobContents.get(blobName).get(i);
                assertThat(actualRecord).isEqualTo(expectedRecord);
            }
        }
    }

    @Test
    void jsonlAvroOutputTest(final TestInfo testInfo) throws ExecutionException, InterruptedException, IOException {
        final var topicName = topicName(testInfo);
        final Map<String, String> connectorConfig = awsSpecificConfig(topicName);
        final CompressionType compression = CompressionType.NONE;
        FileNameFragment.setter(connectorConfig).fileCompression(compression);
        OutputFormatFragment.setter(connectorConfig)
                .withOutputFields(OutputFieldType.KEY, OutputFieldType.VALUE)
                .withOutputFieldEncodingType(OutputFieldEncodingType.NONE)
                .withFormatType(FormatType.JSONL);
        CommonConfigFragment.setter(connectorConfig)
                .keyConverter(AvroConverter.class)
                .valueConverter(AvroConverter.class);
        connectorConfig.put("value.converter.schemas.enable", "false");
        createConnector(connectorConfig);

        final int recordCountPerPartition = 10;
        produceRecords(recordCountPerPartition, topicName);
        waitForConnectToFinishProcessing();

        final List<String> expectedBlobs = Arrays.asList(getBlobName(topicName, 0, 0, compression),
                getBlobName(topicName, 1, 0, compression), getBlobName(topicName, 2, 0, compression),
                getBlobName(topicName, 3, 0, compression));

        for (final String blobName : expectedBlobs) {
            assertThat(testBucketAccessor.doesObjectExist(blobName)).isTrue();
        }

        final Map<String, List<String>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final List<String> items = Collections
                    .unmodifiableList(testBucketAccessor.readLines(blobName, compression));
            blobContents.put(blobName, items);
        }

        int cnt = 0;
        for (int i = 0; i < recordCountPerPartition; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "{" + "\"name\":\"user-" + cnt + "\"}";
                cnt += 1;

                final String blobName = getBlobName(topicName, partition, 0, CompressionType.NONE);
                final String expectedLine = "{\"value\":" + value + ",\"key\":\"" + key + "\"}";

                assertThat(blobContents.get(blobName).get(i)).isEqualTo(expectedLine);
            }
        }
    }

    private void waitForConnectToFinishProcessing() throws InterruptedException {
        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);
    }

    private void produceRecords(final int recordCountPerPartition, final String topicName)
            throws ExecutionException, InterruptedException {
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < recordCountPerPartition; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final GenericRecord value = new GenericData.Record(avroInputDataSchema); // NOPMD
                                                                                         // AvoidInstantiatingObjectsInLoops
                value.put("name", "user-" + cnt);
                cnt += 1;

                sendFutures.add(sendMessageAsync(producer, topicName, partition, key, value));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }
    }

    @Override
    protected KafkaProducer<String, GenericRecord> newProducer() {
        final Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaManager.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put("schema.registry.url", kafkaManager.getSchemaRegistryUrl());
        return new KafkaProducer<>(producerProps);
    }

    private Future<RecordMetadata> sendMessageAsync(final KafkaProducer<String, GenericRecord> producer,
            final String topicName, final int partition, final String key, final GenericRecord value) {
        final ProducerRecord<String, GenericRecord> msg = new ProducerRecord<>(topicName, partition, key, value);
        return producer.send(msg);
    }

    private Map<String, String> basicConnectorConfig(final String connectorName) {
        final Map<String, String> config = new HashMap<>();
        CommonConfigFragment.setter(config)
                .name(connectorName)
                .connector(Connector.class)
                .keyConverter(AvroConverter.class)
                .valueConverter(AvroConverter.class)
                .maxTasks(1);
        config.put("key.converter.schema.registry.url", kafkaManager.getSchemaRegistryUrl());
        config.put("value.converter.schema.registry.url", kafkaManager.getSchemaRegistryUrl());
        return config;
    }

    private Map<String, String> awsSpecificConfig(final String topicName) {
        final Map<String, String> config = basicConnectorConfig(CONNECTOR_NAME);
        CommonConfigFragment.setter(config).connector(AivenKafkaConnectS3SinkConnector.class).maxTasks(1);

        S3ConfigFragment.setter(config)
                .accessKeyId(S3_ACCESS_KEY_ID)
                .endpoint(s3Endpoint)
                .bucketName(TEST_BUCKET_NAME)
                .prefix(s3Prefix);

        config.put("topics", topicName);
        config.put("key.converter.schema.registry.url", kafkaManager.getSchemaRegistryUrl());
        config.put("value.converter.schema.registry.url", kafkaManager.getSchemaRegistryUrl());
        return config;
    }

    private String getAvroBlobName(final String topicName, final int partition, final int startOffset,
            final CompressionType compression) {
        final String result = String.format("%s%s-%d-%020d.avro", s3Prefix, topicName, partition, startOffset);
        return result + compression.extension();
    }

    // WARN: different from GCS
    private String getBlobName(final String topicName, final int partition, final int startOffset,
            final CompressionType compression) {
        final String result = String.format("%s%s-%d-%020d", s3Prefix, topicName, partition, startOffset);
        return result + compression.extension();
    }
}
