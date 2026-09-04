/*
 * Copyright 2026 Aiven Oy
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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

@Testcontainers
/**
 * This test verifies that when an error occurs partway through a multi part upload that the offsets have not been
 * committed and when the connector is restarted it begins processing those events from the first offset.
 */
final class MultiPartUploadIntegrationTest extends AbstractIntegrationTest<String, GenericRecord> {

    private final Schema avroInputDataSchema = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"input_data\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}");

    @Override
    protected Duration getOffsetFlushInterval() {
        // extended flush on this test to allow for failure to occur
        return Duration.ofSeconds(20);
    }

    @Test
    void testMultiPartUploadInterruptAndRecovery(final TestInfo testInfo)
            throws ExecutionException, InterruptedException, IOException, TimeoutException {
        final var topicName = topicName(testInfo);
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(CONNECTOR_NAME), topicName);
        final CompressionType compression = CompressionType.NONE;
        final String contentType = "jsonl";
        connectorConfig.put("file.compression.type", compression.name());
        connectorConfig.put("format.output.type", contentType);
        // initially allow processing of 500 records per partition successfully
        final int initialRecordCountPerPartition = 500;
        // Additional records will fail then when bucket is removed
        final int recordCountPerPartition = 5000;

        final List<String> expectedBlobs = Arrays.asList(getBlobName(topicName, 0, 0, compression),
                getBlobName(topicName, 1, 0, compression), getBlobName(topicName, 2, 0, compression),
                getBlobName(topicName, 3, 0, compression));

        createConnector(connectorConfig);
        produceRecords(0, initialRecordCountPerPartition, topicName);
        wait(3000);
        for (final String blobName : expectedBlobs) {
            assertThatExceptionOfType(NoSuchKeyException.class)
                    .isThrownBy(() -> testBucketAccessor.doesObjectExist(blobName));
        }
        removeBucket();
        produceRecords(initialRecordCountPerPartition * 4, recordCountPerPartition, topicName);

        createBucket();
        restartConnector(CONNECTOR_NAME);
        // Check no offsets have been updated
        verifyOffsetPositions(0, 0);
        // Wait until blobs have all been uploaded after recovery
        Awaitility.await()
                .atLeast(Duration.ofMillis(5000))
                .atMost(Duration.ofMillis(120_000))
                .with()
                .pollInterval(Duration.ofMillis(1000))
                .until(() -> {
                    try {
                        for (final String blobName : expectedBlobs) {
                            testBucketAccessor.doesObjectExist(blobName);
                        }
                        return true;
                    } catch (NoSuchKeyException e) {
                        return false;
                    }
                });

        final Map<String, List<String>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final List<String> items = Collections
                    .unmodifiableList(testBucketAccessor.readLines(blobName, compression));
            blobContents.put(blobName, items);
        }

        validateAllRecordsExist(initialRecordCountPerPartition + recordCountPerPartition, topicName, blobContents);
        // expecting one offset per partition and the initial plus additional record count as the offset
        verifyOffsetPositions(4, initialRecordCountPerPartition + recordCountPerPartition);
    }

    private void validateAllRecordsExist(final int recordCountPerPartition, final String topicName,
            final Map<String, List<String>> blobContents) {
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

    private void wait(final int millis) throws InterruptedException {
        Thread.sleep(millis);
    }

    private void produceRecords(final int initialRecordCountPerPartition, final int recordCountPerPartition,
            final String topicName) throws ExecutionException, InterruptedException {
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = initialRecordCountPerPartition;

        for (int i = initialRecordCountPerPartition; i < recordCountPerPartition
                + initialRecordCountPerPartition; i++) {
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

    /**
     * Verifies the offset position of the sink connector consumer.
     *
     * @param expectedSize
     *            how many offset entries there should be in our case one per partition
     * @param expectedOffset
     *            how many events each one of the partitions are expected to have processed.
     * @throws ExecutionException
     *             Thrown in some scenarios
     * @throws InterruptedException
     *             Request was interrupted
     * @throws TimeoutException
     *             Request timed out and was unable to complete
     */
    private void verifyOffsetPositions(final int expectedSize, final long expectedOffset)
            throws ExecutionException, InterruptedException, TimeoutException {

        final Map<TopicPartition, OffsetAndMetadata> offsets = kafkaManager.getAdminClient()
                .listConsumerGroupOffsets("connect-" + CONNECTOR_NAME)
                .partitionsToOffsetAndMetadata()
                .get(1000, TimeUnit.MILLISECONDS);
        assertThat(offsets.entrySet()).hasSize(expectedSize);

        offsets.forEach((key, value) -> assertThat(value.offset()).isEqualTo(expectedOffset));

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
        config.put("name", connectorName);
        config.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("key.converter.schema.registry.url", kafkaManager.getSchemaRegistryUrl());
        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("value.converter.schema.registry.url", kafkaManager.getSchemaRegistryUrl());
        config.put("tasks.max", "1");
        config.put("offsets.storage.topic", kafkaManager.getOffsetTopic());
        config.put("format.output.fields", "key,value");
        config.put("format.output.fields.value.encoding", "none");
        config.put("value.converter.schemas.enable", "false");
        return config;
    }

    private Map<String, String> awsSpecificConfig(final Map<String, String> config, final String topicName) {
        config.put("connector.class", AivenKafkaConnectS3SinkConnector.class.getName());
        config.put("aws.access.key.id", S3_ACCESS_KEY_ID);
        config.put("aws.secret.access.key", S3_SECRET_ACCESS_KEY);
        config.put("aws.s3.endpoint", s3Endpoint);
        config.put("aws.s3.bucket.name", TEST_BUCKET_NAME);
        config.put("aws.s3.prefix", s3Prefix);
        config.put("topics", topicName);
        config.put("key.converter.schema.registry.url", kafkaManager.getSchemaRegistryUrl());
        config.put("value.converter.schema.registry.url", kafkaManager.getSchemaRegistryUrl());
        config.put("tasks.max", "1");
        return config;
    }

    private String getBlobName(final String topicName, final int partition, final int startOffset,
            final CompressionType compression) {
        return String.format("%s%s-%d-%020d%s", s3Prefix, topicName, partition, startOffset, compression.extension());

    }
}
