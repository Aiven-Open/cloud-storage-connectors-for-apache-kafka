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
import static org.awaitility.Awaitility.await;


import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import io.aiven.kafka.connect.common.integration.AbstractKafkaIntegrationBase;
import io.aiven.kafka.connect.common.integration.KafkaManager;
import io.aiven.kafka.connect.common.source.input.AvroTestDataFixture;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector;
import io.aiven.kafka.connect.s3.testutils.BucketAccessor;

import com.amazonaws.services.s3.AmazonS3;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.connect.connector.Connector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
final class AvroIntegrationTest extends AbstractKafkaIntegrationBase {
    private static final String S3_ACCESS_KEY_ID = "test-key-id0";
    private static final String S3_SECRET_ACCESS_KEY = "test_secret_key0";
    private static final String TEST_BUCKET_NAME = "test-bucket0";

    private static final String COMMON_PREFIX = "s3-connector-for-apache-kafka-test-";
    private static final int OFFSET_FLUSH_INTERVAL_MS = 5000;

    private static String s3Endpoint;
    private static String s3Prefix;
    private static BucketAccessor testBucketAccessor;

    private KafkaManager kafkaManager;

    @Container
    public static final LocalStackContainer LOCALSTACK = IntegrationBase.createS3Container();

    private KafkaProducer<String, GenericRecord> producer;
    private ConnectRunner connectRunner;


    private Class<? extends Connector> getConnectorClass() {
        return AivenKafkaConnectS3SinkConnector.class;
    }

    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
        s3Prefix = COMMON_PREFIX + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";

        final AmazonS3 s3Client = IntegrationBase.createS3Client(LOCALSTACK);
        s3Endpoint = LOCALSTACK.getEndpoint().toString();
        testBucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET_NAME);
        testBucketAccessor.createBucket();
    }

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException, IOException {
        kafkaManager = setupKafka(getConnectorClass());
        kafkaManager.createTopic(getTopic());
        producer = newProducer();
    }

    @AfterEach
    void tearDown() {
        kafkaManager.stop();
        producer.close();
    }

    private static Stream<Arguments> compressionAndCodecTestParameters() {
        return Stream.of(Arguments.of("bzip2", "none"), Arguments.of("deflate", "none"), Arguments.of("null", "none"),
                Arguments.of("snappy", "gzip"), // single test for codec and compression when both set.
                Arguments.of("zstandard", "none"));
    }

    @ParameterizedTest
    @MethodSource("compressionAndCodecTestParameters")
    void avroOutput(final String avroCodec, final String compression, final TestInfo testInfo)
            throws ExecutionException, InterruptedException, IOException {
        final var topicName = getTopic();

        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(getConnectorName(getConnectorClass())), topicName);
        connectorConfig.put("file.compression.type", compression);
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("format.output.type", "avro");
        connectorConfig.put("avro.codec", avroCodec);
        kafkaManager.configureConnector(getConnectorName(getConnectorClass()), connectorConfig);

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final int partitionCount = 4;
        final int recordCountPerPartition = 10;
        final List<GenericRecord> expected = produceRecords(recordCountPerPartition, partitionCount, topicName);

        //waitForConnectToFinishProcessing();

        final List<String> expectedBlobs = Arrays.asList(getAvroBlobName(topicName, 0, 0, compression),
                getAvroBlobName(topicName, 1, 0, compression), getAvroBlobName(topicName, 2, 0, compression),
                getAvroBlobName(topicName, 3, 0, compression));

        await().atMost(timeout).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            List<String> actual = testBucketAccessor.listObjects();
            assertThat(actual.containsAll(expectedBlobs));
        });

        final List<GenericRecord> actual = new ArrayList<>();

        for (final String blobName : expectedBlobs) {
            List<String> objects = testBucketAccessor.listObjects();
            final String s = testBucketAccessor.isReady(blobName);

            final byte[] blobBytes = testBucketAccessor.readBytes(blobName, compression);
            try (SeekableInput sin = new SeekableByteArrayInput(blobBytes)) { // NOPMD AvoidInstantiatingObjectsInLoops
                final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(); // NOPMD
                                                                                                  // AvoidInstantiatingObjectsInLoops
                try (DataFileReader<GenericRecord> reader = new DataFileReader<>(sin, datumReader)) { // NOPMD
                                                                                                      // AvoidInstantiatingObjectsInLoops
                    final List<GenericRecord> items = new ArrayList<>(); // NOPMD AvoidInstantiatingObjectsInLoops
                    reader.forEach(actual::add);
                }
            }
        }
        assertThat(actual).containsExactlyInAnyOrder(expected.toArray(new GenericRecord[0]));
    }

    @Disabled
    @Test
    void jsonlAvroOutputTest(final TestInfo testInfo) throws ExecutionException, InterruptedException, IOException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(getConnectorName(getConnectorClass())), topicName);
        final String compression = "none";
        final String contentType = "jsonl";
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        connectorConfig.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        connectorConfig.put("value.converter.schemas.enable", "false");
        connectorConfig.put("file.compression.type", compression);
        connectorConfig.put("format.output.type", contentType);
        connectRunner.createConnector(connectorConfig);

        final int recordCountPerPartition = 10;
        final int partitionCount = 4;

        produceRecords(recordCountPerPartition, partitionCount, topicName);
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

                final String blobName = getBlobName(topicName, partition, 0, "none");
                final String expectedLine = "{\"value\":" + value + ",\"key\":\"" + key + "\"}";

                assertThat(blobContents.get(blobName).get(i)).isEqualTo(expectedLine);
            }
        }
    }

    private void waitForConnectToFinishProcessing() throws InterruptedException {
        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);
    }

    private List<GenericRecord>  produceRecords(final int recordCountPerPartition, int partitionCount, final String topicName)
            throws ExecutionException, InterruptedException {
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final List<GenericRecord> genericRecords = AvroTestDataFixture.generateAvroRecords(recordCountPerPartition * partitionCount);
        int cnt = 0;
        for (GenericRecord value : genericRecords) {
            int partition = cnt % partitionCount;
            final String key = "key-" + cnt++;
            sendFutures.add(sendMessageAsync(producer, topicName, partition, key, value));
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }
        return genericRecords;
    }

    private KafkaProducer<String, GenericRecord> newProducer() {
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

    private String getAvroBlobName(final String topicName, final int partition, final int startOffset,
            final String compression) {
        final String result = String.format("%s%s-%d-%020d.avro", s3Prefix, topicName, partition, startOffset);
        return result + CompressionType.forName(compression).extension();
    }

    // WARN: different from GCS
    private String getBlobName(final String topicName, final int partition, final int startOffset,
            final String compression) {
        final String result = String.format("%s%s-%d-%020d", s3Prefix, topicName, partition, startOffset);
        return result + CompressionType.forName(compression).extension();
    }
}
