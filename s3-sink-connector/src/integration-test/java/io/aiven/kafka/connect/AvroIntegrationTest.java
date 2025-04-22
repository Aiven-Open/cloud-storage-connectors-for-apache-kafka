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
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.connector.Connector;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.integration.AbstractKafkaIntegrationBase;
import io.aiven.kafka.connect.common.integration.KafkaManager;
import io.aiven.kafka.connect.common.source.input.AvroTestDataFixture;
import io.aiven.kafka.connect.common.source.input.JsonTestDataFixture;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector;
import io.aiven.kafka.connect.s3.testutils.BucketAccessor;

import com.amazonaws.services.s3.AmazonS3;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
final class AvroIntegrationTest extends AbstractKafkaIntegrationBase {
    private static final KafkaProducer<String, GenericRecord> NULL_PRODUCER = null;
    private static final String S3_ACCESS_KEY_ID = "test-key-id0";
    private static final String S3_SECRET_ACCESS_KEY = "test_secret_key0";
    private static final String TEST_BUCKET_NAME = "test-bucket0";

    private static final String COMMON_PREFIX = "s3-connector-for-apache-kafka-test-";

    private static String s3Endpoint;
    private static String s3Prefix;
    private static BucketAccessor testBucketAccessor;

    private KafkaManager kafkaManager;

    @Container
    public static final LocalStackContainer LOCALSTACK = S3IntegrationHelper.createS3Container();

    private KafkaProducer<String, GenericRecord> producer;

    private Class<? extends Connector> getConnectorClass() {
        return AivenKafkaConnectS3SinkConnector.class;
    }

    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
        s3Prefix = COMMON_PREFIX + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";

        final AmazonS3 s3Client = S3IntegrationHelper.createS3Client(LOCALSTACK);
        s3Endpoint = LOCALSTACK.getEndpoint().toString();
        testBucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET_NAME);
    }

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException, IOException {
        testBucketAccessor.createBucket();
        kafkaManager = setupKafka(getConnectorClass());
    }

    @AfterEach
    void tearDown() {
        if (producer != null) {
            producer.close();
            producer = NULL_PRODUCER;
        }
        testBucketAccessor.removeBucket();
    }

    private static Stream<Arguments> compressionAndCodecTestParameters() {
        final List<Arguments> lst = new ArrayList<>();

        // String[] codecs = {"null", "deflate", "snappy", "bzip2", "xz", "zstandard"};
        // for (String codec : codecs) {
        // for (CompressionType compression : CompressionType.values()) {
        // lst.add(Arguments.of(codec, compression));
        // }
        // }
        lst.add(Arguments.of("bzip2", CompressionType.NONE));
        lst.add(Arguments.of("deflate", CompressionType.NONE));
        lst.add(Arguments.of("null", CompressionType.NONE));
        lst.add(Arguments.of("snappy", CompressionType.GZIP));
        lst.add(Arguments.of("zstandard", CompressionType.NONE));

        return lst.stream();
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    @ParameterizedTest
    @MethodSource("compressionAndCodecTestParameters")
    void avroOutput(final String avroCodec, final CompressionType compression)
            throws ExecutionException, InterruptedException, IOException {
        final var topicName = getTopic() + "-" + avroCodec + "-" + compression;
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final Map<String, String> connectorConfig = awsSpecificConfig(
                basicConnectorConfig(getConnectorName(getConnectorClass())), topicName);
        connectorConfig.put("file.compression.type", compression.name());
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("format.output.type", "avro");
        connectorConfig.put("avro.codec", avroCodec);
        kafkaManager.configureConnector(getConnectorName(getConnectorClass()), connectorConfig);

        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final int partitionCount = 4;
        final int recordCountPerPartition = 10;

        assertThat(testBucketAccessor.listObjects()).isEmpty();

        final List<GenericRecord> expectedGenericRecords = produceRecords(recordCountPerPartition, partitionCount,
                topicName);

        // get list of expected blobs
        final String[] expectedBlobs = { getAvroBlobName(topicName, 0, 0, compression),
                getAvroBlobName(topicName, 1, 0, compression), getAvroBlobName(topicName, 2, 0, compression),
                getAvroBlobName(topicName, 3, 0, compression) };

        waitForStorage(timeout, testBucketAccessor::listObjects, expectedBlobs);

        // extract all the actual records.
        final List<GenericRecord> actualValues = new ArrayList<>();
        final Function<GenericRecord, String> idMapper = mapF("id");
        final Function<GenericRecord, String> messageMapper = mapF("message");

        for (final String blobName : expectedBlobs) {
            for (final GenericRecord r : AvroTestDataFixture
                    .readAvroRecords(testBucketAccessor.readBytes(blobName, compression))) {
                final GenericRecord value = (GenericRecord) r.get("value");
                final String key = r.get("key").toString();
                assertThat(key).isEqualTo("key-" + idMapper.apply(value));
                assertThat(messageMapper.apply(value)).endsWith(idMapper.apply(value));
                actualValues.add(value);
            }
        }

        List<String> values = actualValues.stream().map(mapF("message")).collect(Collectors.toList());
        String[] expected = expectedGenericRecords.stream()
                .map(mapF("message"))
                .collect(Collectors.toList())
                .toArray(new String[0]);

        assertThat(values).containsExactlyInAnyOrder(expected);

        values = actualValues.stream().map(mapF("id")).collect(Collectors.toList());
        expected = expectedGenericRecords.stream().map(mapF("id")).collect(Collectors.toList()).toArray(new String[0]);

        assertThat(values).containsExactlyInAnyOrder(expected);
    }

    private Function<GenericRecord, String> mapF(final String key) {
        return r -> r.get(key).toString();
    }

    @Test
    void jsonlAvroOutputTest() throws ExecutionException, InterruptedException, IOException {
        final var topicName = getTopic();
        kafkaManager.createTopic(topicName);
        producer = newProducer();

        final Map<String, String> connectorConfig = awsSpecificConfig(
                basicConnectorConfig(getConnectorName(getConnectorClass())), topicName);
        final CompressionType compression = CompressionType.NONE;
        final String contentType = "jsonl";
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        connectorConfig.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        connectorConfig.put("value.converter.schemas.enable", "false");
        connectorConfig.put("file.compression.type", compression.name());
        connectorConfig.put("format.output.type", contentType);

        kafkaManager.configureConnector(getConnectorName(getConnectorClass()), connectorConfig);
        final Duration timeout = Duration.ofSeconds(getOffsetFlushInterval().toSeconds() * 2);
        final int partitionCount = 4;
        final int recordCountPerPartition = 10;

        assertThat(testBucketAccessor.listObjects()).isEmpty();

        final List<GenericRecord> expectedGenericRecords = produceRecords(recordCountPerPartition, partitionCount,
                topicName);

        // get list of expected blobs
        final String[] expectedBlobs = { getBlobName(topicName, 0, 0, compression),
                getBlobName(topicName, 1, 0, compression), getBlobName(topicName, 2, 0, compression),
                getBlobName(topicName, 3, 0, compression) };

        // wait for them to show up.
        waitForStorage(timeout, testBucketAccessor::listObjects, expectedBlobs);

        // extract all the actual records.
        final List<String> actualValues = new ArrayList<>();

        for (final String blobName : expectedBlobs) {
            for (final JsonNode node : JsonTestDataFixture
                    .readJsonRecords(testBucketAccessor.readLines(blobName, compression))) {
                actualValues.add(node.findValue("message").asText());
            }
        }

        final String[] expected = expectedGenericRecords.stream()
                .map(mapF("message"))
                .collect(Collectors.toList())
                .toArray(new String[0]);

        assertThat(actualValues).containsExactlyInAnyOrder(expected);
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    private List<GenericRecord> produceRecords(final int recordCountPerPartition, final int partitionCount,
            final String topicName) throws ExecutionException, InterruptedException {
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final List<GenericRecord> genericRecords = AvroTestDataFixture
                .generateAvroRecords(recordCountPerPartition * partitionCount);
        int cnt = 0;
        for (final GenericRecord value : genericRecords) {
            final int partition = cnt % partitionCount;
            final String key = "key-" + cnt++;
            sendFutures.add(producer.send(new ProducerRecord<>(topicName, partition, key, value)));
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
