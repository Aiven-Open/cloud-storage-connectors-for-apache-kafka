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

import static io.aiven.kafka.connect.common.config.SchemaRegistryFragment.AVRO_VALUE_SERIALIZER;
import static io.aiven.kafka.connect.common.config.SchemaRegistryFragment.INPUT_FORMAT_KEY;
import static io.aiven.kafka.connect.common.config.SchemaRegistryFragment.SCHEMA_REGISTRY_URL;
import static io.aiven.kafka.connect.common.config.SchemaRegistryFragment.VALUE_CONVERTER_SCHEMA_REGISTRY_URL;
import static io.aiven.kafka.connect.common.config.SourceConfigFragment.TARGET_TOPICS;
import static io.aiven.kafka.connect.common.config.SourceConfigFragment.TARGET_TOPIC_PARTITIONS;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_ENDPOINT_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_PREFIX_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG;
import static io.aiven.kafka.connect.s3.source.S3SourceTask.OBJECT_KEY;
import static io.aiven.kafka.connect.s3.source.utils.OffsetManager.SEPARATOR;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.s3.source.testutils.BucketAccessor;
import io.aiven.kafka.connect.s3.source.testutils.ContentUtils;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.platform.commons.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Testcontainers
@SuppressWarnings("PMD.ExcessiveImports")
final class IntegrationTest implements IntegrationBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationTest.class);
    private static final String CONNECTOR_NAME = "aiven-s3-source-connector";
    private static final String COMMON_PREFIX = "s3-source-connector-for-apache-kafka-test-";
    private static final int OFFSET_FLUSH_INTERVAL_MS = 500;

    private static final String S3_ACCESS_KEY_ID = "test-key-id0";
    private static final String S3_SECRET_ACCESS_KEY = "test_secret_key0";

    private static final String VALUE_CONVERTER_KEY = "value.converter";

    private static final String TEST_BUCKET_NAME = "test-bucket0";

    private static String s3Endpoint;
    private static String s3Prefix;
    private static BucketAccessor testBucketAccessor;

    @Container
    public static final LocalStackContainer LOCALSTACK = IntegrationBase.createS3Container();
    private SchemaRegistryContainer schemaRegistry;

    private AdminClient adminClient;
    private ConnectRunner connectRunner;

    private static S3Client s3Client;

    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
        s3Prefix = COMMON_PREFIX + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";

        s3Client = IntegrationBase.createS3Client(LOCALSTACK);
        s3Endpoint = LOCALSTACK.getEndpoint().toString();
        testBucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET_NAME);

        final Path pluginDir = IntegrationBase.getPluginDir();
        IntegrationBase.extractConnectorPlugin(pluginDir);
    }

    @BeforeEach
    void setUp(final TestInfo testInfo) throws Exception {
        testBucketAccessor.createBucket();

        connectRunner = new ConnectRunner(OFFSET_FLUSH_INTERVAL_MS);
        final List<Integer> ports = IntegrationBase.getKafkaListenerPorts();
        final int localListenerPort = ports.get(0);
        final int containerListenerPort = ports.get(1);
        connectRunner.startConnectCluster(CONNECTOR_NAME, localListenerPort, containerListenerPort);

        adminClient = newAdminClient(connectRunner.getBootstrapServers());
        final String topicName = IntegrationBase.topicName(testInfo);
        final var topics = List.of(topicName);
        IntegrationBase.createTopics(adminClient, topics);

        // This should be done after the process listening the port is already started by host but
        // before the container that will access it is started.
        org.testcontainers.Testcontainers.exposeHostPorts(containerListenerPort);
        schemaRegistry = new SchemaRegistryContainer("host.testcontainers.internal:" + containerListenerPort);
        schemaRegistry.start();
        IntegrationBase.waitForRunningContainer(schemaRegistry);
    }

    @AfterEach
    void tearDown() {
        adminClient.close();
        connectRunner.deleteConnector(CONNECTOR_NAME);
        connectRunner.stopConnectCluster();
        schemaRegistry.stop();
        testBucketAccessor.removeBucket();
    }

    @Test
    void bytesTest(final TestInfo testInfo) {
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> connectorConfig = getConfig(CONNECTOR_NAME, topicName, 2);

        connectorConfig.put(INPUT_FORMAT_KEY, InputFormat.BYTES.getValue());
        connectRunner.configureConnector(CONNECTOR_NAME, connectorConfig);

        final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
        final String testData2 = "Hello, Kafka Connect S3 Source! object 2";

        final List<String> offsetKeys = new ArrayList<>();

        // write 2 objects to s3
        offsetKeys.add(writeToS3(topicName, testData1.getBytes(StandardCharsets.UTF_8), "00000"));
        offsetKeys.add(writeToS3(topicName, testData2.getBytes(StandardCharsets.UTF_8), "00000"));
        offsetKeys.add(writeToS3(topicName, testData1.getBytes(StandardCharsets.UTF_8), "00001"));
        offsetKeys.add(writeToS3(topicName, testData2.getBytes(StandardCharsets.UTF_8), "00001"));
        offsetKeys.add(writeToS3(topicName, new byte[0], "00003"));

        assertThat(testBucketAccessor.listObjects()).hasSize(5);

        // Poll messages from the Kafka topic and verify the consumed data
        final List<String> records = IntegrationBase.consumeByteMessages(topicName, 4,
                connectRunner.getBootstrapServers());

        // Verify that the correct data is read from the S3 bucket and pushed to Kafka
        assertThat(records).containsOnly(testData1, testData2);

        // Verify offset positions
        final Map<String, Object> expectedOffsetRecords = offsetKeys.subList(0, offsetKeys.size() - 1)
                .stream()
                .collect(Collectors.toMap(Function.identity(), s -> 1));
        verifyOffsetPositions(expectedOffsetRecords, connectRunner.getBootstrapServers());
    }

    @Test
    void avroTest(final TestInfo testInfo) throws IOException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> connectorConfig = getAvroConfig(topicName, InputFormat.AVRO);

        connectRunner.configureConnector(CONNECTOR_NAME, connectorConfig);

        // Define Avro schema
        final String schemaJson = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestRecord\",\n"
                + "  \"fields\": [\n" + "    {\"name\": \"message\", \"type\": \"string\"},\n"
                + "    {\"name\": \"id\", \"type\": \"int\"}\n" + "  ]\n" + "}";
        final Schema.Parser parser = new Schema.Parser();
        final Schema schema = parser.parse(schemaJson);

        final int numOfRecsFactor = 5000;

        final byte[] outputStream1 = generateNextAvroMessagesStartingFromId(1, numOfRecsFactor, schema);
        final byte[] outputStream2 = generateNextAvroMessagesStartingFromId(numOfRecsFactor + 1, numOfRecsFactor,
                schema);
        final byte[] outputStream3 = generateNextAvroMessagesStartingFromId(2 * numOfRecsFactor + 1, numOfRecsFactor,
                schema);
        final byte[] outputStream4 = generateNextAvroMessagesStartingFromId(3 * numOfRecsFactor + 1, numOfRecsFactor,
                schema);
        final byte[] outputStream5 = generateNextAvroMessagesStartingFromId(4 * numOfRecsFactor + 1, numOfRecsFactor,
                schema);

        final Set<String> offsetKeys = new HashSet<>();

        offsetKeys.add(writeToS3(topicName, outputStream1, "00001"));
        offsetKeys.add(writeToS3(topicName, outputStream2, "00001"));

        offsetKeys.add(writeToS3(topicName, outputStream3, "00002"));
        offsetKeys.add(writeToS3(topicName, outputStream4, "00002"));
        offsetKeys.add(writeToS3(topicName, outputStream5, "00002"));

        assertThat(testBucketAccessor.listObjects()).hasSize(5);

        // Poll Avro messages from the Kafka topic and deserialize them
        final List<GenericRecord> records = IntegrationBase.consumeAvroMessages(topicName, numOfRecsFactor * 5,
                connectRunner.getBootstrapServers(), schemaRegistry.getSchemaRegistryUrl()); // Ensure this method
                                                                                             // deserializes Avro

        // Verify that the correct data is read from the S3 bucket and pushed to Kafka
        assertThat(records).map(record -> entry(record.get("id"), String.valueOf(record.get("message"))))
                .contains(entry(1, "Hello, Kafka Connect S3 Source! object 1"),
                        entry(2, "Hello, Kafka Connect S3 Source! object 2"),
                        entry(numOfRecsFactor, "Hello, Kafka Connect S3 Source! object " + numOfRecsFactor),
                        entry(2 * numOfRecsFactor, "Hello, Kafka Connect S3 Source! object " + (2 * numOfRecsFactor)),
                        entry(3 * numOfRecsFactor, "Hello, Kafka Connect S3 Source! object " + (3 * numOfRecsFactor)),
                        entry(4 * numOfRecsFactor, "Hello, Kafka Connect S3 Source! object " + (4 * numOfRecsFactor)),
                        entry(5 * numOfRecsFactor, "Hello, Kafka Connect S3 Source! object " + (5 * numOfRecsFactor)));

        verifyOffsetPositions(offsetKeys.stream().collect(Collectors.toMap(Function.identity(), s -> numOfRecsFactor)),
                connectRunner.getBootstrapServers());
    }

    @Test
    void parquetTest(final TestInfo testInfo) throws IOException {
        final var topicName = IntegrationBase.topicName(testInfo);

        final String partition = "00000";
        final String fileName = addPrefixOrDefault("") + topicName + "-" + partition + "-" + System.currentTimeMillis()
                + ".txt";
        final String name = "testuser";

        final Map<String, String> connectorConfig = getAvroConfig(topicName, InputFormat.PARQUET);
        connectRunner.configureConnector(CONNECTOR_NAME, connectorConfig);
        final Path path = ContentUtils.getTmpFilePath(name);

        try {
            s3Client.putObject(PutObjectRequest.builder().bucket(TEST_BUCKET_NAME).key(fileName).build(), path);
        } catch (final Exception e) { // NOPMD broad exception caught
            LOGGER.error("Error in reading file {}", e.getMessage(), e);
        } finally {
            Files.delete(path);
        }

        final List<GenericRecord> records = IntegrationBase.consumeAvroMessages(topicName, 100,
                connectRunner.getBootstrapServers(), schemaRegistry.getSchemaRegistryUrl());
        final List<String> expectedRecordNames = IntStream.range(0, 100)
                .mapToObj(i -> name + i)
                .collect(Collectors.toList());
        assertThat(records).extracting(record -> record.get("name").toString())
                .containsExactlyInAnyOrderElementsOf(expectedRecordNames);
    }

    private Map<String, String> getAvroConfig(final String topicName, final InputFormat inputFormat) {
        final Map<String, String> connectorConfig = getConfig(CONNECTOR_NAME, topicName, 4);
        connectorConfig.put(INPUT_FORMAT_KEY, inputFormat.getValue());
        connectorConfig.put(SCHEMA_REGISTRY_URL, schemaRegistry.getSchemaRegistryUrl());
        connectorConfig.put(VALUE_CONVERTER_KEY, "io.confluent.connect.avro.AvroConverter");
        connectorConfig.put(VALUE_CONVERTER_SCHEMA_REGISTRY_URL, schemaRegistry.getSchemaRegistryUrl());
        connectorConfig.put(AVRO_VALUE_SERIALIZER, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        return connectorConfig;
    }

    @Test
    void jsonTest(final TestInfo testInfo) {
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> connectorConfig = getConfig(CONNECTOR_NAME, topicName, 1);
        connectorConfig.put(INPUT_FORMAT_KEY, InputFormat.JSONL.getValue());
        connectorConfig.put(VALUE_CONVERTER_KEY, "org.apache.kafka.connect.json.JsonConverter");

        connectRunner.configureConnector(CONNECTOR_NAME, connectorConfig);
        final String testMessage = "This is a test ";
        final StringBuilder jsonBuilder = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            final String jsonContent = "{\"message\": \"" + testMessage + "\", \"id\":\"" + i + "\"}";
            jsonBuilder.append(jsonContent).append("\n"); // NOPMD
        }
        final byte[] jsonBytes = jsonBuilder.toString().getBytes(StandardCharsets.UTF_8);

        final String offsetKey = writeToS3(topicName, jsonBytes, "00001");

        // Poll Json messages from the Kafka topic and deserialize them
        final List<JsonNode> records = IntegrationBase.consumeJsonMessages(topicName, 500,
                connectRunner.getBootstrapServers());

        assertThat(records).map(jsonNode -> jsonNode.get("payload")).anySatisfy(jsonNode -> {
            assertThat(jsonNode.get("message").asText()).contains(testMessage);
            assertThat(jsonNode.get("id").asText()).contains("1");
        });

        // Verify offset positions
        verifyOffsetPositions(Map.of(offsetKey, 500), connectRunner.getBootstrapServers());
    }

    private static byte[] generateNextAvroMessagesStartingFromId(final int messageId, final int noOfAvroRecs,
            final Schema schema) throws IOException {
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            dataFileWriter.create(schema, outputStream);
            for (int i = messageId; i < messageId + noOfAvroRecs; i++) {
                final GenericRecord avroRecord = new GenericData.Record(schema); // NOPMD
                avroRecord.put("message", "Hello, Kafka Connect S3 Source! object " + i);
                avroRecord.put("id", i);
                dataFileWriter.append(avroRecord);
            }

            dataFileWriter.flush();
            return outputStream.toByteArray();
        }
    }

    private static String writeToS3(final String topicName, final byte[] testDataBytes, final String partitionId) {
        final String objectKey = addPrefixOrDefault("") + topicName + "-" + partitionId + "-"
                + System.currentTimeMillis() + ".txt";
        final PutObjectRequest request = PutObjectRequest.builder().bucket(TEST_BUCKET_NAME).key(objectKey).build();
        s3Client.putObject(request, RequestBody.fromBytes(testDataBytes));
        return OBJECT_KEY + SEPARATOR + objectKey;
    }

    private static String addPrefixOrDefault(final String defaultValue) {
        return StringUtils.isNotBlank(s3Prefix) ? s3Prefix : defaultValue;
    }

    private Map<String, String> getConfig(final String connectorName, final String topics, final int maxTasks) {
        final Map<String, String> config = new HashMap<>(basicS3ConnectorConfig());
        config.put("name", connectorName);
        config.put(TARGET_TOPICS, topics);
        config.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put(VALUE_CONVERTER_KEY, "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put("tasks.max", String.valueOf(maxTasks));
        return config;
    }

    private static Map<String, String> basicS3ConnectorConfig() {
        final Map<String, String> config = new HashMap<>();
        config.put("connector.class", AivenKafkaConnectS3SourceConnector.class.getName());
        config.put(AWS_ACCESS_KEY_ID_CONFIG, S3_ACCESS_KEY_ID);
        config.put(AWS_SECRET_ACCESS_KEY_CONFIG, S3_SECRET_ACCESS_KEY);
        config.put(AWS_S3_ENDPOINT_CONFIG, s3Endpoint);
        config.put(AWS_S3_BUCKET_NAME_CONFIG, TEST_BUCKET_NAME);
        config.put(AWS_S3_PREFIX_CONFIG, s3Prefix);
        config.put(TARGET_TOPIC_PARTITIONS, "0,1");
        return config;
    }

    static void verifyOffsetPositions(final Map<String, Object> expectedRecords, final String bootstrapServers) {
        final Properties consumerProperties = IntegrationBase.getConsumerProperties(bootstrapServers,
                ByteArrayDeserializer.class, ByteArrayDeserializer.class);

        final Map<String, Object> offsetRecs = new HashMap<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(Collections.singletonList("connect-offset-topic-" + CONNECTOR_NAME));
            await().atMost(Duration.ofMinutes(1)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
                offsetRecs.putAll(IntegrationBase.consumeOffsetMessages(consumer));
                assertThat(offsetRecs).containsExactlyInAnyOrderEntriesOf(expectedRecords);
            });
        }
    }
}
