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

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.AWS_ACCESS_KEY_ID_CONFIG;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.AWS_S3_BUCKET_NAME_CONFIG;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.AWS_S3_ENDPOINT_CONFIG;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.AWS_S3_PREFIX_CONFIG;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.AWS_SECRET_ACCESS_KEY_CONFIG;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.EXPECTED_MAX_MESSAGE_BYTES;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.INPUT_FORMAT_KEY;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.MAX_POLL_RECORDS;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.SCHEMA_REGISTRY_URL;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.TARGET_TOPICS;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.TARGET_TOPIC_PARTITIONS;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.VALUE_CONVERTER_SCHEMA_REGISTRY_URL;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.VALUE_SERIALIZER;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;

import io.aiven.kafka.connect.s3.source.input.InputFormat;
import io.aiven.kafka.connect.s3.source.testutils.BucketAccessor;
import io.aiven.kafka.connect.s3.source.testutils.ContentUtils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
final class IntegrationTest implements IntegrationBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationTest.class);
    private static final String CONNECTOR_NAME = "aiven-s3-source-connector";
    private static final String COMMON_PREFIX = "s3-source-connector-for-apache-kafka-test-";
    private static final int OFFSET_FLUSH_INTERVAL_MS = 500;

    private static final String S3_ACCESS_KEY_ID = "test-key-id0";
    private static final String S3_SECRET_ACCESS_KEY = "test_secret_key0";

    private static final String TEST_BUCKET_NAME = "test-bucket0";

    private static String s3Endpoint;
    private static String s3Prefix;
    private static BucketAccessor testBucketAccessor;

    private static File pluginDir;

    @Container
    public static final LocalStackContainer LOCALSTACK = IntegrationBase.createS3Container();
    private SchemaRegistryContainer schemaRegistry;

    private AdminClient adminClient;
    private ConnectRunner connectRunner;

    private static AmazonS3 s3Client;

    private String topicName;

    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
        s3Prefix = COMMON_PREFIX + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";

        s3Client = IntegrationBase.createS3Client(LOCALSTACK);
        s3Endpoint = LOCALSTACK.getEndpoint().toString();
        testBucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET_NAME);

        pluginDir = IntegrationBase.getPluginDir();
        IntegrationBase.extractConnectorPlugin(pluginDir);
    }

    @BeforeEach
    void setUp(final TestInfo testInfo) throws Exception {
        testBucketAccessor.createBucket();

        connectRunner = new ConnectRunner(OFFSET_FLUSH_INTERVAL_MS);
        final int localListenerPort = IntegrationBase.getRandomPort();
        final int containerListenerPort = IntegrationBase.getRandomPort();
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
    void bytesTest(final TestInfo testInfo) throws ExecutionException, InterruptedException, IOException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> connectorConfig = getConfig(basicConnectorConfig(CONNECTOR_NAME), topicName);

        connectorConfig.put(INPUT_FORMAT_KEY, InputFormat.BYTES.getValue());
        connectRunner.configureConnector(CONNECTOR_NAME, connectorConfig);

        final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
        final String testData2 = "Hello, Kafka Connect S3 Source! object 2";

        // write 2 objects to s3
        writeToS3(topicName, testData1.getBytes(StandardCharsets.UTF_8), "00000");
        writeToS3(topicName, testData2.getBytes(StandardCharsets.UTF_8), "00000");
        writeToS3(topicName, testData1.getBytes(StandardCharsets.UTF_8), "00001");
        writeToS3(topicName, testData2.getBytes(StandardCharsets.UTF_8), "00001");
        writeToS3(topicName, new byte[0], "00003"); // this should be ignored.

        final List<String> objects = testBucketAccessor.listObjects();
        assertThat(objects.size()).isEqualTo(5);

        // Verify that the connector is correctly set up
        assertThat(connectorConfig.get("name")).isEqualTo(CONNECTOR_NAME);

        // Poll messages from the Kafka topic and verify the consumed data
        final List<String> records = IntegrationBase.consumeMessages(topicName, 4, connectRunner.getBootstrapServers());

        // Verify that the correct data is read from the S3 bucket and pushed to Kafka
        assertThat(records).contains(testData1).contains(testData2);
    }

    @Test
    void bytesTestBasedOnMaxMessageBytes(final TestInfo testInfo)
            throws ExecutionException, InterruptedException, IOException {
        final String testData = "AABBCCDDEE";
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> connectorConfig = getConfig(basicConnectorConfig(CONNECTOR_NAME), topicName);
        connectorConfig.put(INPUT_FORMAT_KEY, InputFormat.BYTES.getValue());
        connectorConfig.put(EXPECTED_MAX_MESSAGE_BYTES, "2"); // For above test data of 10 bytes length, with 2 bytes
                                                              // each
        // in source record, we expect 5 records.
        connectorConfig.put(MAX_POLL_RECORDS, "2"); // In 3 polls all the 5 records should be processed

        connectRunner.configureConnector(CONNECTOR_NAME, connectorConfig);

        writeToS3(topicName, testData.getBytes(StandardCharsets.UTF_8), "00000");

        // Poll messages from the Kafka topic and verify the consumed data
        final List<String> records = IntegrationBase.consumeMessages(topicName, 5, connectRunner.getBootstrapServers());

        // Verify that the correct data is read from the S3 bucket and pushed to Kafka
        assertThat(records.size()).isEqualTo(5);
        assertThat(records.get(0)).isEqualTo("AA");
        assertThat(records.get(1)).isEqualTo("BB");
        assertThat(records.get(2)).isEqualTo("CC");
        assertThat(records.get(3)).isEqualTo("DD");
        assertThat(records.get(4)).isEqualTo("EE");
    }

    @Test
    void avroTest(final TestInfo testInfo) throws ExecutionException, InterruptedException, IOException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> connectorConfig = getConfig(basicConnectorConfig(CONNECTOR_NAME), topicName);
        connectorConfig.put(INPUT_FORMAT_KEY, InputFormat.AVRO.getValue());
        connectorConfig.put(SCHEMA_REGISTRY_URL, schemaRegistry.getSchemaRegistryUrl());
        connectorConfig.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        connectorConfig.put(VALUE_CONVERTER_SCHEMA_REGISTRY_URL, schemaRegistry.getSchemaRegistryUrl());
        connectorConfig.put(VALUE_SERIALIZER, "io.confluent.kafka.serializers.KafkaAvroSerializer");

        connectRunner.configureConnector(CONNECTOR_NAME, connectorConfig);

        // Define Avro schema
        final String schemaJson = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestRecord\",\n"
                + "  \"fields\": [\n" + "    {\"name\": \"message\", \"type\": \"string\"},\n"
                + "    {\"name\": \"id\", \"type\": \"int\"}\n" + "  ]\n" + "}";
        final Schema.Parser parser = new Schema.Parser();
        final Schema schema = parser.parse(schemaJson);

        final ByteArrayOutputStream outputStream1 = getAvroRecord(schema, 1, 100);
        final ByteArrayOutputStream outputStream2 = getAvroRecord(schema, 2, 100);

        writeToS3(topicName, outputStream1.toByteArray(), "00001");
        writeToS3(topicName, outputStream2.toByteArray(), "00001");

        writeToS3(topicName, outputStream1.toByteArray(), "00002");
        writeToS3(topicName, outputStream2.toByteArray(), "00002");
        writeToS3(topicName, outputStream2.toByteArray(), "00002");

        final List<String> objects = testBucketAccessor.listObjects();
        assertThat(objects.size()).isEqualTo(5);

        // Verify that the connector is correctly set up
        assertThat(connectorConfig.get("name")).isEqualTo(CONNECTOR_NAME);

        // Poll Avro messages from the Kafka topic and deserialize them
        final List<GenericRecord> records = IntegrationBase.consumeAvroMessages(topicName, 500,
                connectRunner.getBootstrapServers(), schemaRegistry.getSchemaRegistryUrl()); // Ensure this method
                                                                                             // deserializes Avro

        // Verify that the correct data is read from the S3 bucket and pushed to Kafka
        assertThat(records).extracting(record -> record.get("message").toString())
                .contains("Hello, Kafka Connect S3 Source! object 1")
                .contains("Hello, Kafka Connect S3 Source! object 2");
        assertThat(records).extracting(record -> record.get("id").toString()).contains("1").contains("2");
    }

    @Test
    void parquetTest(final TestInfo testInfo) throws ExecutionException, InterruptedException, IOException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> connectorConfig = getConfig(basicConnectorConfig(CONNECTOR_NAME), topicName);
        connectorConfig.put(INPUT_FORMAT_KEY, InputFormat.PARQUET.getValue());
        connectorConfig.put(SCHEMA_REGISTRY_URL, schemaRegistry.getSchemaRegistryUrl());
        connectorConfig.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        connectorConfig.put("value.converter.schema.registry.url", schemaRegistry.getSchemaRegistryUrl());
        connectorConfig.put(VALUE_SERIALIZER, "io.confluent.kafka.serializers.KafkaAvroSerializer");

        final String partition = "00000";
        final String fileName = topicName + "-" + partition + "-" + System.currentTimeMillis() + ".txt";
        final String name = "testuser";

        connectRunner.configureConnector(CONNECTOR_NAME, connectorConfig);
        final Path path = ContentUtils.getTmpFilePath(name);

        try {
            s3Client.putObject(TEST_BUCKET_NAME, fileName, Files.newInputStream(path), null);
        } catch (final Exception e) { // NOPMD broad exception caught
            LOGGER.error("Error in reading file {}", e.getMessage(), e);
        } finally {
            Files.delete(path);
        }

        final List<GenericRecord> records = IntegrationBase.consumeAvroMessages(topicName, 100,
                connectRunner.getBootstrapServers(), schemaRegistry.getSchemaRegistryUrl());
        assertThat(records).extracting(record -> record.get("name").toString())
                .contains(name + "1")
                .contains(name + "2");
    }

    @Test
    void jsonTest(final TestInfo testInfo) throws ExecutionException, InterruptedException, IOException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> connectorConfig = getConfig(basicConnectorConfig(CONNECTOR_NAME), topicName);
        connectorConfig.put(INPUT_FORMAT_KEY, InputFormat.JSONL.getValue());
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");

        connectRunner.configureConnector(CONNECTOR_NAME, connectorConfig);
        final String testMessage = "This is a test ";
        final StringBuilder jsonBuilder = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            final String jsonContent = "{\"message\": \"" + testMessage + "\", \"id\":\"" + i + "\"}";
            jsonBuilder.append(jsonContent).append("\n"); // NOPMD
        }
        final byte[] jsonBytes = jsonBuilder.toString().getBytes(StandardCharsets.UTF_8);

        writeToS3(topicName, jsonBytes, "00001");

        // Poll Json messages from the Kafka topic and deserialize them
        final List<JsonNode> records = IntegrationBase.consumeJsonMessages(topicName, 500,
                connectRunner.getBootstrapServers());

        assertThat(records).extracting(record -> record.get("payload").get("message").asText()).contains(testMessage);
        assertThat(records).extracting(record -> record.get("payload").get("id").asText()).contains("1");
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    private static ByteArrayOutputStream getAvroRecord(final Schema schema, final int messageId, final int noOfAvroRecs)
            throws IOException {
        // Create Avro records
        GenericRecord avroRecord;

        // Serialize Avro records to byte arrays
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, outputStream);
            for (int i = 0; i < noOfAvroRecs; i++) {
                avroRecord = new GenericData.Record(schema);
                avroRecord.put("message", "Hello, Kafka Connect S3 Source! object " + i);
                avroRecord.put("id", messageId);

                dataFileWriter.append(avroRecord);
            }

            dataFileWriter.flush();
        }
        outputStream.close();
        return outputStream;
    }

    private static void writeToS3(final String topicName, final byte[] testDataBytes, final String partitionId)
            throws IOException {
        final String filePrefix = topicName + "-" + partitionId + "-" + System.currentTimeMillis();
        final String fileSuffix = ".txt";

        final Path testFilePath = File.createTempFile(filePrefix, fileSuffix).toPath();
        try {
            Files.write(testFilePath, testDataBytes);
            saveToS3(TEST_BUCKET_NAME, "", filePrefix + fileSuffix, testFilePath.toFile());
        } finally {
            Files.delete(testFilePath);
        }
    }

    private Map<String, String> getConfig(final Map<String, String> config, final String topics) {
        config.put("connector.class", AivenKafkaConnectS3SourceConnector.class.getName());
        config.put(AWS_ACCESS_KEY_ID_CONFIG, S3_ACCESS_KEY_ID);
        config.put(AWS_SECRET_ACCESS_KEY_CONFIG, S3_SECRET_ACCESS_KEY);
        config.put(AWS_S3_ENDPOINT_CONFIG, s3Endpoint);
        config.put(AWS_S3_BUCKET_NAME_CONFIG, TEST_BUCKET_NAME);
        config.put(AWS_S3_PREFIX_CONFIG, s3Prefix);
        config.put(TARGET_TOPIC_PARTITIONS, "0,1");
        config.put(TARGET_TOPICS, topics);
        return config;
    }

    private Map<String, String> basicConnectorConfig(final String connectorName) {
        final Map<String, String> config = new HashMap<>();
        config.put("name", connectorName);
        config.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put("tasks.max", "1");
        return config;
    }

    public static void saveToS3(final String bucketName, final String folderName, final String fileNameInS3,
            final File fileToWrite) {
        final PutObjectRequest request = new PutObjectRequest(bucketName, folderName + fileNameInS3, fileToWrite);
        s3Client.putObject(request);
    }
}
