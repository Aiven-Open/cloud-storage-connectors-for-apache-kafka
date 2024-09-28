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
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.AWS_ACCESS_KEY_ID_CONFIG;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.AWS_S3_BUCKET_NAME_CONFIG;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.AWS_S3_ENDPOINT_CONFIG;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.AWS_S3_PREFIX_CONFIG;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.AWS_SECRET_ACCESS_KEY_CONFIG;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.JSON_OUTPUT_FORMAT;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.OUTPUT_FORMAT;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.PARQUET_OUTPUT_FORMAT;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.SCHEMA_REGISTRY_URL;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.TARGET_TOPICS;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.TARGET_TOPIC_PARTITIONS;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.VALUE_SERIALIZER;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;

import io.aiven.kafka.connect.s3.source.testutils.BucketAccessor;
import io.aiven.kafka.connect.s3.source.testutils.S3OutputStream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.IOUtils;
import org.junit.Ignore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Ignore
@Testcontainers
@SuppressWarnings("PMD.ExcessiveImports")
final class IntegrationTest implements IntegrationBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationTest.class);
    private static final String S3_FILE_NAME = "testtopic-0-0001.txt";
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

    @Container
    private static final KafkaContainer KAFKA_CONTAINER = IntegrationBase.createKafkaContainer();

    @Container
    private static final SchemaRegistryContainer SCHEMA_REGISTRY = new SchemaRegistryContainer(KAFKA_CONTAINER);
    private AdminClient adminClient;
    private ConnectRunner connectRunner;

    private static AmazonS3 s3Client;

    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
        s3Prefix = COMMON_PREFIX + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";

        s3Client = IntegrationBase.createS3Client(LOCALSTACK);
        s3Endpoint = LOCALSTACK.getEndpoint().toString();
        testBucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET_NAME);

        pluginDir = IntegrationBase.getPluginDir();
        IntegrationBase.extractConnectorPlugin(pluginDir);
        IntegrationBase.waitForRunningContainer(KAFKA_CONTAINER);
    }

    @BeforeEach
    void setUp(final TestInfo testInfo) throws ExecutionException, InterruptedException, IOException {
        testBucketAccessor.createBucket();
        adminClient = newAdminClient(KAFKA_CONTAINER);

        final String topicName = IntegrationBase.topicName(testInfo);
        final var topics = List.of(topicName);
        IntegrationBase.createTopics(adminClient, topics);

        connectRunner = newConnectRunner(KAFKA_CONTAINER, pluginDir, OFFSET_FLUSH_INTERVAL_MS);
        connectRunner.start();
    }

    @AfterEach
    void tearDown() {
        testBucketAccessor.removeBucket();
        connectRunner.stop();
        adminClient.close();

        connectRunner.awaitStop();
    }

    @Test
    void basicTest(final TestInfo testInfo) throws ExecutionException, InterruptedException, IOException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> connectorConfig = getConfig(basicConnectorConfig(CONNECTOR_NAME), topicName);

        connectRunner.createConnector(connectorConfig);

        final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
        final String testData2 = "Hello, Kafka Connect S3 Source! object 2";

        // write 2 objects to s3
        writeToS3(topicName, testData1.getBytes(StandardCharsets.UTF_8), 1);
        writeToS3(topicName, testData2.getBytes(StandardCharsets.UTF_8), 2);

        final List<String> objects = testBucketAccessor.listObjects();
        assertThat(objects.size()).isEqualTo(2);

        // Verify that the connector is correctly set up
        assertThat(connectorConfig.get("name")).isEqualTo(CONNECTOR_NAME);

        // Poll messages from the Kafka topic and verify the consumed data
        final List<String> records = IntegrationBase.consumeMessages(topicName, 2, KAFKA_CONTAINER);

        // Verify that the correct data is read from the S3 bucket and pushed to Kafka
        assertThat(records).contains(testData1).contains(testData2);
    }

    @Test
    void multiPartUploadTest(final TestInfo testInfo) throws ExecutionException, InterruptedException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> connectorConfig = getConfig(basicConnectorConfig(CONNECTOR_NAME), topicName);

        connectRunner.createConnector(connectorConfig);
        final String partition = "00001";
        final String offset = "000000000121";
        final String key = topicName + "-" + partition + "-" + offset + ".txt";
        multipartUpload(TEST_BUCKET_NAME, key);
        // Poll messages from the Kafka topic and verify the consumed data
        final List<String> records = IntegrationBase.consumeMessages(topicName, 1, KAFKA_CONTAINER);
        assertThat(records.get(0)).contains("performanceeeqjz");
    }

    @Test
    void avroTest(final TestInfo testInfo) throws ExecutionException, InterruptedException, IOException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> connectorConfig = getConfig(basicConnectorConfig(CONNECTOR_NAME), topicName);
        connectorConfig.put(OUTPUT_FORMAT, AVRO_OUTPUT_FORMAT);
        connectorConfig.put(SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY.getSchemaRegistryUrl());
        connectorConfig.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        connectorConfig.put("value.converter.schema.registry.url", SCHEMA_REGISTRY.getSchemaRegistryUrl());
        connectorConfig.put(VALUE_SERIALIZER, "io.confluent.kafka.serializers.KafkaAvroSerializer");

        connectRunner.createConnector(connectorConfig);

        // Define Avro schema
        final String schemaJson = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestRecord\",\n"
                + "  \"fields\": [\n" + "    {\"name\": \"message\", \"type\": \"string\"},\n"
                + "    {\"name\": \"id\", \"type\": \"int\"}\n" + "  ]\n" + "}";
        final Schema.Parser parser = new Schema.Parser();
        final Schema schema = parser.parse(schemaJson);

        final ByteArrayOutputStream outputStream1 = getAvroRecord(schema, 1);
        final ByteArrayOutputStream outputStream2 = getAvroRecord(schema, 2);

        writeToS3(topicName, outputStream1.toByteArray(), 1);
        writeToS3(topicName, outputStream2.toByteArray(), 2);

        final List<String> objects = testBucketAccessor.listObjects();
        assertThat(objects.size()).isEqualTo(2);

        // Verify that the connector is correctly set up
        assertThat(connectorConfig.get("name")).isEqualTo(CONNECTOR_NAME);

        // Poll Avro messages from the Kafka topic and deserialize them
        final List<GenericRecord> records = IntegrationBase.consumeAvroMessages(topicName, 2, KAFKA_CONTAINER,
                SCHEMA_REGISTRY.getSchemaRegistryUrl()); // Ensure this method deserializes Avro

        // Verify that the correct data is read from the S3 bucket and pushed to Kafka
        assertThat(records).extracting(record -> record.get("message").toString())
                .contains("Hello, Kafka Connect S3 Source! object 1")
                .contains("Hello, Kafka Connect S3 Source! object 2");
        assertThat(records).extracting(record -> record.get("id").toString()).contains("1").contains("2");
    }

    @Test
    @Ignore
    void parquetTest(final TestInfo testInfo) throws ExecutionException, InterruptedException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> connectorConfig = getConfig(basicConnectorConfig(CONNECTOR_NAME), topicName);
        connectorConfig.put(OUTPUT_FORMAT, PARQUET_OUTPUT_FORMAT);
        connectorConfig.put(SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY.getSchemaRegistryUrl());

        final String partition = "00000";
        final String offset = "000000000123";
        final String fileName = topicName + "-" + partition + "-" + offset + ".txt";

        connectRunner.createConnector(connectorConfig);
        try (InputStream resourceStream = Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream("sample1.parquet")) {
            s3Client.putObject(TEST_BUCKET_NAME, fileName, resourceStream, null);
        } catch (final Exception e) { // NOPMD broad exception catched
            LOGGER.error("Error in reading file" + e.getMessage());
        }
        // TODO
        assertThat(1).isEqualTo(1);
    }

    @Test
    void jsonTest(final TestInfo testInfo) throws ExecutionException, InterruptedException, IOException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> connectorConfig = getConfig(basicConnectorConfig(CONNECTOR_NAME), topicName);
        connectorConfig.put(OUTPUT_FORMAT, JSON_OUTPUT_FORMAT);
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");

        connectRunner.createConnector(connectorConfig);
        final String testMessage = "This is a test";
        final String jsonContent = "{\"message\": \"" + testMessage + "\", \"id\":\"1\"}";
        writeToS3(topicName, jsonContent.getBytes(StandardCharsets.UTF_8), 7);

        // Poll Json messages from the Kafka topic and deserialize them
        final List<JsonNode> records = IntegrationBase.consumeJsonMessages(topicName, 1, KAFKA_CONTAINER);

        assertThat(records).extracting(record -> record.get("payload").get("message").asText()).contains(testMessage);
        assertThat(records).extracting(record -> record.get("payload").get("id").asText()).contains("1");
    }

    private static ByteArrayOutputStream getAvroRecord(final Schema schema, final int messageId) throws IOException {
        // Create Avro records
        final GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("message", "Hello, Kafka Connect S3 Source! object " + messageId);
        avroRecord.put("id", messageId);

        // Serialize Avro records to byte arrays
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, outputStream);
            dataFileWriter.append(avroRecord);
            dataFileWriter.flush();
        }
        outputStream.close();
        return outputStream;
    }

    private static void writeToS3(final String topicName, final byte[] testDataBytes, final int offsetId)
            throws IOException {
        final String partition = "00000";
        final String offset = "00000000012" + offsetId;
        final String fileName = topicName + "-" + partition + "-" + offset + ".txt";
        final Path testFilePath = Paths.get("/tmp/" + fileName);
        Files.write(testFilePath, testDataBytes);

        saveToS3(TEST_BUCKET_NAME, "", fileName, testFilePath.toFile());
    }

    @Deprecated
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

    public void multipartUpload(final String bucketName, final String key) {
        try (S3OutputStream s3OutputStream = new S3OutputStream(bucketName, key, S3OutputStream.DEFAULT_PART_SIZE,
                s3Client);
                InputStream resourceStream = Thread.currentThread()
                        .getContextClassLoader()
                        .getResourceAsStream(S3_FILE_NAME);) {
            assert resourceStream != null;
            final byte[] fileBytes = IOUtils.toByteArray(resourceStream);
            s3OutputStream.write(fileBytes);
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }
    }

}
