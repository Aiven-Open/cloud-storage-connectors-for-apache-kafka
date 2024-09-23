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
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.OFFSET_STORAGE_TOPIC;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.OFFSET_STORAGE_TOPIC_PARTITIONS;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.START_MARKER_KEY;
import static org.assertj.core.api.Assertions.assertThat;

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
        final Map<String, String> connectorConfig = getConfig(basicConnectorConfig(CONNECTOR_NAME));

        connectRunner.createConnector(connectorConfig);

        final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
        final String testData2 = "Hello, Kafka Connect S3 Source! object 2";

        // write 2 objects to s3
        writeToS3(topicName, testData1, 1);
        writeToS3(topicName, testData2, 2);

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
    void multiPartUploadTest(final TestInfo testInfo) throws ExecutionException, InterruptedException, IOException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> connectorConfig = getConfig(basicConnectorConfig(CONNECTOR_NAME));

        connectRunner.createConnector(connectorConfig);
        final String partition = "00001";
        final String offset = "000000000121";
        final String key = topicName + "-" + partition + "-" + offset + ".txt";
        multipartUpload(TEST_BUCKET_NAME, key);
        // Poll messages from the Kafka topic and verify the consumed data
        final List<String> records = IntegrationBase.consumeMessages(topicName, 1, KAFKA_CONTAINER);
        assertThat(records.get(0)).contains("performanceeeqjz");
    }

    private static void writeToS3(final String topicName, final String testData1, final int offsetId)
            throws IOException {
        final String partition = "00000";
        final String offset = "00000000012" + offsetId;
        final String fileName = topicName + "-" + partition + "-" + offset + ".txt";
        final Path testFilePath = Paths.get("/tmp/" + fileName);
        Files.write(testFilePath, testData1.getBytes(StandardCharsets.UTF_8));

        saveToS3(TEST_BUCKET_NAME, "", fileName, testFilePath.toFile());
    }

    @Deprecated
    private Map<String, String> getConfig(final Map<String, String> config) {
        config.put("connector.class", AivenKafkaConnectS3SourceConnector.class.getName());
        config.put(AWS_ACCESS_KEY_ID_CONFIG, S3_ACCESS_KEY_ID);
        config.put(AWS_SECRET_ACCESS_KEY_CONFIG, S3_SECRET_ACCESS_KEY);
        config.put(AWS_S3_ENDPOINT_CONFIG, s3Endpoint);
        config.put(AWS_S3_BUCKET_NAME_CONFIG, TEST_BUCKET_NAME);
        config.put(AWS_S3_PREFIX_CONFIG, s3Prefix);
        config.put(START_MARKER_KEY, COMMON_PREFIX);
        config.put(OFFSET_STORAGE_TOPIC_PARTITIONS, "1,2");
        config.put(OFFSET_STORAGE_TOPIC, "connect-storage-offsets");
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
