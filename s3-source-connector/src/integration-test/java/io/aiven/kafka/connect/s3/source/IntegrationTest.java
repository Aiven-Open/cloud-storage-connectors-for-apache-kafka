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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;

import io.aiven.kafka.connect.s3.source.testutils.BucketAccessor;

import com.amazonaws.services.s3.AmazonS3;
import org.junit.Ignore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Ignore
@Testcontainers
final class IntegrationTest implements IntegrationBase {
    private static final String CONNECTOR_NAME = "aiven-s3-source-connector";
    private static final String COMMON_PREFIX = "s3-source-connector-for-apache-kafka-test-";
    private static final int OFFSET_FLUSH_INTERVAL_MS = 5000;

    private static final String TEST_BUCKET_NAME = "test-bucket0";

    private static String s3Endpoint;
    private static String s3Prefix;
    private static BucketAccessor testBucketAccessor;

    private static File pluginDir;

    @Container
    public static final LocalStackContainer LOCALSTACK = IntegrationBase.createS3Container();

    @Container
    private static final KafkaContainer KAFKA = IntegrationBase.createKafkaContainer();
    private AdminClient adminClient;
    private ConnectRunner connectRunner;

    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
        s3Prefix = COMMON_PREFIX + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";

        final AmazonS3 s3Client = IntegrationBase.createS3Client(LOCALSTACK);
        s3Endpoint = LOCALSTACK.getEndpoint().toString();
        testBucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET_NAME);

        pluginDir = IntegrationBase.getPluginDir();
        IntegrationBase.extractConnectorPlugin(pluginDir);
        IntegrationBase.waitForRunningContainer(KAFKA);
    }

    @BeforeEach
    void setUp(final TestInfo testInfo) throws ExecutionException, InterruptedException {
        testBucketAccessor.createBucket();
        adminClient = newAdminClient(KAFKA);

        final var topicName = IntegrationBase.topicName(testInfo);
        final var topics = List.of(topicName);
        IntegrationBase.createTopics(adminClient, topics);

        connectRunner = newConnectRunner(KAFKA, pluginDir, OFFSET_FLUSH_INTERVAL_MS);
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
    void basicTest(final TestInfo testInfo) throws ExecutionException, InterruptedException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> connectorConfig = getConfig(basicConnectorConfig(CONNECTOR_NAME), topicName);
        connectRunner.createConnector(connectorConfig);

        assertThat(connectorConfig.get("name")).isEqualTo(CONNECTOR_NAME);
    }

    private Map<String, String> getConfig(final Map<String, String> config, final String topicName) {
        return getConfig(config, List.of(topicName));
    }

    private Map<String, String> getConfig(final Map<String, String> config, final List<String> topicNames) {
        config.put("connector.class", AivenKafkaConnectS3SourceConnector.class.getName());
        config.put("topics", String.join(",", topicNames));
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
}
