/*
 * Copyright 2025 Aiven Oy
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

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;

import io.aiven.kafka.connect.common.integration.KafkaIntegrationTestBase;
import io.aiven.kafka.connect.common.integration.KafkaManager;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector;
import io.aiven.kafka.connect.s3.testutils.BucketAccessor;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@SuppressWarnings("PMD.MutableStaticState")
abstract class AbstractIntegrationTest<K, V> extends KafkaIntegrationTestBase {
    protected static final String S3_ACCESS_KEY_ID = "test-key-id0";
    protected static final String S3_SECRET_ACCESS_KEY = "test_secret_key0";
    protected static final String TEST_BUCKET_NAME = "test-bucket0";

    protected static final String CONNECTOR_NAME = "aiven-s3-sink-connector";
    protected static final String COMMON_PREFIX = "s3-connector-for-apache-kafka-test-";
    protected static final int OFFSET_FLUSH_INTERVAL_MS = 5000;

    protected static String s3Endpoint;
    protected static String s3Prefix;
    protected static BucketAccessor testBucketAccessor;

    private static final Set<String> CONNECTOR_NAMES = new HashSet<>();

    protected KafkaManager kafkaManager;

    protected KafkaProducer<K, V> producer;

    @Container
    public static final LocalStackContainer LOCALSTACK = createS3Container();

    abstract KafkaProducer<K, V> newProducer();

    static LocalStackContainer createS3Container() {
        return new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.0.2"))
                .withServices(LocalStackContainer.Service.S3);
    }

    static AmazonS3 createS3Client(final LocalStackContainer localStackContainer) {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        localStackContainer.getEndpointOverride(LocalStackContainer.Service.S3).toString(),
                        localStackContainer.getRegion()))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                        localStackContainer.getAccessKey(), localStackContainer.getSecretKey())))
                .build();
    }

    static String topicName(final TestInfo testInfo) {
        return testInfo.getTestMethod().get().getName() + "-" + testInfo.getDisplayName().hashCode();
    }

    @BeforeAll
    static void setUpAll() {
        s3Prefix = COMMON_PREFIX + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";

        final AmazonS3 s3Client = createS3Client(LOCALSTACK);
        s3Endpoint = LOCALSTACK.getEndpoint().toString();
        testBucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET_NAME);
    }

    @BeforeEach
    void setUp(final TestInfo testInfo) throws ExecutionException, InterruptedException, IOException {
        testBucketAccessor.createBucket();
        kafkaManager = setupKafka(AivenKafkaConnectS3SinkConnector.class);
        producer = newProducer();

        final var topicName = topicName(testInfo);
        kafkaManager.createTopic(topicName);
    }

    @AfterEach
    void tearDown() {
        producer.close();
        testBucketAccessor.removeBucket();
        CONNECTOR_NAMES.forEach(kafkaManager::deleteConnector);
        CONNECTOR_NAMES.clear();
    }

    protected void createConnector(final Map<String, String> connectorConfig) {
        CONNECTOR_NAMES.add(connectorConfig.get("name"));
        kafkaManager.configureConnector(connectorConfig.get("name"), connectorConfig);
    }
}
