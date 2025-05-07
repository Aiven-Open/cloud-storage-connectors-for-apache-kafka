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

package io.aiven.kafka.connect.azure.sink;

import static org.awaitility.Awaitility.await;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import io.aiven.kafka.connect.common.integration.KafkaIntegrationTestBase;
import io.aiven.kafka.connect.common.integration.KafkaManager;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.aiven.kafka.connect.azure.sink.testutils.AzureBlobAccessor;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.integration.KafkaIntegrationTestBase;
import io.aiven.kafka.connect.common.integration.KafkaManager;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SuppressWarnings({ "deprecation", "PMD.TestClassWithoutTestCases" })
@Testcontainers
class AbstractIntegrationTest<K, V> extends KafkaIntegrationTestBase {
    protected final String testTopic0;
    protected final String testTopic1;

    private KafkaManager kafkaManager;

    private KafkaProducer<K, V> producer;

    protected static final int OFFSET_FLUSH_INTERVAL_MS = 5000;
    public static final String DEFAULT_TEST_CONTAINER_NAME = "test";
    protected static String azureConnectionString; // NOPMD mutable static state

    protected static String testContainerName; // NOPMD mutable static state

    protected static String azurePrefix; // NOPMD mutable static state

    protected static AzureBlobAccessor testBlobAccessor; // NOPMD mutable static state

    protected static File pluginDir; // NOPMD mutable static state
    protected static String azureEndpoint; // NOPMD mutable static state

    private static final int AZURE_BLOB_PORT = 10_000;
    private static final int AZURE_QUEUE_PORT = 10_001;
    private static final int AZURE_TABLE_PORT = 10_002;
    private static final String AZURE_ENDPOINT = "http://127.0.0.1:10000";
    private static final String ACCOUNT_NAME = "devstoreaccount1";
    private static final String ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    private static final Set<String> CONNECTOR_NAMES = new HashSet<>();

    @Container
    private static final GenericContainer<?> AZURITE_CONTAINER = new FixedHostPortGenericContainer<>( // NOPMD
            "mcr.microsoft.com/azure-storage/azurite") // NOPMD
            .withFixedExposedPort(AZURE_BLOB_PORT, AZURE_BLOB_PORT)
            .withFixedExposedPort(AZURE_QUEUE_PORT, AZURE_QUEUE_PORT)
            .withFixedExposedPort(AZURE_TABLE_PORT, AZURE_TABLE_PORT)
            .withCommand("azurite --blobHost 0.0.0.0  --queueHost 0.0.0.0 --tableHost 0.0.0.0")
            .withReuse(true);

    protected AbstractIntegrationTest() {
        super();
        testTopic0 = "test-topic-0-" + UUID.randomUUID();
        testTopic1 = "test-topic-1-" + UUID.randomUUID();
    }

    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
        azureConnectionString = System.getProperty("integration-test.azure.connection.string");
        final String container = System.getProperty("integration-test.azure.container");
        testContainerName = container == null || container.isEmpty() ? DEFAULT_TEST_CONTAINER_NAME : container;
        final BlobServiceClient blobServiceClient; // NOPMD
        if (useFakeAzure()) {
            azureEndpoint = String.format(
                    "DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=%s/%s;", ACCOUNT_NAME,
                    ACCOUNT_KEY, AZURE_ENDPOINT, ACCOUNT_NAME);

            blobServiceClient = new BlobServiceClientBuilder().connectionString(azureEndpoint).buildClient();
            blobServiceClient.createBlobContainer(testContainerName);
        } else {
            blobServiceClient = new BlobServiceClientBuilder().connectionString(azureConnectionString).buildClient();
        }

        testBlobAccessor = new AzureBlobAccessor(blobServiceClient.getBlobContainerClient(testContainerName));
        testBlobAccessor.ensureWorking();

        azurePrefix = "azure-sink-connector-for-apache-kafka-test-"
                + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";

        final File testDir = Files.createTempDirectory("azure-sink-connector-for-apache-kafka-test-").toFile();

        pluginDir = new File(testDir, "plugins/azure-sink-connector-for-apache-kafka/");
        assert pluginDir.mkdirs();

        final File distFile = new File(System.getProperty("integration-test.distribution.file.path"));
        assert distFile.exists();

        final String cmd = String.format("tar -xf %s --strip-components=1 -C %s", distFile.toString(),
                pluginDir.toString());
        final Process process = Runtime.getRuntime().exec(cmd);
        assert process.waitFor() == 0;
    }

    @BeforeEach
    void setupKafka() throws IOException {
        kafkaManager = setupKafka(true, AzureBlobSinkConnector.class);
    }

    @AfterEach
    void tearDown() {
        producer.close();
        testBlobAccessor.clear(azurePrefix);
        CONNECTOR_NAMES.forEach(kafkaManager::deleteConnector);
    }

    protected static boolean useFakeAzure() {
        return azureConnectionString == null;
    }

    protected String getBaseBlobName(final int partition, final int startOffset) {
        return String.format("%s%s-%d-%d", azurePrefix, testTopic0, partition, startOffset);
    }

    protected String getBlobName(final int partition, final int startOffset, final String compression) {
        final String result = getBaseBlobName(partition, startOffset);
        return result + CompressionType.forName(compression).extension();
    }

    protected String getBlobName(final String key, final String compression) {
        final String result = String.format("%s%s", azurePrefix, key);
        return result + CompressionType.forName(compression).extension();
    }

    protected void awaitAllBlobsWritten(final int expectedBlobCount) {
        await("All expected files stored on Azure").atMost(Duration.ofMillis(OFFSET_FLUSH_INTERVAL_MS * 30))
                .pollInterval(Duration.ofMillis(300))
                .until(() -> testBlobAccessor.getBlobNames(azurePrefix).size() >= expectedBlobCount);

    }

    protected KafkaProducer<K, V> getProducer() {
        return producer;
    }

    protected Future<RecordMetadata> sendMessageAsync(final String topicName, final int partition, final K key,
            final V value) {
        final ProducerRecord<K, V> msg = new ProducerRecord<>(topicName, partition, key, value);
        return producer.send(msg);
    }

    protected void startConnectRunner(final Map<String, Object> testSpecificProducerProperties)
            throws ExecutionException, InterruptedException {
        testBlobAccessor.clear(azurePrefix);

        kafkaManager.createTopics(Arrays.asList(testTopic0, testTopic1));

        final Map<String, Object> producerProps = new HashMap<>(testSpecificProducerProperties);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaManager.bootstrapServers());
        producer = new KafkaProducer<>(producerProps);

    }

    protected void createConnector(final Map<String, String> connectorConfig) {
        CONNECTOR_NAMES.add(connectorConfig.get("name"));
        kafkaManager.configureConnector(connectorConfig.get("name"), connectorConfig);
    }
}
