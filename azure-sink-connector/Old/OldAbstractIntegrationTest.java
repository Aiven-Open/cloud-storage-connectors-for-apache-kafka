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

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Future;

import io.aiven.kafka.connect.azure.testdata.ContainerAccessor;
import io.aiven.kafka.connect.common.integration.AbstractKafkaIntegrationBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.aiven.kafka.connect.azure.sink.testutils.AzureBlobAccessor;
import io.aiven.kafka.connect.common.config.CompressionType;

import org.apache.kafka.connect.connector.Connector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SuppressWarnings({ "deprecation", "PMD.TestClassWithoutTestCases" })
@Testcontainers
class OldAbstractIntegrationTest<K, V> extends AbstractKafkaIntegrationBase {
    private  final KafkaProducer<K, V> NULL_PRODUCER = null;
    protected final String testTopic0;
    protected final String testTopic1;

    protected static final int OFFSET_FLUSH_INTERVAL_MS = 5000;
    protected static String azureConnectionString; // NOPMD mutable static state

    protected static String testContainerName; // NOPMD mutable static state

    protected static String azurePrefix; // NOPMD mutable static state

    protected static AzureBlobAccessor testBlobAccessor; // NOPMD mutable static state

    protected static String azureEndpoint; // NOPMD mutable static state

    private KafkaProducer<K, V> producer;

    protected AzureSinkIntegrationTestData testData;
    protected ContainerAccessor containerAccessor;


    /**
     * The azure container
     */
    @Container
    private static final AzuriteContainer AZURITE_CONTAINER = AzureSinkIntegrationTestData.createContainer();


    protected OldAbstractIntegrationTest() {
        testTopic0 = "test-topic-0-" + UUID.randomUUID();
        testTopic1 = "test-topic-1-" + UUID.randomUUID();
    }



    private Class<? extends Connector> getConnectorClass() {
        return AzureBlobSinkConnector.class;
    }

    @BeforeEach
    void setupAzure() {
        testData = new AzureSinkIntegrationTestData(AZURITE_CONTAINER);
    }

    @AfterEach
    void tearDownAzure() {
        testData.releaseResources();
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



}
