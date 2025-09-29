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

package io.aiven.kafka.connect.common.integration.sink;

//import com.google.cloud.NoCredentials;
//import com.google.cloud.storage.BucketInfo;
//import com.google.cloud.storage.Storage;
//import com.google.cloud.storage.StorageOptions;
import io.aiven.commons.kafka.testkit.KafkaIntegrationTestBase;
import io.aiven.commons.kafka.testkit.KafkaManager;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.source.AbstractSourceRecord;
import io.aiven.kafka.connect.common.source.OffsetManager;
//import io.aiven.kafka.connect.gcs.testutils.BucketAccessor;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests to ensure that data written to the storage layer in various formats can be correctly read by the source
 * implementation.
 *
 * @param <K>
 *            the native key type.
 * @param <N>
 *            the native object type
 * @param <O>
 *            The {@link OffsetManager.OffsetManagerEntry} implementation.
 * @param <T>
 *            The implementation of the {@link AbstractSourceRecord}
 */
@SuppressWarnings({ "deprecation", "PMD.TestClassWithoutTestCases" })
@Testcontainers
public abstract class AbstractSinkIntegrationBase<K extends Comparable<K>, N, I, V> extends KafkaIntegrationTestBase {

    private KafkaManager kafkaManager;

    private KafkaProducer<I, V> producer;

    protected static final int OFFSET_FLUSH_INTERVAL_MS = 5000;

    private static final Set<String> CONNECTOR_NAMES = new HashSet<>();

    protected abstract KafkaProducer<I, V> createProducer();

    protected abstract BucketAccessor<K> getBucketAccessor();

    protected abstract SinkStorage<K, N> getSinkStorage();

    protected String testTopic;

    protected String prefix;

    protected SinkStorage<K, N> sinkStorage;

    static String topicName(final TestInfo testInfo) {
        return testInfo.getTestMethod().get().getName() + "-" + testInfo.getDisplayName().hashCode();
    }

    protected Future<RecordMetadata> sendMessageAsync(final String topicName, final int partition, final I key, final V value) {
        final ProducerRecord<I, V> msg = new ProducerRecord<>(topicName, partition, key, value);
        return producer.send(msg);
    }

    protected final void setPrefix(String prefix) {
        this.prefix = prefix;
    }

//    @BeforeAll
//    static void setUpAll() {
//        s3Prefix = COMMON_PREFIX + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";
//
//        final AmazonS3 s3Client = createS3Client(LOCALSTACK);
//        s3Endpoint = LOCALSTACK.getEndpoint().toString();
//        testBucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET_NAME);
//    }

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException, IOException {
        sinkStorage = getSinkStorage();
        kafkaManager = setupKafka(sinkStorage.getConnectorClass());
        producer = createProducer();
        testTopic = topicName(testInfo);
        kafkaManager.createTopic(testTopic);
    }

    @AfterEach
    void tearDown() {
        producer.close();
        getBucketAccessor().removeBucket();
        CONNECTOR_NAMES.forEach(kafkaManager::deleteConnector);
        CONNECTOR_NAMES.clear();
    }

    protected void createConnector(final Map<String, String> connectorConfig) {
        CONNECTOR_NAMES.add(connectorConfig.get("name"));
        kafkaManager.configureConnector(connectorConfig.get("name"), connectorConfig);
    }

//    protected String getBaseBlobName(final int partition, final int startOffset) {
//        return String.format("%s%s-%d-%d", gcsPrefix, testTopic0, partition, startOffset);
//    }
//
//    protected String getBlobName(final int partition, final int startOffset, final String compression) {
//        final String result = getBaseBlobName(partition, startOffset);
//        return result + CompressionType.forName(compression).extension();
//    }
//
//    protected String getBlobName(final K key, final String compression) {
//        final String result = String.format("%s%s", gcsPrefix, key);
//        return result + CompressionType.forName(compression).extension();
//    }
//
//    protected void awaitAllBlobsWritten(final int expectedBlobCount) {
//        await("All expected files stored on GCS").atMost(Duration.ofMillis(OFFSET_FLUSH_INTERVAL_MS * 30))
//                .pollInterval(Duration.ofMillis(300))
//                .until(() -> testBucketAccessor.getBlobNames(gcsPrefix).size() >= expectedBlobCount);
//
//    }
//

    protected void awaitFutures(List<? extends Future<?>> futures, Duration timeout) {
        producer.flush();
        await("All futures written").atMost(timeout).until(() -> {
            for (final Future<?> future : futures) {
                future.get();
            }
            return true;
        });
    }


//    protected void startConnectRunner(final Map<String, Object> testSpecificProducerProperties)
//            throws ExecutionException, InterruptedException {
//        testBucketAccessor.clear(gcsPrefix);
//
//        kafkaManager.createTopics(Arrays.asList(testTopic0, testTopic1));
//
//        final Map<String, Object> producerProps = new HashMap<>(testSpecificProducerProperties);
//        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaManager.bootstrapServers());
//        producer = new KafkaProducer<>(producerProps);
//
//    }

    protected void awaitAllBlobsWritten(final Collection<K> expectedKeys) {
        await("All expected files on storage").atMost(Duration.ofMillis(OFFSET_FLUSH_INTERVAL_MS * 30))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> assertThat(getBucketAccessor().listKeys(prefix)).containsExactlyElementsOf(expectedKeys));
    }
}
