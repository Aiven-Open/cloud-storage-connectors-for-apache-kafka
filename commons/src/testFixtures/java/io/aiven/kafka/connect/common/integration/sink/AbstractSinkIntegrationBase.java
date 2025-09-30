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
 * @param <I>
 *            The type for the kafka connect key type
 * @param <V>
 *            The type for the kafka connect value type.
 */
@SuppressWarnings({ "deprecation", "PMD.TestClassWithoutTestCases" })
@Testcontainers
public abstract class AbstractSinkIntegrationBase<K extends Comparable<K>, N, I, V> extends KafkaIntegrationTestBase {

    private KafkaManager kafkaManager;

    private KafkaProducer<I, V> producer;

    protected static final int OFFSET_FLUSH_INTERVAL_MS = 5000;

    private static final Set<String> CONNECTOR_NAMES = new HashSet<>();

    protected abstract SinkStorage<K, N> getSinkStorage();

    protected String testTopic;

    protected String prefix;

    protected BucketAccessor<K> bucketAccessor;

    protected SinkStorage<K, N> sinkStorage;

    /**
     * Retreives the topic name from the test info. This ensures that each test has its own topic.
     * @param testInfo the test info to create the topic name from.
     * @return the topic name.
     */
    static String topicName(final TestInfo testInfo) {
        return testInfo.getTestMethod().get().getName() + "-" + testInfo.getDisplayName().hashCode();
    }

    /**
     * Sends a message in an async manner.
     * @param topicName the topic to send the message on.
     * @param partition the partition for the message.
     * @param key the key for the message.
     * @param value the value for the message.
     * @return A future that will return the {@link RecordMetadata} for the message.
     */
    protected Future<RecordMetadata> sendMessageAsync(final String topicName, final int partition, final I key, final V value) {
        final ProducerRecord<I, V> msg = new ProducerRecord<>(topicName, partition, key, value);
        return producer.send(msg);
    }

    /**
     * Sets the prefix used for files in testing.
     * @param prefix the testing prefix.  May be {@code null}.
     */
    protected final void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException, IOException {
        sinkStorage = getSinkStorage();
        kafkaManager = setupKafka(sinkStorage.getConnectorClass());
        producer = createProducer();
        testTopic = topicName(testInfo);
        kafkaManager.createTopic(testTopic);
        bucketAccessor = sinkStorage.getBucketAccessor("testBucket");
        bucketAccessor.createBucket();
    }

    @AfterEach
    void tearDown() {
        producer.close();
        CONNECTOR_NAMES.forEach(kafkaManager::deleteConnector);
        CONNECTOR_NAMES.clear();
        bucketAccessor.removeBucket();
    }

    protected void createConnector(final Map<String, String> connectorConfig) {
        CONNECTOR_NAMES.add(connectorConfig.get("name"));
        String result = kafkaManager.configureConnector(connectorConfig.get("name"), connectorConfig);
        System.out.println(result);
    }

    abstract Map<String, String> getProducerProperties();


    private KafkaProducer<I, V> createProducer() {
        final Map<String, Object> producerProps = new HashMap<>(getProducerProperties());
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaManager.bootstrapServers());
        return new KafkaProducer<>(producerProps);
    }


    protected final K getTimestampBlobName(final int partition, final int startOffset, CompressionType compression) {
        return getSinkStorage().getTimestampBlobName(prefix, testTopic, partition, startOffset, compression);
    }

    protected final K getBlobName(final int partition, final int startOffset, final CompressionType compressionType) {
        return getSinkStorage().getBlobName(prefix, testTopic, partition, startOffset, compressionType);
    }

    protected final K getKeyBlobName(final byte[] key, final CompressionType compressionType) {
        return getSinkStorage().getKeyBlobName(prefix, new String(key), compressionType);
    }

    protected void awaitFutures(List<? extends Future<?>> futures, Duration timeout) {
        producer.flush();
        await("All futures written").atMost(timeout).until(() -> {
            for (final Future<?> future : futures) {
                future.get();
            }
            return true;
        });
    }

    protected void awaitAllBlobsWritten(final Collection<K> expectedKeys, Duration timeout) {
        await("All expected files on storage").atMost(timeout)
                .pollInterval(Duration.ofMillis(OFFSET_FLUSH_INTERVAL_MS))
                .untilAsserted(() -> assertThat(bucketAccessor.listKeys(prefix)).containsExactlyElementsOf(expectedKeys));
    }
}
