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

import io.aiven.commons.kafka.testkit.KafkaIntegrationTestBase;
import io.aiven.commons.kafka.testkit.KafkaManager;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FormatType;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Testcontainers;


import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 * @param <I>
 *            the kafka producer index type
 * @param <V> the kafka producer value type.
 */
@SuppressWarnings({ "PMD.TestClassWithoutTestCases" })
@Testcontainers
public abstract class AbstractSinkIntegrationBase<K extends Comparable<K>, I, V> extends KafkaIntegrationTestBase {

    private KafkaManager kafkaManager;

    protected static final int OFFSET_FLUSH_INTERVAL_MS = 5000;

    private static final Set<String> CONNECTOR_NAMES = new HashSet<>();

    protected String testTopic;

    protected String prefix;

    protected BucketAccessor<K> bucketAccessor;

    protected SinkStorage<K, ?> sinkStorage;

    protected KafkaProducer<I, V> producer;

    /**
     * Gets the SinkStorage implementation for these tests.
     * Sink storage encapsulates the functionality needed to verify items writen to storage.
     *
     * @return the SinkStorage implementation for these tests.
     */
    protected abstract SinkStorage<K, ?> getSinkStorage();

    /**
     * Retrieves the topic name from the test info. This ensures that each test has its own topic.
     *
     * @param testInfo the test info to create the topic name from.
     * @return the topic name.
     */
    static String topicName(final TestInfo testInfo) {
        return testInfo.getTestMethod().get().getName() + "-" + testInfo.getDisplayName().hashCode();
    }

    /**
     * The connector configuration for the specified sink.
     * <ul>
     *     <li>connector specific settings from sink storage</li>
     *     <li>name - connector class simple name</li>
     *     <li>class - sink storage provided class</li>
     *     <li>tasks.max - 1</li>
     *     <li>topics - testTopic</li>
     *     <li>file.name.prefix - specified prefix</li>
     * </ul>
     * @return a Map of configuration properties to string representations.
     */
    protected Map<String, String> basicConnectorConfig() {
        final Map<String, String> config = getSinkStorage().createSinkProperties(bucketAccessor.bucketName);
        config.put("name", getSinkStorage().getConnectorClass().getSimpleName());
        config.put("connector.class", getSinkStorage().getConnectorClass().getName());
        config.put("tasks.max", "1");
        config.put("topics", testTopic);
        config.put(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, Long.toString(OFFSET_FLUSH_INTERVAL_MS));
        if (prefix != null) {
            config.put("file.name.prefix", prefix);
        }
        return config;
    }

    /**
     * Sends a message in an async manner.  All messages are sent with a byte[] key and value type.
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
     *
     * @param prefix the testing prefix.  May be {@code null}.
     */
    protected final void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException, IOException {
        sinkStorage = getSinkStorage();
        kafkaManager = setupKafka(sinkStorage.getConnectorClass());
        testTopic = topicName(testInfo);
        kafkaManager.createTopic(testTopic);
        bucketAccessor = sinkStorage.getBucketAccessor("testBucket");
        bucketAccessor.createBucket();
        producer = createProducer();
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
        kafkaManager.configureConnector(connectorConfig.get("name"), connectorConfig);
    }

    protected abstract Map<String, Object> getProducerConfig();

    private KafkaProducer<I, V> createProducer() {
        final Map<String, Object> producerProps = getProducerConfig();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaManager.bootstrapServers());
        return new KafkaProducer<>(producerProps);
    }

    protected final K getNativeKeyForTimestamp(final int partition, final int startOffset, CompressionType compressionType, FormatType formatType) {
        return getSinkStorage().getTimestampNativeKey(prefix, testTopic, partition, startOffset, compressionType, formatType);
    }

    protected final K getNativeKey(final int partition, final int startOffset, final CompressionType compressionType, FormatType formatType) {
        return getSinkStorage().getNativeKey(prefix, testTopic, partition, startOffset, compressionType, formatType);
    }

    protected final K getNativeKeyForKey(final byte[] key, final CompressionType compressionType, final FormatType formatType) {
        return getSinkStorage().getKeyNativeKey(prefix, new String(key), compressionType, formatType);
    }


    /**
     * Wait until all the specified futures have completed.
     *
     * @param futures the futures to wait for.
     * @param timeout the maximum time to wait for the futures to complete.
     */
    protected void awaitFutures(List<? extends Future<?>> futures, Duration timeout) {
        producer.flush();
        await("All futures written").atMost(timeout).until(() -> {
            for (final Future<?> future : futures) {
                future.get();
            }
            return true;
        });
    }

    /**
     * Wait until the keys specified in the expectedKeys, and only those keys, are found in the storage.
     * System will check every {@link #OFFSET_FLUSH_INTERVAL_MS} for updates.
     *
     * @param expectedKeys the expected keys
     * @param timeout      the maximum time to wait.
     */
    protected void awaitAllBlobsWritten(final Collection<K> expectedKeys, Duration timeout) {
        await("All expected files on storage").atMost(timeout)
                .pollInterval(Duration.ofMillis(OFFSET_FLUSH_INTERVAL_MS))
                .untilAsserted(() -> assertThat(bucketAccessor.listKeys(prefix)).containsExactlyInAnyOrderElementsOf(expectedKeys));
    }
}
