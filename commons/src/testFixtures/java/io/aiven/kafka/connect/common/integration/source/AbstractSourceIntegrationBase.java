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

package io.aiven.kafka.connect.common.integration.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import io.aiven.kafka.connect.common.source.AbstractSourceRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.json.JsonDeserializer;

import io.aiven.kafka.connect.common.integration.ConsumerPropertiesBuilder;
import io.aiven.kafka.connect.common.integration.KafkaManager;
import io.aiven.kafka.connect.common.source.AbstractSourceRecordIterator;
import io.aiven.kafka.connect.common.source.AbstractSourceTask;
import io.aiven.kafka.connect.common.NativeInfo;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.utils.CasedString;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;

/**
 * The base abstract case for Kafka based integration tests.
 * <p>
 * This class handles the creation and destruction of a thread safe {@link KafkaManager}.
 * </p>
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
public abstract class AbstractSourceIntegrationBase<K extends Comparable<K>, N, O extends OffsetManager.OffsetManagerEntry<O>, T extends AbstractSourceRecord<K, N, O, T>> {

    /**
     * The Test info provided before each test. Tests may access this info wihout capturing it themselves.
     */
    protected TestInfo testInfo;

    /** A thread local instance of the KafkaManager */
    private static final ThreadLocal<KafkaManager> KAFKA_MANAGER_THREAD_LOCAL = new ThreadLocal<>();

    /** The thread local instance of the connector name */
    private static final ThreadLocal<String> CONNECTOR_NAME_THREAD_LOCAL = new ThreadLocal<>() {
    };

    protected AbstractSourceIntegrationBase() {
    }


    /**
     * Create the SourceStorage.  Should create the source storage once and then return it.
     * @return the current SourceStorage object.
     */
    abstract protected SourceStorage<K, N, O> getSourceStorage();

    /**
     * Gets logger for the test class
     *
     * @return the logger for the concrete implementation.
     */
    abstract protected Logger getLogger();

    /**
     * Creates the native key.
     *
     * @param prefix
     *            the prefix for the key.
     * @param topic
     *            the topic for the key,
     * @param partition
     *            the partition for the key.
     * @return the native Key.
     */
    final protected K createKey(String prefix, String topic, int partition) {
        return getSourceStorage().createKey(prefix, topic, partition);
    }

    /**
     * Write file to natvie storage with the specified key and data.
     *
     * @param nativeKey
     *            the key.
     * @param testDataBytes
     *            the data.
     */
    final protected SourceStorage.WriteResult<K> writeWithKey(final K nativeKey, final byte[] testDataBytes) {
        return getSourceStorage().writeWithKey(nativeKey, testDataBytes);
    };

    /**
     * Retrieves a list of {@link NativeInfo} implementations, one for each item in native storage.
     *
     * @return the list of {@link NativeInfo} implementations, one for each item in native storage.
     */
    final protected List<? extends NativeInfo<K, N>> getNativeStorage() {
        return getSourceStorage().getNativeStorage();
    }

    /**
     * The Connector class under test.
     *
     * @return the connector class under test.
     */

    final protected Class<? extends Connector> getConnectorClass() {
        return getSourceStorage().getConnectorClass();
    }

    /**
     * Gets the name of the current connector.
     *
     * @return the name of the connector.
     */
    final protected String getConnectorName() {
        String result = CONNECTOR_NAME_THREAD_LOCAL.get();
        if (result == null) {
            result = new CasedString(CasedString.StringCase.CAMEL, getConnectorClass().getSimpleName())
                    .toCase(CasedString.StringCase.KEBAB)
                    .toLowerCase(Locale.ROOT) + "-" + UUID.randomUUID();
            CONNECTOR_NAME_THREAD_LOCAL.set(result);
        }
        return result;
    }

    /**
     * Creates the configuration data for the connector.
     *
     * @return the configuration data for the Connector class under test.
     */
    final protected Map<String, String> createConnectorConfig(final String localPrefix) {
        return getSourceStorage().createConnectorConfig(localPrefix);
    }

    /**
     * Gets the default offset flush interval.
     *
     * @return the default offset flush interval.
     */
    protected Duration getOffsetFlushInterval() {
        return Duration.ofSeconds(5);
    }

    /**
     * Captures the test info for the current test.
     *
     * @param testInfo
     *            the test info.
     */
    @BeforeEach
    void captureTestInfo(final TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    @AfterAll
    static void cleanup() {
        tearDownKafka();
    }

    /**
     * Sets up and returns the KafkaManager. If the KafkaManager has already been set up, this method returns the
     * existing instance.
     *
     * @return a KafkaManager instance. This is equivalent of calling @{code setupKafka(false)}.
     * @throws IOException
     *             on IO error.
     * @throws ExecutionException
     *             on execution error.
     * @throws InterruptedException
     *             on interrupted thread.
     */
    final protected KafkaManager setupKafka() throws IOException, ExecutionException, InterruptedException {
        return setupKafka(false);
    }

    /**
     * Sets up and returns the KafkaManager. If the KafkaManager has already been set up, this method may return an
     * existing instance depending on the state of the @{code forceRestart} flag.
     *
     * @param forceRestart
     *            If true any existing KafkaManager is shutdown and a new one created.
     * @return a KafkaManager instance. This is equivalent of calling @{code setupKafka(false)}.
     * @throws IOException
     *             on IO error.
     * @throws ExecutionException
     *             on execution error.
     * @throws InterruptedException
     *             on interrupted thread.
     */
    final protected KafkaManager setupKafka(final boolean forceRestart)
            throws IOException, ExecutionException, InterruptedException {
        KafkaManager kafkaManager = KAFKA_MANAGER_THREAD_LOCAL.get();
        if (kafkaManager != null && forceRestart) {
            tearDownKafka();
        }
        kafkaManager = KAFKA_MANAGER_THREAD_LOCAL.get();
        if (kafkaManager == null) {
            final String clusterName = new CasedString(CasedString.StringCase.CAMEL,
                    testInfo.getTestClass().get().getSimpleName()).toCase(CasedString.StringCase.KEBAB)
                    .toLowerCase(Locale.ROOT);
            kafkaManager = new KafkaManager(clusterName, getOffsetFlushInterval(), getConnectorClass());
            KAFKA_MANAGER_THREAD_LOCAL.set(kafkaManager);
        }
        return kafkaManager;
    }

    /**
     * Tears down any existing KafkaManager. if the KafkaManager has not be created no action is taken.
     */
    protected static void tearDownKafka() {
        final KafkaManager kafkaManager = KAFKA_MANAGER_THREAD_LOCAL.get();
        if (kafkaManager != null) {
            kafkaManager.stop();
            KAFKA_MANAGER_THREAD_LOCAL.remove();
        }
    }

    /**
     * Delete the current connector from the running kafka.
     */
    final protected void deleteConnector() {
        final KafkaManager kafkaManager = KAFKA_MANAGER_THREAD_LOCAL.get();
        if (kafkaManager != null) {
            kafkaManager.deleteConnector(getConnectorName());
        }
        CONNECTOR_NAME_THREAD_LOCAL.remove();
    }

    /**
     * Get the current KafkaManager.
     *
     * @return the current KafkaManager.
     * @throws IllegalStateException
     *             if the KafkaManager has not been set up.
     */
    final protected KafkaManager getKafkaManager() {
        final KafkaManager kafkaManager = KAFKA_MANAGER_THREAD_LOCAL.get();
        if (kafkaManager == null) {
            throw new IllegalStateException("KafkaManager not initialized");
        }
        return kafkaManager;
    }

    /**
     * Removes/deletes the KafkatManager.
     */
    @AfterAll
    static void removeKafkaManager() {
        KAFKA_MANAGER_THREAD_LOCAL.remove();
    }

    /**
     * Returns a BiFunction that converts OffsetManager key and data into an OffsetManagerEntry for this system.
     * <ul>
     * <li>The first argument to the method is the {@link OffsetManager.OffsetManagerEntry#getManagerKey()} value.</li>
     * <li>The second argument is the {@link OffsetManager.OffsetManagerEntry#getProperties()} value.</li>
     * <li>Method should return a proper {@link OffsetManager.OffsetManagerEntry}</li>
     * </ul>
     *
     * @return A BiFunction that crates an OffsetManagerEntry.
     */
    final protected BiFunction<Map<String, Object>, Map<String, Object>, O> offsetManagerEntryFactory() {
        return getSourceStorage().offsetManagerEntryFactory();
    }

    /**
     * Creates a MessageConsumer for this test environment.
     *
     * @return a MessageConsumer instance.
     */
    protected final MessageConsumer messageConsumer() {
        return new MessageConsumer();
    }

    /**
     * Creates a ConsumerPropertiesBuilder configured with the proper bootstrap server.
     *
     * @return a ConsumerPropertiesBuilder configured with the proper bootstrap server.
     */
    protected final ConsumerPropertiesBuilder consumerPropertiesBuilder() {
        return new ConsumerPropertiesBuilder(getKafkaManager().bootstrapServers());
    }

    /**
     * Writes to the underlying storage. Does not use a prefix. Equivalent to calling
     * {@code write(topic, testDataBytes, partition, null)}
     *
     * @param topic
     *            the topic for the file.
     * @param testDataBytes
     *            the data.
     * @param partition
     *            the partition id fo the file.
     * @return the WriteResult for the operation.
     */
    protected final SourceStorage.WriteResult<K> write(final String topic, final byte[] testDataBytes, final int partition) {
        return write(topic, testDataBytes, partition, null);
    }

    /**
     * Writes to the underling storage. Uses {@link #createKey(String, String, int)} to create the key.
     *
     * @param topic
     *            the topic name to use
     * @param testDataBytes
     *            the data.
     * @param partition
     *            the partition id.
     * @param prefix
     *            the prefix for the key.
     * @return the WriteResult for the result.
     */
    protected final SourceStorage.WriteResult<K> write(final String topic, final byte[] testDataBytes, final int partition,
                                                       final String prefix) {
        final K objectKey = createKey(prefix, topic, partition);
        return writeWithKey(objectKey, testDataBytes);
    }

    /**
     * Get the topic from the TestInfo.
     *
     * @return The topic extracted from the testInfo for the current test.
     */
    public String getTopic() {
        return testInfo.getTestMethod().get().getName();
    }

    /**
     * Handles reading messages from the local kafka.
     */
    public class MessageConsumer {
        /** constructor */
        private MessageConsumer() {
            // use AbstractSourceIntegrationBase.messageConsumer();
        }

        /**
         * Read the data from the topic as byte array key and byte value. Each value is converted into a string and
         * returned in the result. If the expected number of messages is not read in the allotted time the test fails.
         *
         * @param topic
         *            the topic to red.
         * @param expectedMessageCount
         *            the expected number of messages.
         * @param timeout
         *            the maximum time to wait for the messages to arrive.
         * @return A list of values returned.
         */
        public List<String> consumeByteMessages(final String topic, final int expectedMessageCount,
                final Duration timeout) {
            final Properties consumerProperties = consumerPropertiesBuilder()
                    .keyDeserializer(ByteArrayDeserializer.class)
                    .valueDeserializer(ByteArrayDeserializer.class)
                    .build();
            final List<ConsumerRecord<byte[], byte[]>> lst = consumeMessages(topic, consumerProperties,
                    expectedMessageCount, timeout);
            return lst.stream().map(cr -> new String(cr.value(), StandardCharsets.UTF_8)).collect(Collectors.toList());
        }

        /**
         * Read the data from the topic as byte array key and byte value. If the expected number of messages is not read
         * in the allotted time the test fails.
         *
         * @param topic
         *            the topic to red.
         * @param expectedMessageCount
         *            the expected number of messages.
         * @param timeout
         *            the maximum time to wait for the messages to arrive.
         * @return A list of values returned.
         */
        public List<byte[]> consumeRawByteMessages(final String topic, final int expectedMessageCount,
                final Duration timeout) {
            final Properties consumerProperties = consumerPropertiesBuilder()
                    .keyDeserializer(ByteArrayDeserializer.class)
                    .valueDeserializer(ByteArrayDeserializer.class)
                    .build();
            final List<ConsumerRecord<byte[], byte[]>> lst = consumeMessages(topic, consumerProperties,
                    expectedMessageCount, timeout);
            return lst.stream().map(ConsumerRecord::value).collect(Collectors.toList());
        }

        /**
         * Read the data from the topic as Avro data the key is read as a string. This consumer uses the
         * {@link KafkaAvroDeserializer} to deserialize the values. The schema registry URL is the url provided by the
         * local KafkaManager. If the expected number of messages is not read in the allotted time the test fails.
         *
         * @param topic
         *            the topic to red.
         * @param expectedMessageCount
         *            the expected number of messages.
         * @param timeout
         *            the maximum time to wait for the messages to arrive.
         * @return A list of values returned.
         */
        public List<GenericRecord> consumeAvroMessages(final String topic, final int expectedMessageCount,
                final Duration timeout) {
            final Properties consumerProperties = consumerPropertiesBuilder()
                    .valueDeserializer(KafkaAvroDeserializer.class)
                    .schemaRegistry(getKafkaManager().getSchemaRegistryUrl())
                    .build();
            final List<ConsumerRecord<String, GenericRecord>> lst = consumeMessages(topic, consumerProperties,
                    expectedMessageCount, timeout);
            return lst.stream().map(ConsumerRecord::value).collect(Collectors.toList());
        }

        /**
         * Read the data from the topic as JSONL data the key is read as a string. This consumer uses the
         * {@link JsonDeserializer} to deserialize the values. If the expected number of messages is not read in the
         * allotted time the test fails.
         *
         * @param topic
         *            the topic to red.
         * @param expectedMessageCount
         *            the expected number of messages.
         * @param timeout
         *            the maximum time to wait for the messages to arrive.
         * @return A list of values returned.
         */
        public List<JsonNode> consumeJsonMessages(final String topic, final int expectedMessageCount,
                final Duration timeout) {
            final Properties consumerProperties = consumerPropertiesBuilder().valueDeserializer(JsonDeserializer.class)
                    .build();
            final List<ConsumerRecord<String, JsonNode>> lst = consumeMessages(topic, consumerProperties,
                    expectedMessageCount, timeout);
            return lst.stream().map(ConsumerRecord::value).collect(Collectors.toList());
        }

        /**
         * Read the data and key values from the topic. Teh consumer properties should be created with a call to
         * {@link #consumerPropertiesBuilder}. If the expected number of messages is not read in the allotted time the
         * test fails.
         *
         * @param topic
         *            the topic to red.
         * @param consumerProperties
         *            The consumer properties.
         * @param expectedMessageCount
         *            the expected number of messages.
         * @param timeout
         *            the maximum time to wait for the messages to arrive.
         * @param <X>
         *            The key type.
         * @param <V>
         *            the value type.
         * @return A list of values returned.
         */
        public <X, V> List<ConsumerRecord<X, V>> consumeMessages(final String topic,
                final Properties consumerProperties, final int expectedMessageCount, final Duration timeout) {
            try (KafkaConsumer<X, V> consumer = new KafkaConsumer<>(consumerProperties)) {
                consumer.subscribe(Collections.singletonList(topic));
                final List<ConsumerRecord<X, V>> recordValues = new ArrayList<>();
                await().atMost(timeout).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
                    assertThat(consumeRecordsInProgress(consumer, recordValues)).hasSize(expectedMessageCount);
                });
                return recordValues;
            }
        }

        /**
         * Consumes records in blocks and appends them to the {@code recordValues} parameter. This method polls the
         * consumer for 1/2 second and adds the result to the record values. As long as there are at more than 10
         * records returned it continues to poll. Once there are fewer than 10 records returned this mehtod returns.
         *
         * @param consumer
         *            The consumer to read from.
         * @param recordValues
         *            the record values to append to.
         * @return {@code recordValues}
         * @param <X>
         *            the key type.
         * @param <V>
         *            the value type.
         */
        private <X, V> List<ConsumerRecord<X, V>> consumeRecordsInProgress(final KafkaConsumer<X, V> consumer,
                final List<ConsumerRecord<X, V>> recordValues) {
            int recordsRetrieved;
            do {
                final ConsumerRecords<X, V> records = consumer.poll(Duration.ofMillis(500L));
                recordsRetrieved = records.count();
                records.forEach(recordValues::add);
                // Choosing 10 records as it allows for integration tests with a smaller max poll to be added
                // while maintaining efficiency, a slightly larger number could be added but this is slightly more
                // efficient
                // than larger numbers.
            } while (recordsRetrieved > 10);
            return recordValues;
        }

        /**
         * Read the data and key values from the topic. Teh consumer properties should be created with a call to
         * {@link #consumerPropertiesBuilder}. Returns all the records read in the specified time.
         *
         * @param topic
         *            the topic to red.
         * @param consumerProperties
         *            The consumer properties.
         * @param timeout
         *            the maximum time to wait for the messages to arrive.
         * @param <X>
         *            The key type.
         * @param <V>
         *            the value type.
         * @return A list of values returned.
         */
        public <X, V> List<ConsumerRecord<X, V>> consumeMessages(final String topic,
                final Properties consumerProperties, final Duration timeout) {
            try (KafkaConsumer<X, V> consumer = new KafkaConsumer<>(consumerProperties)) {
                consumer.subscribe(Collections.singletonList(topic));
                final List<ConsumerRecord<X, V>> recordValues = new ArrayList<>();
                final AbstractSourceTask.Timer timer = new AbstractSourceTask.Timer(timeout);
                timer.start();
                while (!timer.isExpired()) {
                    consumeRecordsInProgress(consumer, recordValues);
                }
                return recordValues;
            }
        }

        /**
         * Gets the list of consumer offset messages.
         *
         * @param consumer
         *            A consumer configured to return byte array keys and values.
         * @return the list of {@link OffsetManager.OffsetManagerEntry} records created by the system under test.
         * @throws IOException
         *             on IO error.
         */
        public List<O> consumeOffsetMessages(final KafkaConsumer<byte[], byte[]> consumer) throws IOException {
            // Poll messages from the topic
            final BiFunction<Map<String, Object>, Map<String, Object>, O> converter = offsetManagerEntryFactory();
            final ObjectMapper objectMapper = new ObjectMapper();
            final List<O> messages = new ArrayList<>();
            final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
            // TODO there is probably a way to clean this up by using the internal data types from Kafka.
            for (final ConsumerRecord<byte[], byte[]> record : records) {
                final Map<String, Object> data = objectMapper.readValue(record.value(), new TypeReference<>() { // NOPMD
                });
                // the key has the format
                // key[0] = connector name
                // key[1] = Map<String, Object> partition map.
                final List<Object> key = objectMapper.readValue(record.key(), new TypeReference<>() { // NOPMD
                });
                final Map<String, Object> managerEntryKey = (Map<String, Object>) key.get(1);
                messages.add(converter.apply(managerEntryKey, data));
            }
            return messages;
        }
    }

}
