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

package io.aiven.kafka.connect.common.integration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.aiven.kafka.connect.common.source.AbstractSourceRecordIterator;
import io.aiven.kafka.connect.common.source.NativeInfo;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.utils.CasedString;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.runtime.WorkerSourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.nio.file.Path;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 *
 * @param <K> the native key type.
 */
//@SuppressWarnings("PMD.ExcessiveImports")
public abstract class AbstractIntegrationTest<K extends Comparable<K>, O extends OffsetManager.OffsetManagerEntry<O>,
        I extends AbstractSourceRecordIterator<?, K, O, ?>> {

    protected final static String CONNECT_OFFSET_TOPIC_PREFIX = "connect-offset-topic-";

    /**
     * The temporary directory for this integration test
     */
    @TempDir
    protected static Path tempDir;

    protected TestInfo testInfo;

    private static ThreadLocal<KafkaManager> kafkaManagerThreadLocal = new ThreadLocal<>();

    private static ThreadLocal<String> connectorNameThreadLocal = new ThreadLocal<>() {};

    abstract protected Logger getLogger();

    /**
     * Creates the native key.
     * @param prefix the prefix for the key.
     * @param topic the topic for the key,
     * @param partition the partition for the key.
     * @return the native Key.
     */
    abstract protected K createKey(String prefix, String topic, int partition);

    /**
     * Write file to storage with the specified key and data.
     *
     * @param nativeKey
     *            the key.
     * @param testDataBytes
     *            the data.
     */
    abstract protected WriteResult<K> writeWithKey(final K nativeKey, final byte[] testDataBytes);

    abstract protected List<? extends NativeInfo<?, K>> getNativeStorage();

    protected abstract Class<? extends Connector> getConnectorClass();

    final protected String getConnectorName() {
        String result = connectorNameThreadLocal.get();
        if (result == null) {
            result = new CasedString(CasedString.StringCase.CAMEL, getConnectorClass().getSimpleName()).toCase(CasedString.StringCase.KEBAB).toLowerCase(Locale.ROOT) + "-" + UUID.randomUUID();
            connectorNameThreadLocal.set(result);
        }
        return result;
    }

    /**
     * Creates the configuration data for the connector.
     * @return the configuration data for the Connector class under test.
     */
    protected abstract Map<String, String> createConnectorConfig(String localPrefix);

    protected Duration getOffsetFlushInterval() {
        return Duration.ofSeconds(5);
    }

    @BeforeEach
    void captureTestInfo(final TestInfo testInfo) throws IOException, ExecutionException, InterruptedException {
        this.testInfo = testInfo;
    }

    final protected KafkaManager setupKafka() throws IOException, ExecutionException, InterruptedException {
        return setupKafka(false);
    }

    final protected KafkaManager setupKafka(boolean forceRestart) throws IOException, ExecutionException, InterruptedException {
        KafkaManager kafkaManager = kafkaManagerThreadLocal.get();
        if (kafkaManager != null) {
            if (forceRestart) {
                tearDownKafka();
            }
        }
        kafkaManager = kafkaManagerThreadLocal.get();
        if (kafkaManager == null) {
            String clusterName = new CasedString(CasedString.StringCase.CAMEL, testInfo.getTestClass().get().getSimpleName()).toCase(CasedString.StringCase.KEBAB).toLowerCase(Locale.ROOT);
            kafkaManager = new KafkaManager(clusterName, getOffsetFlushInterval(), getConnectorClass());
            kafkaManagerThreadLocal.set(kafkaManager);
        }
        return kafkaManager;
    }

    final protected void tearDownKafka()  {
        KafkaManager kafkaManager = kafkaManagerThreadLocal.get();
        if (kafkaManager != null) {
            kafkaManager.stop();
            kafkaManagerThreadLocal.remove();
        }
    }

    final protected void deleteConnector() {
        KafkaManager kafkaManager = kafkaManagerThreadLocal.get();
        if (kafkaManager != null) {
            kafkaManager.deleteConnector(getConnectorName());
        }
        connectorNameThreadLocal.remove();
    }
    final protected KafkaManager getKafkaManager() {
        KafkaManager kafkaManager = kafkaManagerThreadLocal.get();
        if (kafkaManager == null) {
            throw new IllegalStateException("KafkaManager not initialized");
        }
        return kafkaManager;
    }

    @AfterAll
    static void removeKafkaManager() {
        kafkaManagerThreadLocal.remove();
    }
    /**
     * Returns a BiFunction that converts OffsetManager key and data into an OffsetManagerEntry for this system.
     * <p>
     * <ul>
     *     <li>The first argument to the method is the {@link OffsetManager.OffsetManagerEntry#getManagerKey()} value.</li>
     *     <li>The second argument is the {@link OffsetManager.OffsetManagerEntry#getProperties()} value.</li>
     * <li>Method should return a proper {@link OffsetManager.OffsetManagerEntry}</li>
     * </ul>
     * @return A BiFunction that crates an OffsetManagerEntry.
     */
    abstract protected BiFunction<Map<String, Object>, Map<String, Object>, O> offsetManagerEntryFactory();

    protected final MessageConsumer messageConsumer() {
        return new MessageConsumer();
    }

    /**
     * Creates a ConsumerPropertiesBuilder on our bootstrap server.
     * @return a ConsumerPropertiesBuilder on out bootstrap server.
     */
    protected final ConsumerPropertiesBuilder consumerPropertiesBuilder() {
        return new ConsumerPropertiesBuilder(getKafkaManager().bootstrapServers());
    }


    /**
     * Writes to storage. Does not use a prefix
     *
     * @param topic
     *            the topic for the file.
     * @param testDataBytes
     *            the data.
     * @param partition
     *            the partition id fo the file.
     * @return the WriteResult.
     */
    protected final WriteResult<K> write(final String topic, final byte[] testDataBytes, final int partition) {
        return write(topic, testDataBytes, partition, null);
    }

    /**
     * Writes to storage.  Uses {@link #createKey(String, String, int)} to create the key.
     *
     * @param topic
     *            the topic name to use
     * @param testDataBytes
     *            the data.
     * @param partition
     *            the partition id.
     * @param prefix the prefix for the key.
     * @return the WriteResult
     */
    protected final WriteResult<K> write(final String topic, final byte[] testDataBytes, final int partition, final String prefix) {
        final K objectKey = createKey(prefix, topic, partition);
        return writeWithKey(objectKey, testDataBytes);
    }

    /**
     * Get the topic from the TestInfo.
     * @return The topic extracted from the testInfo object.
     */
    public String getTopic() {
        return testInfo.getTestMethod().get().getName();
    }


    /**
     * Wait for a container to start.  Waits 1 minute.
     * @param container the container to wait for.
     */
    public static void waitForRunningContainer(final Container<?> container) {
        waitForRunningContainer(container, Duration.ofMinutes(1));
    }

    /**
     * Wait for a container to start.
     * @param container the container to wait for.
     * @param timeout the length of time to wait for startup.
     */
    public static void waitForRunningContainer(final Container<?> container, Duration timeout) {
        await().atMost(timeout).until(container::isRunning);
    }

    public OffsetManager<O> createOffsetManager(final Map<String, String> connectorConfig) {
        final WorkerSourceTaskContext context = mock(WorkerSourceTaskContext.class);
        final OffsetStorageReader reader = getKafkaManager().getOffsetReader(connectorConfig, getConnectorName());
        when(context.offsetStorageReader()).thenReturn(reader);
        return new OffsetManager<O>(context);
    }



//    private static final String COMMON_PREFIX = "s3-source-connector-for-apache-kafka-AWS-test-";
//
//    @Container
//    public static final LocalStackContainer LOCALSTACK = IntegrationBase.createS3Container();
//
//    private static String s3Prefix;
//
//    private S3Client s3Client;
//    private String s3Endpoint;
//
//    private BucketAccessor testBucketAccessor;
//
//    @Override
//    public String getS3Prefix() {
//        return s3Prefix;
//    }
//
//    @Override
//    public S3Client getS3Client() {
//        return s3Client;
//    }
//
//    @BeforeAll
//    static void setUpAll() {
//        s3Prefix = COMMON_PREFIX + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";
//    }
//
//    @BeforeEach
//    void setupAWS() {
//        s3Client = IntegrationBase.createS3Client(LOCALSTACK);
//        s3Endpoint = LOCALSTACK.getEndpoint().toString();
//        testBucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET_NAME);
//        testBucketAccessor.createBucket();
//    }
//
//    @AfterEach
//    void tearDownAWS() {
//        testBucketAccessor.removeBucket();
//        s3Client.close();
//    }
//
//    private Map<String, String> getConfig(final String topic, final int maxTasks) {
//        final Map<String, String> config = new HashMap<>();
//        config.put(AWS_ACCESS_KEY_ID_CONFIG, S3_ACCESS_KEY_ID);
//        config.put(AWS_SECRET_ACCESS_KEY_CONFIG, S3_SECRET_ACCESS_KEY);
//        config.put(AWS_S3_ENDPOINT_CONFIG, s3Endpoint);
//        config.put(AWS_S3_BUCKET_NAME_CONFIG, TEST_BUCKET_NAME);
//        config.put(AWS_S3_PREFIX_CONFIG, getS3Prefix());
//        config.put(TARGET_TOPIC, topic);
//        config.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
//        config.put(VALUE_CONVERTER_KEY, "org.apache.kafka.connect.converters.ByteArrayConverter");
//        config.put(MAX_TASKS, String.valueOf(maxTasks));
//        config.put(AWS_S3_FETCH_BUFFER_SIZE, "2");
//        return config;
//    }

    public class MessageConsumer {

        private MessageConsumer() {
            // use AbstractIntegrationTest.messageConsumer();
        }

        //  Duration.ofSeconds(120)
        public List<String> consumeByteMessages(final String topic, final int expectedMessageCount, Duration timeout) {
            final Properties consumerProperties = consumerPropertiesBuilder().keyDeserializer(ByteArrayDeserializer.class).valueDeserializer(ByteArrayDeserializer.class).build();
            List<ConsumerRecord<byte[], byte[]>> lst = consumeMessages(topic, consumerProperties, expectedMessageCount, timeout);
            return lst.stream().map(cr -> new String(cr.value())).collect(Collectors.toList());
        }

        // Duration.ofSeconds(60),
        public List<byte[]> consumeRawByteMessages(final String topic, final int expectedMessageCount, Duration timeout) {
            final Properties consumerProperties = consumerPropertiesBuilder().keyDeserializer(ByteArrayDeserializer.class).valueDeserializer(ByteArrayDeserializer.class).build();
            List<ConsumerRecord<byte[], byte[]>> lst = consumeMessages(topic, consumerProperties, expectedMessageCount, timeout);
            return lst.stream().map(ConsumerRecord::value).collect(Collectors.toList());
        }

        public List<GenericRecord> consumeAvroMessages(final String topic, final int expectedMessageCount, final String schemaRegistryUrl, final Duration timeout) {
            final Properties consumerProperties = consumerPropertiesBuilder().valueDeserializer(KafkaAvroDeserializer.class).schemaRegistry(schemaRegistryUrl).build();
            List<ConsumerRecord<String, GenericRecord>> lst = consumeMessages(topic, consumerProperties, expectedMessageCount, timeout);
            return lst.stream().map(ConsumerRecord::value).collect(Collectors.toList());
        }

        // Duration.ofSeconds(60)
        public List<JsonNode> consumeJsonMessages(final String topic, final int expectedMessageCount, final Duration timeout) {
            final Properties consumerProperties = consumerPropertiesBuilder().valueDeserializer(JsonDeserializer.class).build();
            List<ConsumerRecord<String, JsonNode>> lst = consumeMessages(topic, consumerProperties, expectedMessageCount, timeout);
            return lst.stream().map(ConsumerRecord::value).collect(Collectors.toList());
        }

        public <X, V> List<ConsumerRecord<X, V>> consumeMessages(final String topic, final Properties consumerProperties, final int expectedMessageCount,
                                           final Duration expectedMaxDuration) {
            try (KafkaConsumer<X, V> consumer = new KafkaConsumer<>(consumerProperties)) {
                consumer.subscribe(Collections.singletonList(topic));

                final List<ConsumerRecord<X, V>> recordValues = new ArrayList<>();
                await().atMost(expectedMaxDuration).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
                    assertThat(consumeRecordsInProgress(consumer, recordValues)).hasSize(expectedMessageCount);
                });
                return recordValues;
            }
        }

        private <X, V> List<ConsumerRecord<X, V>> consumeRecordsInProgress(KafkaConsumer<X, V> consumer, List<ConsumerRecord<X, V>> recordValues) {
            int recordsRetrieved;
            do {
                final ConsumerRecords<X, V> records = consumer.poll(Duration.ofMillis(500L));
                recordsRetrieved = records.count();
                records.forEach(recordValues::add);
                // Choosing 10 records as it allows for integration tests with a smaller max poll to be added
                // while maintaining efficiency, a slightly larger number could be added but this is slightly more efficient
                // than larger numbers.
            } while (recordsRetrieved > 10);
            return recordValues;
        }

        public List<O> consumeOffsetMessages(KafkaConsumer<byte[], byte[]> consumer) throws IOException {
            // Poll messages from the topic
            BiFunction<Map<String, Object>, Map<String, Object>, O> converter = offsetManagerEntryFactory();
            final TypeReference<Map<String, Object>> typeReference = new TypeReference<Map<String, Object>>() {};
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
                Map<String, Object> managerEntryKey = (Map<String, Object>) key.get(1);
                messages.add(converter.apply(managerEntryKey, data));
            }
            return messages;
        }
    }

    public static final class WriteResult<K extends Comparable<K>> {
        private final OffsetManager.OffsetManagerKey offsetKey;
        private final K nativeKey;
        public WriteResult(final OffsetManager.OffsetManagerKey offsetKey, final K nativeKey) {
           this.offsetKey = offsetKey;
           this.nativeKey = nativeKey;
        }
        public final OffsetManager.OffsetManagerKey getOffsetManagerKey() {
            return offsetKey;
        }
        K getNativeKey() {
            return nativeKey;
        }
    }
}
