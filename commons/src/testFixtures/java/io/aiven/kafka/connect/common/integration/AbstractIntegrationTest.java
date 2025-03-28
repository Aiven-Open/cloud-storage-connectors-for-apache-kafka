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
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.WorkerSourceTaskContext;
import org.apache.kafka.connect.storage.ConnectorOffsetBackingStore;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.LoggingContext;
import org.apache.kafka.connect.util.TopicAdmin;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
abstract class AbstractIntegrationTest<K extends Comparable<K>, O extends OffsetManager.OffsetManagerEntry<O>,
        I extends AbstractSourceRecordIterator<?, K, O, ?>> {

    protected final static String CONNECT_OFFSET_TOPIC_PREFIX = "connect-offset-topic-";

    /**
     * The temporary directory for this integration test
     */
    @TempDir
    protected static Path tempDir;

    protected TestInfo testInfo;

    private static ThreadLocal<KafkaManager> kafkaManagerThreadLocal = new ThreadLocal<>();
    //private KafkaManager kafkaManager;

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
    abstract protected WriteResult writeWithKey(final K nativeKey, final byte[] testDataBytes);

    abstract protected List<? extends NativeInfo<?, K>> getNativeStorage();

    protected abstract String getConnectorName();

    /**
     * Creates the configuration data for the connector.
     * Must include result of call to {@link io.aiven.kafka.connect.common.config.KafkaFragment.Setter#connector(Class)} to define the connector to run.
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

    protected KafkaManager setupKafka(Map<String, String> connectorProperties) throws IOException, ExecutionException, InterruptedException {
        return setupKafka(false, connectorProperties);
    }

    protected KafkaManager setupKafka(boolean forceRestart, Map<String, String> connectorProperties) throws IOException, ExecutionException, InterruptedException {
        KafkaManager kafkaManager = kafkaManagerThreadLocal.get();
        if (kafkaManager != null) {
            if (forceRestart) {
                tearDownKafka();
            }
        }
        kafkaManager = kafkaManagerThreadLocal.get();
        if (kafkaManager == null) {
            kafkaManager = new KafkaManager(testInfo.getTestClass().get().getName(), getOffsetFlushInterval(), connectorProperties);
            kafkaManagerThreadLocal.set(kafkaManager);
        }
        return kafkaManager;
    }

    protected void tearDownKafka()  {
        KafkaManager kafkaManager = kafkaManagerThreadLocal.get();
        if (kafkaManager != null) {
            kafkaManager.stop();
            kafkaManagerThreadLocal.remove();
            //kafkaManager = null;
        }
    }

    protected KafkaManager getKafkaManager() {
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
    protected final WriteResult write(final String topic, final byte[] testDataBytes, final int partition) {
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
    protected final WriteResult write(final String topic, final byte[] testDataBytes, final int partition, final String prefix) {
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

    /**
     * Finds 2 simultaneously free port for Kafka listeners
     *
     * @return list of 2 ports
     * @throws IOException
     *             when port allocation failure happens
     */
    static List<Integer> getKafkaListenerPorts() throws IOException {
        try (ServerSocket socket = new ServerSocket(0); ServerSocket socket2 = new ServerSocket(0)) {
            return Arrays.asList(socket.getLocalPort(), socket2.getLocalPort());
        } catch (IOException e) {
            throw new IOException("Failed to allocate port for test", e);
        }
    }



    // Create OffsetReader to read back in offsets

    /**
     * Create an offsetReader that is configured to use a preconfigured OffsetBackingStore and configures the
     * JsonConverters correctly.
     *
     * @param backingStore
     *            OffsetBackingStore implementation which will read from the kafka offset topic
     * @param connectorName
     *            The name of the connector.
     * @return Configured OffsetStorageReader
     */
    static OffsetStorageReader getOffsetReader(final OffsetBackingStore backingStore, final String connectorName) {
        final JsonConverter keyConverter = new JsonConverter(); // NOPMD close resource after use
        final JsonConverter valueConverter = new JsonConverter(); // NOPMD close resource after use
        keyConverter.configure(Map.of("schemas.enable", "false", "converter.type", "key"));
        valueConverter.configure(Map.of("schemas.enable", "false", "converter.type", "value"));
        return new OffsetStorageReaderImpl(backingStore, connectorName, keyConverter, valueConverter);
    }

    /**
     *
     * @param bootstrapServers
     *            The bootstrap servers for the Kafka cluster to attach to
     * @param topicAdminConfig
     *            Internal Connector Config for creating and modifying topics
     * @param workerProperties
     *            The worker properties from the Kafka Connect Instance
     * @param connectorName
     *            The name of the connector
     * @return Configured ConnectorOffsetBackingStore
     */

    static ConnectorOffsetBackingStore getConnectorOffsetBackingStore(final String bootstrapServers,
                                                                      final Map<String, String> topicAdminConfig, final Map<String, String> workerProperties,
                                                                      final String connectorName) {
        final Properties consumerProperties = new ConsumerPropertiesBuilder(bootstrapServers).keyDeserializer(ByteArrayDeserializer.class).valueDeserializer(ByteArrayDeserializer.class).build();

        consumerProperties.forEach((key, value) -> topicAdminConfig.putIfAbsent(key.toString(), (String) value));
        // Add config def
        final ConfigDef def = getRequiredConfigDefSettings();

        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        // create connector store
        final KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties); // NOPMD close resource
        // after use
        // Create Topic Admin
        final TopicAdmin topicAdmin = new TopicAdmin(new HashMap<>(topicAdminConfig)); // NOPMD close resource after use

        final KafkaOffsetBackingStore kafkaBackingStore = createKafkaOffsetBackingStore(topicAdmin,
                CONNECT_OFFSET_TOPIC_PREFIX + connectorName, consumer);
        kafkaBackingStore.configure(new WorkerConfig(def, workerProperties));
        return ConnectorOffsetBackingStore.withOnlyWorkerStore(() -> LoggingContext.forConnector("source-connector"),
                kafkaBackingStore, CONNECT_OFFSET_TOPIC_PREFIX + connectorName);

    }

    /**
     * Returns the WorkerConfig ConfigDef with the must have configurations set.
     *
     * @return A configured ConfigDef
     */
    private static @NotNull ConfigDef getRequiredConfigDefSettings() {
        final ConfigDef def = new ConfigDef();
        def.define("offset.storage.partitions", ConfigDef.Type.INT, 25, ConfigDef.Importance.MEDIUM, "partitions");
        def.define("offset.storage.replication.factor", ConfigDef.Type.SHORT, Short.valueOf("2"),
                ConfigDef.Importance.MEDIUM, "partitions");
        return def;
    }

    /**
     *
     * @param topicAdmin
     *            An administrative instance with the power to create topics when they do not exist
     * @param topic
     *            The name of the offset topic
     * @param consumer
     *            A configured consumer that has not been assigned or subscribed to any topic
     * @return A configured KafkaOffsetBackingStore which can be used as a WorkerStore
     */
    static KafkaOffsetBackingStore createKafkaOffsetBackingStore(final TopicAdmin topicAdmin, final String topic,
                                                                 KafkaConsumer<byte[], byte[]> consumer) {
        return new KafkaOffsetBackingStore(() -> topicAdmin) {
            @Override
            public void configure(final WorkerConfig config) {
                this.exactlyOnce = config.exactlyOnceSourceEnabled();
                this.offsetLog = KafkaBasedLog.withExistingClients(topic, consumer, null, topicAdmin, consumedCallback,
                        Time.SYSTEM, topicAdmin1 -> {
                        });

                this.offsetLog.start();

            }
        };
    }

    /**
     * Configure a WorkerSourceTaskContext to return the offsetReader when called
     *
     * @param offsetReader
     *            An instantiated configured offsetReader
     * @return A mock WorkerSourceTaskContext
     */
    static WorkerSourceTaskContext getWorkerSourceTaskContext(final OffsetStorageReader offsetReader) {

        final WorkerSourceTaskContext context = mock(WorkerSourceTaskContext.class);
        when(context.offsetStorageReader()).thenReturn(offsetReader);

        return context;
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

            final List<byte[]> objects = consumeMessages(topic, consumerProperties, expectedMessageCount, timeout);
            return objects.stream().map(String::new).collect(Collectors.toList());
        }

        // Duration.ofSeconds(60),
        public List<byte[]> consumeRawByteMessages(final String topic, final int expectedMessageCount, Duration timeout) {
            final Properties consumerProperties = consumerPropertiesBuilder().keyDeserializer(ByteArrayDeserializer.class).valueDeserializer(ByteArrayDeserializer.class).build();
            final List<byte[]> objects = consumeMessages(topic, consumerProperties, expectedMessageCount,  timeout);
            return objects.stream().map(obj -> {
                final byte[] byteArray = new byte[obj.length];
                System.arraycopy(obj, 0, byteArray, 0, obj.length);
                return byteArray;
            }).collect(Collectors.toList());

        }

        public List<GenericRecord> consumeAvroMessages(final String topic, final int expectedMessageCount, final String schemaRegistryUrl, final Duration timeout) {
            final Properties consumerProperties = consumerPropertiesBuilder().valueDeserializer(KafkaAvroDeserializer.class).schemaRegistry(schemaRegistryUrl).build();
            return consumeMessages(topic, consumerProperties, expectedMessageCount, timeout);
        }

        // Duration.ofSeconds(60)
        public List<JsonNode> consumeJsonMessages(final String topic, final int expectedMessageCount, final Duration timeout) {
            final Properties consumerProperties = consumerPropertiesBuilder().valueDeserializer(JsonDeserializer.class).build();
            return consumeMessages(topic, consumerProperties, expectedMessageCount, timeout);
        }

        public <V> List<V> consumeMessages(final String topic, final Properties consumerProperties, final int expectedMessageCount,
                                           final Duration expectedMaxDuration) {
            try (KafkaConsumer<?, V> consumer = new KafkaConsumer<>(consumerProperties)) {
                consumer.subscribe(Collections.singletonList(topic));

                final List<V> recordValues = new ArrayList<>();
                await().atMost(expectedMaxDuration).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
                    assertThat(consumeRecordsInProgress(consumer, recordValues)).hasSize(expectedMessageCount);
                });
                return recordValues;
            }
        }

        private <V> List<V> consumeRecordsInProgress(KafkaConsumer<?, V> consumer, List<V> recordValues) {
            int recordsRetrieved;
            do {
                final ConsumerRecords<?, V> records = consumer.poll(Duration.ofMillis(500L));
                recordsRetrieved = records.count();
                records.forEach(record -> {recordValues.add(record.value());});
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
            for (final ConsumerRecord<byte[], byte[]> record : records) {
                final Map<String, Object> data = objectMapper.readValue(record.value(), new TypeReference<>() { // NOPMD
                });
                // the key has the format
                // key[0] = connector name
                // key[1] = Map<String, Object> partition map.
                final List<Object> key = objectMapper.readValue(record.key(), new TypeReference<>() { // NOPMD
                });
                messages.add(converter.apply(data, (Map<String, Object>) key.get(1)));
            }
            return messages;
        }
    }

    public final class WriteResult {
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
