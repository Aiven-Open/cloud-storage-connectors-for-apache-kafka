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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
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
import java.util.stream.Collectors;

import static io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry.OBJECT_KEY;
import static io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry.RECORD_COUNT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("PMD.ExcessiveImports")
public interface IntegrationBase {
    String PLUGINS_S3_SOURCE_CONNECTOR_FOR_APACHE_KAFKA = "plugins/s3-source-connector-for-apache-kafka/";
    String S3_SOURCE_CONNECTOR_FOR_APACHE_KAFKA_TEST = "s3-source-connector-for-apache-kafka-test-";
    ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    String TEST_BUCKET_NAME = "test-bucket0";
    String S3_ACCESS_KEY_ID = "test-key-id0";
    String VALUE_CONVERTER_KEY = "value.converter";
    String S3_SECRET_ACCESS_KEY = "test_secret_key0";
    String CONNECT_OFFSET_TOPIC_PREFIX = "connect-offset-topic-";

    static byte[] generateNextAvroMessagesStartingFromId(final int messageId, final int noOfAvroRecs,
            final Schema schema) throws IOException {
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            dataFileWriter.create(schema, outputStream);
            for (int i = messageId; i < messageId + noOfAvroRecs; i++) {
                final GenericRecord avroRecord = new GenericData.Record(schema); // NOPMD
                avroRecord.put("message", "Hello, Kafka Connect S3 Source! object " + i);
                avroRecord.put("id", i);
                dataFileWriter.append(avroRecord);
            }

            dataFileWriter.flush();
            return outputStream.toByteArray();
        }
    }

    S3Client getS3Client();

    String getS3Prefix();

    /**
     * Write file to s3 with the specified key and data.
     *
     * @param objectKey
     *            the key.
     * @param testDataBytes
     *            the data.
     */
    default void writeToS3WithKey(final String objectKey, final byte[] testDataBytes) {
        final PutObjectRequest request = PutObjectRequest.builder()
                .bucket(IntegrationTest.TEST_BUCKET_NAME)
                .key(objectKey)
                .build();
        getS3Client().putObject(request, RequestBody.fromBytes(testDataBytes));

    }

    /**
     * Writes to S3 using a key of the form {@code [prefix]topic-partitionId-systemTime.txt}.
     *
     * @param topic
     *            the topic name to use
     * @param testDataBytes
     *            the data.
     * @param partitionId
     *            the partition id.
     * @return the key prefixed by {@link io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry#OBJECT_KEY} and
     *         {@link OffsetManager}
     */
    default String writeToS3(final String topic, final byte[] testDataBytes, final String partitionId) {
        return writeToS3(topic, testDataBytes, partitionId, getS3Prefix());

    }

    /**
     * Writes to S3 using a key of the form {@code [prefix]topic-partitionId-systemTime.txt}.
     *
     * @param topic
     *            the topic name to use
     * @param testDataBytes
     *            the data.
     * @param partitionId
     *            the partition id.
     * @param s3Prefix
     *            the S3 prefix to add to the S3 Object key
     * @return the key prefixed by {@link io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry#OBJECT_KEY} and
     *         {@link OffsetManager}
     */
    default String writeToS3(final String topic, final byte[] testDataBytes, final String partitionId,
            final String s3Prefix) {
        final String objectKey = org.apache.commons.lang3.StringUtils.defaultIfBlank(s3Prefix, "") + topic + "-"
                + partitionId + "-" + System.currentTimeMillis() + ".txt";
        writeToS3WithKey(objectKey, testDataBytes);
        return objectKey;

    }

    default AdminClient newAdminClient(final String bootstrapServers) {
        final Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(adminClientConfig);
    }

    static void extractConnectorPlugin(Path pluginDir) throws IOException, InterruptedException {
        final File distFile = new File(System.getProperty("integration-test.distribution.file.path"));
        assertThat(distFile).exists();

        final String cmd = String.format("tar -xf %s --strip-components=1 -C %s", distFile, pluginDir.toString());
        final Process process = Runtime.getRuntime().exec(cmd);
        assert process.waitFor() == 0;
    }

    static Path getPluginDir() throws IOException {
        final Path testDir = Files.createTempDirectory(S3_SOURCE_CONNECTOR_FOR_APACHE_KAFKA_TEST);
        return Files.createDirectories(testDir.resolve(PLUGINS_S3_SOURCE_CONNECTOR_FOR_APACHE_KAFKA));
    }

    static String getTopic(final TestInfo testInfo) {
        return testInfo.getTestMethod().get().getName();
    }

    static void createTopics(final AdminClient adminClient, final List<String> topics)
            throws ExecutionException, InterruptedException {
        final var newTopics = topics.stream().map(s -> new NewTopic(s, 4, (short) 1)).collect(Collectors.toList());
        adminClient.createTopics(newTopics).all().get();
    }

    static void waitForRunningContainer(final Container<?> container) {
        await().atMost(Duration.ofMinutes(1)).until(container::isRunning);
    }

    static S3Client createS3Client(final LocalStackContainer localStackContainer) {
        return S3Client.builder()
                .endpointOverride(
                        URI.create(localStackContainer.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
                .region(Region.of(localStackContainer.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials
                        .create(localStackContainer.getAccessKey(), localStackContainer.getSecretKey())))
                .build();
    }

    static LocalStackContainer createS3Container() {
        return new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.0.2"))
                .withServices(LocalStackContainer.Service.S3);
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

    static List<String> consumeByteMessages(final String topic, final int expectedMessageCount,
            String bootstrapServers) {
        final Properties consumerProperties = getConsumerProperties(bootstrapServers, ByteArrayDeserializer.class,
                ByteArrayDeserializer.class);
        final List<byte[]> objects = consumeMessages(topic, expectedMessageCount, Duration.ofSeconds(120),
                consumerProperties);
        return objects.stream().map(String::new).collect(Collectors.toList());
    }

    static List<byte[]> consumeRawByteMessages(final String topic, final int expectedMessageCount,
            String bootstrapServers) {
        final Properties consumerProperties = getConsumerProperties(bootstrapServers, ByteArrayDeserializer.class,
                ByteArrayDeserializer.class);
        final List<byte[]> objects = consumeMessages(topic, expectedMessageCount, Duration.ofSeconds(60),
                consumerProperties);
        return objects.stream().map(obj -> {
            final byte[] byteArray = new byte[obj.length];
            System.arraycopy(obj, 0, byteArray, 0, obj.length);
            return byteArray;
        }).collect(Collectors.toList());

    }

    static List<GenericRecord> consumeAvroMessages(final String topic, final int expectedMessageCount,
            final Duration expectedMaxDuration, final String bootstrapServers, final String schemaRegistryUrl) {
        final Properties consumerProperties = getConsumerProperties(bootstrapServers, StringDeserializer.class,
                KafkaAvroDeserializer.class, schemaRegistryUrl);
        return consumeMessages(topic, expectedMessageCount, expectedMaxDuration, consumerProperties);
    }

    static List<JsonNode> consumeJsonMessages(final String topic, final int expectedMessageCount,
            final String bootstrapServers) {
        final Properties consumerProperties = getConsumerProperties(bootstrapServers, StringDeserializer.class,
                JsonDeserializer.class);
        return consumeMessages(topic, expectedMessageCount, Duration.ofSeconds(60), consumerProperties);
    }

    static <K, V> List<V> consumeMessages(final String topic, final int expectedMessageCount,
            final Duration expectedMaxDuration, final Properties consumerProperties) {
        try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(Collections.singletonList(topic));

            final List<V> recordValues = new ArrayList<>();
            await().atMost(expectedMaxDuration).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
                assertThat(consumeRecordsInProgress(consumer, recordValues)).hasSize(expectedMessageCount);
            });
            return recordValues;
        }
    }

    private static <K, V> List<V> consumeRecordsInProgress(KafkaConsumer<K, V> consumer, List<V> recordValues) {
        int recordsRetrieved;
        do {
            final ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(500L));
            recordsRetrieved = records.count();
            for (final ConsumerRecord<K, V> record : records) {
                recordValues.add(record.value());
            }
            // Choosing 10 records as it allows for integration tests with a smaller max poll to be added
            // while maintaining efficiency, a slightly larger number could be added but this is slightly more efficient
            // than larger numbers.
        } while (recordsRetrieved > 10);
        return recordValues;
    }

    static Map<String, Object> consumeOffsetMessages(KafkaConsumer<byte[], byte[]> consumer) throws IOException {
        // Poll messages from the topic
        final Map<String, Object> messages = new HashMap<>();
        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            final Map<String, Long> offsetRec = OBJECT_MAPPER.readValue(record.value(), new TypeReference<>() { // NOPMD
            });
            final List<Object> key = OBJECT_MAPPER.readValue(record.key(), new TypeReference<>() { // NOPMD
            });
            // key.get(0) will return the name of the connector the commit is from.
            final Map<String, Object> keyDetails = (Map<String, Object>) key.get(1);
            messages.put((String) keyDetails.get(OBJECT_KEY), offsetRec.get(RECORD_COUNT));
        }
        return messages;
    }

    static <K, V> Properties getConsumerProperties(String bootstrapServers,
            Class<? extends Deserializer<K>> keyDeserializer, Class<? extends Deserializer<V>> valueDeserializer,
            String schemaRegistryUrl) {
        final Properties props = getConsumerProperties(bootstrapServers, keyDeserializer, valueDeserializer);
        props.put("specific.avro.reader", "false");    // Use GenericRecord instead of specific Avro classes
        props.put("schema.registry.url", schemaRegistryUrl); // URL of the schema registry
        return props;
    }

    static <K, V> Properties getConsumerProperties(String bootstrapServers,
            Class<? extends Deserializer<K>> keyDeserializer, Class<? extends Deserializer<V>> valueDeserializer) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
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
        final Properties consumerProperties = IntegrationBase.getConsumerProperties(bootstrapServers,
                ByteArrayDeserializer.class, ByteArrayDeserializer.class);

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
}
