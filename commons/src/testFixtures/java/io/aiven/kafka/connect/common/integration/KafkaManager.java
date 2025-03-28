package io.aiven.kafka.connect.common.integration;

import io.aiven.kafka.connect.common.source.KafkaConnectRunner;
import io.aiven.kafka.connect.common.source.SchemaRegistryContainer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.ConnectorOffsetBackingStore;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.LoggingContext;
import org.apache.kafka.connect.util.TopicAdmin;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final  class KafkaManager {

    private final AdminClient adminClient;
    private final KafkaConnectRunner connectRunner;
    private final SchemaRegistryContainer schemaRegistry;

    public KafkaManager(String clusterName, Duration offsetFlushInterval, Map<String, String> workerProperties) throws IOException, ExecutionException, InterruptedException {
        connectRunner = new KafkaConnectRunner(offsetFlushInterval);
        connectRunner.startConnectCluster(clusterName, workerProperties);

        final Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, connectRunner.getBootstrapServers());
        adminClient =  AdminClient.create(adminClientConfig);

        // This should be done after the process listening the port is already started by host but
        // before the container that will access it is started.
        org.testcontainers.Testcontainers.exposeHostPorts(connectRunner.getContainerPort());
        schemaRegistry = new SchemaRegistryContainer("host.testcontainers.internal:" + connectRunner.getContainerPort());
        schemaRegistry.start();
        AbstractIntegrationTest.waitForRunningContainer(schemaRegistry);
    }

    public String getClusterName() {
        return connectRunner.getClusterName();
    }

    public String getOffsetTopic() {
        return connectRunner.getOffsetTopic();
    }

    public String getConfigTopic() {
        return connectRunner.getConfigTopic();
    }

    public String getStorageTopic() {
        return connectRunner.getStorageTopic();
    }

    public String getGroupId() {
        return connectRunner.getGroupId();
    }

    /**
     * Creates a kafka AdminClient.
     * @return tne Ad k  c.ke t
     */
    public AdminClient getAdminClient() {
        return adminClient;
    }

    /**
     * Creates topics on the admin client.  Uses a partition count of 4, and a replication factor of 1.
     * @param topic the topic to create.
     * @throws ExecutionException on topic creation error.
     * @throws InterruptedException if operation is interrupted.
     */
    public void createTopic(final String topic)
            throws ExecutionException, InterruptedException {
        createTopics(List.of(topic), 4, (short) 1);
    }

    /**
     * Creates topics on the admin client.  Uses a partition count of 4, and a replication factor of 1.
     * @param topics the list of topics to create.
     * @throws ExecutionException on topic creation error.
     * @throws InterruptedException if operation is interrupted.
     */
    public void createTopics(final List<String> topics)
            throws ExecutionException, InterruptedException {
        createTopics(topics, 4, (short) 1);
    }

    /**
     * Creates topics on the admin client.
     * @param topics the list of topics to create.
     * @throws ExecutionException on topic creation error.
     * @throws InterruptedException if operation is interrupted.
     */
    public void createTopics(final List<String> topics, final int partitions, final short replicationFactor)
            throws ExecutionException, InterruptedException {
        final var newTopics = topics.stream().map(s -> new NewTopic(s, partitions, replicationFactor)).collect(Collectors.toList());
        adminClient.createTopics(newTopics).all().get();
    }

    public String bootstrapServers() {
        return connectRunner.getBootstrapServers();
    }

    public String configureConnector(String connectorName, Map<String, String> connectorConfig) {
        return connectRunner.configureConnector(connectorName, connectorConfig);
    }

    public void deleteConnector(String connectorName) {
        connectRunner.deleteConnector(connectorName);
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistry.getSchemaRegistryUrl();
    }

    public Map<String, String> getWorkerProperties(Map<String, String> connectorProperties) {
        return connectRunner.getWorkerProperties(connectorProperties);
    }

    public void stop() {
        adminClient.close();
        //connectRunner.deleteConnector(getConnectorName());
        connectRunner.stopConnectCluster();
        schemaRegistry.stop();
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
     * @param topicAdminConfig
     *            the configuration for the administrative instance with the power to create topics when they do not exist.
     * @param consumerProperties
     *            A the properties for the consumer to read the topic.
     * @return A configured KafkaOffsetBackingStore which can be used as a WorkerStore
     */
    private KafkaOffsetBackingStore createKafkaOffsetBackingStore(final Map<String, String> topicAdminConfig, final Properties consumerProperties) {
        // create connector store
        //final KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties); // NOPMD close resource
        // after use
        // Create Topic Admin
        //final TopicAdmin topicAdmin = new TopicAdmin(new HashMap<>(topicAdminConfig)); // NOPMD close resource after use
        final Supplier<TopicAdmin> topicAdminSupplier = () -> new TopicAdmin(new HashMap<>(topicAdminConfig));
        return new KafkaOffsetBackingStore(topicAdminSupplier) {
            @Override
            public void configure(final WorkerConfig config) {
                this.exactlyOnce = config.exactlyOnceSourceEnabled();
                this.offsetLog = KafkaBasedLog.withExistingClients(getOffsetTopic(),  new KafkaConsumer<>(consumerProperties), null, topicAdminSupplier.get(), consumedCallback,
                        Time.SYSTEM, topicAdmin1 -> {
                        });

                this.offsetLog.start();

            }
        };
    }

    /**
     *
     * @param topicAdminConfig
     *            Internal Connector Config for creating and modifying topics.
     * @return Configured ConnectorOffsetBackingStore
     */

    private ConnectorOffsetBackingStore getConnectorOffsetBackingStore(final Map<String, String> topicAdminConfig) {

        final Properties consumerProperties = new ConsumerPropertiesBuilder(bootstrapServers()).keyDeserializer(ByteArrayDeserializer.class).valueDeserializer(ByteArrayDeserializer.class).build();

        consumerProperties.forEach((key, value) -> topicAdminConfig.putIfAbsent(key.toString(), (String) value));
        // Add config def
        final ConfigDef def = getRequiredConfigDefSettings();

        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        final KafkaOffsetBackingStore kafkaBackingStore = createKafkaOffsetBackingStore(topicAdminConfig, consumerProperties);
        kafkaBackingStore.configure(new WorkerConfig(def, getWorkerProperties(topicAdminConfig)));
        return ConnectorOffsetBackingStore.withOnlyWorkerStore(() -> LoggingContext.forConnector("source-connector"),
                kafkaBackingStore, getOffsetTopic());

    }


    // Create OffsetReader to read back in offsets

    /**
     * Create an offsetReader that is configured to use a preconfigured OffsetBackingStore and configures the
     * JsonConverters correctly.
     *
     * @param topicAdminConfig
     *            Internal Connector Config for creating and modifying topics.
     * @param connectorName
     *            The name of the connector.
     * @return Configured OffsetStorageReader
     */
    public OffsetStorageReader getOffsetReader(final Map<String, String> topicAdminConfig,  final String connectorName) {
        ConnectorOffsetBackingStore backingStore = getConnectorOffsetBackingStore(topicAdminConfig);

        final JsonConverter keyConverter = new JsonConverter(); // NOPMD close resource after use
        final JsonConverter valueConverter = new JsonConverter(); // NOPMD close resource after use
        keyConverter.configure(Map.of("schemas.enable", "false", "converter.type", "key"));
        valueConverter.configure(Map.of("schemas.enable", "false", "converter.type", "value"));
        return new OffsetStorageReaderImpl(backingStore, connectorName, keyConverter, valueConverter);
    }
}
