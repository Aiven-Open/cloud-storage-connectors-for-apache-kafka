package io.aiven.kafka.connect.common.integration;

import io.aiven.kafka.connect.common.source.KafkaConnectRunner;
import io.aiven.kafka.connect.common.source.SchemaRegistryContainer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
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
}
