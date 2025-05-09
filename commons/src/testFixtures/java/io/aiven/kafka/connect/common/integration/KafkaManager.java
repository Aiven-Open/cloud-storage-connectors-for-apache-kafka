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

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.util.TopicAdmin;
import org.apache.kafka.connect.util.clusters.WorkerHandle;

import io.aiven.kafka.connect.common.source.KafkaConnectRunner;
import io.aiven.kafka.connect.common.source.SchemaRegistryContainer;

/**
 * Manages a containerized Kafka and some associated components.
 */
public final class KafkaManager {
    /**
     * Default partition count if not specified when creating topics. NOTE: if this is changed, update the javadocs in
     * the methods where this is used.
     */
    private final static int DEFAULT_PARTITION_COUNT = 4;

    /**
     * Default replication factor if not specified when creating topics. NOTE: if this is changed, update the javadocs
     * in the methods where this is used.
     */
    private final static short DEFAULT_REPLICATION_FACTOR = 1;
    /**
     * The topic administrator.
     */
    private final TopicAdmin topicAdmin;
    /**
     * A connect runner instance.
     */
    private final KafkaConnectRunner connectRunner;
    /**
     * A schema registry.
     */
    private final SchemaRegistryContainer schemaRegistry;

    /**
     * Constructor.
     *
     * @param clusterName
     *            The name for the cluster
     * @param offsetFlushInterval
     *            the offset topic flush interval.
     * @param connectorClass
     *            the connector class to execute.
     * @throws IOException
     *             if the cluster can not be started.
     */
    public KafkaManager(final String clusterName, final Duration offsetFlushInterval,
            final Class<? extends Connector> connectorClass) throws IOException {
        connectRunner = new KafkaConnectRunner(offsetFlushInterval);
        connectRunner.startConnectCluster(clusterName, connectorClass);

        final Map<String, Object> adminClientConfig = new HashMap<>();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, connectRunner.getBootstrapServers());
        topicAdmin = new TopicAdmin(adminClientConfig);

        // This should be done after the process listening the port is already started by host but
        // before the container that will access it is started.
        org.testcontainers.Testcontainers.exposeHostPorts(connectRunner.getContainerPort());
        schemaRegistry = new SchemaRegistryContainer(
                "host.testcontainers.internal:" + connectRunner.getContainerPort());
        schemaRegistry.start();
        KafkaIntegrationTestBase.waitForRunningContainer(schemaRegistry);
    }

    /**
     * Gets the cluster name.
     *
     * @return the cluster name
     */
    public String getClusterName() {
        return connectRunner.getClusterName();
    }

    /**
     * Gets the offset topic name.
     *
     * @return the offset topic name.
     */
    public String getOffsetTopic() {
        return connectRunner.getOffsetTopic();
    }

    /**
     * Gets the configuration topic name.
     *
     * @return the topic from the configuration.
     */
    public String getConfigTopic() {
        return connectRunner.getConfigTopic();
    }

    /**
     * Gets the storage topic name
     *
     * @return the storage topic name
     */
    public String getStorageTopic() {
        return connectRunner.getStorageTopic();
    }

    /**
     * Gets the groupID for the connector.
     *
     * @return the groupID for the connector.
     */
    public String getGroupId() {
        return connectRunner.getGroupId();
    }

    /**
     * Gets a list of all the current workers.
     *
     * @return a list of all the c urrent workers.
     */
    public Set<WorkerHandle> listWorkers() {
        return connectRunner.listWorkers();
    }

    /**
     * Creates topics on the admin client. Uses a partition count of 4, and a replication factor of 1.
     *
     * @param topic
     *            one or more topic names to create.
     * @throws ExecutionException
     *             on topic creation error.
     * @throws InterruptedException
     *             if operation is interrupted.
     */
    public void createTopics(final String... topic) throws ExecutionException, InterruptedException {
        createTopics(List.of(topic), DEFAULT_PARTITION_COUNT, DEFAULT_REPLICATION_FACTOR);
    }

    /**
     * Creates topics on the admin client.
     *
     * @param topics
     *            the list of topics to create.
     * @param partitions
     *            the number of partitions to use.
     * @param replicationFactor
     *            the replication factor to use.
     */
    public void createTopics(final List<String> topics, final int partitions, final short replicationFactor) {
        final NewTopic[] newTopics = topics.stream()
                .map(t -> new NewTopic(t, partitions, replicationFactor))
                .collect(Collectors.toList())
                .toArray(new NewTopic[topics.size()]);
        topicAdmin.createTopics(newTopics);
    }

    /**
     * Gets the bootstrap server URL as a string.
     *
     * @return the bootstrap server URL as a string.
     */
    public String bootstrapServers() {
        return connectRunner.getBootstrapServers();
    }

    /**
     * Configure the connector.
     *
     * @param connectorName
     *            the connector name.
     * @param connectorConfig
     *            the configuraiton for the connector.
     * @return the result of the configuration call.
     */
    public String configureConnector(final String connectorName, final Map<String, String> connectorConfig) {
        return connectRunner.configureConnector(connectorName, connectorConfig);
    }

    /**
     * Pauses the named connector.
     *
     * @param connectorName
     *            the connector to pause.
     */
    public void pauseConnector(final String connectorName) {
        connectRunner.pauseConnector(connectorName);
    }

    /**
     * resumes the named connector.
     *
     * @param connectorName
     *            the connector to pause.
     */
    public void resumeConnector(final String connectorName) {
        connectRunner.resumeConnector(connectorName);
    }

    /**
     * Deletes the connector.
     *
     * @param connectorName
     *            the connector to delete.
     */
    public void deleteConnector(final String connectorName) {
        connectRunner.deleteConnector(connectorName);
    }

    /**
     * Gets the schema registry URL as a string.
     *
     * @return the schema registry URL.
     */
    public String getSchemaRegistryUrl() {
        return schemaRegistry.getSchemaRegistryUrl();
    }

    /**
     * Stop the manager and release all objects.
     */
    public void stop() {
        topicAdmin.close();
        connectRunner.stopConnectCluster();
        schemaRegistry.stop();
    }

    /**
     * Restarts the connector.
     *
     * @param connectorName
     *            the connector to restart.
     */
    public void restartConnector(final String connectorName) {
        connectRunner.restartConnector(connectorName);
    }

}
