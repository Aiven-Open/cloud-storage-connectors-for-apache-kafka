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

package io.aiven.commons.kafka.testkit;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.connect.util.clusters.WorkerHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs an embedded connect cluster.
 */
public final class KafkaConnectRunner {
    /**
     * The logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectRunner.class);

    /**
     * The cluster
     */
    private EmbeddedConnectCluster connectCluster;

    /** The time between offset topic flushes */
    private final Duration offsetFlushInterval;

    /** The container listener port */
    private int containerListenerPort;

    /** The cluster name */
    private String clusterName;

    /**
     * Finds 2 simultaneously free port for Kafka listeners
     *
     * @return list of 2 ports
     * @throws IOException
     *             when port allocation failure happens
     */
    static List<Integer> findListenerPorts() throws IOException {
        try (ServerSocket socket = new ServerSocket(0); ServerSocket socket2 = new ServerSocket(0)) {
            return Arrays.asList(socket.getLocalPort(), socket2.getLocalPort());
        } catch (IOException e) {
            throw new IOException("Failed to allocate port for test", e);
        }
    }

    /**
     * Create a connect runner with the specified flush interval.
     *
     * @param offsetFlushInterval
     *            The interval between flush calls to the offset topic.
     */
    public KafkaConnectRunner(final Duration offsetFlushInterval) {
        this.offsetFlushInterval = offsetFlushInterval;
    }

    /**
     * Gets the set of WorkerHandles in the cluster.
     *
     * @return a set of worker handles in the cluster.
     */
    public Set<WorkerHandle> listWorkers() {
        return this.connectCluster.workers();
    }

    /**
     * Gets the cluster name.
     *
     * @return the cluster name.
     */
    public String getClusterName() {
        return clusterName;
    }

    /**
     * Gets the offset topic name.
     *
     * @return the offset topic name.
     */
    public String getOffsetTopic() {
        return "connect-offset-topic-" + clusterName;
    }

    /**
     * Gets the configuration topic name.
     *
     * @return the configuration topic name.
     */
    public String getConfigTopic() {
        return "connect-config-topic-" + clusterName;
    }

    /**
     * Gets the storage topic name.
     *
     * @return the storage topic name.
     */
    public String getStorageTopic() {
        return "connect-storage-topic-" + clusterName;
    }

    /**
     * Gets the group ID.
     *
     * @return the group Id.
     */
    public String getGroupId() {
        return "connect-integration-test-" + clusterName;
    }

    /**
     * Starts a connect cluster.
     *
     * @param clusterName
     *            the name for the cluster
     * @param connectorClass
     *            the class for the connector.
     * @throws IOException
     *             if listener ports can not be found.
     */
    public void startConnectCluster(final String clusterName, final Class<? extends Connector> connectorClass)
            throws IOException {
        final List<Integer> ports = findListenerPorts();
        startConnectCluster(clusterName, ports.get(0), ports.get(1), connectorClass);
    }

    /**
     * Starts a connect cluster
     *
     * @param clusterName
     *            the name for the cluster
     * @param localPort
     *            the local port for the server.
     * @param containerPort
     *            the container port for the server.
     * @param connectorClass
     *            the class for the connector.
     */
    public void startConnectCluster(final String clusterName, final int localPort, final int containerPort,
            final Class<? extends Connector> connectorClass) {
        this.clusterName = clusterName;
        this.containerListenerPort = containerPort;
        final Properties brokerProperties = new Properties();
        brokerProperties.put("advertised.listeners", "PLAINTEXT://localhost:" + localPort
                + ",TESTCONTAINERS://host.testcontainers.internal:" + containerPort);
        brokerProperties.put("listeners",
                "PLAINTEXT://localhost:" + localPort + ",TESTCONTAINERS://localhost:" + containerPort);
        brokerProperties.put("listener.security.protocol.map", "PLAINTEXT:PLAINTEXT,TESTCONTAINERS:PLAINTEXT");

        connectCluster = new EmbeddedConnectCluster.Builder().name(clusterName)
                .brokerProps(brokerProperties)
                .workerProps(getWorkerProperties(connectorClass))
                .numWorkers(1)
                .build();
        connectCluster.start();
        LOGGER.info("connectCluster {} started", clusterName);
    }

    /**
     * Gets the container port.
     *
     * @return the container port
     */
    public int getContainerPort() {
        return containerListenerPort;
    }

    /**
     * Gets the bootstrap server URL as a string.
     *
     * @return the bootstrap server URL as a string.
     */
    public String getBootstrapServers() {
        return connectCluster.kafka().bootstrapServers();
    }

    /**
     * Deletes a container.
     *
     * @param connectorName
     *            the container to delete.
     */
    public void deleteConnector(final String connectorName) {
        if (connectCluster.connectors().contains(connectorName)) {
            connectCluster.deleteConnector(connectorName);
        }
    }

    /**
     * Stops the cluster.
     */
    public void stopConnectCluster() {
        // stop all Connect, Kafka and Zk threads.
        if (connectCluster != null) {
            connectCluster.stop();
        }
        LOGGER.info("connectCluster stopped");
    }

    /**
     * Configures a connector.
     *
     * @param connectorName
     *            The name for the connector.
     * @param connectorConfig
     *            the map of data items for the configuration of the connector.
     * @return the result of the cluster configuration call.
     */
    public String configureConnector(final String connectorName, final Map<String, String> connectorConfig) {
        return connectCluster.configureConnector(connectorName, connectorConfig);
    }

    /**
     * Pauses the named connector.
     *
     * @param connectorName
     *            the connector to pause.
     */
    public void pauseConnector(final String connectorName) {
        connectCluster.pauseConnector(connectorName);
    }

    /**
     * Resumes the named connector.
     *
     * @param connectorName
     *            the connector to resume.
     */
    public void resumeConnector(final String connectorName) {
        connectCluster.resumeConnector(connectorName);
    }

    /**
     * Restarts a connector.
     *
     * @param connectorName
     *            the name of the connector to restart.
     */
    public void restartConnector(final String connectorName) {
        if (connectCluster != null) {
            LOGGER.info("Restarting connector {}", connectorName);
            connectCluster.restartConnector(connectorName);
            LOGGER.info("Connector {} restarted", connectorName);

        }
    }

    /**
     * Get default worker properties. Sets the connector properties as follows:
     * <ul>
     * <li>key converter = ByteArrayConverter</li>
     * <li>value converter = ByteArrayConverter</li>
     * <li>plugin discovery = HYBRID_WARN</li>
     * <li>offset flush interval = interval defined in this class constructor.</li>
     * <li>connector class = connector class (if not {@code null}</li>
     * </ul>
     *
     * @param connectorClass
     *            the connector class to start, may be {@code null}.
     * @return the default set of worker properties.
     */
    public Map<String, String> getWorkerProperties(final Class<? extends Connector> connectorClass) {
        final Map<String, String> workerProperties = new HashMap<>();
        workerProperties.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, ByteArrayConverter.class.getCanonicalName());
        workerProperties.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, ByteArrayConverter.class.getCanonicalName());
        workerProperties.put(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG,
                Long.toString(offsetFlushInterval.toMillis()));
        workerProperties.put("plugin.discovery", "HYBRID_WARN");

        if (connectorClass != null) {
            workerProperties.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass.getName());
        }
        return workerProperties;
    }
}
