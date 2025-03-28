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

package io.aiven.kafka.connect.common.source;

import io.aiven.kafka.connect.common.config.KafkaFragment;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class KafkaConnectRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectRunner.class);

    private EmbeddedConnectCluster connectCluster;

    private final Duration offsetFlushInterval;


    private int containerListenerPort;

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

    public KafkaConnectRunner(final Duration offsetFlushInterval) {
        this.offsetFlushInterval = offsetFlushInterval;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getOffsetTopic() {
        return "connect-offset-topic-" + clusterName;
    }

    public String getConfigTopic() {
        return "connect-config-topic-" + clusterName;
    }

    public String getStorageTopic() {
        return "connect-storage-topic-" + clusterName;
    }

    public String getGroupId() {
        return "connect-integration-test-" + clusterName;
    }

    public void startConnectCluster(final String clusterName, Map<String, String> workerProperties) throws IOException {
        final List<Integer> ports =findListenerPorts();
        startConnectCluster(clusterName, ports.get(0), ports.get(1), workerProperties);
    }

    public void startConnectCluster(final String clusterName, final int localPort, final int containerPort, Map<String, String> workerProperties) {
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
                .workerProps(getWorkerProperties(workerProperties))
                .build();
        connectCluster.start();
        LOGGER.info("connectCluster {} started", clusterName);
    }

    public int getContainerPort() {
        return containerListenerPort;
    }

    public String getBootstrapServers() {
        return connectCluster.kafka().bootstrapServers();
    }

    public void deleteConnector(final String connectorName) {
        connectCluster.deleteConnector(connectorName);
    }

    public void stopConnectCluster() {
        // stop all Connect, Kafka and Zk threads.
        if (connectCluster != null) {
            connectCluster.stop();
        }
        LOGGER.info("connectCluster stopped");
    }

    public String configureConnector(final String connName, final Map<String, String> connConfig) {
        return connectCluster.configureConnector(connName, connConfig);
    }

    public Map<String, String> getWorkerProperties(Map<String, String> workerProperties) {
        final Map<String, String> workerProps = new HashMap<>();
        KafkaFragment.setter(workerProps)
                .keyConverter(ByteArrayConverter.class)
                .valueConverter(ByteArrayConverter.class)
                .internalKeyConverter(JsonConverter.class)
                .internalKeyConverterSchemaEnable(true)
                .internalValueConverter(JsonConverter.class)
                .internalValueConverterSchemaEnable(true)
                .pluginDiscovery(KafkaFragment.PluginDiscovery.HYBRID_WARN)
                .offsetFlushInterval(offsetFlushInterval);
        //workerProps.putAll(workerProperties);
        return workerProps;
    }
}
