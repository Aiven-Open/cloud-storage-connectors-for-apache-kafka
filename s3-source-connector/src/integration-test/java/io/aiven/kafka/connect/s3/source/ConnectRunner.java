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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ConnectRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectRunner.class);

    private EmbeddedConnectCluster connectCluster;

    private final int offsetFlushIntervalMs;

    public ConnectRunner(final int offsetFlushIntervalMs) {
        this.offsetFlushIntervalMs = offsetFlushIntervalMs;
    }

    void startConnectCluster(final String connectorName, final int localPort, final int containerPort) {

        final Properties brokerProperties = new Properties();
        brokerProperties.put("advertised.listeners", "PLAINTEXT://localhost:" + localPort
                + ",TESTCONTAINERS://host.testcontainers.internal:" + containerPort);
        brokerProperties.put("listeners",
                "PLAINTEXT://localhost:" + localPort + ",TESTCONTAINERS://localhost:" + containerPort);
        brokerProperties.put("listener.security.protocol.map", "PLAINTEXT:PLAINTEXT,TESTCONTAINERS:PLAINTEXT");

        connectCluster = new EmbeddedConnectCluster.Builder().name(connectorName)
                .brokerProps(brokerProperties)
                .workerProps(getWorkerProperties())
                .build();
        connectCluster.start();
        LOGGER.info("connectCluster started");
    }

    String getBootstrapServers() {
        return connectCluster.kafka().bootstrapServers();
    }

    void deleteConnector(final String connectorName) {
        connectCluster.deleteConnector(connectorName);
    }

    void stopConnectCluster() {
        // stop all Connect, Kafka and Zk threads.
        if (connectCluster != null) {
            connectCluster.stop();
        }
        LOGGER.info("connectCluster stopped");
    }

    String configureConnector(final String connName, final Map<String, String> connConfig) {
        return connectCluster.configureConnector(connName, connConfig);
    }

    private Map<String, String> getWorkerProperties() {
        final Map<String, String> workerProps = new HashMap<>();

        workerProps.put("offset.flush.interval.ms", Integer.toString(offsetFlushIntervalMs));

        // These don't matter much (each connector sets its own converters), but need to be filled with valid classes.
        workerProps.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        workerProps.put("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter.schemas.enable", "true");
        workerProps.put("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.value.converter.schemas.enable", "true");

        workerProps.put("plugin.discovery", "hybrid_warn");

        return workerProps;
    }
}
