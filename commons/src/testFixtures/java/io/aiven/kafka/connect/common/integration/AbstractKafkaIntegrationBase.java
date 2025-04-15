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


import io.aiven.kafka.connect.common.utils.CasedString;

import org.apache.kafka.connect.connector.Connector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.time.Duration;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.awaitility.Awaitility.await;

/**
 * The base abstract case for Kafka based integration tests.
 * <p>
 * This class handles the creation and destruction of a thread safe {@link KafkaManager}.
 * </p>
 */
public abstract class AbstractKafkaIntegrationBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractKafkaIntegrationBase.class);

    /**
     * The Test info provided before each test. Tests may access this info wihout capturing it themselves.
     */
    protected TestInfo testInfo;

    /** A thread local instance of the KafkaManager */
    private static final ThreadLocal<KafkaManager> KAFKA_MANAGER_THREAD_LOCAL = new ThreadLocal<>();

    /** The thread local instance of the connector name */
    private static final ThreadLocal<String> CONNECTOR_NAME_THREAD_LOCAL = new ThreadLocal<>() {
    };

    /**
     * Wait for a container to start. Waits up to 1 minute.
     *
     * @param container
     *            the container to wait for.
     */
    public static void waitForRunningContainer(final Container<?> container) {
        waitForRunningContainer(container, Duration.ofMinutes(1));
    }

    /**
     * Wait for a container to start.
     *
     * @param container
     *            the container to wait for.
     * @param timeout
     *            the length of time to wait for startup.
     */
    public static void waitForRunningContainer(final Container<?> container, final Duration timeout) {
        await().atMost(timeout).until(container::isRunning);
    }

    /**
     * Gets the name of the current connector.
     *
     * @return the name of the connector.
     */
    final protected String getConnectorName(Class<? extends Connector> connectorClass) {
        String result = CONNECTOR_NAME_THREAD_LOCAL.get();
        if (result == null) {
            result = new CasedString(CasedString.StringCase.CAMEL, connectorClass.getSimpleName())
                    .toCase(CasedString.StringCase.KEBAB)
                    .toLowerCase(Locale.ROOT) + "-" + UUID.randomUUID();
            CONNECTOR_NAME_THREAD_LOCAL.set(result);
        }
        return result;
    }


    /**
     * Get the topic from the TestInfo.
     *
     * @return The topic extracted from the testInfo for the current test.
     */
    final public String getTopic() {
        return testInfo.getTestMethod().get().getName();
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
    final protected KafkaManager setupKafka(Class<? extends Connector> connectorClass) throws IOException, ExecutionException, InterruptedException {
        return setupKafka(false, connectorClass);
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
     */
    final protected KafkaManager setupKafka(final boolean forceRestart, Class<? extends Connector> connectorClass)
            throws IOException {
        KafkaManager kafkaManager = KAFKA_MANAGER_THREAD_LOCAL.get();
        if (kafkaManager != null && forceRestart) {
            tearDownKafka();
        }
        kafkaManager = KAFKA_MANAGER_THREAD_LOCAL.get();
        if (kafkaManager == null) {
            final String clusterName = new CasedString(CasedString.StringCase.CAMEL,
                    testInfo.getTestClass().get().getSimpleName()).toCase(CasedString.StringCase.KEBAB)
                    .toLowerCase(Locale.ROOT);
            kafkaManager = new KafkaManager(clusterName, getOffsetFlushInterval(), connectorClass);
            KAFKA_MANAGER_THREAD_LOCAL.set(kafkaManager);
        }
        return kafkaManager;
    }

    /**
     * Tears down any existing KafkaManager. if the KafkaManager has not be created no action is taken.
     */
    final protected void tearDownKafka() {
        final KafkaManager kafkaManager = KAFKA_MANAGER_THREAD_LOCAL.get();
        if (kafkaManager != null) {
            kafkaManager.stop();
            KAFKA_MANAGER_THREAD_LOCAL.remove();
        }
    }

    /**
     * Delete the current connector from the running kafka.
     */
    final protected void deleteConnector(Class<? extends Connector> connectorClass) {
        final KafkaManager kafkaManager = KAFKA_MANAGER_THREAD_LOCAL.get();
        if (kafkaManager != null) {
            kafkaManager.deleteConnector(getConnectorName(connectorClass));
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
}
