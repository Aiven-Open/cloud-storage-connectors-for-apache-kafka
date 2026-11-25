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

import java.time.Duration;

import com.github.dockerjava.api.model.Ulimit;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * A container for the schema registry.
 */
public final class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {
    /** The schema registry local port */
    public static final int SCHEMA_REGISTRY_PORT = 8081;

    /** The default Karapace version used when none is specified. */
    public static final String DEFAULT_KARAPACE_VERSION = "6.2.4";

    /**
     * Constructs the container with the default Karapace version ({@value #DEFAULT_KARAPACE_VERSION}).
     *
     * @param bootstrapServer
     *            the url of the kafka bootstrap server.
     */
    public SchemaRegistryContainer(final String bootstrapServer) {
        this(DEFAULT_KARAPACE_VERSION, bootstrapServer);
    }

    /**
     * Constructs the container.
     *
     * @param karapaceVersion
     *            the version of karapace to run. Must be 5.0.0 or later; older images used a different start script.
     * @param bootstrapServer
     *            the url of the kafka bootstrap server.
     */
    public SchemaRegistryContainer(final String karapaceVersion, final String bootstrapServer) {
        super("ghcr.io/aiven-open/karapace:" + karapaceVersion);
        withAccessToHost(true);
        withEnv("KARAPACE_KARAPACE_REGISTRY", "true");
        withEnv("KARAPACE_ADVERTISED_HOSTNAME", "karapace-registry");
        withEnv("KARAPACE_BOOTSTRAP_URI", bootstrapServer);
        withEnv("KARAPACE_PORT", String.valueOf(SCHEMA_REGISTRY_PORT));
        // Lets Karapace recognise its own advertised URL as the primary, instead of trying to forward to it.
        withEnv("KARAPACE_ADVERTISED_PORT", String.valueOf(SCHEMA_REGISTRY_PORT));
        withEnv("KARAPACE_HOST", "0.0.0.0");
        withEnv("KARAPACE_CLIENT_ID", "karapace");
        withEnv("KARAPACE_GROUP_ID", "karapace-registry");
        withEnv("KARAPACE_MASTER_ELIGIBILITY", "true");
        withEnv("KARAPACE_TOPIC_NAME", "_schemas");
        withEnv("KARAPACE_LOG_LEVEL", "WARNING");// This can be set to DEBUG for more verbose logging
        withEnv("KARAPACE_COMPATIBILITY", "FULL");
        withEnv("KARAPACE_KAFKA_SCHEMA_READER_STRICT_MODE", "false");
        withEnv("KARAPACE_KAFKA_RETRIABLE_ERRORS_SILENCED", "true");
        withExposedPorts(SCHEMA_REGISTRY_PORT);
        // Forward Karapace output to the test logs (requires an SLF4J provider on the test classpath).
        withLogConsumer(
                new Slf4jLogConsumer(LoggerFactory.getLogger(SchemaRegistryContainer.class)).withPrefix("karapace"));
        // Karapace 5.x+ no longer ships start.sh; the service is launched as a python module.
        withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("python3", "-m", "karapace"));

        // Wait until the schema reader has caught up with Kafka AND this node acts as primary. Since Karapace 5.x a
        // newly elected primary waits a few seconds before accepting writes; registering a schema before that fails.
        waitingFor(Wait.forHttp("/_health")
                .forPort(SCHEMA_REGISTRY_PORT)
                .withReadTimeout(Duration.ofMinutes(1))
                .forResponsePredicate(response -> response.contains("\"schema_registry_ready\":true")
                        && response.contains("\"schema_registry_is_primary\":true"))
                .withStartupTimeout(Duration.ofMinutes(2)));

        withCreateContainerCmdModifier(
                cmd -> cmd.getHostConfig().withUlimits(new Ulimit[] { new Ulimit("nofile", 30_000L, 30_000L) }));
    }

    /**
     * Get the schema registry URL for this container.
     *
     * @return the schema registry URL as a string.
     */
    public String getSchemaRegistryUrl() {
        return String.format("http://%s:%s", getHost(), getMappedPort(SCHEMA_REGISTRY_PORT));

    }
}
