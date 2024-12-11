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

import java.time.Duration;

import com.github.dockerjava.api.model.Ulimit;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public final class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {
    public static final int SCHEMA_REGISTRY_PORT = 8081;

    public SchemaRegistryContainer(final String bootstrapServer) {
        this("4.1.0", bootstrapServer);
    }

    public SchemaRegistryContainer(final String karapaceVersion, final String bootstrapServer) {
        super("ghcr.io/aiven-open/karapace:" + karapaceVersion);
        withAccessToHost(true);
        withEnv("KARAPACE_ADVERTISED_HOSTNAME", "karapace-registry");
        withEnv("KARAPACE_BOOTSTRAP_URI", bootstrapServer);
        withEnv("KARAPACE_PORT", String.valueOf(SCHEMA_REGISTRY_PORT));
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
        withCommand("/bin/bash", "/opt/karapace/start.sh", "registry");

        // When started, check any API to see if the service is ready, which also indicates that it is connected to the
        // Kafka bootstrap server.
        waitingFor(Wait.forHttp("/_health")
                .forPort(8081)
                .withReadTimeout(Duration.ofMinutes(1))
                .forResponsePredicate(response -> response.contains("\"schema_registry_ready\":true")));

        withCreateContainerCmdModifier(
                cmd -> cmd.getHostConfig().withUlimits(new Ulimit[] { new Ulimit("nofile", 30_000L, 30_000L) }));
    }

    public String getSchemaRegistryUrl() {
        return String.format("http://%s:%s", getHost(), getMappedPort(SCHEMA_REGISTRY_PORT));

    }
}
