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

import java.util.List;

import com.github.dockerjava.api.model.Ulimit;
import org.testcontainers.containers.GenericContainer;

public final class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {
    public static final int SCHEMA_REGISTRY_PORT = 8081;

    public SchemaRegistryContainer(final String bootstrapServer) {
        this("4.0.0", bootstrapServer);
    }

    public SchemaRegistryContainer(final String confluentPlatformVersion, final String bootstrapServer) {
        super("confluentinc/cp-schema-registry:" + confluentPlatformVersion);
        withAccessToHost(true);
        withEnv("KARAPACE_ADVERTISED_HOSTNAME", "karapace-registry");
        withEnv("KARAPACE_BOOTSTRAP_URI", "PLAINTEXT://" + bootstrapServer);
        withEnv("KARAPACE_PORT", String.valueOf(SCHEMA_REGISTRY_PORT));
        withEnv("KARAPACE_HOST", "localhost");
        withEnv("KARAPACE_CLIENT_ID", "karapace");
        withEnv("KARAPACE_GROUP_ID", "karapace-registry");
        withEnv("KARAPACE_MASTER_ELIGIBILITY", "true");
        withEnv("KARAPACE_TOPIC_NAME", "_schemas");
        withEnv("KARAPACE_LOG_LEVEL", "DEBUG");
        withEnv("KARAPACE_COMPATIBILITY", "FULL");
        withEnv("KARAPACE_STATSD_HOST", "statsd-exporter");
        withEnv("KARAPACE_STATSD_PORT", "8125");
        withEnv("KARAPACE_KAFKA_SCHEMA_READER_STRICT_MODE", "false");
        withEnv("KARAPACE_KAFKA_RETRIABLE_ERRORS_SILENCED", "true");
        withExposedPorts(SCHEMA_REGISTRY_PORT);

        withCreateContainerCmdModifier(
                cmd -> cmd.getHostConfig().withUlimits(List.of(new Ulimit("nofile", 30_000L, 30_000L))));
    }

    public String getSchemaRegistryUrl() {
        return String.format("http://%s:%s", getHost(), getMappedPort(SCHEMA_REGISTRY_PORT));
    }
}
