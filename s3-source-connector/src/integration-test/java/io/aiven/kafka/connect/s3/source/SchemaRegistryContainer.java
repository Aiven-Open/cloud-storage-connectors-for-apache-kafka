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
        this("5.0.4", bootstrapServer);
    }

    public SchemaRegistryContainer(final String confluentPlatformVersion, final String bootstrapServer) {
        super("confluentinc/cp-schema-registry:" + confluentPlatformVersion);
        withAccessToHost(true);
        withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + bootstrapServer);
        withEnv("SCHEMA_REGISTRY_LOG4J_LOGLEVEL=DEBUG", "DEBUG");
        withExposedPorts(SCHEMA_REGISTRY_PORT);
        withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost");

        withCreateContainerCmdModifier(
                cmd -> cmd.getHostConfig().withUlimits(List.of(new Ulimit("nofile", 30_000L, 30_000L))));
    }

    public String getSchemaRegistryUrl() {
        return String.format("http://%s:%s", getHost(), getMappedPort(SCHEMA_REGISTRY_PORT));
    }
}
