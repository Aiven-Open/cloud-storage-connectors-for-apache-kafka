/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.kafka.connect.gcs;

import java.util.List;

import com.github.dockerjava.api.model.Ulimit;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.Base58;

final class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {
    public static final int SCHEMA_REGISTRY_PORT = 8081;

    public SchemaRegistryContainer(final KafkaContainer kafka) {
        this("5.0.4", kafka);
    }

    public SchemaRegistryContainer(final String confluentPlatformVersion, final KafkaContainer kafka) {
        super("confluentinc/cp-schema-registry:" + confluentPlatformVersion);

        dependsOn(kafka);
        withNetwork(kafka.getNetwork());
        withNetworkAliases("schema-registry-" + Base58.randomString(6));

        withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                String.format("PLAINTEXT://%s:%s", kafka.getNetworkAliases().get(0), 9092));

        withExposedPorts(SCHEMA_REGISTRY_PORT);
        withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost");

        withCreateContainerCmdModifier(
                cmd -> cmd.getHostConfig().withUlimits(List.of(new Ulimit("nofile", 30_000L, 30_000L))));
    }

    public String getSchemaRegistryUrl() {
        return String.format("http://%s:%s", getHost(), getMappedPort(SCHEMA_REGISTRY_PORT));
    }
}
