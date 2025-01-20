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

package io.aiven.kafka.connect.common.source.task;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import org.apache.kafka.common.config.ConfigException;

public enum DistributionType {

    /**
     * Object_Hash takes the context and uses the storage key implementation to get a hash value of the storage key and
     * return a modulus of that relative to the number of maxTasks to decide which task should process a given object
     */
    OBJECT_HASH("object_hash",
            context -> context.getStorageKey().isPresent()
                    ? Optional.of((long) context.getStorageKey().get().hashCode())
                    : Optional.empty()),
    /**
     * Partition takes the context and requires the context contain the partition id for it to be able to decide the
     * distribution across the max tasks, using a modulus to ensure even distribution against the configured max tasks
     */
    PARTITION("partition",
            context -> context.getPartition().isPresent()
                    ? Optional.of((long) context.getPartition().get())
                    : Optional.empty());

    private final String name;
    private final Function<Context<?>, Optional<Long>> mutation;

    public String value() {
        return name;
    }

    /**
     * Get the Object distribution strategy for the configured ObjectDistributionStrategy
     *
     * @param name
     *            the name of the ObjectDistributionStrategy
     * @param mutation
     *            the mutation required to get the correct details from the context for distribution
     */
    DistributionType(final String name, final Function<Context<?>, Optional<Long>> mutation) {
        this.name = name;
        this.mutation = mutation;
    }

    public static DistributionType forName(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        for (final DistributionType distributionType : DistributionType.values()) {
            if (distributionType.name.equalsIgnoreCase(name)) {
                return distributionType;
            }
        }
        throw new ConfigException(String.format("Unknown distribution.type : %s, allowed values %s ", name,
                Arrays.toString(DistributionType.values())));
    }

    /**
     * Returns a configured Distribution Strategy
     *
     * @param maxTasks
     *            the maximum number of configured tasks for this connector
     *
     * @return a configured Distribution Strategy with the correct mutation configured for proper distribution across
     *         tasks of objects being processed.
     */
    public DistributionStrategy getDistributionStrategy(final int maxTasks) {
        return new DistributionStrategy(mutation, maxTasks);
    }
}
