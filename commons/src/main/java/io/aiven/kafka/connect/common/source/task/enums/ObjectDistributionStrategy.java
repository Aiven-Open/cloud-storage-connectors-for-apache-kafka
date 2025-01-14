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

package io.aiven.kafka.connect.common.source.task.enums;

import java.util.Arrays;
import java.util.Objects;

import org.apache.kafka.common.config.ConfigException;

public enum ObjectDistributionStrategy {

    OBJECT_HASH("object_hash"), PARTITION_IN_FILENAME("partition_in_filename");

    private final String name;

    public String value() {
        return name;
    }

    ObjectDistributionStrategy(final String name) {
        this.name = name;
    }

    public static ObjectDistributionStrategy forName(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        for (final ObjectDistributionStrategy objectDistributionStrategy : ObjectDistributionStrategy.values()) {
            if (objectDistributionStrategy.name.equalsIgnoreCase(name)) {
                return objectDistributionStrategy;
            }
        }
        throw new ConfigException(String.format("Unknown object.distribution.strategy type: %s, allowed values %s ",
                name, Arrays.toString(ObjectDistributionStrategy.values())));
    }
}
