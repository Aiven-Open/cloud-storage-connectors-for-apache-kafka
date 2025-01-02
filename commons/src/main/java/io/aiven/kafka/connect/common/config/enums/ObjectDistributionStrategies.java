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

package io.aiven.kafka.connect.common.config.enums;

import java.util.Arrays;
import java.util.Objects;

import org.apache.kafka.common.config.ConfigException;

public enum ObjectDistributionStrategies {

    OBJECT_HASH("object_hash"), PARTITION_IN_FILENAME("partition_in_filename"), PARTITION_IN_FILEPATH(
            "partition_in_filepath");

    private final String name;

    ObjectDistributionStrategies(final String name) {
        this.name = name;
    }

    public static ObjectDistributionStrategies forName(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        for (final ObjectDistributionStrategies objectDistributionStrategies : ObjectDistributionStrategies.values()) {
            if (objectDistributionStrategies.name.equalsIgnoreCase(name)) {
                return objectDistributionStrategies;
            }
        }
        throw new ConfigException(String.format("Unknown object.distribution.strategy type: %s, allowed values %s ",
                name, Arrays.toString(ObjectDistributionStrategies.values())));
    }
}
