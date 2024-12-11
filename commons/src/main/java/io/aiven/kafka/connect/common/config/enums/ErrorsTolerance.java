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

public enum ErrorsTolerance {

    NONE("none"), ALL("all");

    private final String name;

    ErrorsTolerance(final String name) {
        this.name = name;
    }

    public static ErrorsTolerance forName(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        for (final ErrorsTolerance errorsTolerance : ErrorsTolerance.values()) {
            if (errorsTolerance.name.equalsIgnoreCase(name)) {
                return errorsTolerance;
            }
        }
        throw new ConfigException(String.format("Unknown errors.tolerance type: %s, allowed values %s ", name,
                Arrays.toString(ErrorsTolerance.values())));
    }
}
