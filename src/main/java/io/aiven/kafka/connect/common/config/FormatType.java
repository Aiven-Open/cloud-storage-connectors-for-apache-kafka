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

package io.aiven.kafka.connect.common.config;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

public enum FormatType {

    CSV("csv"),
    JSON("json"),
    JSONL("jsonl");

    public static final String SUPPORTED_FORMAT_TYPES =
            FormatType.names().stream()
                    .map(c -> String.format("'%s'", c))
                    .collect(Collectors.joining(", "));

    public final String name;

    private FormatType(final String name) {
        this.name = name;
    }

    public static FormatType forName(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        for (final FormatType ct : FormatType.values()) {
            if (ct.name.equalsIgnoreCase(name)) {
                return ct;
            }
        }
        throw new IllegalArgumentException("Unknown compression type: " + name);
    }

    public static Collection<String> names() {
        return Arrays.stream(values()).map(v -> v.name).collect(Collectors.toList());
    }
}
