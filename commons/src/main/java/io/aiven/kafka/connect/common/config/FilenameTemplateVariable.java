/*
 * Copyright 2021 Aiven Oy
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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public enum FilenameTemplateVariable {
    KEY("key"), //
    TOPIC("topic"), //
    PARTITION("partition",
            new ParameterDescriptor("padding", false, List.of(Boolean.TRUE.toString(), Boolean.FALSE.toString()))), //
    START_OFFSET("start_offset",
            new ParameterDescriptor("padding", false, List.of(Boolean.TRUE.toString(), Boolean.FALSE.toString()))), //
    TIMESTAMP("timestamp", new ParameterDescriptor("unit", true, List.of("yyyy", "MM", "dd", "HH")));

    public final String name;

    public final ParameterDescriptor parameterDescriptor;

    @SuppressWarnings({ "PMD.CompareObjectsWithEquals", "PMD.ConfusingTernary", "PMD.UselessParentheses" })
    public String description() {
        return (parameterDescriptor != ParameterDescriptor.NO_PARAMETER && !parameterDescriptor.values.isEmpty())
                ? String.join("=", String.join(":", name, parameterDescriptor.name), parameterDescriptor.toString())
                : name;
    }

    FilenameTemplateVariable(final String name) {
        this(name, ParameterDescriptor.NO_PARAMETER);
    }

    FilenameTemplateVariable(final String name, final ParameterDescriptor parameterDescriptor) {
        this.name = name;
        this.parameterDescriptor = parameterDescriptor;
    }

    @SuppressWarnings("PMD.ShortMethodName")
    public static FilenameTemplateVariable of(final String name) {
        for (final FilenameTemplateVariable v : FilenameTemplateVariable.values()) {
            if (v.name.equals(name)) {
                return v;
            }
        }
        throw new IllegalArgumentException(String.format("Unknown filename template variable: %s", name));
    }

    public static class ParameterDescriptor {

        public static final ParameterDescriptor NO_PARAMETER = new ParameterDescriptor("__no_parameter__", false,
                Collections.emptyList());

        public final String name;

        public final boolean required;

        public List<String> values;

        public ParameterDescriptor(final String name, final boolean required, final List<String> values) {
            this.name = name;
            this.required = required;
            this.values = values;
        }

        @Override
        public String toString() {
            return values.isEmpty() ? "" : String.join("|", values);
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof ParameterDescriptor)) {
                return false;
            }
            final ParameterDescriptor that = (ParameterDescriptor) other;
            return required == that.required && Objects.equals(name, that.name) && Objects.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, required, values);
        }
    }
}
