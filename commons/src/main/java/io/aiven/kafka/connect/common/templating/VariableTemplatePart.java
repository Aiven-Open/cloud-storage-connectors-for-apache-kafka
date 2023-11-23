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

package io.aiven.kafka.connect.common.templating;

import java.util.Objects;

import io.aiven.kafka.connect.common.config.FilenameTemplateVariable;

public class VariableTemplatePart implements TemplatePart {

    private final String variableName;

    private final Parameter parameter;

    private final String originalPlaceholder;

    protected VariableTemplatePart(final String variableName, final String originalPlaceholder) {
        this(variableName, Parameter.EMPTY, originalPlaceholder);
    }

    protected VariableTemplatePart(final String variableName, final Parameter parameter,
            final String originalPlaceholder) {
        this.variableName = variableName;
        this.parameter = parameter;
        this.originalPlaceholder = originalPlaceholder;
    }

    public final String getVariableName() {
        return variableName;
    }

    public final Parameter getParameter() {
        return parameter;
    }

    public final String getOriginalPlaceholder() {
        return originalPlaceholder;
    }

    public static final class Parameter {

        public static final Parameter EMPTY = new Parameter("__EMPTY__", "__NO_VALUE__");

        private final String name;

        private final String value;

        private Parameter(final String name, final String value) {
            this.name = name;
            this.value = value;
        }

        public boolean isEmpty() {
            return this == EMPTY;
        }

        public String getName() {
            return name;
        }

        public String getValue() {
            return value;
        }

        public Boolean asBoolean() {
            return Boolean.parseBoolean(value);
        }

        @SuppressWarnings("PMD.ShortMethodName")
        public static Parameter of(final String name, final String value) {
            if (Objects.isNull(name) && Objects.isNull(value)) {
                return Parameter.EMPTY;
            } else {
                Objects.requireNonNull(name, "name has not been set");
                Objects.requireNonNull(value, "value has not been set");
                return new Parameter(name, value);
            }
        }

        @Override
        public String toString() {
            return name + "=" + value;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            final Parameter parameter = (Parameter) other;
            return Objects.equals(name, parameter.name) && Objects.equals(value, parameter.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, value);
        }

        public boolean matches(final FilenameTemplateVariable.ParameterDescriptor descriptor) {
            return descriptor.name.equals(name) && descriptor.values.contains(value);
        }
    }

}
