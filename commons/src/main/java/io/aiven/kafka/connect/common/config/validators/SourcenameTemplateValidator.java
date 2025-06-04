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

package io.aiven.kafka.connect.common.config.validators;

import static io.aiven.kafka.connect.common.config.FilenameTemplateVariable.PARTITION;
import static io.aiven.kafka.connect.common.config.FilenameTemplateVariable.ParameterDescriptor;
import static io.aiven.kafka.connect.common.config.FilenameTemplateVariable.START_OFFSET;
import static io.aiven.kafka.connect.common.config.FilenameTemplateVariable.TIMESTAMP;
import static io.aiven.kafka.connect.common.config.FilenameTemplateVariable.TOPIC;
import static io.aiven.kafka.connect.common.grouper.RecordGrouperFactory.ALL_SUPPORTED_VARIABLES;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.source.task.DistributionType;
import io.aiven.kafka.connect.common.templating.Template;

import org.apache.commons.lang3.StringUtils;

public final class SourcenameTemplateValidator implements ConfigDef.Validator {

    static final Map<String, ParameterDescriptor> SUPPORTED_VARIABLE_PARAMETERS = new TreeMap<>();
    static final Map<String, ParameterDescriptor> REQUIRED_VARIABLE_PARAMETERS = new TreeMap<>();

    static {
        REQUIRED_VARIABLE_PARAMETERS.put(PARTITION.name, PARTITION.parameterDescriptor);
        SUPPORTED_VARIABLE_PARAMETERS.putAll(REQUIRED_VARIABLE_PARAMETERS);
        SUPPORTED_VARIABLE_PARAMETERS.put(START_OFFSET.name, START_OFFSET.parameterDescriptor);
        SUPPORTED_VARIABLE_PARAMETERS.put(TIMESTAMP.name, TIMESTAMP.parameterDescriptor);
        SUPPORTED_VARIABLE_PARAMETERS.put(TOPIC.name, TOPIC.parameterDescriptor);
    }

    private final DistributionType distributionType;

    public SourcenameTemplateValidator(final DistributionType distributionType) {
        this.distributionType = distributionType;
    }

    @Override
    public void ensureValid(final String name, final Object value) {
        final String valueStr = value == null ? null : value.toString();
        if (StringUtils.isBlank(valueStr)) {
            throw new ConfigException(name, "must not be empty or not set");
        }
        // See https://cloud.google.com/storage/docs/naming
        if (valueStr.startsWith(".well-known/acme-challenge")) {
            throw new ConfigException(name, value, "cannot start with '.well-known/acme-challenge'");
        }

        // TODO when distributionType expands then add required parameters to each distribution type object
        // and change this to a switch statement
        if (distributionType.equals(DistributionType.PARTITION)) {
            // partition distribution requires the partition to be available.
            try {
                final Template template = Template.of((String) value);
                validateVariables(template.variablesSet());
                validateVariablesWithRequiredParameters(template.toString());
            } catch (final IllegalArgumentException e) {
                throw new ConfigException(name, value, e.getMessage());
            }
        }
    }

    private String formatVariable(final String variableName) {
        return "{{" + variableName + "}}";
    }

    private String formatVariables(final Collection<String> variables) {
        return variables.stream().map(this::formatVariable).collect(Collectors.joining(", "));
    }

    private void validateVariables(final Set<String> variables) {
        final List<String> invalidVariables = new ArrayList<>(variables);
        invalidVariables.removeAll(ALL_SUPPORTED_VARIABLES);
        if (!invalidVariables.isEmpty()) {
            final String variableText = invalidVariables.size() == 1 ? "variable" : "variables";
            throw new IllegalArgumentException(String.format(
                    "unsupported template %s used (%s), supported values are: %s", variableText,
                    formatVariables(invalidVariables), formatVariables(SUPPORTED_VARIABLE_PARAMETERS.keySet())));
        }
    }

    private void validateVariablesWithRequiredParameters(final String sourceNameTemplate) {
        final List<String> missingVariables = new ArrayList<>();
        for (final String requiredVariableParameter : REQUIRED_VARIABLE_PARAMETERS.keySet()) {
            if (!sourceNameTemplate.contains(requiredVariableParameter)) {
                missingVariables.add(requiredVariableParameter);
            }
        }
        if (!missingVariables.isEmpty()) {
            final String parameterText = missingVariables.size() == 1 ? "parameter" : "parameters";
            throw new IllegalArgumentException(
                    String.format("Partition distribution type requires %s %s in the file.name.template", parameterText,
                            formatVariables(missingVariables)));
        }
    }
}
