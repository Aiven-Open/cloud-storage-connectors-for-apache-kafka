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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.source.task.DistributionType;
import io.aiven.kafka.connect.common.templating.Template;


public final class SourcenameTemplateValidator implements ConfigDef.Validator {

    static final Map<String, ParameterDescriptor> SUPPORTED_VARIABLE_PARAMETERS = new LinkedHashMap<>();
    static final Map<String, ParameterDescriptor> REQUIRED_VARIABLE_PARAMETERS = new LinkedHashMap<>();

    static {
        REQUIRED_VARIABLE_PARAMETERS.put(PARTITION.name, PARTITION.parameterDescriptor);
        SUPPORTED_VARIABLE_PARAMETERS.putAll(REQUIRED_VARIABLE_PARAMETERS);
        SUPPORTED_VARIABLE_PARAMETERS.put(START_OFFSET.name, START_OFFSET.parameterDescriptor);
        SUPPORTED_VARIABLE_PARAMETERS.put(TIMESTAMP.name, TIMESTAMP.parameterDescriptor);
        SUPPORTED_VARIABLE_PARAMETERS.put(TOPIC.name, TOPIC.parameterDescriptor);
    }

    private final String configName;
    private final DistributionType distributionType;

    public SourcenameTemplateValidator(final String configName, final DistributionType distributionType) {
        this.configName = configName;
        this.distributionType = distributionType;
    }

    @Override
    public void ensureValid(final String name, final Object value) {
        if (value == null) {
            return;
        }

        assert value instanceof String;

        // See https://cloud.google.com/storage/docs/naming
        final String valueStr = (String) value;
        if (valueStr.startsWith(".well-known/acme-challenge")) {
            throw new ConfigException(configName, value, "cannot start with '.well-known/acme-challenge'");
        }

        if (StringUtils.isNotBlank(valueStr) && distributionType.equals(DistributionType.OBJECT_HASH)) {
            // accepts any string or regex to match on.
            return;
        } else if (StringUtils.isBlank(valueStr)) {
            throw new ConfigException(configName, "Can not be a blank or empty string");
        }
        // if using partition distribution it requires the partition to be available.
        try {
            final Template template = Template.of((String) value);
            validateVariables(template.variablesSet());
            validateVariablesWithRequiredParameters(template.toString());
        } catch (final IllegalArgumentException e) {
            throw new ConfigException(configName, value, e.getMessage());
        }
    }

    private static void validateVariables(final Set<String> variables) {
        for (final String variable : variables) {
            if (!ALL_SUPPORTED_VARIABLES.contains(variable)) {
                throw new IllegalArgumentException(
                        String.format("unsupported template variable used, supported values are: %s",
                                SUPPORTED_VARIABLE_PARAMETERS.keySet()));
            }
        }
    }

    public static void validateVariablesWithRequiredParameters(final String sourceNameTemplate) {
        for (final String requiredVariableParameter : REQUIRED_VARIABLE_PARAMETERS.keySet()) {
            if (!sourceNameTemplate.contains(requiredVariableParameter)) {
                throw new IllegalArgumentException(String
                        .format("parameter %s is required for the the file.name.template", requiredVariableParameter));
            }
        }

    }
}
