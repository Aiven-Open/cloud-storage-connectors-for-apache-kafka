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

package io.aiven.kafka.connect.common.templating;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.aiven.kafka.connect.common.templating.VariableTemplatePart.Parameter;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

/**
 * A simple templating engine that allows to bind variables to supplier functions.
 *
 * <p>
 * Variable syntax: {@code {{ variable_name:parameter_name=parameter_value }}}. Only alphanumeric characters and
 * {@code _} are allowed as a variable name. Any number of spaces/tabs inside the braces is allowed. Parameters for
 * variable name are optional, same as for variable only alphanumeric characters are allowed as a parameter name or a
 * parameter value.
 *
 * <p>
 * Non-bound variables are left as is.
 */
public final class Template {
    private final List<Pair<String, Parameter>> variablesAndParameters;

    private final List<TemplatePart> templateParts;

    private final String originalTemplateString;

    private Template(final String template, final List<Pair<String, Parameter>> variablesAndParameters,
            final List<TemplatePart> templateParts) {
        this.originalTemplateString = template;
        this.variablesAndParameters = variablesAndParameters;
        this.templateParts = templateParts;
    }

    public String originalTemplate() {
        return originalTemplateString;
    }

    public List<String> variables() {
        return variablesAndParameters.stream().map(Pair::getLeft).collect(Collectors.toList());
    }

    public Set<String> variablesSet() {
        return variablesAndParameters.stream().map(Pair::getLeft).collect(Collectors.toSet());
    }

    public List<Pair<String, Parameter>> variablesWithParameters() {
        return Collections.unmodifiableList(variablesAndParameters);
    }

    public List<Pair<String, Parameter>> variablesWithNonEmptyParameters() {
        return variablesAndParameters.stream().filter(e -> !e.getRight().isEmpty()).collect(Collectors.toList());
    }

    public Instance instance() {
        return new Instance();
    }

    @SuppressWarnings("PMD.ShortMethodName")
    public static Template of(final String template) {

        final Pair<List<Pair<String, Parameter>>, List<TemplatePart>> parsingResult = TemplateParser.parse(template);

        return new Template(template, parsingResult.getLeft(), parsingResult.getRight());
    }

    @Override
    public String toString() {
        return originalTemplateString;
    }

    public final class Instance {
        private final Map<String, Function<Parameter, String>> bindings = new HashMap<>();

        private Instance() {
        }

        public Instance bindVariable(final String name, final Supplier<String> binding) {
            return bindVariable(name, x -> binding.get());
        }

        public Instance bindVariable(final String name, final Function<Parameter, String> binding) {
            Objects.requireNonNull(name, "name cannot be null");
            Objects.requireNonNull(binding, "binding cannot be null");

            if (StringUtils.isBlank(name)) {
                throw new IllegalArgumentException("name must not be empty");
            }
            bindings.put(name, binding);
            return this;
        }

        public String render() {
            final StringBuilder stringBuilder = new StringBuilder();
            // FIXME we need better solution instead of instanceof
            for (final TemplatePart templatePart : templateParts) {
                if (templatePart instanceof TextTemplatePart) {
                    stringBuilder.append(((TextTemplatePart) templatePart).getText());
                } else if (templatePart instanceof VariableTemplatePart) {
                    final VariableTemplatePart variableTemplatePart = (VariableTemplatePart) templatePart;
                    final Function<Parameter, String> binding = bindings.get(variableTemplatePart.getVariableName());
                    // Substitute for bound variables, pass the variable pattern as is for non-bound.
                    if (Objects.nonNull(binding)) {
                        stringBuilder.append(binding.apply(variableTemplatePart.getParameter()));
                    } else {
                        stringBuilder.append(variableTemplatePart.getOriginalPlaceholder());
                    }
                }
            }
            return stringBuilder.toString();
        }
    }
}
