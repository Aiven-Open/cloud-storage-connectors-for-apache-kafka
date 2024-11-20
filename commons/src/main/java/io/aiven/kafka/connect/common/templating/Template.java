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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.aiven.kafka.connect.common.templating.VariableTemplatePart.Parameter;

import com.google.common.collect.ImmutableMap;
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

    public Extractor extractor() {
        return new Extractor();
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

    /**
     * Given a template, the {@link Extractor} finds the matching variables from a string.
     *
     * Where an{@link Instance} generates strings by filling in the variables in a template, the {@link Extractor} does
     * the opposite:
     *
     * <pre>
     * // Renders the string "Hello World!"
     * var tmpl = Template.of("Hello {{name}}!");
     * var greeting = tmpl.instance().bindVariable("name", () -> "World").render();
     *
     * // The other way around, extracts a name from a string:
     * tmpl.extractor().extract(greeting); // returns a map {name=World}
     * tmpl.extractor().extract("Hello everyone!"); // returns a map {name=everyone}
     * </pre>
     *
     * The intention is that applying the map back to the template will yield the original string, but the round-trip is
     * not always exact.
     * <ul>
     * <li>Rendering a string uses a supplier and the extractor returns a fixed string.</li>
     * <li>Variable parameters are currently ignored (to be determined).</li>
     * </ul>
     */
    public final class Extractor {
        private final Pattern regex;
        private final Map<String, String> captureGroups = new HashMap<>();

        private Extractor() {
            // Build a regex to match the template pattern. Every variable part is mapped to a capturing group
            // and every text part is quoted to require an exact match. Variable part parameters are ignored.
            final StringBuilder textAsRegex = new StringBuilder();
            for (int i = 0; i < templateParts.size(); i++) {
                if (templateParts.get(i) instanceof TextTemplatePart) {
                    textAsRegex.append(Pattern.quote(((TextTemplatePart) templateParts.get(i)).getText()));
                } else if (templateParts.get(i) instanceof VariableTemplatePart) {
                    final String variableName = ((VariableTemplatePart) templateParts.get(i)).getVariableName();
                    if (captureGroups.containsKey(variableName)) {
                        // If the name was already used, we can capture it with a backreference.
                        textAsRegex.append("\\k<").append(captureGroups.get(variableName)).append('>');
                    } else {
                        textAsRegex.append("(?<capture").append(i).append(">.*?)");
                        // A capturing group cannot contain underscores, so we map every variable name to a valid unique
                        // name.
                        captureGroups.put(((VariableTemplatePart) templateParts.get(i)).getVariableName(),
                                "capture" + i);
                    }
                }
            }
            regex = Pattern.compile(textAsRegex.toString());
        }

        /**
         * Performs the variable extraction from the input string.
         *
         * @param input
         *            A string to extract variables from.
         * @return An immutable map of key-value pairs extracted from the input string, corresponding to the template
         *         variable names.
         * @throws IllegalArgumentException
         *             If the input string does not match the template.
         */
        public Map<String, String> extract(final String input) throws IllegalArgumentException {
            final Matcher matcher = regex.matcher(input);
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Input does not match the template");
            }

            final ImmutableMap.Builder<String, String> found = new ImmutableMap.Builder<>();
            for (final Map.Entry<String, String> entry : captureGroups.entrySet()) {
                found.put(entry.getKey(), matcher.group(entry.getValue()));
            }

            return found.build();
        }
    }

}
