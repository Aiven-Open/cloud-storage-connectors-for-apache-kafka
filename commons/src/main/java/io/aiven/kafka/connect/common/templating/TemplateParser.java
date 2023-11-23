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

import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TemplateParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(TemplateParser.class);

    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\{\\{\\s*([\\w]+)?(:?)([\\w=]+)?\\s*}}"); // {{
                                                                                                                // var:foo=bar
                                                                                                                // }}

    private static final Pattern PARAMETER_PATTERN = Pattern.compile("([\\w]+)?=?([\\w]+)?"); // foo=bar

    private TemplateParser() {
    }

    public static Pair<List<Pair<String, VariableTemplatePart.Parameter>>, List<TemplatePart>> parse(
            final String template) {
        LOGGER.debug("Parse template: {}", template);

        final ImmutableList.Builder<Pair<String, VariableTemplatePart.Parameter>> variablesAndParametersBuilder = ImmutableList
                .builder();
        final ImmutableList.Builder<TemplatePart> templatePartsBuilder = ImmutableList.builder();
        final Matcher matcher = VARIABLE_PATTERN.matcher(template);

        int position = 0;
        while (matcher.find()) {
            templatePartsBuilder.add(new TextTemplatePart(template.substring(position, matcher.start())) // NOPMD
                                                                                                         // AvoidInstantiatingObjectsInLoops
            );

            final String variable = matcher.group(1);

            if (Objects.isNull(variable)) {
                throw new IllegalArgumentException(
                        String.format("Variable name hasn't been set for template: %s", template));
            }

            final String parameterDef = matcher.group(2);
            final String parameter = matcher.group(3);
            if (":".equals(parameterDef) && Objects.isNull(parameter)) { // NOPMD AvoidLiteralsInIfCondition
                throw new IllegalArgumentException("Wrong variable with parameter definition");
            }

            final VariableTemplatePart.Parameter parseParameter = parseParameter(variable, parameter);
            variablesAndParametersBuilder.add(Pair.of(variable, parseParameter));
            templatePartsBuilder.add(new VariableTemplatePart(variable, parseParameter, matcher.group())); // NOPMD
                                                                                                           // AvoidInstantiatingObjectsInLoops
            position = matcher.end();
        }
        templatePartsBuilder.add(new TextTemplatePart(template.substring(position)));

        return Pair.of(variablesAndParametersBuilder.build(), templatePartsBuilder.build());
    }

    private static VariableTemplatePart.Parameter parseParameter(final String variable, final String parameter) {
        LOGGER.debug("Parse {} parameter", parameter);
        if (Objects.nonNull(parameter)) {
            final Matcher matcher = PARAMETER_PATTERN.matcher(parameter);
            if (!matcher.find()) {
                throw new IllegalArgumentException(
                        String.format("Parameter hasn't been set for variable `%s`", variable));
            }

            final String name = matcher.group(1);
            if (Objects.isNull(name)) {
                throw new IllegalArgumentException(
                        String.format("Parameter name for variable `%s` has not been set", variable));
            }

            final String value = matcher.group(2);
            if (Objects.isNull(value)) {
                throw new IllegalArgumentException(String.format(
                        "Parameter value for variable `%s` and parameter `%s` has not been set", variable, name));
            }

            return VariableTemplatePart.Parameter.of(name, value);
        } else {
            return VariableTemplatePart.Parameter.EMPTY;
        }
    }

}
