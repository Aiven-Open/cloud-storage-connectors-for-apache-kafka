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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

final class TemplateTest {
    @Test
    void emptyString() {
        final Template template = Template.of("");
        assertThat(template.instance().render()).isEmpty();
    }

    @Test
    void noVariables() {
        final Template template = Template.of("somestring");
        assertThat(template.instance().render()).isEqualTo("somestring");
    }

    @Test
    void newLine() {
        final Template template = Template.of("some\nstring");
        assertThat(template.instance().render()).isEqualTo("some\nstring");
    }

    @Test
    void emptyVariableName() {
        final String templateStr = "foo{{ }}bar";
        assertThatThrownBy(() -> Template.of(templateStr)).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Variable name hasn't been set for template: foo{{ }}bar");
    }

    @Test
    void variableFormatNoSpaces() {
        final Template template = Template.of("{{foo}}");
        final Template.Instance instance = template.instance();
        instance.bindVariable("foo", () -> "foo");
        assertThat(instance.render()).isEqualTo("foo");
    }

    @Test
    void variableFormatLeftSpace() {
        final Template template = Template.of("{{ foo}}");
        final Template.Instance instance = template.instance();
        instance.bindVariable("foo", () -> "foo");
        assertThat(instance.render()).isEqualTo("foo");
    }

    @Test
    void variableFormatRightSpace() {
        final Template template = Template.of("{{foo }}");
        final Template.Instance instance = template.instance();
        instance.bindVariable("foo", () -> "foo");
        assertThat(instance.render()).isEqualTo("foo");
    }

    @Test
    void variableFormatBothSpaces() {
        final Template template = Template.of("{{ foo }}");
        final Template.Instance instance = template.instance();
        instance.bindVariable("foo", () -> "foo");
        assertThat(instance.render()).isEqualTo("foo");
    }

    @Test
    void variableFormatBothSpacesWithVariable() {
        final Template template = Template.of("{{ foo:tt=true }}");
        final Template.Instance instance = template.instance();
        instance.bindVariable("foo", () -> "foo");
        assertThat(instance.render()).isEqualTo("foo");
    }

    @Test
    void parseVariableWithParameter() {
        final String render = Template.of("{{foo:tt=true}}").instance().bindVariable("foo", parameter -> {
            assertThat(parameter.getName()).isEqualTo("tt");
            assertThat(parameter.getValue()).isEqualTo("true");
            assertThat(parameter.asBoolean()).isTrue();
            return "";
        }).render();
        assertThat(render).isEmpty();
    }

    @Test
    void invalidVariableWithoutParameter() {
        assertThatThrownBy(() -> Template.of("{{foo:}}")).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Wrong variable with parameter definition");
    }

    @Test
    void invalidVariableWithEmptyVariableNameAndWithParameter() {
        assertThatThrownBy(() -> Template.of("{{:foo=bar}}")).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Variable name hasn't been set for template: {{:foo=bar}}");
    }

    @Test
    void invalidVariableWithEmptyParameterValue() {
        assertThatThrownBy(() -> Template.of("{{foo:tt=}}")).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Parameter value for variable `foo` and parameter `tt` has not been set");
    }

    @Test
    void invalidVariableWithoutParameterName() {
        assertThatThrownBy(() -> Template.of("{{foo:=bar}}")).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Parameter name for variable `foo` has not been set");
    }

    @Test
    void variableFormatMultipleSpaces() {
        final Template template = Template.of("{{   foo  }}");
        final Template.Instance instance = template.instance();
        instance.bindVariable("foo", () -> "foo");
        assertThat(instance.render()).isEqualTo("foo");
    }

    @Test
    void variableFormatTabs() {
        final Template template = Template.of("{{\tfoo\t}}");
        final Template.Instance instance = template.instance();
        instance.bindVariable("foo", () -> "foo");
        assertThat(instance.render()).isEqualTo("foo");
    }

    @Test
    void variableUnderscoreAlone() {
        final Template template = Template.of("{{ _ }}");
        final Template.Instance instance = template.instance();
        instance.bindVariable("_", () -> "foo");
        assertThat(instance.render()).isEqualTo("foo");
    }

    @Test
    void variableUnderscoreWithOtherSymbols() {
        final Template template = Template.of("{{ foo_bar }}");
        final Template.Instance instance = template.instance();
        instance.bindVariable("foo_bar", () -> "foo_bar");
        assertThat(instance.render()).isEqualTo("foo_bar");
    }

    @Test
    void placeholderHasCurlyBracesInside() {
        final String templateStr = "{{ { }}";
        final Template template = Template.of(templateStr);
        final Template.Instance instance = template.instance();
        instance.bindVariable("{", () -> "foo");
        assertThat(instance.render()).isEqualTo(templateStr);
    }

    @Test
    void unclosedPlaceholder() {
        final String templateStr = "bb {{ aaa ";
        final Template template = Template.of(templateStr);
        final Template.Instance instance = template.instance();
        instance.bindVariable("aaa", () -> "foo");
        assertThat(instance.render()).isEqualTo(templateStr);
    }

    @Test
    void variableInBeginning() {
        final Template template = Template.of("{{ foo }} END");
        final Template.Instance instance = template.instance();
        instance.bindVariable("foo", () -> "foo");
        assertThat(instance.render()).isEqualTo("foo END");
    }

    @Test
    void variableInMiddle() {
        final Template template = Template.of("BEGINNING {{ foo }} END");
        final Template.Instance instance = template.instance();
        instance.bindVariable("foo", () -> "foo");
        assertThat(instance.render()).isEqualTo("BEGINNING foo END");
    }

    @Test
    void variableInEnd() {
        final Template template = Template.of("BEGINNING {{ foo }}");
        final Template.Instance instance = template.instance();
        instance.bindVariable("foo", () -> "foo");
        assertThat(instance.render()).isEqualTo("BEGINNING foo");
    }

    @Test
    void nonBoundVariable() {
        final Template template = Template.of("BEGINNING {{ foo }}");
        assertThat(template.instance().render()).isEqualTo("BEGINNING {{ foo }}");
    }

    @Test
    void multipleVariables() {
        final Template template = Template.of("1{{foo}}2{{bar}}3{{baz}}4");
        final Template.Instance instance = template.instance();
        instance.bindVariable("foo", () -> "foo");
        instance.bindVariable("bar", () -> "bar");
        instance.bindVariable("baz", () -> "baz");
        assertThat(instance.render()).isEqualTo("1foo2bar3baz4");
    }

    @Test
    void sameVariableMultipleTimes() {
        final Template template = Template.of("{{foo}}{{foo}}{{foo}}");
        final Template.Instance instance = template.instance();
        instance.bindVariable("foo", () -> "foo");
        assertThat(instance.render()).isEqualTo("foofoofoo");
    }

    @Test
    void bigListOfNaughtyStringsJustString() throws IOException {
        for (final String line : getBigListOfNaughtyStrings()) {
            final Template template = Template.of(line);
            final Template.Instance instance = template.instance();
            assertThat(instance.render()).isEqualTo(line);
        }
    }

    @Test
    void bigListOfNaughtyStringsWithVariableInBeginning() throws IOException {
        for (final String line : getBigListOfNaughtyStrings()) {
            final Template template = Template.of("{{ foo }}" + line);
            final Template.Instance instance = template.instance();
            instance.bindVariable("foo", () -> "foo");
            assertThat(instance.render()).isEqualTo("foo" + line);
        }
    }

    @Test
    void bigListOfNaughtyStringsWithVariableInEnd() throws IOException {
        for (final String line : getBigListOfNaughtyStrings()) {
            final Template template = Template.of(line + "{{ foo }}");
            final Template.Instance instance = template.instance();
            instance.bindVariable("foo", () -> "foo");
            assertThat(instance.render()).isEqualTo(line + "foo");
        }
    }

    private Collection<String> getBigListOfNaughtyStrings() throws IOException {
        try (InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("blns.txt");
                InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(reader)) {

            return bufferedReader.lines().filter(s -> !s.isEmpty() && !s.startsWith("#")).collect(Collectors.toList());
        }
    }

    @Test
    void variables() {
        final Template template = Template.of("1{{foo}}2{{bar}}3{{baz}}4");
        assertThat(template.variables()).containsExactly("foo", "bar", "baz");
    }
}
