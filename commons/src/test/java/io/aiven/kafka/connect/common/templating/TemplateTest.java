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

import static io.aiven.kafka.connect.common.templating.TemplateTestUtil.withBindings;
import static io.aiven.kafka.connect.common.templating.TemplateTestUtil.withInput;
import static io.aiven.kafka.connect.common.templating.TemplateTestUtil.withNoBindings;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Collectors;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;

/**
 * A helper class that can be used to test the rendering and extraction of a template in a fluent way:
 *
 * <pre>
 * // To test that a template renders correctly with the given variables.
 * assertThat(Template.of("{{foo}}/{{bar}}")).satisfies(rendersTo("FOO/BAR").whenBinding("foo", "FOO", "bar", "BAR"));
 * </pre>
 *
 * <pre>
 * // To test that a template extracts the expected variables from the given string.
 * assertThat(Template.of("{{foo}}/{{bar}}")).satisfies(extracts("foo", "FOO", "bar", "BAR").whenGiven("FOO/BAR"));
 * </pre>
 */
class TemplateTestUtil {

    private final String rendered;
    private final String[] varNameAndValues;

    TemplateTestUtil(final String rendered, final String... varNameAndValues) {
        assertThat(varNameAndValues.length).as("Must set names and values in pairs").isEven();
        this.rendered = rendered;
        this.varNameAndValues = varNameAndValues; // NOPMD ArrayIsStoredDirectly
    }

    static public TemplateTestUtil withBindings(final String... varNameAndValues) {
        return new TemplateTestUtil("", varNameAndValues);
    }

    static public TemplateTestUtil withNoBindings() {
        return new TemplateTestUtil("");
    }

    static public TemplateTestUtil withInput(final String rendered) {
        return new TemplateTestUtil(rendered);
    }

    Condition<Template> rendersTo(final String rendered) {
        return new TemplateTestUtil(rendered, this.varNameAndValues).testRender();
    }

    Condition<Template> extracts(final String... varNameAndValues) {
        return new TemplateTestUtil(this.rendered, varNameAndValues).testExtract();
    }

    Condition<Template> extractsEmpty() {
        return new TemplateTestUtil(this.rendered).testExtract();
    }

    private Condition<Template> testRender() {
        return new Condition<>(template -> {
            final var instance = template.instance();
            for (int i = 0; i < varNameAndValues.length; i += 2) {
                final var value = varNameAndValues[i + 1];
                instance.bindVariable(varNameAndValues[i], () -> value);
            }
            assertThat(instance.render()).isEqualTo(rendered);
            // Failed tests are indicated by assertions, not by this return value.
            return true;
        }, "Renders to " + rendered);
    }

    private Condition<Template> testExtract() {
        return new Condition<>(template -> {
            final var extractor = template.extractor();
            final var extracted = extractor.extract(rendered);
            final var found = new HashSet<String>();
            for (int i = 0; i < varNameAndValues.length; i += 2) {
                assertThat(extracted).containsEntry(varNameAndValues[i], varNameAndValues[i + 1]);
                found.add(varNameAndValues[i]);
            }
            assertThat(extracted).containsOnlyKeys(found);
            // Failed tests are indicated by assertions, not by this return value.
            return true;
        }, "Extracts " + rendered);
    }
}

final class TemplateTest {

    /**
     * Meta-tests to ensure that the test utility detects the errors it claims and fails with meaningful messages.
     */
    @Test
    void testHelper() {
        // A basic example of a successful test
        assertThat(Template.of("{{foo}}/{{bar}}"))
                .satisfies(withBindings("foo", "FOO", "bar", "BAR").rendersTo("FOO/BAR"))
                .satisfies(withInput("FOO/BAR").extracts("foo", "FOO", "bar", "BAR"));

        // Failure when the test helper is misconfigured.
        assertThatThrownBy(() -> assertThat(Template.of("{{foo}}/{{bar}}"))
                .satisfies(withBindings("foo", "FOO", "bar").rendersTo("FOO/BAR"))).isInstanceOf(AssertionError.class)
                .hasMessageContaining("Must set names and values in pairs")
                .hasMessageContaining("Expecting 3 to be even");

        // Failure when the instance does not rendered as expected
        assertThatThrownBy(() -> assertThat(Template.of("{{foo}}/{{bar}}/{{baz}}"))
                .satisfies(withBindings("foo", "FOO", "bar", "BAR").rendersTo("FOO/bar")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("expected: \"FOO/bar\"")
                .hasMessageContaining("but was: \"FOO/BAR/{{baz}}\"");

        // Failure when the extractor does not extract as expected
        assertThatThrownBy(() -> assertThat(Template.of("{{foo}}/{{bar}}"))
                .satisfies(withInput("FOO/BAR").extracts("foo", "foo", "bar", "bar")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("but the following map entries had different values:")
                .hasMessageContaining("[\"foo\"=\"FOO\" (expected: \"foo\")]");

        // Failure when an expected variable value is missing
        assertThatThrownBy(
                () -> assertThat(Template.of("{{foo}}/{{bar}}")).satisfies(withInput("FOO/BAR").extracts("foo", "FOO")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("to contain only following keys:")
                .hasMessageContaining("[\"foo\"]");

        // Failure when too many variables are expected in the returned
        assertThatThrownBy(() -> assertThat(Template.of("{{foo}}/{{bar}}"))
                .satisfies(withInput("FOO/BAR").extracts("foo", "FOO", "bar", "BAR", "baz", "BAZ")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("but could not find the following map entries:")
                .hasMessageContaining("[\"baz\"=\"BAZ\"]");
    }

    @Test
    void emptyString() {
        assertThat(Template.of("")).satisfies(withBindings("foo", "FOO").rendersTo(""))
                .satisfies(withNoBindings().rendersTo(""))
                .satisfies(withInput("").extractsEmpty());
    }

    @Test
    void noVariables() {
        assertThat(Template.of("somestring")).satisfies(withBindings("foo", "FOO").rendersTo("somestring"))
                .satisfies(withNoBindings().rendersTo("somestring"))
                .satisfies(withInput("somestring").extractsEmpty());
    }

    @Test
    void newLine() {
        assertThat(Template.of("some\nstring")).satisfies(withBindings("foo", "FOO").rendersTo("some\nstring"))
                .satisfies(withNoBindings().rendersTo("some\nstring"))
                .satisfies(withInput("some\nstring").extractsEmpty());
    }

    @Test
    void emptyVariableName() {
        final String templateStr = "foo{{ }}bar";
        assertThatThrownBy(() -> Template.of(templateStr)).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Variable name hasn't been set for template: foo{{ }}bar");
    }

    @Test
    void variableFormatNoSpaces() {
        assertThat(Template.of("{{foo}}")).satisfies(withBindings("foo", "FOO").rendersTo("FOO"))
                .satisfies(withInput("FOO").extracts("foo", "FOO"));
    }

    @Test
    void variableFormatLeftSpace() {
        assertThat(Template.of("{{ foo}}")).satisfies(withBindings("foo", "FOO").rendersTo("FOO"))
                .satisfies(withInput("FOO").extracts("foo", "FOO"));
    }

    @Test
    void variableFormatRightSpace() {
        assertThat(Template.of("{{foo }}")).satisfies(withBindings("foo", "FOO").rendersTo("FOO"))
                .satisfies(withInput("FOO").extracts("foo", "FOO"));
    }

    @Test
    void variableFormatBothSpaces() {
        assertThat(Template.of("{{ foo }}")).satisfies(withBindings("foo", "FOO").rendersTo("FOO"))
                .satisfies(withInput("FOO").extracts("foo", "FOO"));
    }

    @Test
    void variableFormatBothSpacesWithVariable() {
        assertThat(Template.of("{{ foo:tt=true }}")).satisfies(withBindings("foo", "FOO").rendersTo("FOO"))
                .satisfies(withInput("FOO").extracts("foo", "FOO"));
    }

    @Test
    void parseVariableWithParameter() {
        final Template template = Template.of("{{ foo:tt=true }}");
        final String render = template.instance().bindVariable("foo", parameter -> {
            assertThat(parameter.getName()).isEqualTo("tt");
            assertThat(parameter.getValue()).isEqualTo("true");
            assertThat(parameter.asBoolean()).isTrue();
            return "PARAMETER_TESTED";
        }).render();

        assertThat(render).isEqualTo("PARAMETER_TESTED");
        assertThat(template).satisfies(withInput("FOO").extracts("foo", "FOO"));
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
        assertThat(Template.of("{{   foo  }}")).satisfies(withBindings("foo", "FOO").rendersTo("FOO"))
                .satisfies(withInput("FOO").extracts("foo", "FOO"));
    }

    @Test
    void variableFormatTabs() {
        assertThat(Template.of("{{\tfoo\t}}")).satisfies(withBindings("foo", "FOO").rendersTo("FOO"))
                .satisfies(withInput("FOO").extracts("foo", "FOO"));
    }

    @Test
    void variableUnderscoreAlone() {
        assertThat(Template.of("{{ _ }}")).satisfies(withBindings("_", "FOO").rendersTo("FOO"))
                .satisfies(withInput("FOO").extracts("_", "FOO"));
    }

    @Test
    void variableUnderscoreWithOtherSymbols() {
        assertThat(Template.of("{{ foo_bar }}")).satisfies(withBindings("foo_bar", "FOO_BAR").rendersTo("FOO_BAR"))
                .satisfies(withInput("FOO_BAR").extracts("foo_bar", "FOO_BAR"));
    }

    @Test
    void placeholderHasCurlyBracesInside() {
        final String templateStr = "{{ { }}";
        assertThat(Template.of(templateStr)).satisfies(withBindings("{", "FOO").rendersTo(templateStr))
                .satisfies(withInput(templateStr).extractsEmpty());
    }

    @Test
    void unclosedPlaceholder() {
        final String templateStr = "bb {{ aaa ";
        assertThat(Template.of(templateStr)).satisfies(withBindings("aaa", "FOO").rendersTo(templateStr))
                .satisfies(withInput(templateStr).extractsEmpty());
    }

    @Test
    void variableInBeginning() {
        assertThat(Template.of("{{ foo }} END")).satisfies(withBindings("foo", "FOO").rendersTo("FOO END"))
                .satisfies(withInput("FOO END").extracts("foo", "FOO"));
    }

    @Test
    void variableInMiddle() {
        assertThat(Template.of("BEGINNING {{ foo }} END"))
                .satisfies(withBindings("foo", "FOO").rendersTo("BEGINNING FOO END"))
                .satisfies(withInput("BEGINNING FOO END").extracts("foo", "FOO"));
    }

    @Test
    void variableInEnd() {
        assertThat(Template.of("BEGINNING {{ foo }}")).satisfies(withBindings("foo", "FOO").rendersTo("BEGINNING FOO"))
                .satisfies(withInput("BEGINNING FOO").extracts("foo", "FOO"));
    }

    @Test
    void nonBoundVariable() {
        // Note that the round trip is not completely reversible for unbound variables.
        assertThat(Template.of("BEGINNING {{ foo }}")).satisfies(withNoBindings().rendersTo("BEGINNING {{ foo }}"))
                .satisfies(withInput("BEGINNING {{ foo }}").extracts("foo", "{{ foo }}"));
    }

    @Test
    void multipleVariables() {
        assertThat(Template.of("1{{foo}}2{{bar}}3{{baz}}4"))
                .satisfies(withBindings("foo", "FOO", "bar", "BAR", "baz", "BAZ").rendersTo("1FOO2BAR3BAZ4"))
                .satisfies(withInput("1FOO2BAR3BAZ4").extracts("foo", "FOO", "bar", "BAR", "baz", "BAZ"));
    }

    @Test
    void sameVariableMultipleTimes() {
        final Template template = Template.of("{{foo}}{{foo}}{{foo}}");
        final Template.Instance instance = template.instance();
        instance.bindVariable("foo", () -> "foo");
        assertThat(instance.render()).isEqualTo("foofoofoo");
        assertThat(template.extractor().extract("foofoofoo")).hasSize(1).containsEntry("foo", "foo");
    }

    @Test
    void bigListOfNaughtyStringsJustString() throws IOException {
        for (final String line : getBigListOfNaughtyStrings()) {
            assertThat(Template.of(line)).satisfies(withNoBindings().rendersTo(line))
                    .satisfies(withInput(line).extractsEmpty());
        }
    }

    @Test
    void bigListOfNaughtyStringsWithVariableInBeginning() throws IOException {
        for (final String line : getBigListOfNaughtyStrings()) {
            assertThat(Template.of("{{ foo }}" + line)).satisfies(withBindings("foo", "FOO").rendersTo("FOO" + line))
                    .satisfies(withInput("FOO" + line).extracts("foo", "FOO"));
        }
    }

    @Test
    void bigListOfNaughtyStringsWithVariableInEnd() throws IOException {
        for (final String line : getBigListOfNaughtyStrings()) {
            assertThat(Template.of(line + "{{ foo }}")).satisfies(withBindings("foo", "FOO").rendersTo(line + "FOO"))
                    .satisfies(withInput(line + "FOO").extracts("foo", "FOO"));
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
