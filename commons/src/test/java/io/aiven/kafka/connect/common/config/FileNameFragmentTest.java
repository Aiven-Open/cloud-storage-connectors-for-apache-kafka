/*
 * Copyright 2024 Aiven Oy
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;

import java.lang.reflect.Modifier;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

import io.aiven.kafka.connect.common.source.task.DistributionType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

class FileNameFragmentTest {

    private static final String TEST_CONFIG_NAME = "TEST_CONFIG";

    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("configDefSource")
    void configDefTest(final String arg, final ConfigDef.Type type, final Object defaultValue,
            final boolean validatorPresent, final ConfigDef.Importance importance, final boolean recommenderPresent) {
        final ConfigDef configDef = new ConfigDef();
        FileNameFragment.update(configDef, CompressionType.NONE, FileNameFragment.PrefixTemplateSupport.FALSE);
        final ConfigDef.ConfigKey key = configDef.configKeys().get(arg);

        assertThat(arg).as("Wrong key name").isEqualTo(key.name);
        assertThat(type).as("Wrong key type").isEqualTo(key.type);
        assertThat(defaultValue).as("Wrong default value").isEqualTo(key.defaultValue);
        assertThat(validatorPresent)
                .as(() -> String.format("Validator was %spresent.", key.validator == null ? "not " : ""))
                .isEqualTo(key.validator != null);
        assertThat(importance).as("Wrong importance").isEqualTo(key.importance);
        assertThat(key.documentation).as("Documenttion not included").isNotNull();
        assertThat(key.name).isEqualTo(key.displayName);
        assertThat(recommenderPresent)
                .as(() -> String.format("Recommender was %spresent.", key.recommender == null ? "not " : ""))
                .isEqualTo(key.recommender != null);
    }

    @Test
    void allConfigDefsAccountForTest() {
        final List<String> names = Arrays.stream(FileNameFragment.class.getDeclaredFields())
                .filter(field -> Modifier.isStatic(field.getModifiers()) && !Modifier.isPrivate(field.getModifiers())
                        && field.getType().equals(String.class))
                .map(field -> {
                    try {
                        return field.get(null).toString();
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e); // NOPMD
                    }
                })
                .collect(Collectors.toList());
        names.remove(FileNameFragment.GROUP_NAME);
        names.remove(FileNameFragment.DEFAULT_FILENAME_TEMPLATE);
        // TODO remove these vv when we understand what it is for.
        names.remove(FileNameFragment.FILE_PATH_PREFIX_TEMPLATE_CONFIG);
        names.remove(FileNameFragment.FILE_NAME_PREFIX_CONFIG);
        // TODO remove these ^^ when we understand what it is for.
        configDefSource().map(a -> (String) a.get()[0]).forEach(names::remove);
        assertThat(names.isEmpty())
                .as(() -> "Tests do not process the following arguments: " + String.join(", ", names))
                .isTrue();
    }

    private static Stream<Arguments> configDefSource() {
        final List<Arguments> args = new ArrayList<>();

        args.add(Arguments.of(FileNameFragment.FILE_NAME_TEMPLATE_CONFIG, ConfigDef.Type.STRING, null, true,
                ConfigDef.Importance.MEDIUM, false));

        args.add(Arguments.of(FileNameFragment.FILE_COMPRESSION_TYPE_CONFIG, ConfigDef.Type.STRING,
                CompressionType.NONE.name(), true, ConfigDef.Importance.MEDIUM, true));

        args.add(Arguments.of(FileNameFragment.FILE_MAX_RECORDS, ConfigDef.Type.INT, 0, true,
                ConfigDef.Importance.MEDIUM, false));

        args.add(Arguments.of(FileNameFragment.FILE_NAME_TIMESTAMP_TIMEZONE, ConfigDef.Type.STRING,
                ZoneOffset.UTC.toString(), true, ConfigDef.Importance.LOW, false));

        args.add(Arguments.of(FileNameFragment.FILE_NAME_TIMESTAMP_SOURCE, ConfigDef.Type.STRING,
                TimestampSource.Type.WALLCLOCK.name(), true, ConfigDef.Importance.LOW, false));

        return args.stream();
    }

    @ParameterizedTest
    @MethodSource("recordGrouperData")
    void validateRecordGrouperTest(final String sourceName, final int maxRecords, final List<String> messages) {

        final ConfigDef configDef = new ConfigDef();
        FileNameFragment.update(configDef, CompressionType.NONE, FileNameFragment.PrefixTemplateSupport.FALSE);
        final Map<String, String> props = new HashMap<>();
        FileNameFragment.setter(props).template(sourceName).maxRecordsPerFile(maxRecords);

        final AbstractConfig cfg = new AbstractConfig(configDef, props);
        final FileNameFragment underTest = new FileNameFragment(FragmentDataAccess.from(cfg), true);

        final Map<String, ConfigValue> dataMap = configDef.validateAll(props);
        underTest.validate(dataMap);
        if (messages.isEmpty()) {
            assertThat(dataMap.get(FileNameFragment.FILE_MAX_RECORDS).errorMessages()).isEmpty();
        } else {
            assertThat(dataMap.get(FileNameFragment.FILE_MAX_RECORDS).errorMessages())
                    .containsExactlyInAnyOrderElementsOf(messages.stream()
                            .map(msg -> FileNameFragment.validationMessage(FileNameFragment.FILE_MAX_RECORDS,
                                    maxRecords, msg))
                            .collect(Collectors.toList()));
        }
    }

    static List<Arguments> recordGrouperData() {
        final List<Arguments> arguments = new ArrayList<>();
        arguments.add(Arguments.of("my-file-name-template-{{key}}", 50, Arrays.asList(
                "When file.name.template is my-file-name-template-{{key}}, file.max.records must be either 1 or not set")));
        arguments.add(Arguments.of("my-file-name-template-{{key}}", 0, Collections.emptyList()));
        arguments.add(Arguments.of("my-file-name-template-{{key}}", 1, Collections.emptyList()));
        return arguments;
    }

    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("fileNameSource")
    void testGetFilename(final String expected, final Map<String, String> props) {
        final ConfigDef configDef = new ConfigDef();
        FileNameFragment.update(configDef, CompressionType.NONE, FileNameFragment.PrefixTemplateSupport.FALSE);
        OutputFormatFragment.update(configDef, null);
        final AbstractConfig cfg = new AbstractConfig(configDef, props);
        final FileNameFragment underTest = new FileNameFragment(FragmentDataAccess.from(cfg), true);
        assertThat(expected).isEqualTo(underTest.getFilename());
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    private static List<Arguments> fileNameSource() {
        final List<Arguments> args = new ArrayList<>();
        // we have to make new instances of props or the tests will fail.
        final Map<String, String> props = new HashMap<>();
        final FileNameFragment.Setter fileNameSetter = FileNameFragment.setter(props);
        fileNameSetter.template("my-file-name-template-{{key}}");
        args.add(Arguments.of("my-file-name-template-{{key}}", new HashMap<>(props)));

        final OutputFormatFragment.Setter outputSetter = OutputFormatFragment.setter(props);

        props.clear();
        outputSetter.withFormatType(FormatType.CSV);
        args.add(Arguments.of(FileNameFragment.DEFAULT_FILENAME_TEMPLATE, new HashMap<>(props)));

        for (final CompressionType compressionType : CompressionType.values()) {
            fileNameSetter.fileCompression(compressionType);
            args.add(Arguments.of(FileNameFragment.DEFAULT_FILENAME_TEMPLATE + compressionType.extension(),
                    new HashMap<>(props)));
        }

        outputSetter.withFormatType(FormatType.AVRO);
        for (final CompressionType compressionType : CompressionType.values()) {
            fileNameSetter.fileCompression(compressionType);
            args.add(Arguments.of(FileNameFragment.DEFAULT_FILENAME_TEMPLATE + ".avro" + compressionType.extension(),
                    new HashMap<>(props)));
        }

        return args;
    }

    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("fileNameSource")
    void testGetFilenameTemplate(final String expected, final Map<String, String> props) {
        final ConfigDef configDef = new ConfigDef();
        FileNameFragment.update(configDef, CompressionType.NONE, FileNameFragment.PrefixTemplateSupport.FALSE);
        OutputFormatFragment.update(configDef, null);
        final AbstractConfig cfg = new AbstractConfig(configDef, props);
        final FileNameFragment underTest = new FileNameFragment(FragmentDataAccess.from(cfg), true);
        // template does not implement equality
        assertThat(expected).isEqualTo(underTest.getFilenameTemplate().originalTemplate());
    }

    @Test
    void testGetFilenameTimezone() {
        final ConfigDef configDef = new ConfigDef();
        FileNameFragment.update(configDef, CompressionType.NONE, FileNameFragment.PrefixTemplateSupport.FALSE);
        final Map<String, String> props = new HashMap<>();
        props.put(FileNameFragment.FILE_NAME_TIMESTAMP_TIMEZONE, "Europe/Dublin");
        AbstractConfig cfg = new AbstractConfig(configDef, props);
        FileNameFragment underTest = new FileNameFragment(FragmentDataAccess.from(cfg), true);
        assertThat("Europe/Dublin").isEqualTo(underTest.getFilenameTimezone().toString());

        props.clear();
        cfg = new AbstractConfig(configDef, props);
        underTest = new FileNameFragment(FragmentDataAccess.from(cfg), true);
        assertThat("Z").isEqualTo(underTest.getFilenameTimezone().toString());

        props.clear();
        props.put(FileNameFragment.FILE_NAME_TIMESTAMP_TIMEZONE, "NotARealTimezone");
        assertThatThrownBy(() -> new AbstractConfig(configDef, props)).isInstanceOf(ConfigException.class)
                .hasMessage(
                        "Invalid value NotARealTimezone for configuration file.name.timestamp.timezone: Unknown time-zone ID: NotARealTimezone");
    }

    @Test
    void testGetFilenameTimestampSource() {
        final ConfigDef configDef = new ConfigDef();
        FileNameFragment.update(configDef, CompressionType.NONE, FileNameFragment.PrefixTemplateSupport.FALSE);
        final Map<String, String> props = new HashMap<>();
        AbstractConfig cfg = new AbstractConfig(configDef, props);
        FileNameFragment underTest = new FileNameFragment(FragmentDataAccess.from(cfg), true);
        TimestampSource src = underTest.getFilenameTimestampSource();
        assertThat(TimestampSource.Type.WALLCLOCK).isEqualTo(src.type());

        props.put(FileNameFragment.FILE_NAME_TIMESTAMP_SOURCE, TimestampSource.Type.EVENT.name());
        cfg = new AbstractConfig(configDef, props);
        underTest = new FileNameFragment(FragmentDataAccess.from(cfg), true);
        src = underTest.getFilenameTimestampSource();
        assertThat(TimestampSource.Type.EVENT).isEqualTo(src.type());

        props.put(FileNameFragment.FILE_NAME_TIMESTAMP_SOURCE, "NotATimestampSource");
        assertThatThrownBy(() -> new AbstractConfig(configDef, props)).isInstanceOf(ConfigException.class)
                .hasMessage(
                        "Invalid value NotATimestampSource for configuration file.name.timestamp.source: String must be one of (case insensitive): EVENT, WALLCLOCK");
    }

    @Test
    void testGetMaxRecordsPerFile() {
        final ConfigDef configDef = new ConfigDef();
        FileNameFragment.update(configDef, CompressionType.NONE, FileNameFragment.PrefixTemplateSupport.FALSE);
        final Map<String, String> props = new HashMap<>();
        AbstractConfig cfg = new AbstractConfig(configDef, props);
        FileNameFragment underTest = new FileNameFragment(FragmentDataAccess.from(cfg), true);
        int count = underTest.getMaxRecordsPerFile();
        assertThat(0).isEqualTo(count);

        props.put(FileNameFragment.FILE_MAX_RECORDS, "200");
        cfg = new AbstractConfig(configDef, props);
        underTest = new FileNameFragment(FragmentDataAccess.from(cfg), true);
        count = underTest.getMaxRecordsPerFile();
        assertThat(200).isEqualTo(count);

        props.put(FileNameFragment.FILE_MAX_RECORDS, "NotANumber");
        assertThatThrownBy(() -> new AbstractConfig(configDef, props)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value NotANumber for configuration file.max.records: Not a number of type INT");

        props.put(FileNameFragment.FILE_MAX_RECORDS, "-1");
        assertThatThrownBy(() -> new AbstractConfig(configDef, props)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value -1 for configuration file.max.records: Value must be at least 0");
    }

    @ParameterizedTest
    @CsvSource({ "{{topic}}-{{partition}}-{{start_offset}}.avro,partition", "*-{{partition}}.parquet,partition",
            "*.gz,object_hash", "'([a-zA-Z0-9_-]{3,5}.gz',object_hash" })
    void validateBasicSourceNamesPass(final String sourceName, final String distributionType) {

        assertThatCode(() -> FileNameFragment.PREFIX_VALIDATOR.ensureValid(TEST_CONFIG_NAME, sourceName))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest(name = "{index} {0} sink:{2}")
    @MethodSource("invalidTemplateData")
    void validateCorrectExceptionsOnInvalidTemplates(final String sourceName, final String distributionType,
            final boolean isSink, final List<String> messages) {

        final ConfigDef configDef = new ConfigDef();
        FileNameFragment.update(configDef, CompressionType.NONE, FileNameFragment.PrefixTemplateSupport.FALSE);
        SourceConfigFragment.update(configDef);
        final Map<String, String> props = new HashMap<>();
        FileNameFragment.setter(props).template(sourceName);
        SourceConfigFragment.setter(props).distributionType(DistributionType.forName(distributionType));
        final AbstractConfig cfg = new AbstractConfig(configDef, props);
        final FileNameFragment underTest = new FileNameFragment(FragmentDataAccess.from(cfg), isSink);

        final Map<String, ConfigValue> dataMap = configDef.validateAll(props);
        underTest.validate(dataMap);
        if (messages.isEmpty()) {
            assertThat(dataMap.get(FileNameFragment.FILE_NAME_TEMPLATE_CONFIG).errorMessages()).isEmpty();
        } else {
            assertThat(dataMap.get(FileNameFragment.FILE_NAME_TEMPLATE_CONFIG).errorMessages())
                    .containsExactlyInAnyOrderElementsOf(messages.stream()
                            .map(msg -> FileNameFragment.validationMessage(FileNameFragment.FILE_NAME_TEMPLATE_CONFIG,
                                    sourceName, msg))
                            .collect(Collectors.toList()));
        }
    }

    static List<Arguments> invalidTemplateData() {
        final boolean isSink = true;
        final boolean isSource = false;
        final List<Arguments> arguments = new ArrayList<>();
        final String supportedValues = ", supported values are: {{key}}, {{partition}}, {{start_offset}}, {{timestamp}}, {{topic}}";
        final String unsupportedSet = "unsupported set of template variables, supported sets are: topic,partition,start_offset,timestamp; topic,partition,key,start_offset,timestamp; key; key,topic,partition";

        arguments.add(Arguments.of("{{topic}}-{{long}}.avro", "partition", isSink,
                Arrays.asList("unsupported template variable used ({{long}})" + supportedValues, unsupportedSet)));

        arguments.add(Arguments.of("{{topic}}-{{long}}.avro", "partition", isSource,
                Arrays.asList("unsupported template variable used ({{long}})" + supportedValues,
                        "partition distribution type requires {{partition}} in the file.name.template")));

        arguments.add(Arguments.of("{{filename}}-{{partition}}.parquet", "partition", isSink,
                Arrays.asList("unsupported template variable used ({{filename}})" + supportedValues, unsupportedSet)));

        arguments.add(
                Arguments.of("{{topic}}-{{start_offset}}.avro", "partition", isSink, Arrays.asList(unsupportedSet)));

        arguments.add(Arguments.of("{{topic}}-{{start_offset}}.avro", "partition", isSource,
                Arrays.asList("partition distribution type requires {{partition}} in the file.name.template")));

        arguments.add(Arguments.of("*.parquet", "partition", isSource,
                Arrays.asList("partition distribution type requires {{partition}} in the file.name.template")));

        arguments.add(Arguments.of("{{topic}}-{{partition}}-{{start_offset}}.avro", "partition", isSink,
                Collections.emptyList()));

        arguments.add(Arguments.of("{{topic}}-{{partition}}-{{start_offset}}.avro", "partition", isSource,
                Collections.emptyList()));

        arguments.add(Arguments.of("*-{{partition}}.parquet", "partition", isSink, Arrays.asList(unsupportedSet)));

        arguments.add(Arguments.of("*-{{partition}}.parquet", "partition", isSource, Collections.emptyList()));

        arguments.add(Arguments.of("{{start_offset}}-{{partition}}-{{topic}}-{{timestamp}}", "object_hash", isSink,
                Arrays.asList(
                        "parameter unit is required for the the variable timestamp, supported values are: yyyy|MM|dd|HH")));

        arguments.add(Arguments.of("{{start_offset}}-{{partition}}-{{topic}}-{{timestamp}}", "object_hash", isSource,
                Arrays.asList(
                        "parameter unit is required for the the variable timestamp, supported values are: yyyy|MM|dd|HH")));

        arguments.add(Arguments.of("", "object_hash", isSink, Arrays.asList(
                "RecordGrouper requires that the template [] has variables defined. Supported variables are: topic,partition,start_offset,timestamp; topic,partition,key,start_offset,timestamp; key; key,topic,partition")));

        return arguments;
    }

}
