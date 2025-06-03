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

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class FileNameFragmentTest {// NOPMD

    /**
     * An enumeration to expose the FileNameFragment properties names to test cases
     */
    public enum FileNameArgs {
        GROUP_FILE(FileNameFragment.GROUP_FILE), FILE_COMPRESSION_TYPE_CONFIG(
                FileNameFragment.FILE_COMPRESSION_TYPE_CONFIG), FILE_MAX_RECORDS(
                        FileNameFragment.FILE_MAX_RECORDS), FILE_NAME_TIMESTAMP_TIMEZONE(
                                FileNameFragment.FILE_NAME_TIMESTAMP_TIMEZONE), FILE_NAME_TIMESTAMP_SOURCE(
                                        FileNameFragment.FILE_NAME_TIMESTAMP_SOURCE), FILE_NAME_TEMPLATE_CONFIG(
                                                FileNameFragment.FILE_NAME_TEMPLATE_CONFIG), DEFAULT_FILENAME_TEMPLATE(
                                                        FileNameFragment.DEFAULT_FILENAME_TEMPLATE);
        private final String keyValue;

        FileNameArgs(final String key) {
            this.keyValue = key;
        }

        public String key() {
            return keyValue;
        }
    }

    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("configDefSource")
    void configDefTest(final FileNameArgs arg, final ConfigDef.Type type, final Object defaultValue,
            final boolean validatorPresent, final ConfigDef.Importance importance, final boolean recommenderPresent) {
        final ConfigDef configDef = FileNameFragment.update(new ConfigDef());
        final ConfigDef.ConfigKey key = configDef.configKeys().get(arg.key());

        assertThat(arg.key()).as("Wrong key name").isEqualTo(key.name);
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
        // create a modifiable list.
        final List<FileNameArgs> argList = new ArrayList<>(Arrays.asList(FileNameArgs.values()));
        // remove the non-argument values
        argList.remove(FileNameArgs.GROUP_FILE);
        argList.remove(FileNameArgs.DEFAULT_FILENAME_TEMPLATE);
        configDefSource().map(a -> (FileNameArgs) (a.get()[0])).forEach(argList::remove);
        assertThat(argList.isEmpty())
                .as(() -> "Tests do not process the following arguments: "
                        + String.join(", ", argList.stream().map(arg -> arg.toString()).collect(Collectors.toList())))
                .isTrue();
    }

    private static Stream<Arguments> configDefSource() {
        final List<Arguments> args = new ArrayList<>();

        args.add(Arguments.of(FileNameArgs.FILE_NAME_TEMPLATE_CONFIG, ConfigDef.Type.STRING, null, true,
                ConfigDef.Importance.MEDIUM, false));

        args.add(Arguments.of(FileNameArgs.FILE_COMPRESSION_TYPE_CONFIG, ConfigDef.Type.STRING, null, true,
                ConfigDef.Importance.MEDIUM, true));

        args.add(Arguments.of(FileNameArgs.FILE_MAX_RECORDS, ConfigDef.Type.INT, 0, true, ConfigDef.Importance.MEDIUM, false));

        args.add(Arguments.of(FileNameArgs.FILE_NAME_TIMESTAMP_TIMEZONE, ConfigDef.Type.STRING,
                ZoneOffset.UTC.toString(), true, ConfigDef.Importance.LOW, false));

        args.add(Arguments.of(FileNameArgs.FILE_NAME_TIMESTAMP_SOURCE, ConfigDef.Type.STRING,
                TimestampSource.Type.WALLCLOCK.name(), true, ConfigDef.Importance.LOW, false));

        return args.stream();
    }

    @Test
    void validateTest() {
        final ConfigDef configDef = FileNameFragment.update(OutputFormatFragment.update(new ConfigDef(), null));
        final Map<String, String> props = new HashMap<>();
        props.put(FileNameFragment.FILE_NAME_TEMPLATE_CONFIG, "my-file-name-template-{{key}}");
        props.put(FileNameFragment.FILE_MAX_RECORDS, "50");
        final AbstractConfig cfg = new AbstractConfig(configDef, props);
        final FileNameFragment underTest = new FileNameFragment(cfg);
        assertThatThrownBy(underTest::validateRecordGrouper).isInstanceOf(ConfigException.class);
    }

    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("fieldNameSource")
    void testGetFilename(final String expected, final Map<String, String> props) {
        final ConfigDef configDef = FileNameFragment.update(OutputFormatFragment.update(new ConfigDef(), null));
        final AbstractConfig cfg = new AbstractConfig(configDef, props);
        final FileNameFragment underTest = new FileNameFragment(cfg);
        assertThat(expected).isEqualTo(underTest.getFilename());
    }

    private static Stream<Arguments> fieldNameSource() {
        final List<Arguments> args = new ArrayList<>();
        // we have to make new instances of props or the tests will fail.
        Map<String, String> props = new HashMap<>();
        props.put(FileNameFragment.FILE_NAME_TEMPLATE_CONFIG, "my-file-name-template-{{key}}");
        args.add(Arguments.of("my-file-name-template-{{key}}", props));

        props = new HashMap<>();
        props.put(OutputFormatFragment.FORMAT_OUTPUT_TYPE_CONFIG, FormatType.CSV.name);
        args.add(Arguments.of(FileNameFragment.DEFAULT_FILENAME_TEMPLATE, props));

        for (final CompressionType compressionType : CompressionType.values()) {
            args.add(Arguments.of(FileNameFragment.DEFAULT_FILENAME_TEMPLATE + compressionType.extension(),
                    Map.of(OutputFormatFragment.FORMAT_OUTPUT_TYPE_CONFIG, FormatType.CSV.name,
                            CompressionFragment.FILE_COMPRESSION_TYPE_CONFIG, compressionType.name)));
        }

        for (final CompressionType compressionType : CompressionType.values()) {
            args.add(Arguments.of(FileNameFragment.DEFAULT_FILENAME_TEMPLATE + ".avro" + compressionType.extension(),
                    Map.of(OutputFormatFragment.FORMAT_OUTPUT_TYPE_CONFIG, FormatType.AVRO.name,
                            CompressionFragment.FILE_COMPRESSION_TYPE_CONFIG, compressionType.name)));
        }

        return args.stream();
    }

    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("fieldNameSource")
    void testGetFilenameTemplate(final String expected, final Map<String, String> props) {
        final ConfigDef configDef = FileNameFragment.update(OutputFormatFragment.update(new ConfigDef(), null));
        final AbstractConfig cfg = new AbstractConfig(configDef, props);
        final FileNameFragment underTest = new FileNameFragment(cfg);
        // template does not implement equality
        assertThat(expected).isEqualTo(underTest.getFilenameTemplate().originalTemplate());
    }

    @Test
    void testGetFilenameTimezone() {
        final ConfigDef configDef = FileNameFragment.update(new ConfigDef());
        final Map<String, String> props = new HashMap<>();
        props.put(FileNameFragment.FILE_NAME_TIMESTAMP_TIMEZONE, "Europe/Dublin");
        AbstractConfig cfg = new AbstractConfig(configDef, props);
        FileNameFragment underTest = new FileNameFragment(cfg);
        assertThat("Europe/Dublin").isEqualTo(underTest.getFilenameTimezone().toString());

        props.clear();
        cfg = new AbstractConfig(configDef, props);
        underTest = new FileNameFragment(cfg);
        assertThat("Z").isEqualTo(underTest.getFilenameTimezone().toString());

        props.clear();
        props.put(FileNameFragment.FILE_NAME_TIMESTAMP_TIMEZONE, "NotARealTimezone");
        assertThatThrownBy(() -> new AbstractConfig(configDef, props)).isInstanceOf(ConfigException.class);
    }

    @Test
    void testGetFilenameTimestampSource() {
        final ConfigDef configDef = FileNameFragment.update(new ConfigDef());
        final Map<String, String> props = new HashMap<>();
        AbstractConfig cfg = new AbstractConfig(configDef, props);
        FileNameFragment underTest = new FileNameFragment(cfg);
        TimestampSource src = underTest.getFilenameTimestampSource();
        assertThat(TimestampSource.Type.WALLCLOCK).isEqualTo(src.type());

        props.put(FileNameFragment.FILE_NAME_TIMESTAMP_SOURCE, TimestampSource.Type.EVENT.name());
        cfg = new AbstractConfig(configDef, props);
        underTest = new FileNameFragment(cfg);
        src = underTest.getFilenameTimestampSource();
        assertThat(TimestampSource.Type.EVENT).isEqualTo(src.type());

        props.put(FileNameFragment.FILE_NAME_TIMESTAMP_SOURCE, "NotATimestampSource");
        assertThatThrownBy(() -> new AbstractConfig(configDef, props)).isInstanceOf(ConfigException.class);
    }

    @Test
    void testGetMaxRecordsPerFile() {
        final ConfigDef configDef = FileNameFragment.update(new ConfigDef());
        final Map<String, String> props = new HashMap<>();
        AbstractConfig cfg = new AbstractConfig(configDef, props);
        FileNameFragment underTest = new FileNameFragment(cfg);
        int count = underTest.getMaxRecordsPerFile();
        assertThat(0).isEqualTo(count);

        props.put(FileNameFragment.FILE_MAX_RECORDS, "200");
        cfg = new AbstractConfig(configDef, props);
        underTest = new FileNameFragment(cfg);
        count = underTest.getMaxRecordsPerFile();
        assertThat(200).isEqualTo(count);

        props.put(FileNameFragment.FILE_MAX_RECORDS, "NotANumber");
        assertThatThrownBy(() -> new AbstractConfig(configDef, props)).isInstanceOf(ConfigException.class);

        props.put(FileNameFragment.FILE_MAX_RECORDS, "-1");
        assertThatThrownBy(() -> new AbstractConfig(configDef, props)).isInstanceOf(ConfigException.class);
    }
}
