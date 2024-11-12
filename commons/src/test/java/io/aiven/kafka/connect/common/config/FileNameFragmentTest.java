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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
                       final boolean validatorPresent, final ConfigDef.Importance importance, final String group,
                       final int orderInGroup, final ConfigDef.Width width, final boolean recommenderPresent) {
        final ConfigDef configDef = FileNameFragment.update(new ConfigDef());
        final ConfigDef.ConfigKey key = configDef.configKeys().get(arg.key());

        assertEquals(arg.key(), key.name, "Wrong key name");
        assertEquals(type, key.type, "Wrong key type");
        assertEquals(defaultValue, key.defaultValue, "Wrong default value");
        assertEquals(validatorPresent, key.validator != null,
                () -> String.format("Validator was %spresent.", key.validator == null ? "not " : ""));
        assertEquals(importance, key.importance, "Wrong importance");
        assertNotNull(key.documentation, "Documenttion not included");
        assertEquals(group, key.group, "Wrong group");
        assertEquals(orderInGroup, key.orderInGroup, "Wrong order in group");
        assertEquals(width, key.width, "Wrong width");
        assertEquals(key.name, key.displayName);
        assertEquals(recommenderPresent, key.recommender != null,
                () -> String.format("Recommender was %spresent.", key.recommender == null ? "not " : ""));
    }

    @Test
    void allConfigDefsAccountForTest() {
        // create a modifiable list.
        final List<FileNameArgs> argList = new ArrayList<>(Arrays.asList(FileNameArgs.values()));
        // remove the non-argument values
        argList.remove(FileNameArgs.GROUP_FILE);
        argList.remove(FileNameArgs.DEFAULT_FILENAME_TEMPLATE);
        configDefSource().map(a -> (FileNameArgs) (a.get()[0])).forEach(argList::remove);
        assertTrue(argList.isEmpty(), () -> "Tests do not process the following arguments: "
                + String.join(", ", argList.stream().map(arg -> arg.toString()).collect(Collectors.toList())));
    }

    private static Stream<Arguments> configDefSource() {
        final List<Arguments> args = new ArrayList<>();

        args.add(Arguments.of(FileNameArgs.FILE_NAME_TEMPLATE_CONFIG, ConfigDef.Type.STRING, null, true,
                ConfigDef.Importance.MEDIUM, FileNameArgs.GROUP_FILE.key(), 0, ConfigDef.Width.LONG, false));

        args.add(Arguments.of(FileNameArgs.FILE_COMPRESSION_TYPE_CONFIG, ConfigDef.Type.STRING, null, true,
                ConfigDef.Importance.MEDIUM, FileNameArgs.GROUP_FILE.key(), 1, ConfigDef.Width.NONE, true));

        args.add(Arguments.of(FileNameArgs.FILE_MAX_RECORDS, ConfigDef.Type.INT, 0, true, ConfigDef.Importance.MEDIUM,
                FileNameArgs.GROUP_FILE.key(), 2, ConfigDef.Width.SHORT, false));

        args.add(Arguments.of(FileNameArgs.FILE_NAME_TIMESTAMP_TIMEZONE, ConfigDef.Type.STRING,
                ZoneOffset.UTC.toString(), true, ConfigDef.Importance.LOW, FileNameArgs.GROUP_FILE.key(), 3,
                ConfigDef.Width.SHORT, false));

        args.add(Arguments.of(FileNameArgs.FILE_NAME_TIMESTAMP_SOURCE, ConfigDef.Type.STRING,
                TimestampSource.Type.WALLCLOCK.name(), true, ConfigDef.Importance.LOW, FileNameArgs.GROUP_FILE.key(), 4,
                ConfigDef.Width.SHORT, false));

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
        assertThrows(ConfigException.class, () -> underTest.validate());
    }

    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("fieldNameSource")
    void testGetFilename(final String expected, final Map<String, String> props) {
        final ConfigDef configDef = FileNameFragment.update(OutputFormatFragment.update(new ConfigDef(), null));
        final AbstractConfig cfg = new AbstractConfig(configDef, props);
        final FileNameFragment underTest = new FileNameFragment(cfg);
        assertEquals(expected, underTest.getFilename());
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
        assertEquals(expected, underTest.getFilenameTemplate().originalTemplate());
    }

    @Test
    void testGetFilenameTimezone() {
        final ConfigDef configDef = FileNameFragment.update(new ConfigDef());
        final Map<String, String> props = new HashMap<>();
        props.put(FileNameFragment.FILE_NAME_TIMESTAMP_TIMEZONE, "Europe/Dublin");
        AbstractConfig cfg = new AbstractConfig(configDef, props);
        FileNameFragment underTest = new FileNameFragment(cfg);
        assertEquals("Europe/Dublin", underTest.getFilenameTimezone().toString());

        props.clear();
        cfg = new AbstractConfig(configDef, props);
        underTest = new FileNameFragment(cfg);
        assertEquals("Z", underTest.getFilenameTimezone().toString());

        props.clear();
        props.put(FileNameFragment.FILE_NAME_TIMESTAMP_TIMEZONE, "NotARealTimezone");
        assertThrows(ConfigException.class, () -> new AbstractConfig(configDef, props));
    }

    @Test
    void testGetFilenameTimestampSource() {
        final ConfigDef configDef = FileNameFragment.update(new ConfigDef());
        final Map<String, String> props = new HashMap<>();
        AbstractConfig cfg = new AbstractConfig(configDef, props);
        FileNameFragment underTest = new FileNameFragment(cfg);
        TimestampSource src = underTest.getFilenameTimestampSource();
        assertEquals(TimestampSource.Type.WALLCLOCK, src.type());

        props.put(FileNameFragment.FILE_NAME_TIMESTAMP_SOURCE, TimestampSource.Type.EVENT.name());
        cfg = new AbstractConfig(configDef, props);
        underTest = new FileNameFragment(cfg);
        src = underTest.getFilenameTimestampSource();
        assertEquals(TimestampSource.Type.EVENT, src.type());

        props.put(FileNameFragment.FILE_NAME_TIMESTAMP_SOURCE, "NotATimestampSource");
        assertThrows(ConfigException.class, () -> new AbstractConfig(configDef, props));
    }

    @Test
    void testGetMaxRecordsPerFile() {
        final ConfigDef configDef = FileNameFragment.update(new ConfigDef());
        final Map<String, String> props = new HashMap<>();
        AbstractConfig cfg = new AbstractConfig(configDef, props);
        FileNameFragment underTest = new FileNameFragment(cfg);
        int count = underTest.getMaxRecordsPerFile();
        assertEquals(0, count);

        props.put(FileNameFragment.FILE_MAX_RECORDS, "200");
        cfg = new AbstractConfig(configDef, props);
        underTest = new FileNameFragment(cfg);
        count = underTest.getMaxRecordsPerFile();
        assertEquals(200, count);

        props.put(FileNameFragment.FILE_MAX_RECORDS, "NotANumber");
        assertThrows(ConfigException.class, () -> new AbstractConfig(configDef, props));

        props.put(FileNameFragment.FILE_MAX_RECORDS, "-1");
        assertThrows(ConfigException.class, () -> new AbstractConfig(configDef, props));

    }

}