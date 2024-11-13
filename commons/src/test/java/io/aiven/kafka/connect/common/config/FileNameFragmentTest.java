package io.aiven.kafka.connect.common.config;


import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FileNameFragmentTest {

    /**
     * An enumeration to expose the FileNameFragment properties names to test cases
     */
    public enum FileNameArgs {
        GROUP_FILE(FileNameFragment.GROUP_FILE), FILE_COMPRESSION_TYPE_CONFIG(FileNameFragment.FILE_COMPRESSION_TYPE_CONFIG),
        FILE_MAX_RECORDS(FileNameFragment.FILE_MAX_RECORDS),
        FILE_NAME_TIMESTAMP_TIMEZONE(FileNameFragment.FILE_NAME_TIMESTAMP_TIMEZONE),
        FILE_NAME_TIMESTAMP_SOURCE(FileNameFragment.FILE_NAME_TIMESTAMP_SOURCE),
        FILE_NAME_TEMPLATE_CONFIG(FileNameFragment.FILE_NAME_TEMPLATE_CONFIG),
        DEFAULT_FILENAME_TEMPLATE(FileNameFragment.DEFAULT_FILENAME_TEMPLATE);
        String key;

        FileNameArgs(String key) {
            this.key = key;
        }

        public String key() {
            return key;
        }
    }

    @ParameterizedTest( name = "{index} {0}")
    @MethodSource("configDefSource")
    public void configDefTest(FileNameArgs arg, ConfigDef.Type type, Object defaultValue, boolean validatorPresent, ConfigDef.Importance importance,  String group, int orderInGroup, ConfigDef.Width width, boolean recommenderPresent ) {
        ConfigDef configDef = FileNameFragment.update(new ConfigDef());
        ConfigDef.ConfigKey key = configDef.configKeys().get(arg.key());

        assertEquals(arg.key(), key.name, "Wrong key name");
        assertEquals(type, key.type, "Wrong key type");
        assertEquals(defaultValue, key.defaultValue, "Wrong default value");
        assertEquals(validatorPresent, key.validator != null, () -> String.format("Validator was %spresent.", key.validator == null ? "not " : ""));
        assertEquals(importance, key.importance, "Wrong importance");
        assertNotNull(key.documentation, "Documenttion not included");
        assertEquals(group, key.group, "Wrong group");
        assertEquals(orderInGroup, key.orderInGroup, "Wrong order in group");
        assertEquals(width, key.width, "Wrong width");
        assertEquals(key.name, key.displayName);
        assertEquals(recommenderPresent, key.recommender != null, () -> String.format("Recommender was %spresent.", key.recommender == null ? "not " : ""));
    }

    @Test
    public void allConfigDefsAccountForTest() {
        // create a modifiable list.
        List<FileNameArgs> argList = new ArrayList<>(Arrays.asList(FileNameArgs.values()));
        // remove the non-argument values
        argList.remove(FileNameArgs.GROUP_FILE);
        argList.remove(FileNameArgs.DEFAULT_FILENAME_TEMPLATE);
        configDefSource().map(a -> (FileNameArgs)(a.get()[0])).forEach(argList::remove);
        assertTrue(argList.isEmpty(), () -> "Tests do not process the following arguments: " + String.join(", ", argList.stream().map(arg -> arg.toString()).collect(Collectors.toList())));
    }

    private static Stream<Arguments> configDefSource(){
        List<Arguments> args = new ArrayList<>();

        args.add(Arguments.of( FileNameArgs.FILE_NAME_TEMPLATE_CONFIG, ConfigDef.Type.STRING, null, true, ConfigDef.Importance.MEDIUM,
                FileNameArgs.GROUP_FILE.key(), 0, ConfigDef.Width.LONG, false));

       args.add(Arguments.of(FileNameArgs.FILE_COMPRESSION_TYPE_CONFIG, ConfigDef.Type.STRING, null, true, ConfigDef.Importance.MEDIUM,
               FileNameArgs.GROUP_FILE.key(), 1, ConfigDef.Width.NONE, true));

        args.add(Arguments.of(FileNameArgs.FILE_MAX_RECORDS, ConfigDef.Type.INT, 0, true, ConfigDef.Importance.MEDIUM,
                FileNameArgs.GROUP_FILE.key(), 2, ConfigDef.Width.SHORT, false));

        args.add(Arguments.of(FileNameArgs.FILE_NAME_TIMESTAMP_TIMEZONE, ConfigDef.Type.STRING, ZoneOffset.UTC.toString(), true, ConfigDef.Importance.LOW,
                FileNameArgs.GROUP_FILE.key(), 3, ConfigDef.Width.SHORT, false));


        args.add(Arguments.of(FileNameArgs.FILE_NAME_TIMESTAMP_SOURCE, ConfigDef.Type.STRING, TimestampSource.Type.WALLCLOCK.name(), true, ConfigDef.Importance.LOW,
                FileNameArgs.GROUP_FILE.key(), 4, ConfigDef.Width.SHORT, false));

        return args.stream();
    }

    @Test
    public void validateTest() {
        ConfigDef configDef = FileNameFragment.update(OutputFormatFragment.update(new ConfigDef(), null));
        Map<String, String> props = new HashMap<>();
        props.put(FileNameFragment.FILE_NAME_TEMPLATE_CONFIG, "my-file-name-template-{{key}}");
        props.put(FileNameFragment.FILE_MAX_RECORDS, "50");
        AbstractConfig cfg = new AbstractConfig(configDef, props);

        FileNameFragment underTest = new FileNameFragment(cfg);
        assertThrows(ConfigException.class, () -> underTest.validate());
    }

    @ParameterizedTest( name = "{index} {0}")
    @MethodSource("fieldNameSource")
    public void getFilenameTest(String expected, Map<String,String> props) {
        ConfigDef configDef = FileNameFragment.update(OutputFormatFragment.update(new ConfigDef(), null));
        AbstractConfig cfg = new AbstractConfig(configDef, props);

        FileNameFragment underTest = new FileNameFragment(cfg);
        assertEquals(expected, underTest.getFilename());
    }

    private  static Stream<Arguments> fieldNameSource() {
        List<Arguments> args = new ArrayList<>();
        Map<String, String> props = new HashMap<>();
        props.put(FileNameArgs.FILE_NAME_TEMPLATE_CONFIG.key, "my-file-name-template-{{key}}");
        args.add(Arguments.of("my-file-name-template-{{key}}", props));

        props = new HashMap<>();
        props.put(OutputFormatFragment.FORMAT_OUTPUT_TYPE_CONFIG, FormatType.CSV.name);
        args.add(Arguments.of(FileNameArgs.DEFAULT_FILENAME_TEMPLATE.key, props));

        for (CompressionType compressionType : CompressionType.values()) {
            props = new HashMap<>();
            props.put(OutputFormatFragment.FORMAT_OUTPUT_TYPE_CONFIG, FormatType.CSV.name);
            props.put(CompressionFragment.FILE_COMPRESSION_TYPE_CONFIG, compressionType.name);
            args.add(Arguments.of(FileNameArgs.DEFAULT_FILENAME_TEMPLATE.key + compressionType.extension(), props));
        }

        for (CompressionType compressionType : CompressionType.values()) {
            props = new HashMap<>();
            props.put(OutputFormatFragment.FORMAT_OUTPUT_TYPE_CONFIG, FormatType.AVRO.name);
            props.put(CompressionFragment.FILE_COMPRESSION_TYPE_CONFIG, compressionType.name);
            args.add(Arguments.of(FileNameArgs.DEFAULT_FILENAME_TEMPLATE.key + ".avro" + compressionType.extension(), props));
        }

        return args.stream();
    }

    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("fieldNameSource")
    public void getFilenameTemplateTest(String expected, Map<String,String> props) {
        ConfigDef configDef = FileNameFragment.update(OutputFormatFragment.update(new ConfigDef(), null));
        AbstractConfig cfg = new AbstractConfig(configDef, props);

        FileNameFragment underTest = new FileNameFragment(cfg);
        // template does not implement equality
        assertEquals(expected, underTest.getFilenameTemplate().originalTemplate());
    }

    @Test
    public void getFilenameTimezoneTest() {
        final ConfigDef configDef = FileNameFragment.update(new ConfigDef());
        Map<String, String> props = new HashMap<>();
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
    public void getFilenameTimestampSource() {
        final ConfigDef configDef = FileNameFragment.update(new ConfigDef());
        Map<String, String> props = new HashMap<>();
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
    public void getMaxRecordsPerFile() {
        final ConfigDef configDef = FileNameFragment.update(new ConfigDef());
        Map<String, String> props = new HashMap<>();
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
