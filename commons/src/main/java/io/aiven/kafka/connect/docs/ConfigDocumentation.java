package io.aiven.kafka.connect.docs;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Converter;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.kafka.common.config.ConfigDef;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import static java.lang.String.format;

public class ConfigDocumentation {

    enum Format {XML, TEXT, TABLE, HTML, RST, ENRICHED_RST, YAML};

    private static final Converter<Format, IllegalArgumentException> FORMAT = s -> Format.valueOf(s.toUpperCase(Locale.ROOT));
    private static final Converter<ConfigDef, IllegalArgumentException> CONFIG_DEF = className -> {
        String errorFmt = "`%s` does not implement a 'public static ConfigDef configDef()' method";
        try {
            Class<?> configClass = Class.forName(className);
            try {
                Method configDefMethod = configClass.getDeclaredMethod("configDef");
                if (configDefMethod.getReturnType() != ConfigDef.class || !Modifier.isStatic(configDefMethod.getModifiers()) || !Modifier.isPublic(configDefMethod.getModifiers())) {
                    throw new IllegalArgumentException(format(errorFmt, configClass.getName()));
                }
                return (ConfigDef) configDefMethod.invoke(null);
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException(format(errorFmt, configClass.getName()));
            } catch (InvocationTargetException | IllegalAccessException e) {
                throw new IllegalArgumentException(e.getMessage());
            }
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    };

    private static Option classOption = Option.builder("c").longOpt("class").desc("The class name of the config file")
            .required().converter(CONFIG_DEF).hasArg().build();

    private static Option formatOption = Option.builder("f").longOpt("format").desc("The output format. (Defaults to TEXT)")
            .converter(FORMAT).hasArg().build();


    private static Options createOptions() {
        return new Options()
                .addOption(classOption)
                .addOption(Option.builder("?").longOpt("help").desc("Print this message").build())
                .addOption(formatOption)
                .addOption(Option.builder("o").longOpt("output").desc("Output file name (Defaults to System.out")
                        .converter(Converter.FILE).hasArg().build());
    }

    public static void printHelp() {
        HelpFormatter formatter = HelpFormatter.builder().get();
        formatter.printHelp("", createOptions());
    }

    public static void main(final String[] args) throws IOException {
        try {
            CommandLine commandLine = new DefaultParser().parse(createOptions(), args);
            if (commandLine.hasOption("?")) {
                printHelp();
            } else {
                Format format = commandLine.getParsedOptionValue(formatOption, () -> Format.TEXT);
                ConfigDef configDef = commandLine.getParsedOptionValue(classOption);
                File outputFile = commandLine.getParsedOptionValue("output");
                outputFile = outputFile.getAbsoluteFile();
                outputFile.getParentFile().mkdirs();
                Appendable out = outputFile == null ? System.out : new FileWriter(outputFile);
                try {
                    switch (format) {
                        case XML:
                            xml(configDef, out);
                            break;
                        case TEXT:
                            text(configDef, out);
                            break;
                        case TABLE:
                            table(configDef, out);
                            break;
                        case HTML:
                            html(configDef, out);
                            break;
                        case RST:
                            rst(configDef, out);
                            break;
                        case ENRICHED_RST:
                            enrichedRst(configDef, out);
                            break;
                        case YAML:
                            yaml(configDef, out);
                            break;
                        default:
                            throw new IllegalArgumentException("Unsupported format: " + format);
                    }
                } finally {
                    if (outputFile != null) {
                        ((FileWriter) out).close();
                    }
                }

            }
        } catch (ParseException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static void table(ConfigDef config, Appendable out) throws IOException {
        Collection< ConfigDef.ConfigKey> keys = config.configKeys().values();
        int maxColumnWidth = 35;
        int tableWidth = 180;

        List<String> headers = Arrays.asList("Name", "Default", "Description");
        int maxName = 0;
        int maxValue = 0;
        Map<String, List<String>> rows = new TreeMap<>();
        for (ConfigDef.ConfigKey key : keys) {
            maxName = Math.max(maxName, key.name.length());
            String value = key.defaultValue == null ? "" : key.defaultValue.toString();
            maxValue = Math.max(maxValue, value.length());
            rows.put(key.name, Arrays.asList(key.name, value, key.documentation));
        }

        maxName = Math.min(maxName, maxColumnWidth);
        maxValue = Math.min(maxValue, maxColumnWidth);
        int maxDesc = tableWidth - maxName - maxValue;
        List<TextStyle> styles = Arrays.asList(
                TextStyle.builder().setMaxWidth(maxName).get(),
                TextStyle.builder().setMaxWidth(maxValue).get(),
                TextStyle.builder().setMaxWidth(maxDesc).get());

        TableDefinition tableDefinition = TableDefinition.from("", styles, headers, rows.values());

        MarkdownDocAppendable output = new MarkdownDocAppendable(out);
        output.appendTable(tableDefinition);
    }

    public static void text(ConfigDef config, Appendable out) throws IOException {
        Collection< ConfigDef.ConfigKey> keys = config.configKeys().values();

        List<String> headers = Arrays.asList("Name", "Default", "Description");

        Map<String, ConfigDef.ConfigKey> sections = new TreeMap<>();
        for (ConfigDef.ConfigKey key : keys) {
            sections.put(key.name, key);
        }

        /*        out.append("  <section>\n");
            out.append("      <name>" + StringEscapeUtils.escapeXml11(section.name) + "</name>\n");
            out.append("      <documentation>" + StringEscapeUtils.escapeXml11(section.documentation) + "</documentation>\n");
            out.append("      <type>" + StringEscapeUtils.escapeXml11(section.type.toString()) + "</type>\n");
            out.append("      <default>" + StringEscapeUtils.escapeXml11(asString(section.defaultValue)) + "</default>\n");
            out.append("      <validValues>" + StringEscapeUtils.escapeXml11(asString(section.validator)) + "</validValues>\n");
            out.append("      <importance>" + StringEscapeUtils.escapeXml11(section.importance.toString()) + "</importance>\n");
            out.append("  </section>\n");

         */
        MarkdownDocAppendable output = new MarkdownDocAppendable(out);

        for (ConfigDef.ConfigKey section : sections.values()) {
            output.appendHeader(2, section.displayName);
            List<CharSequence> lst = new ArrayList<>();
            lst.add("Default value: " + section.defaultValue);
            lst.add("Type: " + section.type.name());
            lst.add(format("Valid values: %s", section.validator));
            lst.add(format("Importance: %s", section.importance));
            output.appendList(false, lst);

            output.appendParagraph(section.documentation);
        }
    }

    private static String asString(Object o) {
        return o == null ? "NULL" : o.toString();
    }

    public static void xml(ConfigDef config, Appendable out) throws IOException {
        Collection< ConfigDef.ConfigKey> keys = config.configKeys().values();

        List<String> headers = Arrays.asList("Name", "Default", "Description");

        Map<String, ConfigDef.ConfigKey> sections = new TreeMap<>();
        for (ConfigDef.ConfigKey key : keys) {
            sections.put(key.name, key);
        }

        out.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        out.append(format("<%s>%n", config.getClass().getSimpleName()));


        for (ConfigDef.ConfigKey section : sections.values()) {
            out.append("  <section>\n");
            out.append("      <name>" + StringEscapeUtils.escapeXml11(section.name) + "</name>\n");
            out.append("      <documentation>" + StringEscapeUtils.escapeXml11(section.documentation) + "</documentation>\n");
            out.append("      <type>" + StringEscapeUtils.escapeXml11(section.type.toString()) + "</type>\n");
            out.append("      <default>" + StringEscapeUtils.escapeXml11(asString(section.defaultValue)) + "</default>\n");
            out.append("      <validValues>" + StringEscapeUtils.escapeXml11(asString(section.validator)) + "</validValues>\n");
            out.append("      <importance>" + StringEscapeUtils.escapeXml11(section.importance.toString()) + "</importance>\n");
            out.append("  </section>\n");
        }
        out.append(format("</%s>%n", config.getClass().getSimpleName()));
    }

    public static void html(ConfigDef config, Appendable out) throws IOException {
        out.append(config.toHtmlTable());
    }

    public static void rst(ConfigDef config, Appendable out) throws IOException {
        out.append(config.toRst());
    }

    public static void enrichedRst(ConfigDef config, Appendable out) throws IOException {
        out.append(config.toEnrichedRst());
    }

    public static void yaml(ConfigDef config, Appendable out) throws IOException {
        Collection< ConfigDef.ConfigKey> keys = config.configKeys().values();

        List<String> headers = Arrays.asList("Name", "Default", "Description");

        Map<String, ConfigDef.ConfigKey> sections = new TreeMap<>();
        for (ConfigDef.ConfigKey key : keys) {
            sections.put(key.name, key);
        }

        out.append(format("%s:%n", config.getClass().getSimpleName()));

        for (ConfigDef.ConfigKey section : sections.values()) {
            out.append(format("- %n  name: %s%n",  section.name));
            out.append(format("  documentation: %s%n", section.documentation));
            out.append(format("  type: %s%n", section.type));
            out.append(format("  default: %s%n", section.defaultValue));
            out.append(format("  validValues: %s%n", section.validator));
            out.append(format("  importance: %s%n", section.importance));
        }

    }
}
