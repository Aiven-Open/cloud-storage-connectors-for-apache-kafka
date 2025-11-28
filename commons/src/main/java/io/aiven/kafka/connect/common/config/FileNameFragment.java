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

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

import io.aiven.kafka.connect.common.config.validators.PredicateGatedValidator;
import io.aiven.kafka.connect.common.config.validators.TimeZoneValidator;
import io.aiven.kafka.connect.common.grouper.RecordGrouperFactory;
import io.aiven.kafka.connect.common.source.input.utils.FilePatternUtils;
import io.aiven.kafka.connect.common.source.task.DistributionType;
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.common.templating.VariableTemplatePart;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.LoggerFactory;

/**
 * Fragment to handle all file name extraction operations.
 */
public final class FileNameFragment extends ConfigFragment {
    /**
     * Flag to support Prefix Template as opposed to a prefix string.
     * TODO To be removed when all implementations support the prefix template.
     */
    public enum PrefixTemplateSupport {
        TRUE, FALSE
    }
    /**
     * The name of the group that this fragment places items in.
     */
    public static final String GROUP_NAME = "File";
    @VisibleForTesting
    public static final String FILE_COMPRESSION_TYPE_CONFIG = "file.compression.type";
    @VisibleForTesting
    static final String FILE_MAX_RECORDS = "file.max.records";
    @VisibleForTesting
    public static final String FILE_NAME_TIMESTAMP_TIMEZONE = "file.name.timestamp.timezone";
    @VisibleForTesting
    public static final String FILE_NAME_TIMESTAMP_SOURCE = "file.name.timestamp.source";
    @VisibleForTesting
    public static final String FILE_NAME_TEMPLATE_CONFIG = "file.name.template";
    @VisibleForTesting
    static final String DEFAULT_FILENAME_TEMPLATE = "{{topic}}-{{partition}}-{{start_offset}}";
    @VisibleForTesting
    public static final String FILE_PATH_PREFIX_TEMPLATE_CONFIG = "file.prefix.template";
    @VisibleForTesting
    public static final String FILE_NAME_PREFIX_CONFIG = "file.name.prefix";

    public static final ConfigDef.Validator COMPRESSION_TYPE_VALIDATOR = new PredicateGatedValidator(Objects::nonNull,
            ConfigDef.CaseInsensitiveValidString.in(CompressionType.names().toArray(new String[0])));

    public static final ConfigDef.Validator TIMESTAMP_SOURCE_VALIDATOR = ConfigDef.CaseInsensitiveValidString
            .in(Arrays.stream(TimestampSource.Type.values())
                    .map(TimestampSource.Type::name)
                    .collect(Collectors.toList())
                    .toArray(new String[0]));

    public static final ConfigDef.Validator PREFIX_VALIDATOR = ConfigDef.LambdaValidator.with((name, value) -> {
        if (value == null) {
            return;
        }
        try {
            final String valueStr = value.toString();
            if (valueStr.startsWith(".well-known/acme-challenge")) {
                throw new ConfigException(name, value, "cannot start with '.well-known/acme-challenge'");
            }
            Template.of(valueStr);
        } catch (IllegalArgumentException e) {
            throw new ConfigException(name, value, e.getMessage());
        }
    }, () -> "may not start with '.well-known/acme-challenge'");

    /**
     * The flag that indicates this fragment is being used as for a sink connector.
     */
    private final boolean isSink;

    /** Map of template variable name to the template variable definition */
    private static final Map<String, FilenameTemplateVariable> FILENAME_VARIABLES = new TreeMap<>();
    private static final String TEMPLATE_GROUPINGS;

    static {
        Arrays.stream(FilenameTemplateVariable.values())
                .forEach(variable -> FILENAME_VARIABLES.put(variable.name, variable));
        TEMPLATE_GROUPINGS = "[" + RecordGrouperFactory.getSupportedVariableGroups().stream()
                .map(strings -> FilePatternUtils.asPatterns(strings, ", "))
                .collect(Collectors.joining("] \n ["))
        +"]";
    }

    /**
     * Gets a setter for the properties in this fragment.
     *
     * @param data
     *            the data to update.
     * @return the Setter.
     */
    public static Setter setter(final Map<String, String> data) {
        return new Setter(data);
    }

    /**
     * Create an instance of this fragment wrapping the specified config.
     *
     * @param dataAccess
     *            the FragmentDataAccess to read from.
     * @param isSink
     *            {@code true} if this validator is to be used for a Sink connector.
     */
    public FileNameFragment(final FragmentDataAccess dataAccess, final boolean isSink) {
        super(dataAccess);
        this.isSink = isSink;
    }

    private List<String> getInvalidVariables(final Template template, final Map<String, ConfigValue> configMap) {
        final List<String> invalidVariables = new ArrayList<>(template.variablesSet());
        invalidVariables.removeAll(FILENAME_VARIABLES.keySet());
        if (!invalidVariables.isEmpty()) {
            final String variableText = invalidVariables.size() == 1 ? "variable" : "variables";
            configMap.get(FILE_NAME_TEMPLATE_CONFIG)
                    .addErrorMessage(validationMessage(FILE_NAME_TEMPLATE_CONFIG, template,
                            String.format("unsupported template %s used (%s), supported values are: %s", variableText,
                                    FilePatternUtils.asPatterns(invalidVariables, ", "),
                                    FilePatternUtils.asPatterns(FILENAME_VARIABLES.keySet(), ", "))));
        }
        return invalidVariables;
    }

    /**
     * Validate the various variables do not conflict.
     *
     * @param configMap
     *            the distribution type for the validator
     */
    @Override
    @SuppressWarnings("PMD.EmptyCatchBlock")
    public void validate(final Map<String, ConfigValue> configMap) {

        try {
            final Template template = getFilenameTemplate();

            // detect, log and remove invalid variables.
            final List<String> invalidVariables = getInvalidVariables(template, configMap);

            // create list of valid filenameVars found in the template
            final Set<FilenameTemplateVariable> filenameVars = template.variablesSet()
                    .stream()
                    .filter(name -> !invalidVariables.contains(name))
                    .map(FILENAME_VARIABLES::get)
                    .collect(Collectors.toSet());

            // create a mapping of FilenameTemplateVariables that have parameters defined in the template
            final Map<FilenameTemplateVariable, VariableTemplatePart.Parameter> parameterMap = new TreeMap<>();
            template.variablesWithParameters()
                    .stream()
                    .map(p -> Pair.of(FILENAME_VARIABLES.get(p.getKey()), p.getValue()))
                    .filter(var -> var.getKey() != null)
                    .forEach(variable -> parameterMap.put(variable.getKey(), variable.getValue()));

            // verify parameter requirements.
            for (final FilenameTemplateVariable filenameVar : filenameVars) {
                final VariableTemplatePart.Parameter varParameter = parameterMap.get(filenameVar);
                // validate required parameters are present
                if (filenameVar.parameterDescriptor.required) {
                    if (!filenameVar.parameterDescriptor.name.equals(varParameter.getName())) {
                        configMap.get(FILE_NAME_TEMPLATE_CONFIG)
                                .addErrorMessage(validationMessage(FILE_NAME_TEMPLATE_CONFIG, template, String.format(
                                        "parameter %s is required for the the variable %s, supported values are: %s",
                                        filenameVar.parameterDescriptor.name, filenameVar.name,
                                        String.join("|", filenameVar.parameterDescriptor.values))));
                    }
                } else {
                    final boolean templateSpecifiesParameter = !FilenameTemplateVariable.ParameterDescriptor.NO_PARAMETER
                            .equals(filenameVar.parameterDescriptor);
                    final boolean variableParameterIsEmpty = varParameter.equals(VariableTemplatePart.Parameter.EMPTY);

                    if (templateSpecifiesParameter && !variableParameterIsEmpty
                            && !filenameVar.parameterDescriptor.values.contains(varParameter.getValue())) {
                        configMap.get(FILE_NAME_TEMPLATE_CONFIG)
                                .addErrorMessage(validationMessage(FILE_NAME_TEMPLATE_CONFIG, template,
                                        String.format(
                                                "%s is not a valid value for parameter %s, supported values are: %s",
                                                varParameter.getValue(), filenameVar.parameterDescriptor.name,
                                                filenameVar.parameterDescriptor)));
                    }

                }
            }
            if (isSink) {
                validateSink(configMap, template);
            } else {
                validateSource(configMap, template, filenameVars);
            }
        } catch (final IllegalArgumentException e) {
            // potentially thrown by getFilenameTemplate();
            // do nothing as it is already in the configMap.
        }
    }

    private void validateSource(final Map<String, ConfigValue> configMap, final Template template,
            final Set<FilenameTemplateVariable> filenameVars) {
        final SourceConfigFragment srcFragment = new SourceConfigFragment(dataAccess);
        if (srcFragment.hasDistributionType()) {
            final DistributionType distributionType = srcFragment.getDistributionType();
            // TODO when distributionType expands then add required parameters to each distribution type object
            // and change this to a switch statement
            if (distributionType.equals(DistributionType.PARTITION)
                    && !filenameVars.contains(FilenameTemplateVariable.PARTITION)) {
                configMap.get(FILE_NAME_TEMPLATE_CONFIG)
                        .addErrorMessage(validationMessage(FILE_NAME_TEMPLATE_CONFIG, template,
                                String.format("partition distribution type requires %s in the %s",
                                        FilePatternUtils.asPattern(FilenameTemplateVariable.PARTITION.name),
                                        FILE_NAME_TEMPLATE_CONFIG)));
            }
        }

    }

    /**
     * Validate that the record grouper works with the fileNameTemplate and the max records per file.
     */
    private void validateSink(final Map<String, ConfigValue> configMap, final Template template) {
        String groupType = null;
        try {
            groupType = RecordGrouperFactory.resolveRecordGrouperType(template);
        } catch (final IllegalArgumentException e) {
            configMap.get(FILE_NAME_TEMPLATE_CONFIG)
                    .addErrorMessage(validationMessage(FILE_NAME_TEMPLATE_CONFIG, getFilename(), e.getMessage()));
        }
        final boolean isKeyBased = RecordGrouperFactory.KEY_RECORD.equals(groupType)
                || RecordGrouperFactory.KEY_TOPIC_PARTITION_RECORD.equals(groupType);
        if (isKeyBased && getMaxRecordsPerFile() > 1) {
            configMap.get(FILE_MAX_RECORDS)
                    .addErrorMessage(validationMessage(FILE_MAX_RECORDS, getMaxRecordsPerFile(),
                            String.format("When %s is %s, %s must be either 1 or not set", FILE_NAME_TEMPLATE_CONFIG,
                                    getFilename(), FILE_MAX_RECORDS)));
        }
    }

    /**
     * Adds the FileName properties to the configuration definition.
     *
     * @param configDef
     *            the configuration definition to update.
     * @param defaultCompressionType
     *            The default compression type. May be {@code null}.
     * @return number of items in the file group.
     */
    public static int update(final ConfigDef configDef, final CompressionType defaultCompressionType,
            final PrefixTemplateSupport prefixTemplateSupport) {
        int fileGroupCounter = 0;

        configDef.define(FILE_NAME_TEMPLATE_CONFIG, ConfigDef.Type.STRING, null, PREFIX_VALIDATOR,
                ConfigDef.Importance.MEDIUM,
                "The template for file names on storage system. "
                        + "Supports `{{ variable }}` placeholders for substituting variables. "
                        + "Currently supported variables are "
                + String.join(", ", FilePatternUtils.asPatterns(FILENAME_VARIABLES.keySet(), ", "))
                        + ". Only some combinations of variables are valid, which currently are: "
                        + TEMPLATE_GROUPINGS,
                GROUP_NAME, ++fileGroupCounter, ConfigDef.Width.LONG, FILE_NAME_TEMPLATE_CONFIG);

        if (prefixTemplateSupport.equals(PrefixTemplateSupport.TRUE)) {
            configDef.define(FILE_PATH_PREFIX_TEMPLATE_CONFIG, ConfigDef.Type.STRING, null, PREFIX_VALIDATOR,
                    ConfigDef.Importance.MEDIUM,
                    "The template for file names prefixes on storage system. "
                            + "Supports `{{ variable }}` placeholders for substituting variables. "
                            + "Currently supported variables are `topic`, `partition`, and `start_offset` "
                            + "(the offset of the first record in the file). "
                            + "Only some combinations of variables are valid, which currently are:\n"
                            + "- `topic`, `partition`, `start_offset`."
                            + "There is also `key` only variable {{key}} for grouping by keys",
                    GROUP_NAME, ++fileGroupCounter, ConfigDef.Width.LONG, FILE_PATH_PREFIX_TEMPLATE_CONFIG);
        } else {
            configDef.define(FILE_NAME_PREFIX_CONFIG, ConfigDef.Type.STRING, "", PREFIX_VALIDATOR,
                    ConfigDef.Importance.MEDIUM, "The prefix to be added to the name of each file.",
                    FileNameFragment.GROUP_NAME, ++fileGroupCounter, ConfigDef.Width.LONG, FILE_NAME_PREFIX_CONFIG);
        }
        configDef.define(FILE_COMPRESSION_TYPE_CONFIG, ConfigDef.Type.STRING, defaultCompressionType.name(),
                COMPRESSION_TYPE_VALIDATOR, ConfigDef.Importance.MEDIUM, "The compression type used for files.",
                GROUP_NAME, ++fileGroupCounter, ConfigDef.Width.NONE, FILE_COMPRESSION_TYPE_CONFIG,
                FixedSetRecommender.ofSupportedValues(CompressionType.names()));

        configDef.define(FILE_MAX_RECORDS, ConfigDef.Type.INT, 0, ConfigDef.Range.between(0, Integer.MAX_VALUE),
                ConfigDef.Importance.MEDIUM,
                "The maximum number of records to put in a single file. " + "Must be a non-negative integer number. "
                        + "0 is interpreted as \"unlimited\", which is the default.",
                GROUP_NAME, ++fileGroupCounter, ConfigDef.Width.SHORT, FILE_MAX_RECORDS);

        configDef.define(FILE_NAME_TIMESTAMP_TIMEZONE, ConfigDef.Type.STRING, ZoneOffset.UTC.toString(),
                new TimeZoneValidator(), ConfigDef.Importance.LOW,
                "Specifies the timezone in which the dates and time for the timestamp variable will be treated. "
                        + "Use standard shot and long names. Default is UTC",
                GROUP_NAME, ++fileGroupCounter, ConfigDef.Width.SHORT, FILE_NAME_TIMESTAMP_TIMEZONE);

        configDef.define(FILE_NAME_TIMESTAMP_SOURCE, ConfigDef.Type.STRING, TimestampSource.Type.WALLCLOCK.name(),
                TIMESTAMP_SOURCE_VALIDATOR, ConfigDef.Importance.LOW, "Specifies the the timestamp variable source.",
                GROUP_NAME, ++fileGroupCounter, ConfigDef.Width.SHORT, FILE_NAME_TIMESTAMP_SOURCE);

        return fileGroupCounter;
    }

    /**
     * Returns the text of the filename template. May throw {@link ConfigException} if the property
     * {@link #FILE_NAME_TEMPLATE_CONFIG} is not set.
     *
     * @return the text of the filename template.
     */
    public String getFilename() {
        if (has(FILE_NAME_TEMPLATE_CONFIG)) {
            return getString(FILE_NAME_TEMPLATE_CONFIG);
        }
        final CompressionType compressionType = getCompressionType();
        return FormatType.AVRO.equals(new OutputFormatFragment(dataAccess).getFormatType())
                ? DEFAULT_FILENAME_TEMPLATE + ".avro" + compressionType.extension()
                : DEFAULT_FILENAME_TEMPLATE + compressionType.extension();
    }

    /**
     * Retrieves the defined compression type.
     *
     * @return the defined compression type or {@link CompressionType#NONE} if there is no defined compression type.
     */
    public CompressionType getCompressionType() {
        return has(FILE_COMPRESSION_TYPE_CONFIG)
                ? CompressionType.forName(getString(FILE_COMPRESSION_TYPE_CONFIG))
                : CompressionType.NONE;
    }

    /**
     * Gets the filename template.
     *
     * @return the Filename template.
     */
    public Template getFilenameTemplate() {
        return Template.of(getFilename());
    }

    /**
     * Gets the filename timezone
     *
     * @return The timezone specified for filenames.
     */
    public ZoneId getFilenameTimezone() {
        return ZoneId.of(getString(FILE_NAME_TIMESTAMP_TIMEZONE));
    }

    /**
     * Gets the timestamp source for the file name.
     *
     * @return the timestamp source for the file name.
     */
    public TimestampSource getFilenameTimestampSource() {
        return TimestampSource.of(getFilenameTimezone(),
                TimestampSource.Type.of(getString(FILE_NAME_TIMESTAMP_SOURCE)));
    }

    /**
     * Gets the maximum number of records allowed in a file.
     *
     * @return the maximum number of records allowed in a file.
     */
    public int getMaxRecordsPerFile() {
        return getInt(FILE_MAX_RECORDS);
    }

    public String getSourceName() {
        return getString(FILE_NAME_TEMPLATE_CONFIG);
    }

    public String getPrefixTemplate() {
        return getString(FILE_PATH_PREFIX_TEMPLATE_CONFIG);
    }

    public String getPrefix() {
        return getString(FILE_NAME_PREFIX_CONFIG);
    }
    public static void replaceYyyyUppercase(final String name, final Map<String, String> properties) {
        String template = properties.get(name);
        if (template != null) {
            final String originalTemplate = template;

            final var unitYyyyPattern = Pattern.compile("\\{\\{\\s*timestamp\\s*:\\s*unit\\s*=\\s*YYYY\\s*}}");
            template = unitYyyyPattern.matcher(template)
                    .replaceAll(matchResult -> matchResult.group().replace("YYYY", "yyyy"));

            if (!template.equals(originalTemplate)) {
                LoggerFactory.getLogger(FileNameFragment.class)
                        .warn("{{timestamp:unit=YYYY}} is no longer supported, "
                                + "please use {{timestamp:unit=yyyy}} instead. " + "It was automatically replaced: {}",
                                template);
            }
            properties.put(name, template);
        }
    }

    /**
     * Handle the deprecated YYYY template pattern
     *
     * @param properties
     *            the properties to modify
     * @return the properties with the YYYY templage fixed.
     */
    public static Map<String, String> handleDeprecatedYyyyUppercase(final Map<String, String> properties) {
        if (properties.containsKey(FILE_NAME_TEMPLATE_CONFIG)
                || properties.containsKey(FILE_PATH_PREFIX_TEMPLATE_CONFIG)) {
            final Map<String, String> result = new HashMap<>(properties);
            for (final String key : Arrays.asList(FILE_NAME_TEMPLATE_CONFIG, FILE_PATH_PREFIX_TEMPLATE_CONFIG)) {
                replaceYyyyUppercase(key, result);
            }
            return result;
        }
        return properties;
    }

    /**
     * Setter for the FileNameFragment.
     */
    public static final class Setter extends AbstractFragmentSetter<Setter> {
        /**
         * Constructs the Setter.
         *
         * @param data
         *            the data to update.
         */
        private Setter(final Map<String, String> data) {
            super(data);
        }

        /**
         * Sets the file compression type.
         *
         * @param compressionType
         *            the compression type.
         * @return this
         */
        public Setter fileCompression(final CompressionType compressionType) {
            return setValue(FILE_COMPRESSION_TYPE_CONFIG, compressionType.name());
        }

        /**
         * Sets the maximum records per file.
         *
         * @param maxRecordsPerFile
         *            the maximum records per file.
         * @return this.
         */
        public Setter maxRecordsPerFile(final int maxRecordsPerFile) {
            return setValue(FILE_MAX_RECORDS, maxRecordsPerFile);
        }

        /**
         * Sets the time stamp source.
         *
         * @param timestampSource
         *            the time stamp source.
         * @return this.
         */
        public Setter timestampSource(final TimestampSource timestampSource) {
            return setValue(FILE_NAME_TIMESTAMP_SOURCE, timestampSource.type().name());
        }

        /**
         * Sets the timestamp source from a type.
         *
         * @param type
         *            the type to set the timestamp source to.
         * @return this.
         */
        public Setter timestampSource(final TimestampSource.Type type) {
            return setValue(FILE_NAME_TIMESTAMP_SOURCE, type.name());
        }

        /**
         * Sets the timestamp timezone.
         *
         * @param timeZone
         *            the timezone to se.t
         * @return this
         */
        public Setter timestampTimeZone(final ZoneId timeZone) {
            return setValue(FILE_NAME_TIMESTAMP_TIMEZONE, timeZone.toString());
        }

        /**
         * Sets the file name template.
         *
         * @param template
         *            the prefix template to use.
         * @return this.
         */
        public Setter template(final String template) {
            return setValue(FILE_NAME_TEMPLATE_CONFIG, template);
        }

        /**
         * Sets the file name prefix template.
         *
         * @param prefixTemplate
         *            the prefix template to use.
         * @return this
         */
        public Setter prefixTemplate(final String prefixTemplate) {
            return setValue(FILE_PATH_PREFIX_TEMPLATE_CONFIG, prefixTemplate);
        }

        /**
         * Sets the file name prefix template.
         *
         * @param prefix
         *            the prefix to use.
         * @return this
         */
        public Setter prefix(final String prefix) {
            return setValue(FILE_NAME_PREFIX_CONFIG, prefix);
        }
    }
}
