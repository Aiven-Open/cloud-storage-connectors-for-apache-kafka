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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

import com.google.common.annotations.VisibleForTesting;

/**
 * Handles the format.output configuration options.
 */
public final class OutputFormatFragment extends ConfigFragment {
    @VisibleForTesting
    public static final String GROUP_NAME = "Format";
    @VisibleForTesting
    static public final String FORMAT_OUTPUT_FIELDS_CONFIG = "format.output.fields";
    @VisibleForTesting
    static final String FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG = "format.output.fields.value.encoding";
    @VisibleForTesting
    static final String FORMAT_OUTPUT_TYPE_CONFIG = "format.output.type";
    @VisibleForTesting
    static final String FORMAT_OUTPUT_ENVELOPE_CONFIG = "format.output.envelope";

    @VisibleForTesting
    public static final ConfigDef.Validator OUTPUT_TYPE_VALIDATOR = ConfigDef.CompositeValidator.of(
            new ConfigDef.NonEmptyString(),
            ConfigDef.CaseInsensitiveValidString.in(FormatType.names().toArray(new String[0])));
    @VisibleForTesting
    public static final ConfigDef.Validator OUTPUT_FIELDS_VALIDATOR = ConfigDef.LambdaValidator.with((name, value) -> {
        if (Objects.nonNull(value)) {
            @SuppressWarnings("unchecked")
            final List<String> valueList = (List<String>) value;
            if (valueList.isEmpty()) {
                throw new ConfigException(name, valueList, "cannot be empty");
            }
            for (final String fieldName : valueList) {
                if (!OutputFieldType.isValidName(fieldName)) {
                    throw new ConfigException(name, value,
                            "supported values are (case insensitive): " + String.join(", ", OutputFieldType.names()));
                }
            }
        }
    }, () -> String.join(", ", OutputFieldType.names()));

    @VisibleForTesting
    public static final ConfigDef.Validator OUTPUT_FIELDS_ENCODING_VALIDATOR = ConfigDef.CaseInsensitiveValidString
            .in(OutputFieldEncodingType.names().toArray(new String[0]));

    /**
     * Constructor.
     *
     * @param dataAccess
     *            the data to use.
     */
    public OutputFormatFragment(final FragmentDataAccess dataAccess) {
        super(dataAccess);
    }

    /**
     * Defines the parameters for the OutputFormatFragment.
     *
     * @param configDef
     *            the configuration definition to update.
     * @param defaultFieldType
     *            the default FieldType. May be {@code null}.
     * @return the number of OutputFormat groups items.
     */
    public static int update(final ConfigDef configDef, final OutputFieldType defaultFieldType) {
        int formatGroupCounter = 0;

        configDef.define(FORMAT_OUTPUT_TYPE_CONFIG, ConfigDef.Type.STRING, FormatType.CSV.name, OUTPUT_TYPE_VALIDATOR,
                ConfigDef.Importance.MEDIUM, "The format type of output content.", GROUP_NAME, ++formatGroupCounter,
                ConfigDef.Width.NONE, FORMAT_OUTPUT_TYPE_CONFIG,
                FixedSetRecommender.ofSupportedValues(FormatType.names()));

        configDef.define(FORMAT_OUTPUT_FIELDS_CONFIG, ConfigDef.Type.LIST,
                Objects.isNull(defaultFieldType) ? null : defaultFieldType.name, // NOPMD NullAssignment
                OUTPUT_FIELDS_VALIDATOR, ConfigDef.Importance.MEDIUM, "Fields to put into output files.", GROUP_NAME,
                ++formatGroupCounter, ConfigDef.Width.NONE, FORMAT_OUTPUT_FIELDS_CONFIG,
                FixedSetRecommender.ofSupportedValues(OutputFieldType.names()));

        configDef.define(FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG, ConfigDef.Type.STRING,
                OutputFieldEncodingType.BASE64.name, OUTPUT_FIELDS_ENCODING_VALIDATOR, ConfigDef.Importance.MEDIUM,
                "The type of encoding for the value field. " + "The supported values are: "
                        + OutputFieldEncodingType.SUPPORTED_FIELD_ENCODING_TYPES + ".",
                GROUP_NAME, ++formatGroupCounter, ConfigDef.Width.NONE, FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG,
                FixedSetRecommender.ofSupportedValues(OutputFieldEncodingType.names()));

        configDef.define(FORMAT_OUTPUT_ENVELOPE_CONFIG, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                "Whether to enable envelope for entries with single field.", GROUP_NAME, ++formatGroupCounter,
                ConfigDef.Width.SHORT, FORMAT_OUTPUT_ENVELOPE_CONFIG);
        return formatGroupCounter;
    }

    /**
     * Creates a setter for this fragment
     *
     * @param data
     *            the data to update
     * @return the Setter.
     */
    public static Setter setter(final Map<String, String> data) {
        return new Setter(data);
    }

    @Override
    public void validate() {
        // Special checks for output json envelope config.
        final int outputFieldCount = hasOutputFields() ? getOutputFields().size() : 0;
        if (!envelopeEnabled() && outputFieldCount != 1) {
            throw new ConfigException(FORMAT_OUTPUT_ENVELOPE_CONFIG,
                    String.format("When %s is %s, %s must contain only one field", FORMAT_OUTPUT_ENVELOPE_CONFIG, false,
                            FORMAT_OUTPUT_FIELDS_CONFIG));
        }
    }

    @Override
    public void validate(final Map<String, ConfigValue> configMap) {
        // Special checks for output json envelope config.
        final int outputFieldCount = hasOutputFields() ? getOutputFields().size() : 0;
        if (!envelopeEnabled() && outputFieldCount != 1) {
            configMap
                    .compute(FORMAT_OUTPUT_ENVELOPE_CONFIG,
                            (key, value) -> value == null ? new ConfigValue(key) : value)
                    .addErrorMessage(validationMessage(FORMAT_OUTPUT_ENVELOPE_CONFIG, false,
                            String.format("When %s is %s, %s must contain only one field",
                                    FORMAT_OUTPUT_ENVELOPE_CONFIG, false, FORMAT_OUTPUT_FIELDS_CONFIG)));
        }
    }

    /**
     * Gets the defined format type.
     *
     * @return the FormatType.
     */
    public FormatType getFormatType() {
        return FormatType.forName(getString(FORMAT_OUTPUT_TYPE_CONFIG));
    }

    /**
     * Gets the envelope enabled state.
     *
     * @return the envelope enabled state.
     */
    public Boolean envelopeEnabled() {
        return getBoolean(FORMAT_OUTPUT_ENVELOPE_CONFIG);
    }

    /**
     * Gets the output field encoding type.
     *
     * @return the Output field encodging type.
     */
    public OutputFieldEncodingType getOutputFieldEncodingType() {
        return OutputFieldEncodingType.forName(getString(FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG));
    }

    /**
     * Returns a list of OutputField objects as specified by {@code FORMAT_OUTPUT_FIELDS_CONFIG}.
     *
     * @return a list of OutputField objects as specified by {@code FORMAT_OUTPUT_FIELDS_CONFIG}.
     */
    public List<OutputField> getOutputFields() {
        return getOutputFields(FORMAT_OUTPUT_FIELDS_CONFIG);
    }

    public List<OutputFieldType> getOutputFieldTypes() {
        return getList(FORMAT_OUTPUT_FIELDS_CONFIG).stream().map(OutputFieldType::forName).collect(Collectors.toList());
    }

    /**
     * Returns {@code true} if {@link #FORMAT_OUTPUT_FIELDS_CONFIG} is set.
     *
     * @return {@code true} if {@link #FORMAT_OUTPUT_FIELDS_CONFIG} is set.
     */
    public boolean hasOutputFields() {
        return has(FORMAT_OUTPUT_FIELDS_CONFIG);
    }

    /**
     * Returns a list of OutputField objects as specified by the {@code configEntry} param. May throw a ConfigException
     * if the configEntry is not present in the configuraiton.
     *
     * @param configEntry
     *            the configuration property that specifies the output field formats.
     * @return a list of OutputField objects as specified by {@code configEntry}.
     */
    public List<OutputField> getOutputFields(final String configEntry) {
        final List<String> fields = getList(configEntry);
        return fields.stream().map(fieldName -> {
            final var type = OutputFieldType.forName(fieldName);
            final var encoding = type == OutputFieldType.KEY || type == OutputFieldType.VALUE
                    ? getOutputFieldEncodingType()
                    : OutputFieldEncodingType.NONE;
            return new OutputField(type, encoding);
        }).collect(Collectors.toUnmodifiableList());
    }

    /**
     * The setter for OutputFormatFragment.
     */
    public static class Setter extends AbstractFragmentSetter<Setter> {

        /**
         * Constructor.
         *
         * @param data
         *            the map of data items being set.
         */
        private Setter(final Map<String, String> data) {
            super(data);
        }

        /**
         * Sets the format type.
         *
         * @param formatType
         *            the format type.
         * @return this
         */
        public Setter withFormatType(final FormatType formatType) {
            return setValue(FORMAT_OUTPUT_TYPE_CONFIG, formatType.name());
        }

        /**
         * Sets the output field encoding type.
         *
         * @param encodingType
         *            the output field encoding type.
         * @return this
         */
        public Setter withOutputFieldEncodingType(final OutputFieldEncodingType encodingType) {
            return setValue(FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG, encodingType.name());
        }

        /**
         * Sets the list of output fields. The order of output fields will match the order they are added.
         *
         * @param outputFields
         *            the list of output fields
         * @return this
         */
        public Setter withOutputFields(final List<OutputFieldType> outputFields) {
            return setValue(FORMAT_OUTPUT_FIELDS_CONFIG,
                    outputFields.stream().map(OutputFieldType::toString).collect(Collectors.joining(",")));
        }

        /**
         * Sets the list of output fields. The order of output fields will match the order they are added.
         *
         * @param outputFields
         *            the list of output fields
         * @return this
         */
        public Setter withOutputFields(final OutputFieldType... outputFields) {
            return withOutputFields(List.of(outputFields));
        }

        /**
         * Sets the envelope enabled flag.
         *
         * @param envelopeEnabled
         *            the desired flag state.
         * @return this
         */
        public Setter envelopeEnabled(final boolean envelopeEnabled) {
            return setValue(FORMAT_OUTPUT_ENVELOPE_CONFIG, envelopeEnabled);
        }
    }
}
