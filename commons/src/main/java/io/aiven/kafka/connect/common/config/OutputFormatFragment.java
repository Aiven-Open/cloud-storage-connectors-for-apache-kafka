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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.config.validators.OutputFieldsEncodingValidator;
import io.aiven.kafka.connect.common.config.validators.OutputFieldsValidator;
import io.aiven.kafka.connect.common.config.validators.OutputTypeValidator;

public final class OutputFormatFragment extends ConfigFragment {
    // package protected for testing
    static final String GROUP_FORMAT = "Format";
    static final String FORMAT_OUTPUT_FIELDS_CONFIG = "format.output.fields";
    static final String FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG = "format.output.fields.value.encoding";
    static final String FORMAT_OUTPUT_TYPE_CONFIG = "format.output.type";
    static final String FORMAT_OUTPUT_ENVELOPE_CONFIG = "format.output.envelope";

    public OutputFormatFragment(final AbstractConfig cfg) {
        super(cfg);
    }

    /**
     * Defines the parameters for the OutputFormatFragment.
     *
     * @param configDef
     *            the configuration definition to update.
     * @param defaultFieldType
     *            the default FieldType. May be {@code null}.
     * @return The update ConfigDef.
     */
    public static ConfigDef update(final ConfigDef configDef, final OutputFieldType defaultFieldType) {
        int formatGroupCounter = 0;

        configDef.define(FORMAT_OUTPUT_TYPE_CONFIG, ConfigDef.Type.STRING, FormatType.CSV.name,
                new OutputTypeValidator(), ConfigDef.Importance.MEDIUM, "The format type of output content.",
                GROUP_FORMAT, 0, ConfigDef.Width.NONE, FORMAT_OUTPUT_TYPE_CONFIG,
                FixedSetRecommender.ofSupportedValues(FormatType.names()));

        configDef.define(FORMAT_OUTPUT_FIELDS_CONFIG, ConfigDef.Type.LIST,
                Objects.isNull(defaultFieldType) ? null : defaultFieldType.name, // NOPMD NullAssignment
                new OutputFieldsValidator(), ConfigDef.Importance.MEDIUM,
                "Fields to put into output files. " + "The supported values are: " + OutputField.SUPPORTED_OUTPUT_FIELDS
                        + ".",
                GROUP_FORMAT, formatGroupCounter++, ConfigDef.Width.NONE, FORMAT_OUTPUT_FIELDS_CONFIG,
                FixedSetRecommender.ofSupportedValues(OutputFieldType.names()));

        configDef.define(FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG, ConfigDef.Type.STRING,
                OutputFieldEncodingType.BASE64.name, new OutputFieldsEncodingValidator(), ConfigDef.Importance.MEDIUM,
                "The type of encoding for the value field. " + "The supported values are: "
                        + OutputFieldEncodingType.SUPPORTED_FIELD_ENCODING_TYPES + ".",
                GROUP_FORMAT, formatGroupCounter++, ConfigDef.Width.NONE, FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG,
                FixedSetRecommender.ofSupportedValues(OutputFieldEncodingType.names()));

        configDef.define(FORMAT_OUTPUT_ENVELOPE_CONFIG, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                "Whether to enable envelope for entries with single field.", GROUP_FORMAT, formatGroupCounter,
                ConfigDef.Width.SHORT, FORMAT_OUTPUT_ENVELOPE_CONFIG);
        return configDef;
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
        final Boolean outputEnvelopConfig = envelopeEnabled();
        if (!outputEnvelopConfig && outputFieldCount != 1) {
            final String msg = String.format("When %s is %s, %s must contain only one field",
                    FORMAT_OUTPUT_ENVELOPE_CONFIG, false, FORMAT_OUTPUT_FIELDS_CONFIG);
            throw new ConfigException(msg);
        }
    }

    /**
     * Gets the defined format type.
     *
     * @return the FormatType.
     */
    public FormatType getFormatType() {
        return FormatType.forName(cfg.getString(FORMAT_OUTPUT_TYPE_CONFIG));
    }

    /**
     * Gets the envelope enabled state.
     *
     * @return the envelope enabled state.
     */
    public Boolean envelopeEnabled() {
        return cfg.getBoolean(FORMAT_OUTPUT_ENVELOPE_CONFIG);
    }

    /**
     * Gets the output field encoding type.
     *
     * @return the Output field encodging type.
     */
    public OutputFieldEncodingType getOutputFieldEncodingType() {
        return OutputFieldEncodingType.forName(cfg.getString(FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG));
    }

    /**
     * Returns a list of OutputField objects as specified by {@code FORMAT_OUTPUT_FIELDS_CONFIG}.
     *
     * @return a list of OutputField objects as specified by {@code FORMAT_OUTPUT_FIELDS_CONFIG}.
     */
    public List<OutputField> getOutputFields() {
        return getOutputFields(FORMAT_OUTPUT_FIELDS_CONFIG);
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
        final List<String> fields = cfg.getList(configEntry);
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
         * Sets the list of output fields.
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
         * Sets the list of output fields.
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
