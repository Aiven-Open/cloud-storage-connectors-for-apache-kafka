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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.config.validators.OutputFieldsEncodingValidator;
import io.aiven.kafka.connect.common.config.validators.OutputFieldsValidator;
import io.aiven.kafka.connect.common.config.validators.OutputTypeValidator;

public class OutputFormatFragment extends ConfigFragment {
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

        final String supportedFormatTypes = FormatType.names()
                .stream()
                .map(f -> "'" + f + "'")
                .collect(Collectors.joining(", "));

        configDef.define(FORMAT_OUTPUT_TYPE_CONFIG, ConfigDef.Type.STRING, FormatType.CSV.name,
                new OutputTypeValidator(), ConfigDef.Importance.MEDIUM,
                "The format type of output content" + "The supported values are: " + supportedFormatTypes + ".",
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

    @Override
    public void validate() {
        // Special checks for output json envelope config.
        final List<OutputField> outputFields = getOutputFields();
        final Boolean outputEnvelopConfig = envelopeEnabled();
        if (!outputEnvelopConfig && outputFields.toArray().length != 1) {
            final String msg = String.format("When %s is %s, %s must contain only one field",
                    FORMAT_OUTPUT_ENVELOPE_CONFIG, false, FORMAT_OUTPUT_FIELDS_CONFIG);
            throw new ConfigException(msg);
        }
    }
    public FormatType getFormatType() {
        return FormatType.forName(cfg.getString(FORMAT_OUTPUT_TYPE_CONFIG));
    }

    public Boolean envelopeEnabled() {
        return cfg.getBoolean(FORMAT_OUTPUT_ENVELOPE_CONFIG);
    }

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
}
