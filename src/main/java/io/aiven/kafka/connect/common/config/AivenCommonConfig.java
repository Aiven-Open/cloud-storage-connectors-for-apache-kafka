/*
 * Copyright (C) 2020 Aiven Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.aiven.kafka.connect.common.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.connect.common.config.validators.FileCompressionTypeValidator;
import io.aiven.kafka.connect.common.config.validators.OutputFieldsEncodingValidator;
import io.aiven.kafka.connect.common.config.validators.OutputFieldsValidator;

public class AivenCommonConfig extends AbstractConfig {

    private static final String GROUP_COMPRESSION = "File Compression";
    public static final String FILE_COMPRESSION_TYPE_CONFIG = "file.compression.type";

    private static final String GROUP_FORMAT = "Format";
    public static final String FORMAT_OUTPUT_FIELDS_CONFIG = "format.output.fields";
    public static final String FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG = "format.output.fields.value.encoding";

    protected AivenCommonConfig(final ConfigDef definition, final Map<?, ?> originals) {
        super(definition, originals);
    }

    protected static void addOutputFieldsFormatConfigGroup(final ConfigDef configDef,
                                                           final OutputFieldType defaultFieldType) {
        int formatGroupCounter = 0;

        configDef.define(
            FORMAT_OUTPUT_FIELDS_CONFIG,
            ConfigDef.Type.LIST,
            !Objects.isNull(defaultFieldType) ? defaultFieldType.name : null,
            new OutputFieldsValidator(),
            ConfigDef.Importance.MEDIUM,
            "Fields to put into output files. "
                + "The supported values are: " + OutputField.SUPPORTED_OUTPUT_FIELDS + ".",
            GROUP_FORMAT,
            formatGroupCounter++,
            ConfigDef.Width.NONE,
            FORMAT_OUTPUT_FIELDS_CONFIG,
            FixedSetRecommender.ofSupportedValues(OutputFieldType.names())
        );

        configDef.define(
            FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG,
            ConfigDef.Type.STRING,
            OutputFieldEncodingType.BASE64.name,
            new OutputFieldsEncodingValidator(),
            ConfigDef.Importance.MEDIUM,
            "The type of encoding for the value field. "
                + "The supported values are: " + OutputField.SUPPORTED_OUTPUT_FIELDS + ".",
            GROUP_FORMAT,
            formatGroupCounter++,
            ConfigDef.Width.NONE,
            FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG,
            FixedSetRecommender.ofSupportedValues(OutputFieldEncodingType.names())
        );
    }

    protected static void addCompressionTypeConfig(final ConfigDef configDef,
                                                   final CompressionType defaultCompressionType) {
        configDef.define(
            FILE_COMPRESSION_TYPE_CONFIG,
            ConfigDef.Type.STRING,
            !Objects.isNull(defaultCompressionType) ? defaultCompressionType.name : null,
            new FileCompressionTypeValidator(),
            ConfigDef.Importance.MEDIUM,
            "The compression type used for files put on GCS. "
                + "The supported values are: " + CompressionType.SUPPORTED_COMPRESSION_TYPES + ".",
            GROUP_COMPRESSION,
            1,
            ConfigDef.Width.NONE,
            FILE_COMPRESSION_TYPE_CONFIG,
            FixedSetRecommender.ofSupportedValues(CompressionType.names())
        );

    }

    public CompressionType getCompressionType() {
        return CompressionType.forName(getString(FILE_COMPRESSION_TYPE_CONFIG));
    }

    public OutputFieldEncodingType getOutputFieldEncodingType() {
        return OutputFieldEncodingType.forName(getString(FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG));
    }

    public List<OutputField> getOutputFields() {
        final List<OutputField> result = new ArrayList<>();
        for (final String outputFieldTypeStr : getList(FORMAT_OUTPUT_FIELDS_CONFIG)) {
            final OutputFieldType fieldType = OutputFieldType.forName(outputFieldTypeStr);
            final OutputFieldEncodingType encodingType;
            if (fieldType == OutputFieldType.VALUE || fieldType == OutputFieldType.KEY) {
                encodingType = getOutputFieldEncodingType();
            } else {
                encodingType = OutputFieldEncodingType.NONE;
            }
            result.add(new OutputField(fieldType, encodingType));
        }
        return result;
    }

}
