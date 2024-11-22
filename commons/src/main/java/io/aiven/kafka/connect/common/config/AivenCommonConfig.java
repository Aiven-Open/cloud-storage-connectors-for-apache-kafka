/*
 * Copyright 2020 Aiven Oy
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

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.connect.common.config.validators.OutputTypeValidator;
/**
 * @deprecated Use {@link SinkCommonConfig} instead
 */
@Deprecated
public class AivenCommonConfig extends SinkCommonConfig {
    public static final String FORMAT_OUTPUT_FIELDS_CONFIG = "format.output.fields";
    public static final String FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG = "format.output.fields.value.encoding";
    public static final String FORMAT_OUTPUT_TYPE_CONFIG = "format.output.type";
    public static final String FORMAT_OUTPUT_ENVELOPE_CONFIG = "format.output.envelope";
    public static final String FILE_COMPRESSION_TYPE_CONFIG = "file.compression.type";
    public static final String FILE_MAX_RECORDS = "file.max.records";
    public static final String FILE_NAME_TEMPLATE_CONFIG = "file.name.template";

    protected AivenCommonConfig(final ConfigDef definition, final Map<?, ?> originals) {
        super(definition, originals);
    }

    /**
     * This class and method is deprecated please use OutputFormatFragment instead to replace this functionality.
     * alternatively switch to using the SinkCommonConfig and it will be automatically loaded in the super() method.
     *
     * @param configDef
     *            The configuration definition
     * @param formatGroupCounter
     *            the counter to use to increment the config id
     */
    protected static void addFormatTypeConfig(final ConfigDef configDef, final int formatGroupCounter) {
        final String supportedFormatTypes = FormatType.names()
                .stream()
                .map(f -> "'" + f + "'")
                .collect(Collectors.joining(", "));
        configDef.define(FORMAT_OUTPUT_TYPE_CONFIG, ConfigDef.Type.STRING, FormatType.CSV.name,
                new OutputTypeValidator(), ConfigDef.Importance.MEDIUM,
                "The format type of output content" + "The supported values are: " + supportedFormatTypes + ".",
                GROUP_FORMAT, formatGroupCounter, ConfigDef.Width.NONE, FORMAT_OUTPUT_TYPE_CONFIG,
                FixedSetRecommender.ofSupportedValues(FormatType.names()));
    }

}
