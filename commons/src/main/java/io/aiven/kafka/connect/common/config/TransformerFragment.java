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

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.source.input.InputFormat;

/**
 * Fragment to manage transformer configuration.
 */
public final class TransformerFragment extends ConfigFragment {
    private static final String TRANSFORMER_GROUP = "Transformer group";
    public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
    private static final String VALUE_CONVERTER_SCHEMA_REGISTRY_URL = "value.converter.schema.registry.url";
    public static final String INPUT_FORMAT_KEY = "input.format";
    public static final String SCHEMAS_ENABLE = "schemas.enable";
    public static final String TRANSFORMER_MAX_BUFFER_SIZE = "transformer.max.buffer.size";
    private static final int DEFAULT_MAX_BUFFER_SIZE = 4096;

    /**
     * Creates a Setter for this fragment.
     *
     * @param data
     *            the data map to modify.
     * @return the Setter
     */
    public static Setter setter(final Map<String, String> data) {
        return new Setter(data);
    }

    /**
     * Construct the ConfigFragment.
     *
     * @param cfg
     *            the configuration that this fragment is associated with.
     */
    public TransformerFragment(final AbstractConfig cfg) {
        super(cfg);
    }

    /**
     * Update the configuration definition with the properties for this fragment.
     *
     * @param configDef
     *            the configuration definition to update.
     * @return the updated configuration definition.
     */
    public static ConfigDef update(final ConfigDef configDef) {
        int transformerCounter = 0;
        configDef.define(SCHEMA_REGISTRY_URL, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "SCHEMA REGISTRY URL", TRANSFORMER_GROUP, ++transformerCounter,
                ConfigDef.Width.NONE, SCHEMA_REGISTRY_URL);
        configDef.define(VALUE_CONVERTER_SCHEMA_REGISTRY_URL, ConfigDef.Type.STRING, null,
                new ConfigDef.NonEmptyString(), ConfigDef.Importance.MEDIUM, "SCHEMA REGISTRY URL", TRANSFORMER_GROUP,
                ++transformerCounter, ConfigDef.Width.NONE, VALUE_CONVERTER_SCHEMA_REGISTRY_URL);
        configDef.define(INPUT_FORMAT_KEY, ConfigDef.Type.STRING, InputFormat.BYTES.getValue(),
                new InputFormatValidator(), ConfigDef.Importance.MEDIUM, "Input format of messages read from source",
                TRANSFORMER_GROUP, transformerCounter++, ConfigDef.Width.NONE, INPUT_FORMAT_KEY);
        configDef.define(TRANSFORMER_MAX_BUFFER_SIZE, ConfigDef.Type.INT, DEFAULT_MAX_BUFFER_SIZE,
                ConfigDef.Range.between(1, Integer.MAX_VALUE), ConfigDef.Importance.MEDIUM,
                "Max Size of the byte buffer when using the BYTE Transformer", TRANSFORMER_GROUP, ++transformerCounter,
                ConfigDef.Width.NONE, TRANSFORMER_MAX_BUFFER_SIZE);

        return configDef;
    }

    /**
     * Gets the input format for the transformer.
     *
     * @return the Input format for the
     */
    public InputFormat getInputFormat() {
        return InputFormat.valueOf(cfg.getString(INPUT_FORMAT_KEY).toUpperCase(Locale.ROOT));
    }

    /**
     * Get the schema registry URL.
     *
     * @return the schema registry URL
     */
    public String getSchemaRegistryUrl() {
        return cfg.getString(SCHEMA_REGISTRY_URL);
    }

    /**
     * Gets the maximum buffer size for the BYTE input.
     *
     * @return the maximum buffer size fo the BYTE input.
     */
    public int getTransformerMaxBufferSize() {
        return cfg.getInt(TRANSFORMER_MAX_BUFFER_SIZE);
    }

    public static class InputFormatValidator extends ConfigDef.NonEmptyString {

        @Override
        public void ensureValid(final String name, final Object value) {
            super.ensureValid(name, value);
            final String inputFormat = value.toString().toUpperCase(Locale.ROOT);
            try {
                InputFormat.valueOf(inputFormat);
            } catch (final IllegalArgumentException e) {
                throw new ConfigException(name, value, "String must be one of " + this);
            }
        }

        @Override
        public String toString() {
            return Arrays.stream(InputFormat.values())
                    .map(f -> "'" + f.getValue() + "'")
                    .collect(Collectors.joining(", "));
        }
    }
    /**
     * The setter for the TransformerFragment
     */
    public final static class Setter extends AbstractFragmentSetter<Setter> {
        /**
         * Constructor.
         *
         * @param data
         *            data to modify.
         */
        private Setter(final Map<String, String> data) {
            super(data);
        }

        /**
         * Sets the schema registry URL.
         *
         * @param schemaRegistryUrl
         *            the schema registry URL.
         * @return this
         */
        public Setter schemaRegistry(final String schemaRegistryUrl) {
            return setValue(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        }

        /**
         * Sets the schema registry for the value converter schema registry URL.
         *
         * @param valueConverterSchemaRegistryUrl
         *            the schema registry URL.
         * @return this
         */
        public Setter valueConverterSchemaRegistry(final String valueConverterSchemaRegistryUrl) {
            return setValue(VALUE_CONVERTER_SCHEMA_REGISTRY_URL, valueConverterSchemaRegistryUrl);
        }

        /**
         * Sets the input format.
         *
         * @param inputFormat
         *            the input format for the transformer.
         * @return this
         */
        public Setter inputFormat(final InputFormat inputFormat) {
            return setValue(INPUT_FORMAT_KEY, inputFormat.name());
        }

        /**
         * Sets the max buffer size for a BYTE transformer.
         *
         * @param maxBufferSize
         *            the maximum buffer size.
         * @return this
         */
        public Setter maxBufferSize(final int maxBufferSize) {
            return setValue(TRANSFORMER_MAX_BUFFER_SIZE, maxBufferSize);
        }
    }
}
