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

import io.aiven.kafka.connect.common.source.input.InputFormat;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Locale;
import java.util.Map;

public final class TransformerFragment extends ConfigFragment {
    private static final String TRANSFORMER_GROUP = "Transformer group";
    public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
    public static final String VALUE_CONVERTER_SCHEMA_REGISTRY_URL = "value.converter.schema.registry.url";
    public static final String AVRO_VALUE_SERIALIZER = "value.serializer";
    public static final String INPUT_FORMAT_KEY = "input.format";
    public static final String SCHEMAS_ENABLE = "schemas.enable";
    public static final String TRANSFORMER_MAX_BUFFER_SIZE = "transformer.max.buffer.size";
    private static final int DEFAULT_MAX_BUFFER_SIZE = 4096;


    public static Setter setter(Map<String, String> data) {
        return new Setter(data);
    }

    /**
     * Construct the ConfigFragment..
     *
     * @param cfg
     *            the configuration that this fragment is associated with.
     */
    public TransformerFragment(final AbstractConfig cfg) {
        super(cfg);
    }

    public static ConfigDef update(final ConfigDef configDef) {
        int transformerCounter = 0;
        configDef.define(SCHEMA_REGISTRY_URL, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "SCHEMA REGISTRY URL", TRANSFORMER_GROUP, transformerCounter++,
                ConfigDef.Width.NONE, SCHEMA_REGISTRY_URL);
        configDef.define(VALUE_CONVERTER_SCHEMA_REGISTRY_URL, ConfigDef.Type.STRING, null,
                new ConfigDef.NonEmptyString(), ConfigDef.Importance.MEDIUM, "SCHEMA REGISTRY URL", TRANSFORMER_GROUP,
                transformerCounter++, ConfigDef.Width.NONE, VALUE_CONVERTER_SCHEMA_REGISTRY_URL);
        configDef.define(INPUT_FORMAT_KEY, ConfigDef.Type.STRING, InputFormat.BYTES.getValue(),
                new ConfigDef.NonEmptyString(), ConfigDef.Importance.MEDIUM,
                "Input format of messages read from source avro/json/parquet/bytes", TRANSFORMER_GROUP,
                transformerCounter++, ConfigDef.Width.NONE, INPUT_FORMAT_KEY);
        configDef.define(TRANSFORMER_MAX_BUFFER_SIZE, ConfigDef.Type.INT, DEFAULT_MAX_BUFFER_SIZE,
                new ByteArrayTransformerMaxBufferSizeValidator(), ConfigDef.Importance.MEDIUM,
                "Max Size of the byte buffer when using the BYTE Transformer", TRANSFORMER_GROUP, transformerCounter++,
                ConfigDef.Width.NONE, TRANSFORMER_MAX_BUFFER_SIZE);
        configDef.define(AVRO_VALUE_SERIALIZER, ConfigDef.Type.CLASS, null, ConfigDef.Importance.MEDIUM,
                "Avro value serializer", TRANSFORMER_GROUP, transformerCounter++, // NOPMD
                // UnusedAssignment
                ConfigDef.Width.NONE, AVRO_VALUE_SERIALIZER);
        return configDef;
    }

    public InputFormat getInputFormat() {
        return InputFormat.valueOf(cfg.getString(INPUT_FORMAT_KEY).toUpperCase(Locale.ROOT));
    }

    public String getSchemaRegistryUrl() {
        return cfg.getString(SCHEMA_REGISTRY_URL);
    }

    public Class<?> getAvroValueSerializer() {
        return cfg.getClass(AVRO_VALUE_SERIALIZER);
    }

    public int getTransformerMaxBufferSize() {
        return cfg.getInt(TRANSFORMER_MAX_BUFFER_SIZE);
    }

    private static class ByteArrayTransformerMaxBufferSizeValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {

            // ConfigDef will throw an error if this is not an int that is supplied
            if ((int) value <= 0) {
                throw new ConfigException(
                        String.format("%s must be larger then 0 and less then %s", name, Integer.MAX_VALUE));
            }

        }
    }

    public final static class Setter extends AbstractFragmentSetter<Setter> {
        private Setter(Map<String, String> data) {
            super(data);
        }

        public Setter schemaRegistry(final String schemaRegistryUrl) {
            return setValue(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        }

        public Setter valueConverterSchemaRegistry(final String valueConverterSchemaRegistryUrl) {
            return setValue(VALUE_CONVERTER_SCHEMA_REGISTRY_URL, valueConverterSchemaRegistryUrl);
        }

        public Setter inputFormat(final InputFormat inputFormat) {
            return setValue(INPUT_FORMAT_KEY, inputFormat.name());
        }

        public Setter maxBufferSize(final int maxBufferSize) {
            return setValue(TRANSFORMER_MAX_BUFFER_SIZE, maxBufferSize);
        }

        public Setter avroValueSerializer(final Class<?> avroValueSerializer) {
            return setValue(AVRO_VALUE_SERIALIZER, avroValueSerializer);
        }
     }
}
