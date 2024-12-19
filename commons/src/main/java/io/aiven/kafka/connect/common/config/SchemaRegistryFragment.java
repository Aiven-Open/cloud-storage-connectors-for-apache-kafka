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

import java.util.Locale;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.connect.common.source.input.InputFormat;

public final class SchemaRegistryFragment extends ConfigFragment {
    private static final String SCHEMAREGISTRY_GROUP = "Schema registry group";
    public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
    public static final String VALUE_CONVERTER_SCHEMA_REGISTRY_URL = "value.converter.schema.registry.url";
    public static final String AVRO_VALUE_SERIALIZER = "value.serializer";
    public static final String INPUT_FORMAT_KEY = "input.format";
    public static final String SCHEMAS_ENABLE = "schemas.enable";

    /**
     * Construct the ConfigFragment..
     *
     * @param cfg
     *            the configuration that this fragment is associated with.
     */
    public SchemaRegistryFragment(final AbstractConfig cfg) {
        super(cfg);
    }

    public static ConfigDef update(final ConfigDef configDef) {
        int srCounter = 0;
        configDef.define(SCHEMA_REGISTRY_URL, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "SCHEMA REGISTRY URL", SCHEMAREGISTRY_GROUP, srCounter++,
                ConfigDef.Width.NONE, SCHEMA_REGISTRY_URL);
        configDef.define(VALUE_CONVERTER_SCHEMA_REGISTRY_URL, ConfigDef.Type.STRING, null,
                new ConfigDef.NonEmptyString(), ConfigDef.Importance.MEDIUM, "SCHEMA REGISTRY URL",
                SCHEMAREGISTRY_GROUP, srCounter++, ConfigDef.Width.NONE, VALUE_CONVERTER_SCHEMA_REGISTRY_URL);
        configDef.define(INPUT_FORMAT_KEY, ConfigDef.Type.STRING, InputFormat.BYTES.getValue(),
                new ConfigDef.NonEmptyString(), ConfigDef.Importance.MEDIUM,
                "Input format of messages read from source avro/json/parquet/bytes", SCHEMAREGISTRY_GROUP, srCounter++, // NOPMD
                ConfigDef.Width.NONE, INPUT_FORMAT_KEY);

        configDef.define(AVRO_VALUE_SERIALIZER, ConfigDef.Type.CLASS, null, ConfigDef.Importance.MEDIUM,
                "Avro value serializer", SCHEMAREGISTRY_GROUP, srCounter++, // NOPMD
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

}
