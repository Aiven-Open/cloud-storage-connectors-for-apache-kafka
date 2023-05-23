/*
 * Copyright 2023 Aiven Oy
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

package io.aiven.kafka.connect.common.output.avro;

import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;

final class AvroConfig extends AbstractConfig {

    private static final String GROUP_AVRO = "Avro";
    private static final String AVRO_CODEC_CONFIG = DataFileConstants.CODEC;
    private static final String DEFAULT_AVRO_CODEC = DataFileConstants.NULL_CODEC;

    AvroConfig(final ConfigDef configDef, final Map<?, ?> originals) {
        super(configDef, originals);
    }

    CodecFactory codecFactory() {
        final String codecName = originals()
            .getOrDefault(AVRO_CODEC_CONFIG, DEFAULT_AVRO_CODEC)
            .toString();
        return CodecFactory.fromString(codecName);
    }

    private static ConfigDef createAvroConfigDefinition() {
        final ConfigDef configDef = new ConfigDef();
        configDef.define(
            AVRO_CODEC_CONFIG,
            ConfigDef.Type.STRING,
            DEFAULT_AVRO_CODEC,
            new CodecValidator(),
            ConfigDef.Importance.MEDIUM,
            "Avro Container File codec.",
            GROUP_AVRO,
            0,
            ConfigDef.Width.NONE,
            AVRO_CODEC_CONFIG
        );
        return configDef;
    }

    public static AvroConfig createAvroConfiguration(final Map<?, ?> originals) {
        return new AvroConfig(createAvroConfigDefinition(), originals);
    }

    private static class CodecValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            Objects.requireNonNull(name, "Avro codec cannot be null.");
            @SuppressWarnings("unchecked") final String proposedCodecName = (String) value;
            if (((String) value).isEmpty()) {
                throw new ConfigException(name, proposedCodecName, "cannot be empty");
            }
            try {
                CodecFactory.fromString(proposedCodecName);
            } catch (final AvroRuntimeException exception) {
                throw new ConfigException(
                    name, value, "Unknown or not supported codec " + proposedCodecName);
            }
        }
    }
}
