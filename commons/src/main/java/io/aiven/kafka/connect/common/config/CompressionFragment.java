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

import java.util.Objects;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.connect.common.config.validators.FileCompressionTypeValidator;

/**
 * The configuration fragment that defines the compression characteristics.
 */
public final class CompressionFragment extends ConfigFragment {

    static final String GROUP_COMPRESSION = "File Compression";
    static final String FILE_COMPRESSION_TYPE_CONFIG = "file.compression.type";

    /**
     * Constructor.
     *
     * @param cfg
     *            the configuration to resolve requests against.
     */
    public CompressionFragment(final AbstractConfig cfg) {
        super(cfg);
    }

    /**
     * Adds the configuration options for compression to the configuration definition.
     *
     * @param configDef
     *            the Configuration definition.
     * @param defaultCompressionType
     *            the default compression type. If {@code null}, {@link CompressionType#NONE} will be configured.
     * @return the update configuration definition
     */
    public static ConfigDef update(final ConfigDef configDef, final CompressionType defaultCompressionType) {
        configDef.define(FILE_COMPRESSION_TYPE_CONFIG, ConfigDef.Type.STRING,
                Objects.isNull(defaultCompressionType) ? CompressionType.NONE.name : defaultCompressionType.name, // NOPMD
                                                                                                                  // NullAssignment
                new FileCompressionTypeValidator(), ConfigDef.Importance.MEDIUM,
                "The compression type used for files put on GCS.", GROUP_COMPRESSION, 1, ConfigDef.Width.NONE,
                FILE_COMPRESSION_TYPE_CONFIG, FixedSetRecommender.ofSupportedValues(CompressionType.names()));
        return configDef;
    }

    /**
     * Retrieves the defined compression type.
     *
     * @return the defined compression type or {@link CompressionType#NONE} if there is no defined compression type.
     */
    public CompressionType getCompressionType() {
        return has(FILE_COMPRESSION_TYPE_CONFIG)
                ? CompressionType.forName(cfg.getString(FILE_COMPRESSION_TYPE_CONFIG))
                : CompressionType.NONE;
    }
}
