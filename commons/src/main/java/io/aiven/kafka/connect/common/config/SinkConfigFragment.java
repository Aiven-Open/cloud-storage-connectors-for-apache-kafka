/*
 * Copyright 2026 Aiven Oy
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

import static io.aiven.kafka.connect.common.config.SinkCommonConfig.FILE_MAX_BYTES;
import static io.aiven.kafka.connect.common.config.SinkCommonConfig.MAX_LINGER_MS;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import io.aiven.commons.kafka.config.validator.TimeScaleValidator;

/**
 * Defines properties that are shared across all Sink implementations.
 */
public final class SinkConfigFragment extends ConfigFragment {

    /**
     * Gets a setter for this fragment.
     *
     * @param data
     *            the data map to modify.
     * @return the Setter.
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
    public SinkConfigFragment(final AbstractConfig cfg) {
        super(cfg);
    }

    public static ConfigDef update(final ConfigDef configDef) {
        configDef.define(FILE_MAX_BYTES, ConfigDef.Type.LONG, 0L, ConfigDef.Range.atLeast(0L),
                ConfigDef.Importance.MEDIUM,
                "The maximum number of bytes to put in a single file. " + "Must be a non-negative integer number. "
                        + "0 is interpreted as \"unlimited\", which is the default.");
        configDef.define(MAX_LINGER_MS, ConfigDef.Type.LONG, 10_000L, TimeScaleValidator.between(1000, 60_000),
                ConfigDef.Importance.MEDIUM,
                "The maximum amount of time in milliseconds for the sink connector to wait before starting to upload data if no other threshold is met."
                        + " The default is 10 seconds, while the minimum allowed is 1 second.");

        return configDef;
    }

    /**
     * A long value that represents the number of bytes per file
     *
     * @return Get the max bytes configured per file
     *
     */
    public long getMaxBytesPerFile() {
        return cfg.getLong(FILE_MAX_BYTES);
    }

    /**
     * A long value that represents the minimum amount of time in milliseconds to wait before a Sink Task will being
     * uploading data to its target system
     *
     * @return Get the minimum amount of time a sink task should wait before uploading data to its target system
     */
    public long getMaxLingerMs() {
        return cfg.getLong(MAX_LINGER_MS);
    }

    /**
     * The SourceConfigFragment setter.
     */
    public static class Setter extends AbstractFragmentSetter<Setter> {
        /**
         * Constructor.
         *
         * @param data
         *            the data to modify.
         */
        protected Setter(final Map<String, String> data) {
            super(data);
        }

        /**
         * Set the maximum bytes allowed in a file.
         *
         * @param maxBytesPerFile
         *            the maximum number of bytes to allocate to a file.
         * @return this
         */
        public Setter maxBytesPerFile(final int maxBytesPerFile) {
            return setValue(FILE_MAX_BYTES, maxBytesPerFile);
        }

    }
}
