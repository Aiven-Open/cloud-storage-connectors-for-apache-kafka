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

import org.apache.kafka.common.config.AbstractConfig;

/**
 * Base for all configuration fragments.
 */
public class ConfigFragment {
    /** The configuration that this fragment is associated with */
    protected final AbstractConfig cfg;

    /**
     * Construct the ConfigFragment..
     *
     * @param cfg
     *            the configuration that this fragment is associated with.
     */
    protected ConfigFragment(final AbstractConfig cfg) {
        this.cfg = cfg;
    }

    /**
     * Validate that the data in the configuration matches any restrictions. Default implementation does nothing.
     */
    public void validate() {
        // does nothing
    }

    /**
     * Determines if a key has been set.
     *
     * @param key
     *            The key to check.
     * @return {@code true} if the key was set, {@code false} if the key was not set or does not exist in the config.
     */
    public final boolean has(final String key) {
        return cfg.values().get(key) != null;
    }
}
