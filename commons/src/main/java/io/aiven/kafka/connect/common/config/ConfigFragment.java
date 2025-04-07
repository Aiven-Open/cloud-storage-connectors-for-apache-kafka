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

import org.slf4j.Logger;

/**
 * Config fragments encapsulate logical fragments of configuration that may be used across multiple Connectors or across
 * the source/sink of a connector pair.
 *
 * All implementing classes should be final. All configuration keys should be specified as {@code static final String}
 * within the fragment. They should be package private so that test code can use them. All implementing classes must
 * implement a {@code public static ConfigDef update(final ConfigDef configDef)} method, though more arguments are
 * allowed. The method must add the defintions for the configuration options the fragment supports to the configDef
 * parameter and return it. Access to the configuraiton within the fragment is via the {@link #cfg} property. All access
 * to configuration values should be through methods. For example if the string value of the "foo" config options should
 * be returned then the method { @code public String getFoo() } should be created. All complex access to configuration
 * values should be encapsulated within methods. For example if the Baz object requires the "foo" and "bar" values then
 * something like {@code public Baz getBaz() { return new Baz(getFoo(), cfg.getString("bar")); }} should be created. Any
 * fragment depends on another fragment may create it inline and use it. The {@link #validate()} method may call the
 * {@code
 * validate} methods on the dependant fragments during validation.
 */
public class ConfigFragment {
    /** The configuration that this fragment is associated with */
    protected final AbstractConfig cfg;

    /**
     * Logs a deprecated message for a deprecated configuration key.
     *
     * @param logger
     *            the logger to log to.
     * @param old
     *            the deprecated configuration key.
     * @param replacement
     *            the replacement configuration key.
     */
    public static void logDeprecated(final Logger logger, final String old, final String replacement) {
        logger.warn("{} property is deprecated please use {}.", old, replacement);
    }

    /**
     * Logs a deprecated message for a deprecated configuration key. This method should only be used when the key does
     * not have a replacement and the documentation describes what to do.
     *
     * @param logger
     *            the logger to log to.
     * @param old
     *            the deprecated configuration key.
     */
    public static void logDeprecated(final Logger logger, final String old) {
        logger.warn("{} property is deprecated please read documentation for the new name.", old);
    }

    /**
     * Construct the ConfigFragment.
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
