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

import static java.lang.String.format;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.types.Password;

import org.slf4j.Logger;

/**
 * Config fragments encapsulate logical fragments of configuration that may be used across multiple Connectors or across
 * the source/sink of a connector pair.
 * <p>
 * All implementing classes should be final. All configuration keys should be specified as {@code static final String}
 * within the fragment. They should be package private so that test code can use them. All implementing classes must
 * implement a {@code public static ConfigDef update(final ConfigDef configDef)} method, though more arguments are
 * allowed. The method must add the definitions for the configuration options the fragment supports to the configDef
 * parameter and return it. Access to the configuration within the fragment is via the {@link #dataAccess} property. All
 * access to configuration values should be through methods. For example if the string value of the "foo" config options
 * should be returned then the method { @code public String getFoo() } should be created. All complex access to
 * configuration values should be encapsulated within methods. For example if the Baz object requires the "foo" and
 * "bar" values then something like {@code public Baz getBaz() { return new Baz(getFoo(), cfg.getString("bar")); }}
 * should be created. Any fragment depends on another fragment may create it inline and use it. The
 * {@link #validate(Map)} method may call the {@code validate} methods on the dependant fragments during validation.
 * </p>
 */
public class ConfigFragment implements FragmentDataAccess {

    protected final FragmentDataAccess dataAccess;

    /**
     * Create a validation message.
     *
     * @param name
     *            the Name of the configuration property.
     * @param value
     *            the value associated with that property.
     * @param message
     *            additional info May be {@code null}.
     * @return A formatted validatio nmessage.
     */
    protected static String validationMessage(final String name, final Object value, final String message) {
        return format("Invalid value %s for configuration %s%s.", value, name, message == null ? "" : ": " + message);
    }

    /**
     * Registers an issue in the Config map.
     *
     * @param configMap
     *            The map of name ot ConfigValue.
     * @param name
     *            the name of the item with the error.
     * @param value
     *            the value of the item.
     * @param message
     *            the message for the error.
     */
    public static void registerIssue(final Map<String, ConfigValue> configMap, final String name, final Object value,
            final String message) {
        configMap.get(name).addErrorMessage(validationMessage(name, value, message));
    }
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
        logDeprecated(logger, old, "Use property %s instead", replacement);
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
    public static void logDeprecated(final Logger logger, final String old, final String formatStr,
            final Object... args) {
        logger.warn("{} property is deprecated. {}", old, format(formatStr, args));
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
     * @param dataAccess
     *            the FragmentDataAccess that this fragment is associated with.
     */

    protected ConfigFragment(final FragmentDataAccess dataAccess) {
        this.dataAccess = dataAccess;
    }

    /**
     * Validate that the data in the configuration matches any restrictions other than those from the validator. Default
     * implementation does nothing.
     *
     * @param configMap
     *            The map of ConfigValues that have issues
     */
    public void validate(final Map<String, ConfigValue> configMap) {
    }

    /**
     * Validate that the data in the configuration matches any restrictions. Default implementation does nothing.
     *
     */
    @Deprecated
    public void validate() {
    }

    @Override
    public final Map<String, ?> values() {
        return dataAccess.values();
    }

    @Override
    public final String getString(final String key) {
        return dataAccess.getString(key);
    }

    @Override
    public final boolean has(final String key) {
        return dataAccess.has(key);
    }

    @Override
    public final Integer getInt(final String key) {
        return dataAccess.getInt(key);
    }

    @Override
    public final Long getLong(final String key) {
        return dataAccess.getLong(key);
    }

    @Override
    public Boolean getBoolean(final String key) {
        return dataAccess.getBoolean(key);
    }

    @Override
    public List<String> getList(final String key) {
        return dataAccess.getList(key);
    }

    @Override
    public Password getPassword(final String key) {
        return dataAccess.getPassword(key);
    }

    @Override
    public <T> T getConfiguredInstance(final String key, final Class<? extends T> clazz) {
        return dataAccess.getConfiguredInstance(key, clazz);
    }
}
