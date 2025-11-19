/*
 * Copyright 2025 Aiven Oy
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.Utils;

/**
 * The data access for a {@link ConfigFragment}.
 */
public interface FragmentDataAccess {
    static FragmentDataAccess from(final AbstractConfig cfg) {
        return new FragmentDataAccess() {
            @Override
            public Map<String, ?> values() {
                return cfg.values();
            }

            @Override
            public String getString(final String key) {
                return cfg.getString(key);
            }

            @Override
            public boolean has(final String key) {
                return cfg.values().get(key) != null;
            }

            @Override
            public Integer getInt(final String key) {
                return cfg.getInt(key);
            }

            @Override
            public Long getLong(final String key) {
                return cfg.getLong(key);
            }

            @Override
            public Boolean getBoolean(final String key) {
                return cfg.getBoolean(key);
            }

            @Override
            public List<String> getList(final String key) {
                return cfg.getList(key);
            }

            @Override
            public Password getPassword(final String key) {
                return cfg.getPassword(key);
            }

            @Override
            public <T> T getConfiguredInstance(final String key, final Class<? extends T> clazz) {
                return cfg.getConfiguredInstance(key, clazz);
            }
        };
    }

    static FragmentDataAccess from(final Map<String, ConfigValue> configValues) {
        return new FragmentDataAccess() {
            @Override
            public Map<String, ?> values() {
                final Map<String, Object> values = new HashMap<>();
                configValues.values().forEach(configValue -> values.put(configValue.name(), configValue.value()));
                return values;
            }

            @Override
            public String getString(final String key) {
                return (String) configValues.get(key).value();
            }

            @Override
            public boolean has(final String key) {
                return configValues.containsKey(key) && configValues.get(key).value() != null;
            }

            @Override
            public Integer getInt(final String key) {
                return (Integer) configValues.get(key).value();
            }

            @Override
            public Long getLong(final String key) {
                return (Long) configValues.get(key).value();
            }

            @Override
            public Boolean getBoolean(final String key) {
                return (Boolean) configValues.get(key).value();
            }

            @Override
            public List<String> getList(final String key) {
                return (List<String>) configValues.get(key).value();
            }

            @Override
            public Password getPassword(final String key) {
                return (Password) configValues.get(key).value();
            }

            @Override
            public <T> T getConfiguredInstance(final String key, final Class<? extends T> clazz) {
                Object obj = configValues.get(key).value();
                if (obj == null) {
                    return null;
                }
                if (obj instanceof String) {
                    try {
                        obj = Utils.newInstance((String) obj, clazz);
                    } catch (ClassNotFoundException e) {
                        throw new KafkaException("Class " + obj + " cannot be found", e);
                    }
                } else if (obj instanceof Class<?>) {
                    obj = Utils.newInstance((Class<?>) obj);
                } else {
                    throw new KafkaException(
                            "Unexpected element of type " + obj.getClass().getName() + ", expected String or Class");
                }
                if (!clazz.isInstance(obj)) {
                    throw new KafkaException(obj + " is not an instance of " + clazz.getName());
                }
                if (obj instanceof Configurable) {
                    ((Configurable) obj).configure(configValues);
                }
                return clazz.cast(obj);
            }
        };
    }

    Map<String, ?> values();

    String getString(String key);

    /**
     * Determines if a key has been set.
     *
     * @param key
     *            The key to check.
     * @return {@code true} if the key was set, {@code false} if the key was not set or does not exist in the config.
     */
    boolean has(String key);

    /**
     * Get the integer value associated with the key.
     *
     * @param key
     *            the key to look up.
     * @return the integer value.
     */
    Integer getInt(String key);

    /**
     * Get the long value associated with the key.
     *
     * @param key
     *            the key to look up.
     * @return the long value.
     */
    Long getLong(String key);

    /**
     * Get the boolean value associated with the key.
     *
     * @param key
     *            the key to look up.
     * @return the boolean value.
     */
    Boolean getBoolean(String key);

    /**
     * Get the list of strings associated with the key.
     *
     * @param key
     *            the key to look up.
     * @return the list of strings.
     */
    List<String> getList(String key);

    /**
     * Get the password associated with the key.
     *
     * @param key
     * @return the password associated with the key.
     */
    Password getPassword(String key);

    /**
     * Creates a configured instance of the class specified by the key. The value of the key may be either a class name
     * or the instance of the class itsef.
     *
     * @param key
     *            the key to look up.
     * @param clazz
     *            class to return.
     * @return an object of class clazz.
     * @param <T>
     *            the expected type of the return value.
     */
    <T> T getConfiguredInstance(String key, Class<? extends T> clazz);

}
