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

import java.net.URI;
import java.util.Map;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * An abstract class that is the bases for programmatic configuration easier.
 *
 * @param <T>
 *            the concrete class.
 */
@SuppressWarnings("PMD.LinguisticNaming")
public class AbstractFragmentSetter<T extends AbstractFragmentSetter<T>> {
    /** The data being set */
    private final Map<String, String> dataMap;

    /**
     * A value to return from the setter methods. Support a fluent setter class.
     */
    @SuppressWarnings("unchecked")
    protected T self = (T) this;

    /**
     * Constructor.
     *
     * @param data
     *            the map of data items being set.
     */
    protected AbstractFragmentSetter(final Map<String, String> data) {
        this.dataMap = data;
    }

    /**
     * Gets the data map.
     *
     * @return the data map.
     */
    @SuppressFBWarnings(value = { "EI_EXPOSE_REP", "EI_EXPOSE_REP2" }, justification = "returning data as modified")
    final public Map<String, String> data() {
        return dataMap;
    }

    /**
     * Sets the value.
     *
     * @param key
     *            the key to set the value for.
     * @param value
     *            the value to set.
     * @return this.
     */
    final protected T setValue(final String key, final String value) {
        dataMap.put(key, value);
        return self;
    }

    /**
     * Sets the value.
     *
     * @param key
     *            the key to set the value for.
     * @param value
     *            the value to set.
     * @return this.
     */
    final protected T setValue(final String key, final int value) {
        return setValue(key, Integer.toString(value));
    }

    /**
     * Sets the value.
     *
     * @param key
     *            the key to set the value for.
     * @param value
     *            the value to set.
     * @return this.
     */
    final protected T setValue(final String key, final long value) {
        return setValue(key, Long.toString(value));
    }

    /**
     * Sets the value.
     *
     * @param key
     *            the key to set the value for.
     * @param value
     *            the value to set.
     * @return this.
     */
    final protected T setValue(final String key, final Class<?> value) {
        return setValue(key, value.getCanonicalName());
    }

    /**
     * Sets the value.
     *
     * @param key
     *            the key to set the value for.
     * @param value
     *            the value to set.
     * @return this.
     */
    final protected T setValue(final String key, final URI value) {
        return setValue(key, value.toString());
    }

    /**
     * Sets the value.
     *
     * @param key
     *            the key to set the value for.
     * @param state
     *            the value to set.
     * @return this.
     */
    final protected T setValue(final String key, final boolean state) {
        return setValue(key, Boolean.toString(state));
    }
}
