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

package io.aiven.kafka.connect.tools;

import java.util.List;

import org.apache.kafka.common.config.ConfigDef;

/**
 * Defines the variables that are available for the Velocity template to access a {@link ConfigDef.ConfigKey} object.
 *
 * @see <a href="https://velocity.apache.org/">Apache Velocity</a>
 */
public class ConfigData {
    /** The key */
    private final ConfigDef.ConfigKey key;

    /**
     * Constructor.
     *
     * @param key
     *            the Key to wrap.
     */
    public ConfigData(final ConfigDef.ConfigKey key) {
        this.key = key;
    }

    /**
     * Gets the name of the key.
     *
     * @return The name of the key.
     */
    public final String getName() {
        return key.name;
    }

    /**
     * Gets the data type of the entry.
     *
     * @return The data type of the entry.
     */
    public final ConfigDef.Type getType() {
        return key.type;
    }

    /**
     * Gets the documentation for the entry.
     *
     * @return The documentation for the entry.
     */
    public final String getDocumentation() {
        return key.documentation;
    }

    /**
     * Gets the default value for the entry.
     *
     * @return The default value for the entry. May be {@code null}.
     */
    public final Object getDefaultValue() {
        return key.defaultValue == ConfigDef.NO_DEFAULT_VALUE ? null : key.defaultValue;
    }

    /**
     * Gets the validator for the entry.
     *
     * @return The Validator for the entry.
     */
    public final ConfigDef.Validator getValidator() {
        return key.validator;
    }

    /**
     * Gets the importance of the entry.
     *
     * @return the importance of the entry.
     */
    public final ConfigDef.Importance getImportance() {
        return key.importance;
    }

    /**
     * Gets the group of the entry.
     *
     * @return The group of the entry,
     */
    public final String getGroup() {
        return key.group;
    }

    /**
     * Gets the order in the group of the entry.
     *
     * @return the order in the group of the entry.
     */
    public final int getOrderInGroup() {
        return key.orderInGroup;
    }

    /**
     * Gets the width estimate for the entry.
     *
     * @return The width estimate for the entry.
     */
    public final ConfigDef.Width getWidth() {
        return key.width;
    }

    /**
     * Gets the display name for the entry.
     *
     * @return The display name for the entry.
     */
    public final String getDisplayName() {
        return key.displayName;
    }

    /**
     * Gets the list of dependents for the entry.
     *
     * @return The list of dependents for the entry.
     */
    public final List<String> getDependents() {
        return key.dependents;
    }

    /**
     * Gets the recommender for the entry.
     *
     * @return the recommender for the entry.
     */
    public final ConfigDef.Recommender getRecommender() {
        return key.recommender;
    }

    /**
     * Gets the internal config flag.
     *
     * @return The internal config flag.
     */
    public final boolean isInternalConfig() {
        return key.internalConfig;
    }

}
