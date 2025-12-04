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

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.storage.Converter;

/**
 * The common configuration fragment.
 */
public class CommonConfigFragment extends ConfigFragment {
    /** The task id configuration option */
    private static final String TASK_ID = "task.id";
    /**
     * Gets a setter for this fragment.
     *
     * @param data
     *            the data to modify.
     * @return The setter.
     */
    public static Setter setter(final Map<String, String> data) {
        return new Setter(data);
    }

    /**
     * Update the ConfigDef with the values from the fragment.
     *
     * @param configDef
     *            the configuraiton def to update.
     * @return the updated configuration def.
     */
    public static ConfigDef update(final ConfigDef configDef) {
        int orderInGroup = 0;
        final String commonGroup = "commons";

        // make TASK_ID an internal configuration (not visible to users)
        ConfigDef.ConfigKey key = new ConfigDef.ConfigKey(TASK_ID, ConfigDef.Type.INT, 0, atLeast(0),
                ConfigDef.Importance.HIGH, "The task ID that this connector is working with.", commonGroup,
                ++orderInGroup, ConfigDef.Width.SHORT, TASK_ID, Collections.emptyList(), null, true);
        configDef.define(key);

        // change the validator errors tolerance.
        key = configDef.configKeys().get(CommonConfig.ERRORS_TOLERANCE_CONFIG);

        // configDef.configKeys().remove(CommonConfig.ERRORS_TOLERANCE_CONFIG);

        final ConfigDef.ConfigKey newKey = new ConfigDef.ConfigKey(key.name, key.type, key.defaultValue,
                ConfigDef.CaseInsensitiveValidString.in(ToleranceType.NONE.value(), ToleranceType.ALL.value()),
                key.importance, key.documentation, key.group, key.orderInGroup, key.width, key.displayName,
                key.dependents, key.recommender, key.internalConfig);
        configDef.configKeys().replace(CommonConfig.ERRORS_TOLERANCE_CONFIG, newKey);
        return configDef;
    }

    /**
     * Create a fragment instance from an AbstractConfig.
     *
     * @param dataAccess
     *            the FragmentDataAccess to retrieve data from.
     */
    public CommonConfigFragment(final FragmentDataAccess dataAccess) {
        super(dataAccess);
    }

    /**
     * Get the task Id.
     *
     * @return the task Id.
     */
    public Integer getTaskId() {
        return getInt(TASK_ID);
    }

    public String getConnectorName() {
        return getString(ConnectorConfig.NAME_CONFIG);
    }

    public Integer getMaxTasks() {
        return getInt(ConnectorConfig.TASKS_MAX_CONFIG);
    }

    /**
     * Setter to programmatically set values in the configuraiotn.
     */
    public static class Setter extends AbstractFragmentSetter<Setter> {
        /**
         * Creates the setter.
         *
         * @param data
         *            the map of data to update.
         */
        private Setter(final Map<String, String> data) {
            super(data);
        }

        /**
         * Sets the task ID value.
         *
         * @param taskId
         *            the task Id value.
         * @return this
         */
        public Setter taskId(final int taskId) {
            return setValue(TASK_ID, taskId);
        }

        /**
         * Sets the max tasks value.
         *
         * @param maxTasks
         *            the maximum number of tasks for this connector to run.
         * @return this
         */
        public Setter maxTasks(final int maxTasks) {
            return setValue(ConnectorConfig.TASKS_MAX_CONFIG, maxTasks);
        }

        /**
         * The class for the connector.
         *
         * @param connectorClass
         *            the class for the connector.
         * @return this
         */
        public Setter connector(final Class<? extends Connector> connectorClass) {
            return setValue(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass);
        }

        /**
         * Sets the key converter.
         *
         * @param converter
         *            the Key converter class.
         * @return this
         */
        public Setter keyConverter(final Class<? extends Converter> converter) {
            return setValue(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, converter);
        }

        /**
         * Sets the value converter.
         *
         * @param converter
         *            the value converter class.
         * @return this
         */
        public Setter valueConverter(final Class<? extends Converter> converter) {
            return setValue(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, converter);
        }

        /**
         * Sets the header converter.
         *
         * @param header
         *            the header converter class.
         * @return this.
         */
        public Setter headerConverter(final Class<? extends Converter> header) {
            return setValue(ConnectorConfig.HEADER_CONVERTER_CLASS_CONFIG, header);
        }

        /**
         * Sets the connector name.
         *
         * @param name
         *            the connector name.
         * @return this
         */
        public Setter name(final String name) {
            return setValue(ConnectorConfig.NAME_CONFIG, name);
        }
    }
}
