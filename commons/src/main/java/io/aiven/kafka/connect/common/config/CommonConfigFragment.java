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

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.ConnectorConfig;

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

        return configDef
                .define(ConnectorConfig.TASKS_MAX_CONFIG, ConfigDef.Type.INT, 1, atLeast(1), ConfigDef.Importance.HIGH,
                        "Maximum number of tasks to use for this connector.", commonGroup, ++orderInGroup,
                        ConfigDef.Width.SHORT, ConnectorConfig.TASKS_MAX_CONFIG)
                .define(TASK_ID, ConfigDef.Type.INT, 1, atLeast(0), ConfigDef.Importance.HIGH,
                        "The task ID that this connector is working with.", commonGroup, ++orderInGroup,
                        ConfigDef.Width.SHORT, TASK_ID);
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

    /**
     * Get the maximum number of tasks.
     *
     * @return the maximum number of tasks.
     */
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
    }
}
