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
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.ConnectorConfig;

import java.util.Map;

/**
 * The base configuration or all connectors.
 */
public class CommonConfig extends AbstractConfig {
    protected static final String GROUP_COMPRESSION = "File Compression";
    protected static final String GROUP_FORMAT = "Format";
    public static final String TASK_ID = "task.id";

    private final CommonConfigFragment commonConfigFragment;

    private final BackoffPolicyConfig backoffPolicyConfig;
    /**
     * @deprecated Use {@link ConnectorConfig#TASKS_MAX_CONFIG}
     */
    @Deprecated
    public static final String MAX_TASKS = ConnectorConfig.TASKS_MAX_CONFIG;

    /**
     * @deprecated No longer needed.
     */
    @Deprecated
    protected static void addKafkaBackoffPolicy(final ConfigDef configDef) { // NOPMD
        // not required since it is loaded in automatically when AivenCommonConfig is called, in the super method of
        // Common Config.
    }

    private static ConfigDef update(final ConfigDef configDef) {
        CommonConfigFragment.update(configDef);
        BackoffPolicyConfig.update(configDef);
        return configDef;
    }
    /**
     * Constructs the CommonConfig with the backoff policy.
     *
     * @param definition
     *            the definition to add the backoff policy too.
     * @param props
     *            The properties to construct the configuration with.
     */
    protected CommonConfig(ConfigDef definition, Map<?, ?> props) { // NOPMD
        super(update(definition), props);
        commonConfigFragment = new CommonConfigFragment(this);
        backoffPolicyConfig = new BackoffPolicyConfig(this);
    }

    /**
     * Gets the Kafka retry backoff time in MS.
     *
     * @return The Kafka retry backoff time in MS.
     */
    public final Long getKafkaRetryBackoffMs() {
        return backoffPolicyConfig.getKafkaRetryBackoffMs();
    }

    /**
     *
     * Get the maximum number of tasks that should be run by this connector configuration Max Tasks is set within the
     * Kafka Connect framework and so is retrieved slightly differently in ConnectorConfig.java
     *
     * @return The maximum number of tasks that should be run by this connector configuration
     */
    public final int getMaxTasks() {
        return commonConfigFragment.getMaxTasks();
    }


    /**
     * Get the task id for this configuration
     *
     * @return The task id for this configuration
     */
    public final  int getTaskId() {
        return commonConfigFragment.getTaskId();
    }

}
