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

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

/**
 * The base configuration or all connectors.
 */
public class CommonConfig extends AbstractConfig {
    protected static final String GROUP_COMPRESSION = "File Compression";
    protected static final String GROUP_FORMAT = "Format";
    public static final String TASK_ID = "task.id";
    public static final String MAX_TASKS = "tasks.max";

    /**
     * @deprecated No longer needed.
     */
    @Deprecated
    protected static void addKafkaBackoffPolicy(final ConfigDef configDef) { // NOPMD
        // not required since it is loaded in automatically when AivenCommonConfig is called, in the super method of
        // Common Config.
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
        super(BackoffPolicyConfig.update(definition), props);
    }

    /**
     * Gets the Kafka retry backoff time in MS.
     *
     * @return The Kafka retry backoff time in MS.
     */
    public Long getKafkaRetryBackoffMs() {
        return new BackoffPolicyConfig(this).getKafkaRetryBackoffMs();
    }

    /**
     *
     * Get the maximum number of tasks that should be run by this connector configuration Max Tasks is set within the
     * Kafka Connect framework and so is retrieved slightly differently in ConnectorConfig.java
     *
     * @return The maximum number of tasks that should be run by this connector configuration
     */
    public int getMaxTasks() {
        // TODO when Connect framework is upgraded it will be possible to retrieve this information from the configDef
        // as tasksMax
        return Integer.parseInt(this.originalsStrings().get(MAX_TASKS));
    }
    /**
     * Get the task id for this configuration
     *
     * @return The task id for this configuration
     */
    public int getTaskId() {
        return Integer.parseInt(this.originalsStrings().get(TASK_ID));
    }

}
