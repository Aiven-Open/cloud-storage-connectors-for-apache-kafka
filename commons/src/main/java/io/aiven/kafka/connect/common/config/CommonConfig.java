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
public abstract class CommonConfig extends AbstractConfig {
    protected static final String GROUP_COMPRESSION = "File Compression";
    protected static final String GROUP_FORMAT = "Format";

    /**
     * @deprecated No longer needed.
     */
    @Deprecated
    protected static void addKafkaBackoffPolicy(final ConfigDef configDef) { // NOPMD
        // not required since it is loaded in
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

}
