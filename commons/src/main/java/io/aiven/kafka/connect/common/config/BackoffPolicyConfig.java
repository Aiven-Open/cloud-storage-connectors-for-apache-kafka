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

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * Defines the backoff policy for connectors.
 */
public class BackoffPolicyConfig extends ConfigFragment {

    static final String GROUP_RETRY_BACKOFF_POLICY = "Retry backoff policy";
    static final String KAFKA_RETRY_BACKOFF_MS_CONFIG = "kafka.retry.backoff.ms";

    protected BackoffPolicyConfig(final AbstractConfig cfg) {
        super(cfg);
    }

    /**
     * Adds configuration options to the configuration definition.
     *
     * @param configDef
     *            the configuration definition ot update.
     * @return the configuraiton definition with additional options.
     */
    public static ConfigDef update(final ConfigDef configDef) {
        configDef.define(KAFKA_RETRY_BACKOFF_MS_CONFIG, ConfigDef.Type.LONG, null, new ConfigDef.Validator() {

            final long maximumBackoffPolicy = TimeUnit.HOURS.toMillis(24);

            @Override
            public void ensureValid(final String name, final Object value) {
                if (Objects.isNull(value)) {
                    return;
                }
                assert value instanceof Long;
                final var longValue = (Long) value;
                if (longValue < 0) {
                    throw new ConfigException(name, value, "Value must be at least 0");
                } else if (longValue > maximumBackoffPolicy) {
                    throw new ConfigException(name, value,
                            "Value must be no more than " + maximumBackoffPolicy + " (24 hours)");
                }
            }
        }, ConfigDef.Importance.MEDIUM,
                "The retry backoff in milliseconds. "
                        + "This config is used to notify Kafka Connect to retry delivering a message batch or "
                        + "performing recovery in case of transient exceptions. Maximum value is "
                        + TimeUnit.HOURS.toMillis(24) + " (24 hours).",
                GROUP_RETRY_BACKOFF_POLICY, 1, ConfigDef.Width.NONE, KAFKA_RETRY_BACKOFF_MS_CONFIG);

        return configDef;
    }

    /**
     * Gets the kafka retry backoff time..
     *
     * @return the Kafka retry backoff time in MS.
     */
    public Long getKafkaRetryBackoffMs() {
        return cfg.getLong(KAFKA_RETRY_BACKOFF_MS_CONFIG);
    }
}
