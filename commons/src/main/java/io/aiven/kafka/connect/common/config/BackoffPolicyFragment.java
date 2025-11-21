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

import org.apache.kafka.common.config.ConfigDef;

import io.aiven.commons.collections.TimeScale;
import io.aiven.kafka.connect.common.config.validators.PredicateGatedValidator;
import io.aiven.kafka.connect.common.config.validators.TimeScaleValidator;

/**
 * Defines the backoff policy for connectors.
 */
public final class BackoffPolicyFragment extends ConfigFragment {

    static final String GROUP_RETRY_BACKOFF_POLICY = "Retry backoff policy";
    static final String KAFKA_RETRY_BACKOFF_MS_CONFIG = "kafka.retry.backoff.ms";

    public BackoffPolicyFragment(final FragmentDataAccess dataAccess) {
        super(dataAccess);
    }

    /**
     * Adds configuration options to the configuration definition.
     *
     * @param configDef
     *            the configuration definition ot update.
     * @return the number of items in the backoff policy group.
     */
    public static int update(final ConfigDef configDef) {
        configDef.define(KAFKA_RETRY_BACKOFF_MS_CONFIG, ConfigDef.Type.LONG, null,
                new PredicateGatedValidator(
                        Objects::nonNull, TimeScaleValidator.between(0, TimeScale.DAYS.asMilliseconds(1))),
                ConfigDef.Importance.MEDIUM,
                "The retry backoff in milliseconds. "
                        + "This config is used to notify Kafka Connect to retry delivering a message batch or "
                        + "performing recovery in case of transient exceptions. Maximum value is "
                        + TimeScale.DAYS.displayValue(TimeScale.DAYS.asMilliseconds(1)),
                GROUP_RETRY_BACKOFF_POLICY, 1, ConfigDef.Width.NONE, KAFKA_RETRY_BACKOFF_MS_CONFIG);
        return 1;
    }

    /**
     * Gets the kafka retry backoff time.
     *
     * @return the Kafka retry backoff time in MS.
     */
    public Long getKafkaRetryBackoffMs() {
        return getLong(KAFKA_RETRY_BACKOFF_MS_CONFIG);
    }
}
