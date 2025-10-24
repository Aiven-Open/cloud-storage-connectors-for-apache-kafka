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

import static io.aiven.kafka.connect.common.source.task.DistributionType.OBJECT_HASH;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.connect.common.config.enums.ErrorsTolerance;
import io.aiven.kafka.connect.common.source.task.DistributionType;

import org.apache.commons.lang3.StringUtils;

/**
 * Defines properties that are shared across all Source implementations.
 */
public final class SourceConfigFragment extends ConfigFragment {

    private static final String MAX_POLL_RECORDS = "max.poll.records";
    public static final String TARGET_TOPIC = "topic";
    private static final String ERRORS_TOLERANCE = "errors.tolerance";
    private static final String DISTRIBUTION_TYPE = "distribution.type";

    /* public so that deprecated users can reference it */
    public static final String RING_BUFFER_SIZE = "ring.buffer.size";

    public static final String NATIVE_START_KEY = "native.start.key";

    /**
     * Gets a setter for this fragment.
     *
     * @param data
     *            the data map to modify.
     * @return the Setter.
     */
    public static Setter setter(final Map<String, String> data) {
        return new Setter(data);
    }

    /**
     * Construct the ConfigFragment.
     *
     * @param cfg
     *            the configuration that this fragment is associated with.
     */
    public SourceConfigFragment(final AbstractConfig cfg) {
        super(cfg);
    }

    public static ConfigDef update(final ConfigDef configDef) {
        configDef.define(RING_BUFFER_SIZE, ConfigDef.Type.INT, 1000, ConfigDef.Range.atLeast(0),
                ConfigDef.Importance.MEDIUM, "The number of storage key to store in the ring buffer.");

        configDef.define(MAX_POLL_RECORDS, ConfigDef.Type.INT, 500, ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.MEDIUM, "Max poll records");
        // KIP-298 Error Handling in Connect
        configDef.define(ERRORS_TOLERANCE, ConfigDef.Type.STRING, ErrorsTolerance.NONE.name(),
                new ErrorsToleranceValidator(), ConfigDef.Importance.MEDIUM,
                "Indicates to the connector what level of exceptions are allowed before the connector stops.");

        // Offset Storage config group includes target topics
        configDef.define(TARGET_TOPIC, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "eg : logging-topic");
        configDef.define(DISTRIBUTION_TYPE, ConfigDef.Type.STRING, OBJECT_HASH.name(),
                new ObjectDistributionStrategyValidator(), ConfigDef.Importance.MEDIUM,
                "Based on tasks.max config and the type of strategy selected, objects are processed in distributed"
                        + " way by Kafka connect workers.");

        // TODO FIX ME this should be updated to add 'since version 3.4.2' when ExtendedConfigKey is used.
        configDef.define(NATIVE_START_KEY, ConfigDef.Type.STRING, null, null, ConfigDef.Importance.MEDIUM,
                "An identifier for the source connector to know which key to start processing from, on a restart it will also begin reading messages from this point as well. Available since 3.4.2");

        return configDef;
    }

    /**
     * Gets the target topic.
     *
     * @return the target topic.
     */
    public String getTargetTopic() {
        return cfg.getString(TARGET_TOPIC);
    }

    /**
     * Gets the maximum number of records to poll at one time.
     *
     * @return The maximum number of records to poll at one time.
     */
    public int getMaxPollRecords() {
        return cfg.getInt(MAX_POLL_RECORDS);
    }

    /**
     * Gets the errors tolerance.
     *
     * @return the errors tolerance.
     */
    public ErrorsTolerance getErrorsTolerance() {
        return ErrorsTolerance.forName(cfg.getString(ERRORS_TOLERANCE));
    }

    /**
     * Gets the distribution type
     *
     * @return the distribution type.
     */
    public DistributionType getDistributionType() {
        return DistributionType.forName(cfg.getString(DISTRIBUTION_TYPE));
    }

    /**
     * Gets the ring buffer size.
     *
     * @return the ring buffer size.
     */
    public int getRingBufferSize() {
        return cfg.getInt(RING_BUFFER_SIZE);
    }

    /**
     * Gets the nativeStartKey.
     *
     * @return the key to start consuming records from.
     */
    public String getNativeStartKey() {
        return cfg.getString(NATIVE_START_KEY);
    }

    /**
     * The errors tolerance validator.
     */
    private static class ErrorsToleranceValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            final String errorsTolerance = (String) value;
            if (StringUtils.isNotBlank(errorsTolerance)) {
                // This will throw an Exception if not a valid value.
                ErrorsTolerance.forName(errorsTolerance);
            }
        }

        @Override
        public String toString() {
            return Arrays.stream(ErrorsTolerance.values())
                    .map(ErrorsTolerance::toString)
                    .collect(Collectors.joining(", "));
        }

    }

    /**
     * The distribution strategy validator.
     */
    private static class ObjectDistributionStrategyValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            final String objectDistributionStrategy = (String) value;
            if (StringUtils.isNotBlank(objectDistributionStrategy)) {
                // This will throw an Exception if not a valid value.
                DistributionType.forName(objectDistributionStrategy);
            }
        }

        @Override
        public String toString() {
            return Arrays.stream(DistributionType.values())
                    .map(DistributionType::name)
                    .collect(Collectors.joining(", "));
        }
    }

    /**
     * The SourceConfigFragment setter.
     */
    public static class Setter extends AbstractFragmentSetter<Setter> {
        /**
         * Constructor.
         *
         * @param data
         *            the data to modify.
         */
        protected Setter(final Map<String, String> data) {
            super(data);
        }

        /**
         * Set the maximum poll records.
         *
         * @param maxPollRecords
         *            the maximum number of records to poll.
         * @return this
         */
        public Setter maxPollRecords(final int maxPollRecords) {
            return setValue(MAX_POLL_RECORDS, maxPollRecords);
        }

        /**
         * Sets the error tolerance.
         *
         * @param tolerance
         *            the error tolerance
         * @return this.
         */
        public Setter errorsTolerance(final ErrorsTolerance tolerance) {
            return setValue(ERRORS_TOLERANCE, tolerance.name());
        }

        /**
         * Sets the target topic.
         *
         * @param targetTopic
         *            the target topic.
         * @return this.
         */
        public Setter targetTopic(final String targetTopic) {
            return setValue(TARGET_TOPIC, targetTopic);
        }

        /**
         * Sets the distribution type.
         *
         * @param distributionType
         *            the distribution type.
         * @return this
         */
        public Setter distributionType(final DistributionType distributionType) {
            return setValue(DISTRIBUTION_TYPE, distributionType.name());
        }

        /**
         * Sets the ring buffer size.
         *
         * @param ringBufferSize
         *            the ring buffer size
         * @return this.
         */
        public Setter ringBufferSize(final int ringBufferSize) {
            return setValue(RING_BUFFER_SIZE, ringBufferSize);
        }

        /**
         * Sets the initial native key to start from.
         *
         * @param nativeStartKey
         *            the key to start reading new messages from.
         * @return this.
         */
        public Setter nativeStartKey(final String nativeStartKey) {
            return setValue(NATIVE_START_KEY, nativeStartKey);
        }
    }
}
