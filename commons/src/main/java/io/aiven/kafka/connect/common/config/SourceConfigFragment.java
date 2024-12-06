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

import io.aiven.kafka.connect.common.config.enums.ErrorsTolerance;

import org.codehaus.plexus.util.StringUtils;

public final class SourceConfigFragment extends ConfigFragment {
    private static final String GROUP_OTHER = "OTHER_CFG";
    public static final String MAX_POLL_RECORDS = "max.poll.records";
    public static final String EXPECTED_MAX_MESSAGE_BYTES = "expected.max.message.bytes";
    private static final String GROUP_OFFSET_TOPIC = "OFFSET_TOPIC";
    public static final String TARGET_TOPIC_PARTITIONS = "topic.partitions";
    public static final String TARGET_TOPICS = "topics";
    public static final String ERRORS_TOLERANCE = "errors.tolerance";

    /**
     * Construct the ConfigFragment..
     *
     * @param cfg
     *            the configuration that this fragment is associated with.
     */
    public SourceConfigFragment(final AbstractConfig cfg) {
        super(cfg);
    }

    public static ConfigDef update(final ConfigDef configDef) {
        int sourcePollingConfigCounter = 0;

        configDef.define(MAX_POLL_RECORDS, ConfigDef.Type.INT, 500, ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.MEDIUM, "Max poll records", GROUP_OTHER, sourcePollingConfigCounter++,
                ConfigDef.Width.NONE, MAX_POLL_RECORDS);
        // KIP-298 Error Handling in Connect
        configDef.define(ERRORS_TOLERANCE, ConfigDef.Type.STRING, ErrorsTolerance.NONE.name(),
                new ErrorsToleranceValidator(), ConfigDef.Importance.MEDIUM,
                "Indicates to the connector what level of exceptions are allowed before the connector stops, supported values : none,all",
                GROUP_OTHER, sourcePollingConfigCounter++, ConfigDef.Width.NONE, ERRORS_TOLERANCE);

        configDef.define(EXPECTED_MAX_MESSAGE_BYTES, ConfigDef.Type.INT, 1_048_588, ConfigDef.Importance.MEDIUM,
                "The largest record batch size allowed by Kafka config max.message.bytes", GROUP_OTHER,
                sourcePollingConfigCounter++, // NOPMD
                // UnusedAssignment
                ConfigDef.Width.NONE, EXPECTED_MAX_MESSAGE_BYTES);

        // Offset Storage config group includes target topics
        int offsetStorageGroupCounter = 0;
        configDef.define(TARGET_TOPIC_PARTITIONS, ConfigDef.Type.STRING, "0", new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "eg : 0,1", GROUP_OFFSET_TOPIC, offsetStorageGroupCounter++,
                ConfigDef.Width.NONE, TARGET_TOPIC_PARTITIONS);
        configDef.define(TARGET_TOPICS, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "eg : connect-storage-offsets", GROUP_OFFSET_TOPIC,
                offsetStorageGroupCounter++, ConfigDef.Width.NONE, TARGET_TOPICS); // NOPMD

        return configDef;
    }

    public String getTargetTopics() {
        return cfg.getString(TARGET_TOPICS);
    }

    public String getTargetTopicPartitions() {
        return cfg.getString(TARGET_TOPIC_PARTITIONS);
    }

    public int getMaxPollRecords() {
        return cfg.getInt(MAX_POLL_RECORDS);
    }

    public int getExpectedMaxMessageBytes() {
        return cfg.getInt(EXPECTED_MAX_MESSAGE_BYTES);
    }

    public String getErrorsTolerance() {
        return cfg.getString(ERRORS_TOLERANCE);
    }

    private static class ErrorsToleranceValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            final String errorsTolerance = (String) value;
            if (StringUtils.isNotBlank(errorsTolerance)) {
                // This will throw an Exception if not a valid value.
                ErrorsTolerance.forName(errorsTolerance);
            }
        }
    }

}
