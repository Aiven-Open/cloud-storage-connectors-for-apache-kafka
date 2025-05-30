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

import static io.aiven.kafka.connect.common.config.FileNameFragment.FILE_NAME_TEMPLATE_CONFIG;
import static io.aiven.kafka.connect.common.source.task.DistributionType.OBJECT_HASH;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.connect.common.config.enums.ErrorsTolerance;
import io.aiven.kafka.connect.common.config.validators.SourcenameTemplateValidator;
import io.aiven.kafka.connect.common.source.task.DistributionType;

import org.apache.commons.lang3.StringUtils;

public final class SourceConfigFragment extends ConfigFragment {
    public static final String MAX_POLL_RECORDS = "max.poll.records";
    public static final String EXPECTED_MAX_MESSAGE_BYTES = "expected.max.message.bytes";
    public static final String TARGET_TOPIC = "topic";
    public static final String ERRORS_TOLERANCE = "errors.tolerance";

    public static final String DISTRIBUTION_TYPE = "distribution.type";

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

        // Offset Storage config group includes target topics
        configDef.define(TARGET_TOPIC, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "eg : logging-topic");
        configDef.define(DISTRIBUTION_TYPE, ConfigDef.Type.STRING, OBJECT_HASH.name(),
                new ObjectDistributionStrategyValidator(), ConfigDef.Importance.MEDIUM,
                "Based on tasks.max config and the type of strategy selected, objects are processed in distributed"
                        + " way by Kafka connect workers."
                        + Arrays.stream(DistributionType.values())
                                .map(DistributionType::value)
                                .collect(Collectors.joining(", ")));

        configDef.define(MAX_POLL_RECORDS, ConfigDef.Type.INT, 500, ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.MEDIUM, "Max poll records");
        // KIP-298 Error Handling in Connect
        configDef.define(ERRORS_TOLERANCE, ConfigDef.Type.STRING, ErrorsTolerance.NONE.name(),
                new ErrorsToleranceValidator(), ConfigDef.Importance.MEDIUM,
                "Indicates to the connector what level of exceptions are allowed before the connector stops.");

        configDef.define(EXPECTED_MAX_MESSAGE_BYTES, ConfigDef.Type.INT, 1_048_588, ConfigDef.Importance.MEDIUM,
                "The largest record batch size allowed by Kafka config max.message.bytes");

        // step on earlier definition.
        configDef.configKeys().remove(FILE_NAME_TEMPLATE_CONFIG);
        configDef.define(FILE_NAME_TEMPLATE_CONFIG, ConfigDef.Type.STRING, null, null, ConfigDef.Importance.MEDIUM,
                "The template for file names on S3. "
                        + "Supports `{{ variable }}` placeholders for substituting variables. "
                        + "Currently supported variables are `topic`, `partition`, and `start_offset` "
                        + "(the offset of the first record in the file). "
                        + "Only some combinations of variables are valid, which currently are:\n"
                        + "- `topic`, `partition`, `start_offset`."
                        + "There is also `*` only available when using `hash` distribution.",
                "", -1, ConfigDef.Width.LONG, FILE_NAME_TEMPLATE_CONFIG);

        return configDef;
    }

    @Override
    public void validate() {
        new SourcenameTemplateValidator(FILE_NAME_TEMPLATE_CONFIG, getDistributionType())
                .ensureValid(FILE_NAME_TEMPLATE_CONFIG, getSourceName());
    }

    public String getTargetTopic() {
        return cfg.getString(TARGET_TOPIC);
    }
    public String getSourceName() {
        return cfg.getString(FILE_NAME_TEMPLATE_CONFIG);
    }

    public int getMaxPollRecords() {
        return cfg.getInt(MAX_POLL_RECORDS);
    }

    public int getExpectedMaxMessageBytes() {
        return cfg.getInt(EXPECTED_MAX_MESSAGE_BYTES);
    }

    public ErrorsTolerance getErrorsTolerance() {
        return ErrorsTolerance.forName(cfg.getString(ERRORS_TOLERANCE));
    }

    public DistributionType getDistributionType() {
        return DistributionType.forName(cfg.getString(DISTRIBUTION_TYPE));
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

        @Override
        public String toString() {
            return Arrays.stream(ErrorsTolerance.values())
                    .map(ErrorsTolerance::toString)
                    .collect(Collectors.joining(", "));
        }

    }

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

}
