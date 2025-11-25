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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

/**
 * The base configuration or all connectors.
 */
public class CommonConfig extends AbstractConfig {

    public static final String TASK_ID = "task.id";

    private final BackoffPolicyFragment backoffPolicyConfig;
    private final CommonConfigFragment commonConfigFragment;
    /**
     * @deprecated No longer needed.
     */
    @Deprecated
    protected static void addKafkaBackoffPolicy(final ConfigDef configDef) { // NOPMD
        // not required since it is loaded in automatically when AivenCommonConfig is called, in the super method of
        // Common Config.
    }

    /**
     * Checks the configuration definition for errors. If any errors are found an exception is thrown. Due to the point
     * at which this is called there are no {@link ConfigDef.ConfigKey#validator} errors to worry about.
     *
     * @param definition
     *            the ConfigDefinition to validate.
     */
    private void doVerification(final ConfigDef definition, final Map<String, String> values) {
        // if the call multiValidateValues if it is available otherwise just validate
        final Stream<ConfigValue> configValueStream = definition instanceof CommonConfigDef
                ? ((CommonConfigDef) definition).multiValidateValues(values).stream()
                : definition.validate(values).stream();
        // extract all the values with associated errors.
        final List<ConfigValue> errorConfigs = configValueStream
                .filter(configValue -> !configValue.errorMessages().isEmpty())
                .collect(Collectors.toList());
        if (!errorConfigs.isEmpty()) {
            final String msg = errorConfigs.stream()
                    .flatMap(configValue -> configValue.errorMessages().stream())
                    .collect(Collectors.joining("\n"));
            throw new ConfigException("There are errors in the configuration:\n" + msg);
        }
    }

    /**
     * Verifies that all the settings are strings or null.
     *
     * @return the map of key to setting string.
     */
    private Map<String, String> originalsNullableStrings() {
        final Map<String, String> result = new HashMap<>();
        for (final Map.Entry<String, Object> entry : originals().entrySet()) {
            if (entry.getValue() != null && !(entry.getValue() instanceof String)) {
                throw new ClassCastException("Non-string value found in original settings for key " + entry.getKey()
                        + ": " + entry.getValue().getClass().getName());
            }
            if (entry.getKey() == null) {
                throw new ClassCastException("Null key found in original settings.");
            }
            result.put(entry.getKey(), (String) entry.getValue());
        }
        return result;
    }

    public CommonConfig(final ConfigDef definition, final Map<?, ?> originals) {
        super(definition, originals);
        doVerification(definition, originalsNullableStrings());
        final FragmentDataAccess dataAccess = FragmentDataAccess.from(this);
        commonConfigFragment = new CommonConfigFragment(dataAccess);
        backoffPolicyConfig = new BackoffPolicyFragment(dataAccess);
    }

    /**
     * Avoid Finalizer attack
     */
    @Override
    @SuppressWarnings("PMD.EmptyFinalizer")
    protected final void finalize() {
        // Do nothing
    }

    /**
     * Gets the Kafka retry backoff time in MS.
     *
     * @return The Kafka retry backoff time in MS.
     */
    public Long getKafkaRetryBackoffMs() {
        return backoffPolicyConfig.getKafkaRetryBackoffMs();
    }

    /**
     *
     * Get the maximum number of tasks that should be run by this connector configuration Max Tasks is set within the
     * Kafka Connect framework and so is retrieved slightly differently in ConnectorConfig.java
     *
     * @return The maximum number of tasks that should be run by this connector configuration
     */
    public int getMaxTasks() {
        return commonConfigFragment.getMaxTasks();
    }
    /**
     * Get the task id for this configuration
     *
     * @return The task id for this configuration
     */
    public int getTaskId() {
        return commonConfigFragment.getTaskId();
    }

    public static class CommonConfigDef extends ConfigDef {
        /**
         * Constructor .
         */
        public CommonConfigDef() {
            super();
            BackoffPolicyFragment.update(this);
            CommonConfigFragment.update(this);
        }

        /**
         * Gathers in depth, multi argument configuration issues. This method should be overridden when the Fragments
         * added to the config have validation rules that required inspection of multiple properties.
         * <p>
         * Overriding methods should call the parent method to update the map and then add error messages to the
         * {@link ConfigValue} associated with property name that is in error.
         * </p>
         *
         * @param valueMap
         *            the map of configuration names to values.
         * @return the updated map.
         */
        protected Map<String, ConfigValue> multiValidate(final Map<String, ConfigValue> valueMap) {
            new BackoffPolicyFragment(FragmentDataAccess.from(valueMap)).validate(valueMap);
            return valueMap;
        }

        /**
         * Validate the configuration properties using the multiValidate process.
         *
         * @param props
         *            the properties to validate
         * @return the collection of ConfigValues from the validation.
         */
        final Collection<ConfigValue> multiValidateValues(final Map<String, String> props) {
            return multiValidate(validateAll(props)).values();
        }

        @Override
        @SuppressWarnings("PMD.AvoidCatchingGenericException")
        public final List<ConfigValue> validate(final Map<String, String> props) {
            final Map<String, ConfigValue> valueMap = validateAll(props);

            try {
                return new ArrayList<>(multiValidate(valueMap).values());
            } catch (RuntimeException e) {
                // any exceptions thrown in the above block are accounted for in the validateAll(props) call.
                return new ArrayList<>(valueMap.values());
            }
        }
    }
}
