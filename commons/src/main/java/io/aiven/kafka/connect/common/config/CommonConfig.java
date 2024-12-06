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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

/**
 * The base configuration or all connectors.
 */
public class CommonConfig extends AbstractConfig {
    protected static final String GROUP_COMPRESSION = "File Compression";
    protected static final String GROUP_FORMAT = "Format";

    public static ConfigDef generateFullConfigurationDefinition(Class<? extends CommonConfig> clazz) {
        ConfigDef configDef = new ConfigDef();
        Class<?> workingClazz = clazz;
        while (CommonConfig.class.isAssignableFrom(workingClazz)) {
            try {
                Method m = workingClazz.getDeclaredMethod("update", ConfigDef.class);
                int modifiers = m.getModifiers();
                if (!Modifier.isPublic(modifiers) || !Modifier.isStatic(modifiers)) {
                    throw new IllegalArgumentException("Class " + workingClazz.getName() + " does not have a public static method 'update(ConfigDef)'");
                }
                m.invoke(null, configDef);
                workingClazz = workingClazz.getSuperclass();
            } catch (InvocationTargetException e) {
                throw new IllegalArgumentException("Can not invoke '" + workingClazz.getName() + ".update(ConfigDef)'");
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException("Class " + workingClazz.getName() + " does not have a public static method 'update(ConfigDef)'");
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("Can not access '" + workingClazz.getName() + ".update(ConfigDef)'");
            }
        }
        return configDef;
    }

    /**
     * Adds the definitions for Common config to the configuration definition..
     * @param configDef the configuration definition to update.
     * @return the updated configuration definition.
     */
    public static ConfigDef update(ConfigDef configDef) {
        return BackoffPolicyConfig.update(configDef);
    }

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
        super(update(definition), props);
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
