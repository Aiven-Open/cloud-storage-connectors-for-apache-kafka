/*
 * Copyright 2020 Aiven Oy
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

import org.apache.kafka.common.config.ConfigDef;
/**
 * @deprecated Use {@link SinkCommonConfig} instead
 */
@Deprecated
public class AivenCommonConfig extends SinkCommonConfig {

    protected AivenCommonConfig(final ConfigDef definition, final Map<?, ?> originals) {
        super(definition, originals);
    }

    protected static void addKafkaBackoffPolicy(final ConfigDef configDef) {
        SinkCommonConfig.addKafkaBackoffPolicy(configDef);
    }

    protected static void addOutputFieldsFormatConfigGroup(final ConfigDef configDef,
            final OutputFieldType defaultFieldType) {
        SinkCommonConfig.addOutputFieldsFormatConfigGroup(configDef, defaultFieldType);
    }

    protected static void addFormatTypeConfig(final ConfigDef configDef, final int formatGroupCounter) {
        SinkCommonConfig.addFormatTypeConfig(configDef, formatGroupCounter);
    }

    protected static void addCompressionTypeConfig(final ConfigDef configDef,
            final CompressionType defaultCompressionType) {
        SinkCommonConfig.addCompressionTypeConfig(configDef, defaultCompressionType);
    }

}
