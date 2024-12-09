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

import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.connect.common.source.input.InputFormat;

public class SourceCommonConfig extends CommonConfig {

    private final SchemaRegistryFragment schemaRegistryFragment;
    private final SourceConfigFragment sourceConfigFragment;
    private final FileNameFragment fileNameFragment;
    private final OutputFormatFragment outputFormatFragment;

    public static ConfigDef update(final ConfigDef configDef) {
        SchemaRegistryFragment.update(configDef);
        SourceConfigFragment.update(configDef);
        FileNameFragment.update(configDef);
        OutputFormatFragment.update(configDef, OutputFieldType.VALUE);
        return configDef;
    }

    public SourceCommonConfig(final ConfigDef definition, final Map<?, ?> originals) {
        super(update(definition), originals);
        // Construct Fragments
        schemaRegistryFragment = new SchemaRegistryFragment(this);
        sourceConfigFragment = new SourceConfigFragment(this);
        fileNameFragment = new FileNameFragment(this);
        outputFormatFragment = new OutputFormatFragment(this);
    }

    public void validate() {
        schemaRegistryFragment.validate();
        sourceConfigFragment.validate();
        fileNameFragment.validate();
        outputFormatFragment.validate();
        super.doValidate();
    }

    public InputFormat getInputFormat() {
        return schemaRegistryFragment.getInputFormat();
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryFragment.getSchemaRegistryUrl();
    }

    public String getTargetTopics() {
        return sourceConfigFragment.getTargetTopics();
    }
    public String getTargetTopicPartitions() {
        return sourceConfigFragment.getTargetTopicPartitions();
    }

    /**
     * Get the maximum number of records to return from a poll request.
     *
     * @return The maximum number of records to return from a poll request.
     */
    public int getMaxPollRecords() {
        return sourceConfigFragment.getMaxPollRecords();
    }
}
