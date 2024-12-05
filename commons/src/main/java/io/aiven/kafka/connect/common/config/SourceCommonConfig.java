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

import io.aiven.kafka.connect.common.config.enums.ErrorsTolerance;
import io.aiven.kafka.connect.common.source.input.InputFormat;

public class SourceCommonConfig extends CommonConfig {

    private final SchemaRegistryFragment schemaRegistryFragment;
    private final SourceConfigFragment sourceConfigFragment;
    private final FileNameFragment fileNameFragment;
    private final OutputFormatFragment outputFormatFragment;

    public SourceCommonConfig(ConfigDef definition, Map<?, ?> originals) {// NOPMD
        super(definition, originals);
        // Construct Fragments
        schemaRegistryFragment = new SchemaRegistryFragment(this);
        sourceConfigFragment = new SourceConfigFragment(this);
        fileNameFragment = new FileNameFragment(this);
        outputFormatFragment = new OutputFormatFragment(this);

        validate(); // NOPMD ConstructorCallsOverridableMethod
    }

    private void validate() {
        schemaRegistryFragment.validate();
        sourceConfigFragment.validate();
        fileNameFragment.validate();
        outputFormatFragment.validate();
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

    public ErrorsTolerance getErrorsTolerance() {
        return ErrorsTolerance.forName(sourceConfigFragment.getErrorsTolerance());
    }

    public int getMaxPollRecords() {
        return sourceConfigFragment.getMaxPollRecords();
    }

}
