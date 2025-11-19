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

import org.apache.kafka.common.config.ConfigValue;

import io.aiven.kafka.connect.common.config.enums.ErrorsTolerance;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.TransformerFactory;
import io.aiven.kafka.connect.common.source.task.DistributionType;

public class SourceCommonConfig extends CommonConfig {

    private final TransformerFragment transformerFragment;
    private final SourceConfigFragment sourceConfigFragment;
    private final FileNameFragment fileNameFragment;

    public SourceCommonConfig(final SourceCommonConfigDef definition, final Map<?, ?> originals) {
        super(definition, originals);
        final FragmentDataAccess dataAccess = FragmentDataAccess.from(this);
        transformerFragment = new TransformerFragment(dataAccess);
        sourceConfigFragment = new SourceConfigFragment(dataAccess);
        fileNameFragment = new FileNameFragment(dataAccess, false);
    }

    public InputFormat getInputFormat() {
        return transformerFragment.getInputFormat();
    }

    public String getSchemaRegistryUrl() {
        return transformerFragment.getSchemaRegistryUrl();
    }

    public String getTargetTopic() {
        return sourceConfigFragment.getTargetTopic();
    }

    public ErrorsTolerance getErrorsTolerance() {
        return sourceConfigFragment.getErrorsTolerance();
    }

    public DistributionType getDistributionType() {
        return sourceConfigFragment.getDistributionType();
    }

    public int getMaxPollRecords() {
        return sourceConfigFragment.getMaxPollRecords();
    }

    public Transformer getTransformer() {
        return TransformerFactory.getTransformer(transformerFragment.getInputFormat());
    }

    public int getTransformerMaxBufferSize() {
        return transformerFragment.getTransformerMaxBufferSize();
    }

    public String getSourceName() {
        return fileNameFragment.getSourceName();
    }

    public String getNativeStartKey() {
        return sourceConfigFragment.getNativeStartKey();
    }

    public int getRingBufferSize() {
        return sourceConfigFragment.getRingBufferSize();
    }

    public static class SourceCommonConfigDef extends CommonConfigDef {

        public SourceCommonConfigDef() {
            super();
            TransformerFragment.update(this);
            SourceConfigFragment.update(this);
            FileNameFragment.update(this);
        }

        @Override
        public Map<String, ConfigValue> multiValidate(final Map<String, ConfigValue> valueMap) {
            final Map<String, ConfigValue> values = super.multiValidate(valueMap);
            final FragmentDataAccess fragmentDataAccess = FragmentDataAccess.from(valueMap);
            new TransformerFragment(fragmentDataAccess).validate(values);
            new SourceConfigFragment(fragmentDataAccess).validate(values);
            new FileNameFragment(fragmentDataAccess, false).validate(values);
            return values;
        }
    }
}
