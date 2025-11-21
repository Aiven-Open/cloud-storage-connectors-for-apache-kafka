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

/**
 * The common definitions for source connectors.
 */
public class SourceCommonConfig extends CommonConfig {
    /**
     * The transformer configuration fragment.
     */
    private final TransformerFragment transformerFragment;
    /**
     * The standard source configuration fragment.
     */
    private final SourceConfigFragment sourceConfigFragment;
    /**
     * The file name fragment.
     */
    private final FileNameFragment fileNameFragment;

    /**
     * Constructor.
     *
     * @param definition
     *            the configuration definition.
     * @param originals
     *            the initial configuration data.
     */
    public SourceCommonConfig(final SourceCommonConfigDef definition, final Map<?, ?> originals) {
        super(definition, originals);
        final FragmentDataAccess dataAccess = FragmentDataAccess.from(this);
        transformerFragment = new TransformerFragment(dataAccess);
        sourceConfigFragment = new SourceConfigFragment(dataAccess);
        fileNameFragment = new FileNameFragment(dataAccess, false);
    }

    /**
     * Gets the input format.
     *
     * @return the input format.
     */
    public InputFormat getInputFormat() {
        return transformerFragment.getInputFormat();
    }

    /**
     * Gets the compression type for the file.
     *
     * @return the compression type for the file.
     */
    public CompressionType getCompressionType() {
        return fileNameFragment.getCompressionType();
    }

    /**
     * Gets the schema registry URL.
     *
     * @return the schema registry URL.
     */
    public String getSchemaRegistryUrl() {
        return transformerFragment.getSchemaRegistryUrl();
    }

    /**
     * Gets the target topic to write messages to.
     *
     * @return the target topic to write messages to.
     */
    public String getTargetTopic() {
        return sourceConfigFragment.getTargetTopic();
    }

    /**
     * Gets the tolerance for errors.
     *
     * @return the tolerance for errors.
     */
    public ErrorsTolerance getErrorsTolerance() {
        return sourceConfigFragment.getErrorsTolerance();
    }

    /**
     * Gets the distribution type.
     *
     * @return the distribution type.
     */
    public DistributionType getDistributionType() {
        return sourceConfigFragment.getDistributionType();
    }

    /**
     * Gets the maximum number of records to return in a single poll.
     *
     * @return the maximum number of records to return in a single poll.
     */
    public int getMaxPollRecords() {
        return sourceConfigFragment.getMaxPollRecords();
    }

    /**
     * Gets the transformer.
     *
     * @return the transformer.
     */
    public Transformer getTransformer() {
        return TransformerFactory.getTransformer(transformerFragment.getInputFormat());
    }

    /**
     * Gets the maximum buffer size for the transformer.
     *
     * @return the maximum buffer size for hte transformer.
     */
    public int getTransformerMaxBufferSize() {
        return transformerFragment.getTransformerMaxBufferSize();
    }

    /**
     * Gets the name of the source file.
     *
     * @return the name of the source file.
     */
    public String getSourceName() {
        return fileNameFragment.getSourceName();
    }

    /**
     * Gets the native start key for the initial file to scan.
     *
     * @return the native start key.
     */
    public String getNativeStartKey() {
        return sourceConfigFragment.getNativeStartKey();
    }

    /**
     * Gets the size of the ring buffer used to track read files.
     *
     * @return the size of the ring buffer.
     */
    public int getRingBufferSize() {
        return sourceConfigFragment.getRingBufferSize();
    }

    /**
     * The common source configuration definition.
     */
    public static class SourceCommonConfigDef extends CommonConfigDef {

        /**
         * Constructor.
         */
        public SourceCommonConfigDef() {
            super();
            TransformerFragment.update(this);
            SourceConfigFragment.update(this);
            FileNameFragment.update(this, CompressionType.NONE, FileNameFragment.PrefixTemplateSupport.TRUE);
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
