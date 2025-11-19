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

import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import io.aiven.kafka.connect.common.templating.Template;

/**
 * The configuration common to all Sink implementations.
 */
public class SinkCommonConfig extends CommonConfig {

    /**
     * FileNameFragment to handle FileName based configuration queries.
     */
    protected final FileNameFragment fileNameFragment;
    /**
     * OutputFormatFragment to handle Output format base configuration queries.
     */
    protected final OutputFormatFragment outputFormatFragment;

    /**
     * Constructor.
     *
     * @param definition
     *            the definition for this SinkCommonConfig.
     * @param originals
     *            the original data for the configuraiton.
     */
    public SinkCommonConfig(final SinkCommonConfigDef definition, final Map<String, String> originals) {
        super(definition, FileNameFragment.handleDeprecatedYyyyUppercase(originals));
        final FragmentDataAccess fragmentDataAccess = FragmentDataAccess.from(this);
        fileNameFragment = new FileNameFragment(fragmentDataAccess, false);
        outputFormatFragment = new OutputFormatFragment(fragmentDataAccess);
    }

    /**
     * This method to be removed.
     *
     * @param configDef
     *            the definition.
     * @param defaultFieldType
     *            the field type.
     * @deprecated To be removed when AivenCommonConfig is removed
     */
    @Deprecated
    public static void addOutputFieldsFormatConfigGroup(final ConfigDef configDef,
            final OutputFieldType defaultFieldType) {
        // Does nothing as output fields handled elsewhere.
    }

    /**
     * Gets the format type for the output.
     *
     * @return the format type for the output.
     */
    public FormatType getFormatType() {
        return outputFormatFragment.getFormatType();
    }

    /**
     * Gets the compression type for the output.
     *
     * @return The compression type for the output.
     */
    public CompressionType getCompressionType() {
        return fileNameFragment.getCompressionType();
    }

    /**
     * Checks the envelope enabled flag.
     *
     * @return {@code true} if the envelope is enabled, {@code false} otherwise.
     */
    public Boolean envelopeEnabled() {
        return outputFormatFragment.envelopeEnabled();
    }

    /**
     * Gets the output field encoding type.
     *
     * @return the output field encoding type.
     */
    public OutputFieldEncodingType getOutputFieldEncodingType() {
        return outputFormatFragment.getOutputFieldEncodingType();
    }

    /**
     * Gets the file name template.
     *
     * @return the file name template.
     */
    public final Template getFilenameTemplate() {
        return fileNameFragment.getFilenameTemplate();
    }

    /**
     * Gets the file name. This is parsed to create the Template.
     *
     * @return the file name.
     * @see #getFilenameTemplate()
     */
    public final String getFilename() {
        return fileNameFragment.getFilename();
    }

    /**
     * Gets the timezone for the file name timestamp.
     *
     * @return the timezone for the filename timestamp.
     */
    public final ZoneId getFilenameTimezone() {
        return fileNameFragment.getFilenameTimezone();
    }

    /**
     * Gets the source for the file name timestamp.
     *
     * @return the source for the file name timestamp.
     */
    public final TimestampSource getFilenameTimestampSource() {
        return fileNameFragment.getFilenameTimestampSource();
    }

    /**
     * Gets the maximum records allowed in a single file.
     *
     * @return the maximum records allowed in a single file.
     */
    public final int getMaxRecordsPerFile() {
        return fileNameFragment.getMaxRecordsPerFile();
    }

    /**
     * Gets the list of output fields.
     *
     * @return the list of output fields.
     */
    public List<OutputField> getOutputFields() {
        return outputFormatFragment.getOutputFields();
    }

    /**
     * The ConfigDef for the SinkCommonConfig
     */
    public static class SinkCommonConfigDef extends CommonConfigDef {

        /**
         * Constructor.
         *
         * @param defaultFieldType
         *            the default output field type.
         * @param compressionType
         *            the default file compression type.
         */
        public SinkCommonConfigDef(final OutputFieldType defaultFieldType, final CompressionType compressionType) {
            super();
            OutputFormatFragment.update(this, defaultFieldType);
            FileNameFragment.update(this, compressionType, FileNameFragment.PrefixTemplateSupport.FALSE);
        }

        @Override
        public Map<String, ConfigValue> multiValidate(final Map<String, ConfigValue> valueMap) {
            final Map<String, ConfigValue> values = super.multiValidate(valueMap);
            final FragmentDataAccess fragmentDataAccess = FragmentDataAccess.from(valueMap);
            new OutputFormatFragment(fragmentDataAccess).validate(values);
            new FileNameFragment(fragmentDataAccess, true).validate(values);
            return values;
        }
    }
}
