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

public class SinkCommonConfig extends CommonConfig {

    /**
     * FileNameFragment to handle FileName based configuration queries.
     */
    protected final FileNameFragment fileNameFragment;
    /**
     * OutputFormatFragment to handle Output format base configuration queries.
     */
    protected final OutputFormatFragment outputFormatFragment;

    public SinkCommonConfig(final SinkCommonConfigDef definition, final Map<String, String> originals) {
        super(definition, FileNameFragment.handleDeprecatedYyyyUppercase(originals));
        final FragmentDataAccess fragmentDataAccess = FragmentDataAccess.from(this);
        fileNameFragment = new FileNameFragment(fragmentDataAccess, false);
        outputFormatFragment = new OutputFormatFragment(fragmentDataAccess);
    }

    @Deprecated
    public static void addOutputFieldsFormatConfigGroup(final ConfigDef configDef,
            final OutputFieldType defaultFieldType) {
        // OutputFormatFragment.update(configDef, defaultFieldType);
    }

    public FormatType getFormatType() {
        return outputFormatFragment.getFormatType();
    }

    public CompressionType getCompressionType() {
        return fileNameFragment.getCompressionType();
    }

    public Boolean envelopeEnabled() {
        return outputFormatFragment.envelopeEnabled();
    }

    public OutputFieldEncodingType getOutputFieldEncodingType() {
        return outputFormatFragment.getOutputFieldEncodingType();
    }

    public final Template getFilenameTemplate() {
        return fileNameFragment.getFilenameTemplate();
    }

    public final String getFilename() {
        return fileNameFragment.getFilename();
    }

    public final ZoneId getFilenameTimezone() {
        return fileNameFragment.getFilenameTimezone();
    }

    public final TimestampSource getFilenameTimestampSource() {
        return fileNameFragment.getFilenameTimestampSource();
    }

    public final int getMaxRecordsPerFile() {
        return fileNameFragment.getMaxRecordsPerFile();
    }

    public List<OutputField> getOutputFields() {
        return outputFormatFragment.getOutputFields();
    }

    public static class SinkCommonConfigDef extends CommonConfigDef {

        public SinkCommonConfigDef(final OutputFieldType defaultFieldType, final CompressionType compressionType) {
            super();
            OutputFormatFragment.update(this, defaultFieldType);
            FileNameFragment.update(this, compressionType);
            // not supported at this time.
            configKeys().remove(FileNameFragment.FILE_PATH_PREFIX_TEMPLATE_CONFIG);
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
