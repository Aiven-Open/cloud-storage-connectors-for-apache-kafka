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

import io.aiven.kafka.connect.common.templating.Template;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class SinkCommonConfig extends CommonConfig {

    public static final String FORMAT_OUTPUT_FIELDS_CONFIG = "format.output.fields";
    public static final String FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG = "format.output.fields.value.encoding";
    public static final String FORMAT_OUTPUT_TYPE_CONFIG = "format.output.type";
    public static final String FORMAT_OUTPUT_ENVELOPE_CONFIG = "format.output.envelope";
    public static final String FILE_COMPRESSION_TYPE_CONFIG = "file.compression.type";
    public static final String FILE_NAME_TEMPLATE_CONFIG = "file.name.template";
    public static final String FILE_MAX_RECORDS = "file.max.records";

    public static final String FILE_MAX_BYTES = "file.max.bytes";
    /**
     * FileNameFragment to handle FileName based configuration queries.
     */
    protected final FileNameFragment fileNameFragment;
    /**
     * OutputFormatFragment to handle Output format base configuration queries.
     */
    protected final OutputFormatFragment outputFormatFragment;

    protected final SinkConfigFragment sinkConfigFragment;

    @SuppressFBWarnings("CT_CONSTRUCTOR_THROW")
    public SinkCommonConfig(ConfigDef definition, Map<?, ?> originals) { // NOPMD
        super(definition, originals);
        // Construct FileNameFragment
        fileNameFragment = new FileNameFragment(this);
        outputFormatFragment = new OutputFormatFragment(this);
        sinkConfigFragment = new SinkConfigFragment(this);
        // TODO: calls getOutputFields, can be overridden in subclasses.
        validate(); // NOPMD ConstructorCallsOverridableMethod
    }

    private void validate() {
        outputFormatFragment.validate();
        fileNameFragment.validateRecordGrouper();
    }

    protected static void addOutputFieldsFormatConfigGroup(final ConfigDef configDef,
            final OutputFieldType defaultFieldType) {
        OutputFormatFragment.update(configDef, defaultFieldType);
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
        return getInt(FILE_MAX_RECORDS);
    }

    public long getMaxBytesPerFile() {
        return sinkConfigFragment.getMaxBytesPerFile();
    }

    public boolean isMaxBytesPerFileLimited() {
        return getMaxBytesPerFile() > 0;
    }

    public List<OutputField> getOutputFields() {
        return outputFormatFragment.getOutputFields();
    }

}
