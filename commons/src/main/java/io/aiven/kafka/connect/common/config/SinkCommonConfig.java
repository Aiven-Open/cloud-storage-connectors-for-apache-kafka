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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.config.validators.FileCompressionTypeValidator;
import io.aiven.kafka.connect.common.config.validators.OutputFieldsEncodingValidator;
import io.aiven.kafka.connect.common.config.validators.OutputFieldsValidator;
import io.aiven.kafka.connect.common.config.validators.OutputTypeValidator;
import io.aiven.kafka.connect.common.grouper.RecordGrouperFactory;
import io.aiven.kafka.connect.common.templating.Template;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public abstract class SinkCommonConfig extends CommonConfig {

    public static final String FILE_COMPRESSION_TYPE_CONFIG = "file.compression.type";
    public static final String FILE_MAX_RECORDS = "file.max.records";
    public static final String FILE_NAME_TEMPLATE_CONFIG = "file.name.template";

    @SuppressFBWarnings("CT_CONSTRUCTOR_THROW")
    public SinkCommonConfig(ConfigDef definition, Map<?, ?> originals) { // NOPMD
        super(definition, originals);
        // TODO: calls getOutputFields, can be overridden in subclasses.
        validate(); // NOPMD ConstructorCallsOverridableMethod
    }

    private void validate() {
        new OutputFormatFragment(this).validate();
        new FileNameFragment(this).validate();
    }

    /**
     * @deprecated use {@link OutputFormatFragment#update(ConfigDef, OutputFieldType)}
     */
    @Deprecated
    protected static void addOutputFieldsFormatConfigGroup(final ConfigDef configDef,
            final OutputFieldType defaultFieldType) {
        OutputFormatFragment.update(configDef, defaultFieldType);
    }


    /**
     * @deprecated use {@link OutputFormatFragment#getFormatType()}
     */
    @Deprecated
    public FormatType getFormatType() {
        return new OutputFormatFragment(this).getFormatType();
    }


    /**
     * @deprecated use {@link CompressionFragment#update(ConfigDef, CompressionType)}
     */
    @Deprecated
    protected static void addCompressionTypeConfig(final ConfigDef configDef,
            final CompressionType defaultCompressionType) {
        CompressionFragment.update(configDef, defaultCompressionType);
    }

    /**
     * @deprecated use {@link CompressionFragment#getCompressionType()}
     */
    @Deprecated
    public CompressionType getCompressionType() {
        return new CompressionFragment(this).getCompressionType();
    }

    /**
     * @deprecated use {@link OutputFormatFragment#envelopeEnabled()}
     */
    @Deprecated
    public Boolean envelopeEnabled() {
        return new OutputFormatFragment(this).envelopeEnabled();
    }

    /**
     * @deprecated use {@link OutputFormatFragment#getOutputFieldEncodingType()}
     */
    @Deprecated
    public OutputFieldEncodingType getOutputFieldEncodingType() {
        return new OutputFormatFragment(this).getOutputFieldEncodingType();
    }

    /**
     * @deprecated use {@link FileNameFragment#getFilenameTemplate()}
     */
    @Deprecated
    public final Template getFilenameTemplate() {
        return new FileNameFragment(this).getFilenameTemplate();
    }

    /**
     * @deprecated use {@link FileNameFragment#getFilename()}
     */
    @Deprecated
    public final String getFilename() {
        return new FileNameFragment(this).getFilename();
    }


    /**
     * @deprecated use {@link FileNameFragment#getFilename()}
     */
    @Deprecated
    public final ZoneId getFilenameTimezone() {
        return new FileNameFragment(this).getFilenameTimezone();
    }

    /**
     * @deprecated use {@link FileNameFragment#getFilenameTimestampSource()}
     */
    @Deprecated
    public final TimestampSource getFilenameTimestampSource() {
        return new FileNameFragment(this).getFilenameTimestampSource();
    }

    public final int getMaxRecordsPerFile() {
        return getInt(FILE_MAX_RECORDS);
    }

    private Boolean isKeyBased(final String groupType) {
        return RecordGrouperFactory.KEY_RECORD.equals(groupType)
                || RecordGrouperFactory.KEY_TOPIC_PARTITION_RECORD.equals(groupType);
    }

    /**
     * @deprecated use {@link OutputFormatFragment#getOutputFields()}
     */
    @Deprecated
    public List<OutputField> getOutputFields() {
        return new OutputFormatFragment(this).getOutputFields();
    }

}
