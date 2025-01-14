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
import java.time.ZoneOffset;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.config.validators.FileCompressionTypeValidator;
import io.aiven.kafka.connect.common.config.validators.FilenameTemplateValidator;
import io.aiven.kafka.connect.common.config.validators.TimeZoneValidator;
import io.aiven.kafka.connect.common.config.validators.TimestampSourceValidator;
import io.aiven.kafka.connect.common.grouper.RecordGrouperFactory;
import io.aiven.kafka.connect.common.templating.Template;

/**
 * Fragment to handle all file name extraction operations. Requires {@link OutputFormatFragment} and (@link
 * CompressionFragment}
 */
public final class FileNameFragment extends ConfigFragment {

    // package private so that testing can access.
    static final String GROUP_FILE = "File";
    static final String FILE_COMPRESSION_TYPE_CONFIG = "file.compression.type";
    static final String FILE_MAX_RECORDS = "file.max.records";
    static final String FILE_NAME_TIMESTAMP_TIMEZONE = "file.name.timestamp.timezone";
    static final String FILE_NAME_TIMESTAMP_SOURCE = "file.name.timestamp.source";
    public static final String FILE_NAME_TEMPLATE_CONFIG = "file.name.template";
    static final String DEFAULT_FILENAME_TEMPLATE = "{{topic}}-{{partition}}-{{start_offset}}";

    public static final String FILE_PATH_PREFIX_TEMPLATE_CONFIG = "file.prefix.template";
    static final String DEFAULT_FILE_PATH_PREFIX_TEMPLATE = "topics/{{topic}}/partition={{partition}}/";

    public FileNameFragment(final AbstractConfig cfg) {
        super(cfg);
    }

    /**
     * Adds the FileName properties to the configuration definition.
     *
     * @param configDef
     *            the configuration definition to update.
     * @return the updated configuration definition.
     */
    public static ConfigDef update(final ConfigDef configDef) {
        int fileGroupCounter = 0;

        configDef.define(FILE_NAME_TEMPLATE_CONFIG, ConfigDef.Type.STRING, null,
                new FilenameTemplateValidator(FILE_NAME_TEMPLATE_CONFIG), ConfigDef.Importance.MEDIUM,
                "The template for file names on S3. "
                        + "Supports `{{ variable }}` placeholders for substituting variables. "
                        + "Currently supported variables are `topic`, `partition`, and `start_offset` "
                        + "(the offset of the first record in the file). "
                        + "Only some combinations of variables are valid, which currently are:\n"
                        + "- `topic`, `partition`, `start_offset`."
                        + "There is also `key` only variable {{key}} for grouping by keys",
                GROUP_FILE, fileGroupCounter++, // NOPMD UnusedAssignment
                ConfigDef.Width.LONG, FILE_NAME_TEMPLATE_CONFIG);

        final String supportedCompressionTypes = CompressionType.names()
                .stream()
                .map(f -> "'" + f + "'")
                .collect(Collectors.joining(", "));

        configDef.define(FILE_COMPRESSION_TYPE_CONFIG, ConfigDef.Type.STRING, null, new FileCompressionTypeValidator(),
                ConfigDef.Importance.MEDIUM,
                "The compression type used for files put on S3. " + "The supported values are: "
                        + supportedCompressionTypes + ".",
                GROUP_FILE, fileGroupCounter++, // NOPMD UnusedAssignment
                ConfigDef.Width.NONE, FILE_COMPRESSION_TYPE_CONFIG,
                FixedSetRecommender.ofSupportedValues(CompressionType.names()));

        configDef.define(FILE_MAX_RECORDS, ConfigDef.Type.INT, 0, new ConfigDef.Validator() {
            @Override
            public void ensureValid(final String name, final Object value) {
                assert value instanceof Integer;
                if ((Integer) value < 0) {
                    throw new ConfigException(FILE_MAX_RECORDS, value, "must be a non-negative integer number");
                }
            }
        }, ConfigDef.Importance.MEDIUM,
                "The maximum number of records to put in a single file. " + "Must be a non-negative integer number. "
                        + "0 is interpreted as \"unlimited\", which is the default.",
                GROUP_FILE, fileGroupCounter++, // NOPMD UnusedAssignment
                ConfigDef.Width.SHORT, FILE_MAX_RECORDS);

        configDef.define(FILE_NAME_TIMESTAMP_TIMEZONE, ConfigDef.Type.STRING, ZoneOffset.UTC.toString(),
                new TimeZoneValidator(), ConfigDef.Importance.LOW,
                "Specifies the timezone in which the dates and time for the timestamp variable will be treated. "
                        + "Use standard shot and long names. Default is UTC",
                GROUP_FILE, fileGroupCounter++, // NOPMD UnusedAssignment
                ConfigDef.Width.SHORT, FILE_NAME_TIMESTAMP_TIMEZONE);

        configDef.define(FILE_NAME_TIMESTAMP_SOURCE, ConfigDef.Type.STRING, TimestampSource.Type.WALLCLOCK.name(),
                new TimestampSourceValidator(), ConfigDef.Importance.LOW,
                "Specifies the the timestamp variable source. Default is wall-clock.", GROUP_FILE, fileGroupCounter++, // NOPMD
                ConfigDef.Width.SHORT, FILE_NAME_TIMESTAMP_SOURCE);

        configDef.define(FILE_PATH_PREFIX_TEMPLATE_CONFIG, ConfigDef.Type.STRING, DEFAULT_FILE_PATH_PREFIX_TEMPLATE,
                new ConfigDef.NonEmptyString(), ConfigDef.Importance.MEDIUM,
                "The template for file prefix on S3. "
                        + "Supports `{{ variable }}` placeholders for substituting variables. "
                        + "Currently supported variables are `topic` and `partition` "
                        + "and are mandatory to have these in the directory structure."
                        + "Example prefix : topics/{{topic}}/partition/{{partition}}/",
                GROUP_FILE, fileGroupCounter++, // NOPMD UnusedAssignment
                ConfigDef.Width.LONG, FILE_PATH_PREFIX_TEMPLATE_CONFIG);

        return configDef;
    }

    @Override
    public void validate() {
        // Special checks for {{key}} filename template.
        final Template filenameTemplate = getFilenameTemplate();
        final String groupType = RecordGrouperFactory.resolveRecordGrouperType(filenameTemplate);
        if (isKeyBased(groupType) && getMaxRecordsPerFile() > 1) {
            final String msg = String.format("When %s is %s, %s must be either 1 or not set", FILE_NAME_TEMPLATE_CONFIG,
                    filenameTemplate, FILE_MAX_RECORDS);
            throw new ConfigException(msg);
        }
    }

    private Boolean isKeyBased(final String groupType) {
        return RecordGrouperFactory.KEY_RECORD.equals(groupType)
                || RecordGrouperFactory.KEY_TOPIC_PARTITION_RECORD.equals(groupType);
    }

    /**
     * Returns the text of the filename template. May throw {@link ConfigException} if the property
     * {@link #FILE_NAME_TEMPLATE_CONFIG} is not set.
     *
     * @return the text of the filename template.
     */
    public String getFilename() {
        if (has(FILE_NAME_TEMPLATE_CONFIG)) {
            return cfg.getString(FILE_NAME_TEMPLATE_CONFIG);
        }
        final CompressionType compressionType = new CompressionFragment(cfg).getCompressionType();
        return FormatType.AVRO.equals(new OutputFormatFragment(cfg).getFormatType())
                ? DEFAULT_FILENAME_TEMPLATE + ".avro" + compressionType.extension()
                : DEFAULT_FILENAME_TEMPLATE + compressionType.extension();
    }

    /**
     * Gets the filename template.
     *
     * @return the Filename template.
     */
    public Template getFilenameTemplate() {
        return Template.of(getFilename());
    }

    /**
     * Gets the filename timezone
     *
     * @return The timezone specified for filenames.
     */
    public ZoneId getFilenameTimezone() {
        return ZoneId.of(cfg.getString(FILE_NAME_TIMESTAMP_TIMEZONE));
    }

    /**
     * Gets the timestamp source for the file name.
     *
     * @return the timestamp source for the file name.
     */
    public TimestampSource getFilenameTimestampSource() {
        return TimestampSource.of(getFilenameTimezone(),
                TimestampSource.Type.of(cfg.getString(FILE_NAME_TIMESTAMP_SOURCE)));
    }

    /**
     * Gets the maximum number of records allowed in a file.
     *
     * @return the maximum number of records allowed in a file.
     */
    public int getMaxRecordsPerFile() {
        return cfg.getInt(FILE_MAX_RECORDS);
    }

    public String getFilePathPrefixTemplateConfig() {
        return cfg.getString(FILE_PATH_PREFIX_TEMPLATE_CONFIG);
    }

}
