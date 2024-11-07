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

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.config.validators.FileCompressionTypeValidator;
import io.aiven.kafka.connect.common.config.validators.OutputFieldsEncodingValidator;
import io.aiven.kafka.connect.common.config.validators.OutputFieldsValidator;
import io.aiven.kafka.connect.common.config.validators.OutputTypeValidator;
import io.aiven.kafka.connect.common.grouper.RecordGrouperFactory;
import io.aiven.kafka.connect.common.templating.Template;

public class AivenCommonConfig extends AbstractConfig {
    public static final String FORMAT_OUTPUT_FIELDS_CONFIG = "format.output.fields";
    public static final String FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG = "format.output.fields.value.encoding";
    public static final String FORMAT_OUTPUT_TYPE_CONFIG = "format.output.type";
    public static final String FORMAT_OUTPUT_ENVELOPE_CONFIG = "format.output.envelope";
    public static final String FILE_COMPRESSION_TYPE_CONFIG = "file.compression.type";
    public static final String FILE_MAX_RECORDS = "file.max.records";
    public static final String FILE_NAME_TIMESTAMP_TIMEZONE = "file.name.timestamp.timezone";
    public static final String FILE_NAME_TIMESTAMP_SOURCE = "file.name.timestamp.source";
    public static final String FILE_NAME_TEMPLATE_CONFIG = "file.name.template";

    private static final String GROUP_COMPRESSION = "File Compression";
    private static final String GROUP_FORMAT = "Format";
    private static final String DEFAULT_FILENAME_TEMPLATE = "{{topic}}-{{partition}}-{{start_offset}}";

    private static final String GROUP_RETRY_BACKOFF_POLICY = "Retry backoff policy";
    public static final String KAFKA_RETRY_BACKOFF_MS_CONFIG = "kafka.retry.backoff.ms";

    private static final String GROUP_OVERLOAD = "Overload Control";
    private static final String OVERLOAD_MAX_RECORDS_HARD_LIMIT = "overload.hard.record.limit";

    protected AivenCommonConfig(final ConfigDef definition, final Map<?, ?> originals) {
        super(definition, originals);
        // TODO: calls getOutputFields, can be overridden in subclasses.
        validate(); // NOPMD ConstructorCallsOverridableMethod
    }

    private void validate() {
        // Special checks for output json envelope config.
        final List<OutputField> outputFields = getOutputFields();
        final Boolean outputEnvelopConfig = envelopeEnabled();
        if (!outputEnvelopConfig && outputFields.toArray().length != 1) {
            final String msg = String.format("When %s is %s, %s must contain only one field",
                    FORMAT_OUTPUT_ENVELOPE_CONFIG, false, FORMAT_OUTPUT_FIELDS_CONFIG);
            throw new ConfigException(msg);
        }
        validateKeyFilenameTemplate();
    }

    protected static void addKafkaBackoffPolicy(final ConfigDef configDef) {
        configDef.define(KAFKA_RETRY_BACKOFF_MS_CONFIG, ConfigDef.Type.LONG, null, new ConfigDef.Validator() {

                    final long maximumBackoffPolicy = TimeUnit.HOURS.toMillis(24);

                    @Override
                    public void ensureValid(final String name, final Object value) {
                        if (Objects.isNull(value)) {
                            return;
                        }
                        assert value instanceof Long;
                        final var longValue = (Long) value;
                        if (longValue < 0) {
                            throw new ConfigException(name, value, "Value must be at least 0");
                        } else if (longValue > maximumBackoffPolicy) {
                            throw new ConfigException(name, value,
                                    "Value must be no more than " + maximumBackoffPolicy + " (24 hours)");
                        }
                    }
                }, ConfigDef.Importance.MEDIUM,
                "The retry backoff in milliseconds. "
                        + "This config is used to notify Kafka Connect to retry delivering a message batch or "
                        + "performing recovery in case of transient exceptions. Maximum value is "
                        + TimeUnit.HOURS.toMillis(24) + " (24 hours).",
                GROUP_RETRY_BACKOFF_POLICY, 1, ConfigDef.Width.NONE, KAFKA_RETRY_BACKOFF_MS_CONFIG);
    }

    public Long getKafkaRetryBackoffMs() {
        return getLong(KAFKA_RETRY_BACKOFF_MS_CONFIG);
    }

    protected static void addOutputFieldsFormatConfigGroup(final ConfigDef configDef,
                                                           final OutputFieldType defaultFieldType) {
        int formatGroupCounter = 0;

        addFormatTypeConfig(configDef, formatGroupCounter);

        configDef.define(FORMAT_OUTPUT_FIELDS_CONFIG, ConfigDef.Type.LIST,
                Objects.isNull(defaultFieldType) ? null : defaultFieldType.name, // NOPMD NullAssignment
                new OutputFieldsValidator(), ConfigDef.Importance.MEDIUM,
                "Fields to put into output files. " + "The supported values are: " + OutputField.SUPPORTED_OUTPUT_FIELDS
                        + ".",
                GROUP_FORMAT, formatGroupCounter++, ConfigDef.Width.NONE, FORMAT_OUTPUT_FIELDS_CONFIG,
                FixedSetRecommender.ofSupportedValues(OutputFieldType.names()));

        configDef.define(FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG, ConfigDef.Type.STRING,
                OutputFieldEncodingType.BASE64.name, new OutputFieldsEncodingValidator(), ConfigDef.Importance.MEDIUM,
                "The type of encoding for the value field. " + "The supported values are: "
                        + OutputFieldEncodingType.SUPPORTED_FIELD_ENCODING_TYPES + ".",
                GROUP_FORMAT, formatGroupCounter++, ConfigDef.Width.NONE, FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG,
                FixedSetRecommender.ofSupportedValues(OutputFieldEncodingType.names()));

        configDef.define(FORMAT_OUTPUT_ENVELOPE_CONFIG, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                "Whether to enable envelope for entries with single field.", GROUP_FORMAT, formatGroupCounter,
                ConfigDef.Width.SHORT, FORMAT_OUTPUT_ENVELOPE_CONFIG);
    }

    protected static void addFormatTypeConfig(final ConfigDef configDef, final int formatGroupCounter) {
        final String supportedFormatTypes = FormatType.names()
                .stream()
                .map(f -> "'" + f + "'")
                .collect(Collectors.joining(", "));
        configDef.define(FORMAT_OUTPUT_TYPE_CONFIG, ConfigDef.Type.STRING, FormatType.CSV.name,
                new OutputTypeValidator(), ConfigDef.Importance.MEDIUM,
                "The format type of output content" + "The supported values are: " + supportedFormatTypes + ".",
                GROUP_FORMAT, formatGroupCounter, ConfigDef.Width.NONE, FORMAT_OUTPUT_TYPE_CONFIG,
                FixedSetRecommender.ofSupportedValues(FormatType.names()));
    }

    public FormatType getFormatType() {
        return FormatType.forName(getString(FORMAT_OUTPUT_TYPE_CONFIG));
    }

    protected static void addCompressionTypeConfig(final ConfigDef configDef,
                                                   final CompressionType defaultCompressionType) {
        configDef.define(FILE_COMPRESSION_TYPE_CONFIG, ConfigDef.Type.STRING,
                Objects.isNull(defaultCompressionType) ? null : defaultCompressionType.name, // NOPMD NullAssignment
                new FileCompressionTypeValidator(), ConfigDef.Importance.MEDIUM,
                "The compression type used for files put on GCS. " + "The supported values are: "
                        + CompressionType.SUPPORTED_COMPRESSION_TYPES + ".",
                GROUP_COMPRESSION, 1, ConfigDef.Width.NONE, FILE_COMPRESSION_TYPE_CONFIG,
                FixedSetRecommender.ofSupportedValues(CompressionType.names()));

    }

    public CompressionType getCompressionType() {
        return CompressionType.forName(getString(FILE_COMPRESSION_TYPE_CONFIG));
    }

    public Boolean envelopeEnabled() {
        return getBoolean(FORMAT_OUTPUT_ENVELOPE_CONFIG);
    }

    public OutputFieldEncodingType getOutputFieldEncodingType() {
        return OutputFieldEncodingType.forName(getString(FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG));
    }

    public final Template getFilenameTemplate() {
        return Template.of(getFilename());
    }

    protected final void validateKeyFilenameTemplate() {
        // Special checks for {{key}} filename template.
        final Template filenameTemplate = getFilenameTemplate();
        final String groupType = RecordGrouperFactory.resolveRecordGrouperType(filenameTemplate);
        if (isKeyBased(groupType) && getMaxRecordsPerFile() > 1) {
            final String msg = String.format("When %s is %s, %s must be either 1 or not set", FILE_NAME_TEMPLATE_CONFIG,
                    filenameTemplate, FILE_MAX_RECORDS);
            throw new ConfigException(msg);
        }
    }

    public final String getFilename() {
        return resolveFilenameTemplate();
    }

    private String resolveFilenameTemplate() {
        String fileNameTemplate = getString(FILE_NAME_TEMPLATE_CONFIG);
        if (fileNameTemplate == null) {
            fileNameTemplate = FormatType.AVRO.equals(getFormatType())
                    ? DEFAULT_FILENAME_TEMPLATE + ".avro" + getCompressionType().extension()
                    : DEFAULT_FILENAME_TEMPLATE + getCompressionType().extension();
        }
        return fileNameTemplate;
    }

    public final ZoneId getFilenameTimezone() {
        return ZoneId.of(getString(FILE_NAME_TIMESTAMP_TIMEZONE));
    }

    public final TimestampSource getFilenameTimestampSource() {
        return TimestampSource.of(getFilenameTimezone(),
                TimestampSource.Type.of(getString(FILE_NAME_TIMESTAMP_SOURCE)));
    }

    public final int getMaxRecordsPerFile() {
        return getInt(FILE_MAX_RECORDS);
    }

    public List<OutputField> getOutputFields() {
        final List<OutputField> result = new ArrayList<>();
        for (final String outputFieldTypeStr : getList(FORMAT_OUTPUT_FIELDS_CONFIG)) {
            final OutputFieldType fieldType = OutputFieldType.forName(outputFieldTypeStr);
            final OutputFieldEncodingType encodingType;
            if (fieldType == OutputFieldType.VALUE || fieldType == OutputFieldType.KEY) {
                encodingType = getOutputFieldEncodingType();
            } else {
                encodingType = OutputFieldEncodingType.NONE;
            }
            result.add(new OutputField(fieldType, encodingType));
        }
        return result;
    }

    private Boolean isKeyBased(final String groupType) {
        return RecordGrouperFactory.KEY_RECORD.equals(groupType)
                || RecordGrouperFactory.KEY_TOPIC_PARTITION_RECORD.equals(groupType);
    }

    public long getBackPressureHardLimit() {
        return getLong(OVERLOAD_MAX_RECORDS_HARD_LIMIT);
    }

    protected static void addOverloadConfigGroup(final ConfigDef configDef) {
        int groupCounter = 0;

        configDef.define(OVERLOAD_MAX_RECORDS_HARD_LIMIT, ConfigDef.Type.LONG, 1000000L, ConfigDef.Importance.MEDIUM,
                "The maximum number of records to buffer before requesting a flush.", GROUP_OVERLOAD, groupCounter++,
                ConfigDef.Width.NONE, OVERLOAD_MAX_RECORDS_HARD_LIMIT);
    }
}
