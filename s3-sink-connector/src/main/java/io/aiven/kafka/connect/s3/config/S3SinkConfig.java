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

package io.aiven.kafka.connect.s3.config;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.config.s3.S3CommonConfig;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;
import io.aiven.kafka.connect.config.s3.S3SinkBaseConfig;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({ "PMD.TooManyMethods", "PMD.GodClass", "PMD.ExcessiveImports", "PMD.TooManyStaticImports" })
final public class S3SinkConfig extends S3SinkBaseConfig {

    public static final Logger LOGGER = LoggerFactory.getLogger(S3SinkConfig.class);


    // Default values from AWS SDK, since they are hidden
    public static final int AWS_S3_RETRY_BACKOFF_DELAY_MS_DEFAULT = 100;
    public static final int AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT = 20_000;
    // Comment in AWS SDK for max retries:
    // Maximum retry limit. Avoids integer overflow issues.
    //
    // NOTE: If the value is greater than 30, there can be integer overflow
    // issues during delay calculation.
    // in other words we can't use values greater than 30
    public static final int S3_RETRY_BACKOFF_MAX_RETRIES_DEFAULT = 3;

    public S3SinkConfig(final Map<String, String> properties) {
        super(new S3SinkConfigDef(), preprocessProperties(properties));
    }

    static Map<String, String> preprocessProperties(final Map<String, String> properties) {
        final Map<String, String> result = S3ConfigFragment.handleDeprecations(properties);
    }

    @Override
    public CompressionType getCompressionType() {
        // we have priority of properties if old one not set or both old and new one set
        // the new property value will be selected
        // default value is GZIP
        if (Objects.nonNull(getString(FileNameFragment.FILE_COMPRESSION_TYPE_CONFIG))) {
            return CompressionType.forName(getString(FileNameFragment.FILE_COMPRESSION_TYPE_CONFIG));
        }
        if (Objects.nonNull(getString(S3ConfigFragment.OUTPUT_COMPRESSION))) {
            return CompressionType.forName(getString(S3ConfigFragment.OUTPUT_COMPRESSION));
        }
        return CompressionType.GZIP;
    }

    /**
     * Gets the list of output fields. Will check {OutputFormatFragment#FORMAT_OUTPUT_FIELDS_CONFIG} and then
     * {OutputFormatFragment#OUTPUT_FIELDS}. If neither is set will create an output field of
     * {@link OutputFieldType#VALUE} and {@link OutputFieldEncodingType#BASE64}.
     *
     * @return The list of output fields. WIll not be {@code null}.
     */
    @Override
    public List<OutputField> getOutputFields() {
        if (outputFormatFragment.hasOutputFields()) {
            return super.getOutputFields();
        }

        if (outputFormatFragment.has(S3ConfigFragment.OUTPUT_FIELDS)) {
            return outputFormatFragment.getOutputFields(S3ConfigFragment.OUTPUT_FIELDS);
        }
        return List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.BASE64));
    }

    /**
     * Gets the list of output fields for the specified name Deprecated please use getOutputFields()
     *
     * @param format
     *            the name of the configuration key to check.
     * @return a list of output fields as defined in the configuration or {@code null} if not defined.
     */
    @Deprecated
    public List<OutputField> getOutputFields(final String format) {
        return getList(format).stream().map(fieldName -> {
            final var type = OutputFieldType.forName(fieldName);
            final var encoding = type == OutputFieldType.KEY || type == OutputFieldType.VALUE
                    ? getOutputFieldEncodingType()
                    : OutputFieldEncodingType.NONE;
            return new OutputField(type, encoding);
        }).collect(Collectors.toUnmodifiableList());
    }

    public Template getPrefixTemplate() {
        final var template = Template.of(getAwsS3Prefix());
        template.instance().bindVariable("utc_date", () -> {
            LOGGER.info("utc_date variable is deprecated please read documentation for the new name");
            return "";
        }).bindVariable("local_date", () -> {
            LOGGER.info("local_date variable is deprecated please read documentation for the new name");
            return "";
        }).render();
        return template;
    }

    public ZoneId getTimezone() {
        return ZoneId.of(getString(S3ConfigFragment.TIMESTAMP_TIMEZONE));
    }

    public TimestampSource getTimestampSource() {
        return TimestampSource.of(getTimezone(), TimestampSource.Type.of(getString(S3ConfigFragment.TIMESTAMP_SOURCE)));
    }

    /**
     * Deprecated please use S3ConfigFragment.AwsRegionValidator
     */
    @Deprecated
    protected static class AwsRegionValidator implements ConfigDef.Validator {
        private static final String SUPPORTED_AWS_REGIONS = Arrays.stream(Regions.values())
                .map(Regions::getName)
                .collect(Collectors.joining(", "));

        @Override
        public void ensureValid(final String name, final Object value) {
            if (Objects.nonNull(value)) {
                final String valueStr = (String) value;
                final Region region = RegionUtils.getRegion(valueStr);
                if (!RegionUtils.getRegions().contains(region)) {
                    throw new ConfigException(name, valueStr, "supported values are: " + SUPPORTED_AWS_REGIONS);
                }
            }
        }
    }

    public Boolean usesFileNameTemplate() {
        return Objects.isNull(getString(S3ConfigFragment.AWS_S3_PREFIX_CONFIG))
                && Objects.isNull(getString(S3ConfigFragment.AWS_S3_PREFIX));
    }

}
