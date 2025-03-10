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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import io.aiven.kafka.connect.common.config.SinkCommonConfig;
import io.aiven.kafka.connect.config.s3.S3Config;
import io.aiven.kafka.connect.iam.AwsStsRole;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.config.s3.S3CommonConfig;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

@SuppressWarnings({ "PMD.TooManyMethods", "PMD.GodClass", "PMD.ExcessiveImports", "PMD.TooManyStaticImports" })
final public class S3SinkConfig extends SinkCommonConfig implements S3Config {

    public static final Logger LOGGER = LoggerFactory.getLogger(S3SinkConfig.class);

    private final S3ConfigFragment configFragment;

    public S3SinkConfig(final Map<String, String> properties) {
        super(new S3SinkConfigDef(), preprocessProperties(properties));
        configFragment = new S3ConfigFragment(this);
    }

    static Map<String, String> preprocessProperties(final Map<String, String> properties) {
        // Add other preprocessings when needed here. Mind the order.
        return S3CommonConfig.handleDeprecatedYyyyUppercase(properties);
    }

    @Override
    public CompressionType getCompressionType() {
        // we have priority of properties if old one not set or both old and new one set
        // the new property value will be selected
        // default value is GZIP
        if (compressionFragment.has(FILE_COMPRESSION_TYPE_CONFIG)) {
            return compressionFragment.getCompressionType();
        }
        if (configFragment.has(S3ConfigFragment.OUTPUT_COMPRESSION)) {
            return CompressionType.forName(getString(configFragment.OUTPUT_COMPRESSION));
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

    public Boolean usesFileNameTemplate() {
        return Objects.isNull(getString(S3ConfigFragment.AWS_S3_PREFIX_CONFIG))
                && Objects.isNull(getString(S3ConfigFragment.AWS_S3_PREFIX));
    }

    @Override
    public long getS3RetryBackoffDelayMs() {
        return configFragment.getS3RetryBackoffDelayMs();
    }

    @Override
    public long getS3RetryBackoffMaxDelayMs() {
        return configFragment.getS3RetryBackoffMaxDelayMs();
    }

    @Override
    public int getS3RetryBackoffMaxRetries() {
        return configFragment.getS3RetryBackoffMaxRetries();
    }

    @Override
    public String getAwsS3EndPoint() {
        return configFragment.getAwsS3EndPoint();
    }

    @Override
    public software.amazon.awssdk.regions.Region getAwsS3RegionV2() {
        return configFragment.getAwsS3RegionV2();
    }

    @Override
    public boolean hasAwsStsRole() {
        return configFragment.hasAwsStsRole();
    }

    @Override
    public AwsBasicCredentials getAwsCredentialsV2() {
        return configFragment.getAwsCredentialsV2();
    }

    @Override
    public AwsCredentialsProvider getCustomCredentialsProviderV2() {
       return configFragment.getCustomCredentialsProviderV2();
    }

    @Override
    public AwsStsRole getStsRole() {
        return configFragment.getStsRole();
    }

    @Override
    public String getAwsS3Prefix() {
        return configFragment.getAwsS3Prefix();
    }

    @Override
    public String getAwsS3BucketName() {
        return configFragment.getAwsS3BucketName();
    }

    public int getAwsS3PartSize() {
        return configFragment.getAwsS3PartSize();
    }

    public String getServerSideEncryptionAlgorithmName() {
        return configFragment.getServerSideEncryptionAlgorithmName();
    }
}
