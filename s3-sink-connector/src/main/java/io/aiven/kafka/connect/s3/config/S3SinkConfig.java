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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.SinkCommonConfig;
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;
import io.aiven.kafka.connect.iam.AwsStsRole;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({ "PMD.TooManyMethods", "PMD.GodClass", "PMD.ExcessiveImports", "PMD.TooManyStaticImports" })
public final class S3SinkConfig extends SinkCommonConfig {

    public static final Logger LOGGER = LoggerFactory.getLogger(S3SinkConfig.class);

    private final S3ConfigFragment s3ConfigFragment;

    public S3SinkConfig(final Map<String, String> properties) {
        super(new S3SinkConfigDef(), preprocessProperties(properties));
        s3ConfigFragment = new S3ConfigFragment(dataAccess);
    }

    public S3ConfigFragment getS3ConfigFragment() {
        return s3ConfigFragment;
    }

    static Map<String, String> preprocessProperties(final Map<String, String> properties) {
        return S3ConfigFragment.handleDeprecatedOptions(properties, S3SinkConfigDef.DEFAULT_COMPRESSION);
    }

    public long getS3RetryBackoffDelayMs() {
        return s3ConfigFragment.getS3RetryBackoffDelayMs();
    }

    public long getS3RetryBackoffMaxDelayMs() {
        return s3ConfigFragment.getS3RetryBackoffMaxDelayMs();
    }

    public int getS3RetryBackoffMaxRetries() {
        return s3ConfigFragment.getS3RetryBackoffMaxRetries();
    }

    @Deprecated
    public Region getAwsS3Region() {
        return s3ConfigFragment.getAwsS3Region();
    }

    public String getAwsS3BucketName() {
        return s3ConfigFragment.getAwsS3BucketName();
    }

    public int getAwsS3PartSize() {
        return s3ConfigFragment.getAwsS3PartSize();
    }

    public String getServerSideEncryptionAlgorithmName() {
        return s3ConfigFragment.getServerSideEncryptionAlgorithmName();
    }

    public String getAwsS3EndPoint() {
        return s3ConfigFragment.getAwsS3EndPoint();
    }

    @Deprecated
    public BasicAWSCredentials getAwsCredentials() {
        return s3ConfigFragment.getAwsCredentials();
    }

    @Deprecated
    public AWSCredentialsProvider getCustomCredentialsProvider() {
        return s3ConfigFragment.getCustomCredentialsProvider();
    }

    public AwsStsRole getStsRole() {
        return s3ConfigFragment.getStsRole();
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
        final var template = Template.of(getPrefix());
        template.instance().bindVariable("utc_date", () -> {
            LOGGER.info("utc_date variable is deprecated please read documentation for the new name");
            return "";
        }).bindVariable("local_date", () -> {
            LOGGER.info("local_date variable is deprecated please read documentation for the new name");
            return "";
        }).render();
        return template;
    }

    /**
     * Uses file name template if the prefix is not set.
     *
     * @return true if the file name prefix was not set.
     */
    public Boolean usesFileNameTemplate() {
        return !fileNameFragment.hasPrefix();
    }

}
