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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Connector;

import io.aiven.kafka.connect.common.config.CommonConfigFragment;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.common.config.OutputFormatFragmentFixture.OutputFormatArgs;
import io.aiven.kafka.connect.common.config.StableTimeFormatter;
import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;

import com.amazonaws.regions.RegionUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

@SuppressWarnings("deprecation")
final class S3SinkConfigTest {

    private static final String EAST = "us-east-1";
    private static final String WEST = "us-west-1";

    static Map<String, String> defaultProperties() {
        final Map<String, String> properties = new HashMap<>();
        CommonConfigFragment.setter(properties).connector(Connector.class).name("test-connector");
        S3ConfigFragment.setter(properties).bucketName("the-bucket");
        return properties;
    }

    static Map<String, String> defaultProperties(final Map<String, String> overrides) {
        final Map<String, String> properties = defaultProperties();
        properties.putAll(overrides);
        return properties;
    }

    @Test
    void correctFullConfig() {
        final var props = defaultProperties();
        S3ConfigFragment.setter(props)
                .accessKeyId("AWS_ACCESS_KEY_ID")
                .accessKeySecret("AWS_SECRET_ACCESS_KEY")
                .endpoint("AWS_S3_ENDPOINT")
                .prefix("AWS_S3_PREFIX")
                .region(EAST);

        FileNameFragment.setter(props).fileCompression(CompressionType.GZIP);
        OutputFormatFragment.setter(props)
                .withOutputFields(OutputFieldType.values())
                .withOutputFieldEncodingType(OutputFieldEncodingType.NONE);

        final var conf = new S3SinkConfig(props);
        final var awsCredentials = conf.getAwsCredentials();

        assertThat(awsCredentials.getAWSAccessKeyId()).isEqualTo("AWS_ACCESS_KEY_ID");
        assertThat(awsCredentials.getAWSSecretKey()).isEqualTo("AWS_SECRET_ACCESS_KEY");
        assertThat(conf.getAwsS3BucketName()).isEqualTo("the-bucket");
        assertThat(conf.getPrefix()).isEqualTo("AWS_S3_PREFIX");
        assertThat(conf.getAwsS3EndPoint()).isEqualTo("AWS_S3_ENDPOINT");
        assertThat(conf.getAwsS3Region()).isEqualTo(RegionUtils.getRegion(EAST));
        assertThat(conf.getCompressionType()).isEqualTo(CompressionType.GZIP);
        assertThat(conf.getOutputFieldEncodingType()).isEqualTo(OutputFieldEncodingType.NONE);
        assertThat(conf.getOutputFields()).containsExactly(
                new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.HEADERS, OutputFieldEncodingType.NONE));
        assertThat(conf.getFormatType()).isEqualTo(FormatType.forName("csv"));
        assertThat(conf.getAwsS3PartSize()).isEqualTo(S3ConfigFragment.DEFAULT_PART_SIZE);
        assertThat(conf.getKafkaRetryBackoffMs()).isNull();
        assertThat(conf.getS3RetryBackoffDelayMs()).isEqualTo(S3ConfigFragment.AWS_S3_RETRY_BACKOFF_DELAY_MS_DEFAULT);
        assertThat(conf.getS3RetryBackoffMaxDelayMs())
                .isEqualTo(S3ConfigFragment.AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT);
        assertThat(conf.getS3RetryBackoffMaxRetries()).isEqualTo(S3ConfigFragment.S3_RETRY_BACKOFF_MAX_RETRIES_DEFAULT);
    }

    @Test
    void correctFullConfigForOldStyleConfigParameters() {
        final var props = defaultProperties();
        props.remove(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG);
        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID, "AWS_ACCESS_KEY_ID");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY, "AWS_SECRET_ACCESS_KEY");
        props.put(S3ConfigFragment.AWS_S3_BUCKET, "the-bucket");
        props.put(S3ConfigFragment.AWS_S3_ENDPOINT, "AWS_S3_ENDPOINT");
        props.put(S3ConfigFragment.AWS_S3_PREFIX, "AWS_S3_PREFIX");
        props.put(S3ConfigFragment.AWS_S3_REGION, EAST);
        props.put(S3ConfigFragment.OUTPUT_COMPRESSION, CompressionType.GZIP.name);
        props.put(S3ConfigFragment.OUTPUT_FIELDS,
                Arrays.stream(OutputFieldType.values())
                        .map(OutputFieldType::name)
                        .map(String::toLowerCase)
                        .collect(Collectors.joining(",")));

        final var conf = new S3SinkConfig(props);
        final var awsCredentials = conf.getAwsCredentials();

        assertThat(awsCredentials.getAWSAccessKeyId()).isEqualTo("AWS_ACCESS_KEY_ID");
        assertThat(awsCredentials.getAWSSecretKey()).isEqualTo("AWS_SECRET_ACCESS_KEY");
        assertThat(conf.getAwsS3BucketName()).isEqualTo("the-bucket");
        assertThat(conf.getPrefix()).isEqualTo("AWS_S3_PREFIX");
        assertThat(conf.getAwsS3EndPoint()).isEqualTo("AWS_S3_ENDPOINT");
        assertThat(conf.getAwsS3Region()).isEqualTo(RegionUtils.getRegion(EAST));
        assertThat(conf.getCompressionType()).isEqualTo(CompressionType.GZIP);
        assertThat(conf.getOutputFieldEncodingType()).isEqualTo(OutputFieldEncodingType.BASE64);
        assertThat(conf.getOutputFields()).containsExactlyInAnyOrderElementsOf(
                Arrays.asList(new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.BASE64),
                        new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.BASE64),
                        new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
                        new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE),
                        new OutputField(OutputFieldType.HEADERS, OutputFieldEncodingType.NONE)));
    }

    @Test
    void newConfigurationPropertiesHaveHigherPriorityOverOldOne() {
        final var props = defaultProperties();

        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG, "AWS_ACCESS_KEY_ID");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG, "AWS_SECRET_ACCESS_KEY");
        props.put(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG, "the-bucket");
        props.put(S3ConfigFragment.AWS_S3_ENDPOINT_CONFIG, "AWS_S3_ENDPOINT");
        props.put(S3ConfigFragment.AWS_S3_PREFIX_CONFIG, "AWS_S3_PREFIX");
        props.put(S3ConfigFragment.AWS_S3_REGION_CONFIG, EAST);

        FileNameFragment.setter(props).fileCompression(CompressionType.SNAPPY);
        OutputFormatFragment.setter(props)
                .withOutputFields(OutputFieldType.values())
                .withOutputFieldEncodingType(OutputFieldEncodingType.NONE);

        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID, "AWS_ACCESS_KEY_ID_#1");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY, "AWS_SECRET_ACCESS_KEY_#1");
        props.put(S3ConfigFragment.AWS_S3_BUCKET, "the-bucket1");
        props.put(S3ConfigFragment.AWS_S3_ENDPOINT, "AWS_S3_ENDPOINT_#1");
        props.put(S3ConfigFragment.AWS_S3_PREFIX, "AWS_S3_PREFIX_#1");
        props.put(S3ConfigFragment.AWS_S3_REGION, WEST);

        props.put(S3ConfigFragment.OUTPUT_COMPRESSION, CompressionType.ZSTD.name);
        props.put(S3ConfigFragment.OUTPUT_FIELDS, "key, value");

        final var conf = new S3SinkConfig(props);
        final var awsCredentials = conf.getAwsCredentials();

        assertThat(awsCredentials.getAWSAccessKeyId()).isEqualTo("AWS_ACCESS_KEY_ID");
        assertThat(awsCredentials.getAWSSecretKey()).isEqualTo("AWS_SECRET_ACCESS_KEY");
        assertThat(conf.getAwsS3BucketName()).isEqualTo("the-bucket");
        assertThat(conf.getPrefix()).isEqualTo("AWS_S3_PREFIX");
        assertThat(conf.getAwsS3EndPoint()).isEqualTo("AWS_S3_ENDPOINT");
        assertThat(conf.getAwsS3Region()).isEqualTo(RegionUtils.getRegion(EAST));
        assertThat(conf.getCompressionType()).isEqualTo(CompressionType.SNAPPY);
        assertThat(conf.getOutputFieldEncodingType()).isEqualTo(OutputFieldEncodingType.NONE);
        assertThat(conf.getOutputFields()).containsExactlyInAnyOrderElementsOf(
                Arrays.asList(new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
                        new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE),
                        new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
                        new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE),
                        new OutputField(OutputFieldType.HEADERS, OutputFieldEncodingType.NONE)));
    }

    @Test
    void wrongPartSize() {
        final Map<String, String> wrongPartSizeProps = defaultProperties();
        S3ConfigFragment.setter(wrongPartSizeProps)
                .accessKeyId("blah-blah-key-id")
                .accessKeySecret("bla-bla-access-key")
                .bucketName("bla-bucket-name")
                .partSize((long) Integer.MAX_VALUE + 1);

        assertThatThrownBy(() -> new S3SinkConfig(wrongPartSizeProps)).isInstanceOf(ConfigException.class)
                .hasMessage(
                        "Invalid value 2147483648 for configuration aws.s3.part.size.bytes: Value must be no more than 2.0 GiB (2147483647 B)");

        S3ConfigFragment.setter(wrongPartSizeProps).partSize(0);
        assertThatThrownBy(() -> new S3SinkConfig(wrongPartSizeProps)).isInstanceOf(ConfigException.class)
                .hasMessage(
                        "Invalid value 0 for configuration aws.s3.part.size.bytes: Value must be at least 1.0 MiB (1048576 B)");
    }

    @Test
    void emptyAwsS3Bucket() {
        final Map<String, String> props = defaultProperties();
        props.remove(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG);

        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_BUCKET, "");
        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage("Illegal bucket name: Bucket name should be between 3 and 63 characters long");

        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG, "");
        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage("Illegal bucket name: Bucket name should be between 3 and 63 characters long");
    }

    @Test
    void invalidAwsS3Bucket() {
        final Map<String, String> props = defaultProperties();
        props.remove(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG);

        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_BUCKET, "BUCKET-NAME");
        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage("Illegal bucket name: Bucket name should not contain uppercase characters");

        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG, "BUCKET-NAME");
        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage("Illegal bucket name: Bucket name should not contain uppercase characters");

        props.put(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG, "bucket_name");
        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage("Illegal bucket name: Bucket name should not contain '_'");
    }

    @Test
    void emptyAwsS3Region() {
        final Map<String, String> props = defaultProperties();
        props.remove(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG);

        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_BUCKET, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_REGION, "");
        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage(
                        "Invalid value  for configuration aws.s3.region: See documentation for list of valid regions.");

        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_REGION_CONFIG, "");
        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage(
                        "Invalid value  for configuration aws.s3.region: See documentation for list of valid regions.");
    }

    @Test
    void unknownAwsS3Region() {
        final Map<String, String> props = defaultProperties();
        props.remove(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG);

        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_BUCKET, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_REGION, "unknown");
        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage(
                        "Invalid value unknown for configuration aws.s3.region: See documentation for list of valid regions.");

        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_REGION_CONFIG, "");
        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage(
                        "Invalid value  for configuration aws.s3.region: See documentation for list of valid regions.");
    }

    @Test
    void validAwsS3Region() {
        final Map<String, String> props = defaultProperties();
        props.remove(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG);

        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_BUCKET, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_REGION, EAST);
        assertThat(new S3SinkConfig(props).getAwsS3Region()).isEqualTo(RegionUtils.getRegion(EAST));
    }

    @Test
    void emptyDeprecatedAwsS3Prefix() {
        final Map<String, String> props = defaultProperties();
        props.remove(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG);

        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_BUCKET, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_REGION, WEST);
        props.put(S3ConfigFragment.AWS_S3_PREFIX, "");
        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value  for configuration file.name.prefix: String must be non-empty");
    }
    @Test
    void emptyAwsS3Prefix() {
        final Map<String, String> props = defaultProperties();
        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_REGION_CONFIG, WEST);
        props.put(S3ConfigFragment.AWS_S3_PREFIX_CONFIG, "");
        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value  for configuration file.name.prefix: String must be non-empty");
    }

    @Test
    void emptyDeprecatedAwsS3EndPoint() {
        final Map<String, String> props = defaultProperties();
        props.remove(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG);

        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_BUCKET, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_REGION, EAST);
        props.put(S3ConfigFragment.AWS_S3_PREFIX, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_ENDPOINT, "");
        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value  for configuration aws.s3.endpoint: String must be non-empty");

    }

    @Test
    void emptyAwsS3EndPoint() {
        final Map<String, String> props = defaultProperties();
        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_REGION_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_PREFIX_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_ENDPOINT_CONFIG, "");
        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value  for configuration aws.s3.endpoint: String must be non-empty");
    }

    @Test
    void wrongDeprecactedAwsS3EndPoint() {
        final Map<String, String> props = defaultProperties();
        props.remove(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG);

        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_BUCKET, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_REGION, EAST);
        props.put(S3ConfigFragment.AWS_S3_PREFIX, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_ENDPOINT, "ffff://asdsadas");
        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value ffff://asdsadas for configuration aws.s3.endpoint: should be valid URL");
    }

    @Test
    void wrongAwsS3EndPoint() {
        final Map<String, String> props = defaultProperties();
        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_REGION_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_PREFIX_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_ENDPOINT_CONFIG, "ffff://asdsadas");
        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value ffff://asdsadas for configuration aws.s3.endpoint: should be valid URL");
    }

    @Test
    void emptyOutputField() {
        final Map<String, String> props = defaultProperties();
        props.remove(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG);

        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_REGION_CONFIG, WEST);
        props.put(S3ConfigFragment.AWS_S3_PREFIX_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.OUTPUT_FIELDS, "");

        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value [] for configuration format.output.fields: cannot be empty");

        props.remove(S3ConfigFragment.OUTPUT_FIELDS);
        props.put(OutputFormatArgs.FORMAT_OUTPUT_FIELDS_CONFIG.key(), "");

        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value [] for configuration format.output.fields: cannot be empty");
    }

    @Test
    void supportPriorityForOutputFields() {
        final Map<String, String> props = defaultProperties();
        props.remove(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG);

        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_REGION_CONFIG, WEST);
        props.put(S3ConfigFragment.AWS_S3_PREFIX_CONFIG, "blah-blah-blah");

        props.put(S3ConfigFragment.OUTPUT_FIELDS, "key,value,offset,timestamp");
        props.put(OutputFormatArgs.FORMAT_OUTPUT_FIELDS_CONFIG.key(), "key");

        final var conf = new S3SinkConfig(props);

        assertThat(conf.getOutputFields())
                .containsExactly(new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.BASE64));
    }

    @Test
    void unsupportedOutputField() {
        final Map<String, String> props = defaultProperties();
        props.remove(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG);

        props.put(S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3ConfigFragment.AWS_S3_REGION_CONFIG, WEST);
        props.put(S3ConfigFragment.OUTPUT_FIELDS, "key,value,offset,timestamp,unsupported");

        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value [key, value, offset, timestamp, unsupported] "
                        + "for configuration format.output.fields: "
                        + "supported values are (case insensitive): key, value, offset, timestamp, headers");

        props.remove(S3ConfigFragment.OUTPUT_FIELDS);
        props.put(OutputFormatArgs.FORMAT_OUTPUT_FIELDS_CONFIG.key(), "key,value,offset,timestamp,unsupported");

        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value [key, value, offset, timestamp, unsupported] "
                        + "for configuration format.output.fields: "
                        + "supported values are (case insensitive): key, value, offset, timestamp, headers");
    }

    @Test
    void customAwsS3BackoffPolicy() {
        final Map<String, String> props = defaultProperties();
        S3ConfigFragment.setter(props)
                .accessKeyId("blah-blah-blah")
                .accessKeySecret("blah-blah-blah")
                .retryBackoffDelay(Duration.ofMillis(2000))
                .retryBackoffMaxDelay(Duration.ofMillis(4000))
                .retryBackoffMaxRetries(10);
        final var config = new S3SinkConfig(props);

        assertThat(config.getS3RetryBackoffDelayMs()).isEqualTo(2000L);
        assertThat(config.getS3RetryBackoffMaxDelayMs()).isEqualTo(4000L);
        assertThat(config.getS3RetryBackoffMaxRetries()).isEqualTo(10);
    }

    @Test
    void wrongAwsS3BackoffPolicy() {
        final var wrongDelayProps = defaultProperties(Map.of(S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG,
                "blah-blah-blah", S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah",
                S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah",
                S3ConfigFragment.AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG, "0"));
        assertThatThrownBy(() -> new S3SinkConfig(wrongDelayProps)).isInstanceOf(ConfigException.class)
                .hasMessage(
                        "Invalid value 0 for configuration aws.s3.backoff.delay.ms: Value must be at least 1 Milliseconds");

        final var wrongMaxDelayProps = defaultProperties(Map.of(S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG,
                "blah-blah-blah", S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah",
                S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah",
                S3ConfigFragment.AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG, "0"));
        assertThatThrownBy(() -> new S3SinkConfig(wrongMaxDelayProps)).isInstanceOf(ConfigException.class)
                .hasMessage(
                        "Invalid value 0 for configuration aws.s3.backoff.max.delay.ms: Value must be at least 1 Milliseconds");

        final var wrongMaxRetriesProps = defaultProperties(Map.of(S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG,
                "blah-blah-blah", S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah",
                S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah",
                S3ConfigFragment.AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG, "0"));
        assertThatThrownBy(() -> new S3SinkConfig(wrongMaxRetriesProps)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value 0 for configuration aws.s3.backoff.max.retries: Value must be at least 1");

        final var tooBigMaxRetriesProps = defaultProperties(Map.of(S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG,
                "blah-blah-blah", S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah",
                S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah",
                S3ConfigFragment.AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG, "35"));
        assertThatThrownBy(() -> new S3SinkConfig(tooBigMaxRetriesProps)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value 35 for configuration aws.s3.backoff.max.retries: "
                        + "Value must be no more than 30");
    }

    @ParameterizedTest
    @NullSource
    @EnumSource(CompressionType.class)
    void supportedCompression(final CompressionType compression) {
        final Map<String, String> props = defaultProperties();
        S3ConfigFragment.setter(props)
                .accessKeyId("blah-blah-blah")
                .accessKeySecret("blah-blah-blah")
                .region(WEST)
                .prefix("blah-blah-blah");
        CompressionType expected = compression;
        if (compression != null) {
            props.put(S3ConfigFragment.OUTPUT_COMPRESSION, compression.name());
        } else {
            expected = S3SinkConfigDef.DEFAULT_COMPRESSION;
        }

        var config = new S3SinkConfig(props);

        assertThat(config.getCompressionType()).isEqualTo(expected);

        props.remove(S3ConfigFragment.OUTPUT_COMPRESSION);
        props.remove(FileNameFragment.FILE_COMPRESSION_TYPE_CONFIG);
        if (compression != null) {
            FileNameFragment.setter(props).fileCompression(compression);
        }

        config = new S3SinkConfig(props);
        assertThat(config.getCompressionType()).isEqualTo(expected);
    }

    @Test
    void supportPriorityForCompressionTypeConfig() {
        final Map<String, String> props = defaultProperties();
        S3ConfigFragment.setter(props)
                .accessKeyId("blah-blah-blah")
                .accessKeySecret("blah-blah-blah")
                .region(WEST)
                .prefix("blah-blah-blah");
        props.put(S3ConfigFragment.OUTPUT_COMPRESSION, CompressionType.GZIP.name);
        FileNameFragment.setter(props).fileCompression(CompressionType.NONE);

        final var config = new S3SinkConfig(props);

        assertThat(config.getCompressionType()).isEqualTo(CompressionType.NONE);
    }

    @Test
    void unsupportedCompressionType() {
        final Map<String, String> props = defaultProperties();
        S3ConfigFragment.setter(props).accessKeyId("blah-blah-blah").accessKeySecret("blah-blah-blah").region(WEST);
        OutputFormatFragment.setter(props)
                .withOutputFields(OutputFieldType.KEY, OutputFieldType.VALUE, OutputFieldType.OFFSET,
                        OutputFieldType.TIMESTAMP);
        props.put(S3ConfigFragment.OUTPUT_COMPRESSION, "unsupported");

        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "Invalid value unsupported for configuration file.compression.type: String must be one of (case insensitive): ZSTD, GZIP, NONE, SNAPPY");

        props.remove(S3ConfigFragment.OUTPUT_COMPRESSION);
        props.put(FileNameFragment.FILE_COMPRESSION_TYPE_CONFIG, "unsupported");

        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "Invalid value unsupported for configuration file.compression.type: String must be one of (case insensitive): ZSTD, GZIP, NONE, SNAPPY");
    }

    @ParameterizedTest
    @ValueSource(strings = { "YYYY", "yyyy" })
    void shouldBuildPrefixTemplate(final String yearPattern) {
        final String prefix = String.format("{{timestamp:unit=%s}}/{{timestamp:unit=MM}}/{{timestamp:unit=dd}}/",
                yearPattern);

        final Map<String, String> props = defaultProperties();
        S3ConfigFragment.setter(props).accessKeyId("blah-blah-blah").accessKeySecret("blah-blah-blah").region(WEST);

        OutputFormatFragment.setter(props)
                .withOutputFields(OutputFieldType.KEY, OutputFieldType.VALUE, OutputFieldType.OFFSET,
                        OutputFieldType.TIMESTAMP, OutputFieldType.HEADERS);
        FileNameFragment.setter(props)
                .timestampTimeZone(ZoneId.of("Europe/Berlin"))
                .timestampSource(TimestampSource.Type.WALLCLOCK)
                .prefix(prefix);

        final var config = new S3SinkConfig(props);

        // null record is fine here, because it's not needed for the wallclock timestamp source
        final var expectedTimestamp = config.getFilenameTimestampSource().time(null);

        final var renderedPrefix = config.getPrefixTemplate()
                .instance()
                .bindVariable("timestamp",
                        // null record is fine here, because it's not needed for the wall clock timestamp source
                        new StableTimeFormatter(config.getFilenameTimestampSource()).apply(null))
                .render();

        assertThat(renderedPrefix)
                .isEqualTo(String.format("%s/%s/%s/", expectedTimestamp.format(DateTimeFormatter.ofPattern("yyyy")),
                        expectedTimestamp.format(DateTimeFormatter.ofPattern("MM")),
                        expectedTimestamp.format(DateTimeFormatter.ofPattern("dd"))));
    }

    @ParameterizedTest
    @EnumSource(FormatType.class)
    void supportedFormatTypeConfig(final FormatType formatType) {
        final Map<String, String> props = defaultProperties();
        S3ConfigFragment.setter(props)
                .accessKeyId("blah-blah-blah")
                .accessKeySecret("blah-blah-blah")
                .prefix("any_prefix");
        OutputFormatFragment.setter(props).withFormatType(formatType);

        final S3SinkConfig s3SinkConfig = new S3SinkConfig(props);

        assertThat(s3SinkConfig.getFormatType()).isEqualTo(formatType);
    }

    @Test
    void wrongFormatTypeConfig() {
        final Map<String, String> props = defaultProperties();
        S3ConfigFragment.setter(props)
                .accessKeyId("blah-blah-blah")
                .accessKeySecret("blah-blah-blah")
                .prefix("any_prefix");
        props.put(OutputFormatArgs.FORMAT_OUTPUT_TYPE_CONFIG.key(), "unknown");

        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessageContaining("Invalid value unknown for configuration format.output.type: "
                        + "String must be one of (case insensitive): PARQUET, CSV, JSON, AVRO, JSONL");

    }

    @Test
    void stsRoleCorrectConfig() {
        final Map<String, String> props = defaultProperties();
        S3ConfigFragment.setter(props)
                .stsRoleArn("arn:aws:iam::12345678910:role/S3SinkTask")
                .stsRoleExternalId("EXTERNAL_ID")
                .stsRoleSessionName("SESSION_NAME")
                .region(EAST);

        final var conf = new S3SinkConfig(props);

        assertThat(conf.getStsRole().getArn()).isEqualTo("arn:aws:iam::12345678910:role/S3SinkTask");
        assertThat(conf.getStsRole().getExternalId()).isEqualTo("EXTERNAL_ID");
        assertThat(conf.getStsRole().getSessionName()).isEqualTo("SESSION_NAME");
        assertThat(conf.getAwsS3Region()).isEqualTo(RegionUtils.getRegion(EAST));
    }

    @Test
    void stsWrongSessionDuration() {
        final Map<String, String> props = defaultProperties();
        S3ConfigFragment.setter(props)
                .stsRoleArn("arn:aws:iam::12345678910:role/S3SinkTask")
                .stsRoleSessionName("SESSION_NAME")
                .stsRoleSessionDuration(Duration.ofSeconds(30))
                .region(EAST);

        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage(
                        "Invalid value 30 for configuration aws.sts.role.session.duration: Value must be at least 900");

        S3ConfigFragment.setter(props).stsRoleSessionDuration(Duration.ofSeconds(50_000));

        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value 50000 for configuration aws.sts.role.session.duration: "
                        + "Value must be no more than 43200");
    }

    @Test
    void stsCorrectSessionDuration() {
        final Map<String, String> props = defaultProperties();
        S3ConfigFragment.setter(props)
                .stsRoleArn("arn:aws:iam::12345678910:role/S3SinkTask")
                .stsRoleSessionName("SESSION_NAME")
                .stsRoleSessionDuration(Duration.ofSeconds(900))
                .region(EAST);

        final var conf = new S3SinkConfig(props);

        assertThat(conf.getStsRole().getSessionDurationSeconds()).isEqualTo(900);
    }

    @Test
    void stsEndpointShouldNotBeSetWithoutRegion() {
        final Map<String, String> props = defaultProperties();
        S3ConfigFragment.setter(props)
                .stsRoleArn("arn:aws:iam::12345678910:role/S3SinkTask")
                .stsRoleExternalId("EXTERNAL_ID")
                .stsRoleSessionName("SESSION_NAME")
                .stsEndpoint("https://sts.eu-north-1.amazonaws.com");

        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessageContaining("aws.s3.region should be specified together with aws.sts.config.endpoint");
    }

    @ParameterizedTest
    @ValueSource(strings = { "{{key}}", "{{topic}}/{{partition}}/{{key}}" })
    void notSupportedFileMaxRecords(final String fileNameTemplate) {
        final Map<String, String> props = defaultProperties();
        S3ConfigFragment.setter(props).accessKeyId("any_access_key_id").accessKeySecret("any_secret_key");
        FileNameFragment.setter(props).template(fileNameTemplate).maxRecordsPerFile(2);

        assertThatThrownBy(() -> new S3SinkConfig(props)).isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        String.format("When file.name.template is %s, file.max.records must be either 1 or not set",
                                fileNameTemplate));
    }

}
