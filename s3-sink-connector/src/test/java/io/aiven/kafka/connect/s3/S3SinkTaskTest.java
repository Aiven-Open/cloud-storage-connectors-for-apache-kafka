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

package io.aiven.kafka.connect.s3;

import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_ACCESS_KEY_ID;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_BUCKET;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_ENDPOINT;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_PREFIX;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_PREFIX_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_REGION;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_SECRET_ACCESS_KEY;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.OUTPUT_COMPRESSION;
import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.OUTPUT_FIELDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

import io.aiven.kafka.connect.common.config.BackoffPolicyFragmentFixture.BackoffPolicyArgs;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.OutputFormatFragmentFixture.OutputFormatArgs;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;
import io.aiven.kafka.connect.iam.AwsCredentialProviderFactory;
import io.aiven.kafka.connect.s3.config.S3SinkConfig;
import io.aiven.kafka.connect.s3.testutils.BucketAccessor;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.github.luben.zstd.ZstdInputStream;
import com.google.common.collect.Lists;
import io.findify.s3mock.S3Mock;
import org.assertj.core.util.introspection.FieldSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.xerial.snappy.SnappyInputStream;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings({ "PMD.ExcessiveImports", "PMD.TooManyStaticImports", "deprecation" })
final class S3SinkTaskTest {

    private static final String TEST_BUCKET = "test-bucket";

    private static S3Mock s3Api;
    private static AmazonS3 s3Client;

    private static Map<String, String> commonProperties;

    private final ByteArrayConverter byteArrayConverter = new ByteArrayConverter();

    private Map<String, String> properties;

    private static BucketAccessor testBucketAccessor;

    @Mock
    private SinkTaskContext mockedSinkTaskContext;

    private static final Random RANDOM = new Random();

    @BeforeAll
    public static void setUpClass() {
        final int s3Port = RANDOM.nextInt(10_000) + 10_000;

        s3Api = new S3Mock.Builder().withPort(s3Port).withInMemoryBackend().build();
        s3Api.start();

        commonProperties = Map.of(AWS_ACCESS_KEY_ID, "test_key_id", AWS_SECRET_ACCESS_KEY, "test_secret_key",
                AWS_S3_BUCKET, TEST_BUCKET, AWS_S3_ENDPOINT, "http://localhost:" + s3Port, AWS_S3_REGION, "us-west-2");

        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        final BasicAWSCredentials awsCreds = new BasicAWSCredentials(commonProperties.get(AWS_ACCESS_KEY_ID),
                commonProperties.get(AWS_SECRET_ACCESS_KEY));
        builder.withCredentials(new AWSStaticCredentialsProvider(awsCreds));
        builder.withEndpointConfiguration(
                new EndpointConfiguration(commonProperties.get(AWS_S3_ENDPOINT), commonProperties.get(AWS_S3_REGION)));
        builder.withPathStyleAccessEnabled(true);

        s3Client = builder.build();

        testBucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET);
        testBucketAccessor.createBucket();
    }

    @AfterAll
    public static void tearDownClass() {
        s3Api.stop();
    }

    @BeforeEach
    public void setUp() {
        properties = new HashMap<>(commonProperties);

        s3Client.createBucket(TEST_BUCKET);
    }

    @AfterEach
    public void tearDown() {
        s3Client.deleteBucket(TEST_BUCKET);
    }

    @ParameterizedTest
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void testAivenKafkaConnectS3SinkTask(final String compression) throws IOException {
        // Create SinkTask
        final S3SinkTask task = new S3SinkTask();

        final CompressionType compressionType = CompressionType.forName(compression);

        properties.put(OUTPUT_FIELDS, "value,key,timestamp,offset,headers");
        properties.put(AWS_S3_PREFIX, "aiven--");
        properties.put(OUTPUT_COMPRESSION, compression);
        task.start(properties);

        final TopicPartition topicPartition = new TopicPartition("test-topic", 0);
        final Collection<TopicPartition> tps = Collections.singletonList(topicPartition);
        task.open(tps);

        // * Simulate periodical flush() cycle - ensure that data files are written

        // Push batch of records
        final Collection<SinkRecord> sinkRecords = createBatchOfRecord(0, 100);
        task.put(sinkRecords);

        assertThat(s3Client.doesObjectExist(TEST_BUCKET,
                "aiven--test-topic-0-00000000000000000000" + compressionType.extension())).isFalse();

        // Flush data - this is called by Connect on offset.flush.interval
        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(topicPartition, new OffsetAndMetadata(100));
        task.flush(offsets);

        final ConnectHeaders expectedConnectHeaders = createTestHeaders();

        assertThat(s3Client.doesObjectExist(TEST_BUCKET,
                "aiven--test-topic-0-00000000000000000000" + compressionType.extension())).isTrue();

        try (S3Object s3Object = s3Client.getObject(TEST_BUCKET,
                "aiven--test-topic-0-00000000000000000000" + compressionType.extension());
                S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent();
                InputStream inputStream = getCompressedInputStream(s3ObjectInputStream, compressionType);
                BufferedReader bufferedReader = new BufferedReader(
                        new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            for (String line; (line = bufferedReader.readLine()) != null;) { // NOPMD AssignmentInOperand
                final String[] parts = line.split(",");
                final ConnectHeaders actualConnectHeaders = readHeaders(parts[4]);
                assertThat(headersEquals(actualConnectHeaders, expectedConnectHeaders)).isTrue();
            }
        }

        // * Verify that we store data on partition unassignment
        task.put(createBatchOfRecord(100, 200));

        assertThat(s3Client.doesObjectExist(TEST_BUCKET,
                "aiven--test-topic-0-00000000000000000100" + compressionType.extension())).isFalse();

        offsets.clear();
        offsets.put(topicPartition, new OffsetAndMetadata(100));
        task.flush(offsets);

        assertThat(s3Client.doesObjectExist(TEST_BUCKET,
                "aiven--test-topic-0-00000000000000000100" + compressionType.extension())).isTrue();

        // * Verify that we store data on SinkTask shutdown
        task.put(createBatchOfRecord(200, 300));

        assertThat(s3Client.doesObjectExist(TEST_BUCKET,
                "aiven--test-topic-0-00000000000000000200" + compressionType.extension())).isFalse();

        offsets.clear();
        offsets.put(topicPartition, new OffsetAndMetadata(200));
        task.flush(offsets);
        task.stop();

        assertThat(s3Client.doesObjectExist(TEST_BUCKET,
                "aiven--test-topic-0-00000000000000000200" + compressionType.extension())).isTrue();
    }

    private InputStream getCompressedInputStream(final InputStream inputStream, final CompressionType compressionType)
            throws IOException {
        Objects.requireNonNull(inputStream, "inputStream cannot be null");

        switch (compressionType) {
            case ZSTD :
                return new ZstdInputStream(inputStream);
            case GZIP :
                return new GZIPInputStream(inputStream);
            case SNAPPY :
                return new SnappyInputStream(inputStream);
            default :
                return inputStream;
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void testS3ConstantPrefix(final String compression) {
        final S3SinkTask task = new S3SinkTask();

        final CompressionType compressionType = CompressionType.forName(compression);

        properties.put(OUTPUT_COMPRESSION, compression);
        properties.put(OUTPUT_FIELDS, "value,key,timestamp,offset");
        properties.put(AWS_S3_PREFIX, "prefix--");
        task.start(properties);

        final TopicPartition topicPartition = new TopicPartition("test-topic", 0);
        final Collection<TopicPartition> tps = Collections.singletonList(topicPartition);
        task.open(tps);

        task.put(createBatchOfRecord(0, 100));

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(topicPartition, new OffsetAndMetadata(100));
        task.flush(offsets);

        assertThat(s3Client.doesObjectExist(TEST_BUCKET,
                "prefix--test-topic-0-00000000000000000000" + compressionType.extension())).isTrue();
    }

    @Test
    void setKafkaBackoffTimeout() {
        final S3SinkTask task = new S3SinkTask();
        task.initialize(mockedSinkTaskContext);
        final var props = Map.of(OutputFormatArgs.FORMAT_OUTPUT_FIELDS_CONFIG.key(), "key,value",
                OutputFormatArgs.FORMAT_OUTPUT_TYPE_CONFIG.key(), "jsonl", AWS_ACCESS_KEY_ID_CONFIG,
                "AWS_ACCESS_KEY_ID_CONFIG", AWS_SECRET_ACCESS_KEY_CONFIG, "AWS_SECRET_ACCESS_KEY_CONFIG",
                AWS_S3_BUCKET_NAME_CONFIG, "aws-s3-bucket-name-config",
                BackoffPolicyArgs.KAFKA_RETRY_BACKOFF_MS_CONFIG.key(), "3000");
        task.start(props);

        verify(mockedSinkTaskContext).timeout(3000L);
    }

    @Test
    void skipKafkaBackoffTimeout() {
        final S3SinkTask task = new S3SinkTask();
        task.initialize(mockedSinkTaskContext);
        final var props = Map.of(OutputFormatArgs.FORMAT_OUTPUT_FIELDS_CONFIG.key(), "key,value",
                OutputFormatArgs.FORMAT_OUTPUT_TYPE_CONFIG.key(), "jsonl", AWS_ACCESS_KEY_ID_CONFIG,
                "AWS_ACCESS_KEY_ID_CONFIG", AWS_SECRET_ACCESS_KEY_CONFIG, "AWS_SECRET_ACCESS_KEY_CONFIG",
                AWS_S3_BUCKET_NAME_CONFIG, "aws-s3-bucket-name-config");
        task.start(props);

        verify(mockedSinkTaskContext, never()).timeout(any(Long.class));
    }

    @Test
    void setupDefaultS3Policy() {
        final S3SinkTask task = new S3SinkTask();
        task.initialize(mockedSinkTaskContext);
        final var props = Map.of(OutputFormatArgs.FORMAT_OUTPUT_FIELDS_CONFIG.key(), "key,value",
                OutputFormatArgs.FORMAT_OUTPUT_TYPE_CONFIG.key(), "jsonl", AWS_ACCESS_KEY_ID_CONFIG,
                "AWS_ACCESS_KEY_ID_CONFIG", AWS_SECRET_ACCESS_KEY_CONFIG, "AWS_SECRET_ACCESS_KEY_CONFIG",
                AWS_S3_BUCKET_NAME_CONFIG, "aws-s3-bucket-name-config");
        task.start(props);

        final var s3Client = FieldSupport.EXTRACTION.fieldValue("s3Client", AmazonS3.class, task);
        final var s3RetryPolicy = ((AmazonS3Client) s3Client).getClientConfiguration().getRetryPolicy();

        final var fullJitterBackoffStrategy = (PredefinedBackoffStrategies.FullJitterBackoffStrategy) s3RetryPolicy
                .getBackoffStrategy();

        final var defaultDelay = FieldSupport.EXTRACTION.fieldValue("baseDelay", Integer.class,
                fullJitterBackoffStrategy);
        final var defaultMaxDelay = FieldSupport.EXTRACTION.fieldValue("maxBackoffTime", Integer.class,
                fullJitterBackoffStrategy);

        assertThat(s3RetryPolicy.getMaxErrorRetry()).isEqualTo(S3SinkConfig.S3_RETRY_BACKOFF_MAX_RETRIES_DEFAULT);
        assertThat(defaultDelay).isEqualTo(S3SinkConfig.AWS_S3_RETRY_BACKOFF_DELAY_MS_DEFAULT);
        assertThat(defaultMaxDelay).isEqualTo(S3SinkConfig.AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT);
    }

    @Test
    void setupCustomS3Policy() {
        final S3SinkTask task = new S3SinkTask();
        task.initialize(mockedSinkTaskContext);
        final var props = Map.of(OutputFormatArgs.FORMAT_OUTPUT_FIELDS_CONFIG.key(), "key,value",
                OutputFormatArgs.FORMAT_OUTPUT_TYPE_CONFIG.key(), "jsonl", AWS_ACCESS_KEY_ID_CONFIG,
                "AWS_ACCESS_KEY_ID_CONFIG", AWS_SECRET_ACCESS_KEY_CONFIG, "AWS_SECRET_ACCESS_KEY_CONFIG",
                AWS_S3_BUCKET_NAME_CONFIG, "the-bucket", "aws.s3.backoff.delay.ms", "1", "aws.s3.backoff.max.delay.ms",
                "2", "aws.s3.backoff.max.retries", "3");
        task.start(props);

        final var s3Client = FieldSupport.EXTRACTION.fieldValue("s3Client", AmazonS3.class, task);
        final var s3RetryPolicy = ((AmazonS3Client) s3Client).getClientConfiguration().getRetryPolicy();

        final var fullJitterBackoffStrategy = (PredefinedBackoffStrategies.FullJitterBackoffStrategy) s3RetryPolicy
                .getBackoffStrategy();

        final var defaultDelay = FieldSupport.EXTRACTION.fieldValue("baseDelay", Integer.class,
                fullJitterBackoffStrategy);
        final var defaultMaxDelay = FieldSupport.EXTRACTION.fieldValue("maxBackoffTime", Integer.class,
                fullJitterBackoffStrategy);

        assertThat(defaultDelay).isOne();
        assertThat(defaultMaxDelay).isEqualTo(2);
        assertThat(s3RetryPolicy.getMaxErrorRetry()).isEqualTo(3);
    }

    @ParameterizedTest
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void testS3UtcDatePrefix(final String compression) {
        final S3SinkTask task = new S3SinkTask();

        final CompressionType compressionType = CompressionType.forName(compression);

        properties.put(OUTPUT_COMPRESSION, compression);
        properties.put(OUTPUT_FIELDS, "value,key,timestamp,offset");
        properties.put(AWS_S3_PREFIX, "prefix-{{ utc_date }}--");
        task.start(properties);

        final TopicPartition topicPartition = new TopicPartition("test-topic", 0);
        final Collection<TopicPartition> tps = Collections.singletonList(topicPartition);
        task.open(tps);

        task.put(createBatchOfRecord(0, 100));

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(topicPartition, new OffsetAndMetadata(100));
        task.flush(offsets);

        final String expectedFileName = String.format(
                "prefix-%s--test-topic-0-00000000000000000000" + compressionType.extension(),
                ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_LOCAL_DATE));
        assertThat(s3Client.doesObjectExist(TEST_BUCKET, expectedFileName)).isTrue();

        task.stop();
    }

    @ParameterizedTest
    @ValueSource(strings = { "none", "gzip", "snappy", "zstd" })
    void testS3LocalDatePrefix(final String compression) {
        final S3SinkTask task = new S3SinkTask();

        final CompressionType compressionType = CompressionType.forName(compression);

        properties.put(OUTPUT_COMPRESSION, compression);
        properties.put(OUTPUT_FIELDS, "value,key,timestamp,offset");
        properties.put(AWS_S3_PREFIX, "prefix-{{ local_date }}--");
        task.start(properties);

        final TopicPartition topicPartition = new TopicPartition("test-topic", 0);
        final Collection<TopicPartition> tps = Collections.singletonList(topicPartition);
        task.open(tps);

        task.put(createBatchOfRecord(0, 100));

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(topicPartition, new OffsetAndMetadata(100));
        task.flush(offsets);

        final String expectedFileName = String.format(
                "prefix-%s--test-topic-0-00000000000000000000" + compressionType.extension(),
                LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE));
        assertThat(s3Client.doesObjectExist(TEST_BUCKET, expectedFileName)).isTrue();

        task.stop();
    }

    @Test
    void failedForStringValuesByDefault() {
        final S3SinkTask task = new S3SinkTask();

        final String compression = "none";
        properties.put(S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(OutputFormatArgs.FORMAT_OUTPUT_FIELDS_CONFIG.key(), "key,value");
        properties.put(AWS_S3_PREFIX_CONFIG, "any_prefix");
        task.start(properties);

        final List<SinkRecord> records = List.of(
                createRecordWithStringValueSchema("topic0", 0, "key0", "value0", 10, 1000),
                createRecordWithStringValueSchema("topic0", 1, "key1", "value1", 20, 1001),
                createRecordWithStringValueSchema("topic1", 0, "key2", "value2", 30, 1002)

        );

        assertThatThrownBy(() -> task.put(records)).isInstanceOf(ConnectException.class)
                .hasMessage("Record value schema type must be BYTES, STRING given");
    }

    @Test
    void supportStringValuesForJsonL() throws IOException {
        final S3SinkTask task = new S3SinkTask();

        final String compression = "none";
        properties.put(S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(OutputFormatArgs.FORMAT_OUTPUT_FIELDS_CONFIG.key(), "key,value");
        properties.put(OutputFormatArgs.FORMAT_OUTPUT_TYPE_CONFIG.key(), "jsonl");
        properties.put(AWS_S3_PREFIX_CONFIG, "prefix-");
        task.start(properties);

        final List<SinkRecord> records = List.of(
                createRecordWithStringValueSchema("topic0", 0, "key0", "value0", 10, 1000),
                createRecordWithStringValueSchema("topic0", 1, "key1", "value1", 20, 1001),
                createRecordWithStringValueSchema("topic1", 0, "key2", "value2", 30, 1002));

        final TopicPartition tp00 = new TopicPartition("topic0", 0);
        final TopicPartition tp01 = new TopicPartition("topic0", 1);
        final TopicPartition tp10 = new TopicPartition("topic1", 0);
        final Collection<TopicPartition> tps = List.of(tp00, tp01, tp10);
        task.open(tps);

        task.put(records);

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp00, new OffsetAndMetadata(10));
        offsets.put(tp01, new OffsetAndMetadata(20));
        offsets.put(tp10, new OffsetAndMetadata(30));
        task.flush(offsets);

        final CompressionType compressionType = CompressionType.forName(compression);

        final List<String> expectedBlobs = Lists.newArrayList(
                "prefix-topic0-0-00000000000000000010" + compressionType.extension(),
                "prefix-topic0-1-00000000000000000020" + compressionType.extension(),
                "prefix-topic1-0-00000000000000000030" + compressionType.extension());

        for (final String blobName : expectedBlobs) {
            assertThat(testBucketAccessor.doesObjectExist(blobName)).isTrue();
        }

        assertThat(testBucketAccessor.readLines("prefix-topic0-0-00000000000000000010", compression))
                .containsExactly("{\"value\":\"value0\",\"key\":\"key0\"}");
        assertThat(testBucketAccessor.readLines("prefix-topic0-1-00000000000000000020", compression))
                .containsExactly("{\"value\":\"value1\",\"key\":\"key1\"}");
        assertThat(testBucketAccessor.readLines("prefix-topic1-0-00000000000000000030", compression))
                .containsExactly("{\"value\":\"value2\",\"key\":\"key2\"}");
    }

    @Test
    void failedForStructValuesByDefault() {
        final S3SinkTask task = new S3SinkTask();

        final String compression = "none";
        properties.put(S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(OutputFormatArgs.FORMAT_OUTPUT_FIELDS_CONFIG.key(), "key,value");
        properties.put(AWS_S3_PREFIX_CONFIG, "prefix-");
        task.start(properties);

        final List<SinkRecord> records = List.of(
                createRecordWithStructValueSchema("topic0", 0, "key0", "name0", 10, 1000),
                createRecordWithStructValueSchema("topic0", 1, "key1", "name1", 20, 1001),
                createRecordWithStructValueSchema("topic1", 0, "key2", "name2", 30, 1002));

        assertThatThrownBy(() -> task.put(records)).isInstanceOf(ConnectException.class)
                .hasMessage("Record value schema type must be BYTES, STRUCT given");
    }

    @Test
    void supportStructValuesForJsonL() throws IOException {
        final S3SinkTask task = new S3SinkTask();

        final String compression = "none";
        properties.put(S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(OutputFormatArgs.FORMAT_OUTPUT_FIELDS_CONFIG.key(), "key,value");
        properties.put(OutputFormatArgs.FORMAT_OUTPUT_TYPE_CONFIG.key(), "jsonl");
        properties.put(AWS_S3_PREFIX_CONFIG, "prefix-");
        task.start(properties);

        final List<SinkRecord> records = List.of(
                createRecordWithStructValueSchema("topic0", 0, "key0", "name0", 10, 1000),
                createRecordWithStructValueSchema("topic0", 1, "key1", "name1", 20, 1001),
                createRecordWithStructValueSchema("topic1", 0, "key2", "name2", 30, 1002)

        );
        final TopicPartition tp00 = new TopicPartition("topic0", 0);
        final TopicPartition tp01 = new TopicPartition("topic0", 1);
        final TopicPartition tp10 = new TopicPartition("topic1", 0);
        final Collection<TopicPartition> tps = List.of(tp00, tp01, tp10);
        task.open(tps);

        task.put(records);

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp00, new OffsetAndMetadata(10));
        offsets.put(tp01, new OffsetAndMetadata(20));
        offsets.put(tp10, new OffsetAndMetadata(30));
        task.flush(offsets);

        final CompressionType compressionType = CompressionType.forName(compression);

        final List<String> expectedBlobs = Lists.newArrayList(
                "prefix-topic0-0-00000000000000000010" + compressionType.extension(),
                "prefix-topic0-1-00000000000000000020" + compressionType.extension(),
                "prefix-topic1-0-00000000000000000030" + compressionType.extension());

        for (final String blobName : expectedBlobs) {
            assertThat(testBucketAccessor.doesObjectExist(blobName)).isTrue();
        }

        assertThat(testBucketAccessor.readLines("prefix-topic0-0-00000000000000000010", compression))
                .containsExactly("{\"value\":{\"name\":\"name0\"},\"key\":\"key0\"}");
        assertThat(testBucketAccessor.readLines("prefix-topic0-1-00000000000000000020", compression))
                .containsExactly("{\"value\":{\"name\":\"name1\"},\"key\":\"key1\"}");
        assertThat(testBucketAccessor.readLines("prefix-topic1-0-00000000000000000030", compression))
                .containsExactly("{\"value\":{\"name\":\"name2\"},\"key\":\"key2\"}");
    }

    @Test
    void supportUnwrappedJsonEnvelopeForStructAndJsonL() throws IOException {
        final String compression = "none";
        properties.put(S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(OutputFormatArgs.FORMAT_OUTPUT_FIELDS_CONFIG.key(), "value");
        properties.put(OutputFormatArgs.FORMAT_OUTPUT_ENVELOPE_CONFIG.key(), "false");
        properties.put(OutputFormatArgs.FORMAT_OUTPUT_TYPE_CONFIG.key(), "jsonl");
        properties.put(AWS_S3_PREFIX_CONFIG, "prefix-");

        final S3SinkTask task = new S3SinkTask();
        task.start(properties);

        final List<SinkRecord> records = List.of(
                createRecordWithStructValueSchema("topic0", 0, "key0", "name0", 10, 1000),
                createRecordWithStructValueSchema("topic0", 1, "key1", "name1", 20, 1001),
                createRecordWithStructValueSchema("topic1", 0, "key2", "name2", 30, 1002));

        final TopicPartition tp00 = new TopicPartition("topic0", 0);
        final TopicPartition tp01 = new TopicPartition("topic0", 1);
        final TopicPartition tp10 = new TopicPartition("topic1", 0);
        final Collection<TopicPartition> tps = List.of(tp00, tp01, tp10);
        task.open(tps);

        task.put(records);

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp00, new OffsetAndMetadata(10));
        offsets.put(tp01, new OffsetAndMetadata(20));
        offsets.put(tp10, new OffsetAndMetadata(30));
        task.flush(offsets);

        final CompressionType compressionType = CompressionType.forName(compression);

        final List<String> expectedBlobs = Lists.newArrayList(
                "prefix-topic0-0-00000000000000000010" + compressionType.extension(),
                "prefix-topic0-1-00000000000000000020" + compressionType.extension(),
                "prefix-topic1-0-00000000000000000030" + compressionType.extension());
        assertThat(expectedBlobs).allMatch(blobName -> testBucketAccessor.doesObjectExist(blobName));

        assertThat(testBucketAccessor.readLines("prefix-topic0-0-00000000000000000010", compression))
                .containsExactly("{\"name\":\"name0\"}");
        assertThat(testBucketAccessor.readLines("prefix-topic0-1-00000000000000000020", compression))
                .containsExactly("{\"name\":\"name1\"}");
        assertThat(testBucketAccessor.readLines("prefix-topic1-0-00000000000000000030", compression))
                .containsExactly("{\"name\":\"name2\"}");
    }

    @Test
    void supportStructValuesForClassicJson() throws IOException {
        final S3SinkTask task = new S3SinkTask();

        final String compression = "none";
        properties.put(S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(OutputFormatArgs.FORMAT_OUTPUT_FIELDS_CONFIG.key(), "key,value");
        properties.put(OutputFormatArgs.FORMAT_OUTPUT_TYPE_CONFIG.key(), "json");
        task.start(properties);

        final List<SinkRecord> records = List.of(
                createRecordWithStructValueSchema("topic0", 0, "key0", "name0", 10, 1000),
                createRecordWithStructValueSchema("topic0", 1, "key1", "name1", 20, 1001),
                createRecordWithStructValueSchema("topic1", 0, "key2", "name2", 30, 1002));

        task.put(records);
        task.flush(null);

        final CompressionType compressionType = CompressionType.forName(compression);

        final List<String> expectedBlobs = Lists.newArrayList("topic0-0-10" + compressionType.extension(),
                "topic0-1-20" + compressionType.extension(), "topic1-0-30" + compressionType.extension());
        for (final String blobName : expectedBlobs) {
            assertThat(testBucketAccessor.doesObjectExist(blobName)).isTrue();
        }

        assertThat(testBucketAccessor.readLines("topic0-0-10", compression)).containsExactly("[",
                "{\"value\":{\"name\":\"name0\"},\"key\":\"key0\"}", "]");
        assertThat(testBucketAccessor.readLines("topic0-1-20", compression)).containsExactly("[",
                "{\"value\":{\"name\":\"name1\"},\"key\":\"key1\"}", "]");
        assertThat(testBucketAccessor.readLines("topic1-0-30", compression)).containsExactly("[",
                "{\"value\":{\"name\":\"name2\"},\"key\":\"key2\"}", "]");
    }

    @Test
    void supportUnwrappedJsonEnvelopeForStructAndClassicJson() throws IOException {
        final String compression = "none";
        properties.put(S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(OutputFormatArgs.FORMAT_OUTPUT_FIELDS_CONFIG.key(), "value");
        properties.put(OutputFormatArgs.FORMAT_OUTPUT_ENVELOPE_CONFIG.key(), "false");
        properties.put(OutputFormatArgs.FORMAT_OUTPUT_TYPE_CONFIG.key(), "json");
        properties.put(AWS_S3_PREFIX_CONFIG, "prefix-");

        final S3SinkTask task = new S3SinkTask();
        task.start(properties);

        final List<SinkRecord> records = List.of(
                createRecordWithStructValueSchema("topic0", 0, "key0", "name0", 10, 1000),
                createRecordWithStructValueSchema("topic0", 1, "key1", "name1", 20, 1001),
                createRecordWithStructValueSchema("topic1", 0, "key2", "name2", 30, 1002));

        final TopicPartition tp00 = new TopicPartition("topic0", 0);
        final TopicPartition tp01 = new TopicPartition("topic0", 1);
        final TopicPartition tp10 = new TopicPartition("topic1", 0);
        final Collection<TopicPartition> tps = List.of(tp00, tp01, tp10);
        task.open(tps);

        task.put(records);

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp00, new OffsetAndMetadata(10));
        offsets.put(tp01, new OffsetAndMetadata(20));
        offsets.put(tp10, new OffsetAndMetadata(30));
        task.flush(offsets);

        final CompressionType compressionType = CompressionType.forName(compression);

        final List<String> expectedBlobs = Lists.newArrayList(
                "prefix-topic0-0-00000000000000000010" + compressionType.extension(),
                "prefix-topic0-1-00000000000000000020" + compressionType.extension(),
                "prefix-topic1-0-00000000000000000030" + compressionType.extension());
        assertThat(expectedBlobs).allMatch(blobName -> testBucketAccessor.doesObjectExist(blobName));

        assertThat(testBucketAccessor.readLines("prefix-topic0-0-00000000000000000010", compression))
                .containsExactly("[", "{\"name\":\"name0\"}", "]");
        assertThat(testBucketAccessor.readLines("prefix-topic0-1-00000000000000000020", compression))
                .containsExactly("[", "{\"name\":\"name1\"}", "]");
        assertThat(testBucketAccessor.readLines("prefix-topic1-0-00000000000000000030", compression))
                .containsExactly("[", "{\"name\":\"name2\"}", "]");
    }

    @Test
    void requestCredentialProviderFromFactoryOnStart() {
        final S3SinkTask task = new S3SinkTask();

        final AwsCredentialProviderFactory mockedFactory = mock(AwsCredentialProviderFactory.class);
        final AWSCredentialsProvider provider = mock(AWSCredentialsProvider.class);

        task.credentialFactory = mockedFactory;
        Mockito.when(mockedFactory.getProvider(any(S3ConfigFragment.class))).thenReturn(provider);

        task.start(properties);

        verify(mockedFactory, Mockito.times(1)).getProvider(any(S3ConfigFragment.class));
    }

    @Test
    void mutliPartUploadWriteOnlyExpectedRecordsAndFilesToS3() throws IOException {
        final String compression = "none";
        properties.put(S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(OutputFormatArgs.FORMAT_OUTPUT_FIELDS_CONFIG.key(), "value");
        properties.put(OutputFormatArgs.FORMAT_OUTPUT_ENVELOPE_CONFIG.key(), "false");
        properties.put(OutputFormatArgs.FORMAT_OUTPUT_TYPE_CONFIG.key(), "json");
        properties.put(AWS_S3_PREFIX_CONFIG, "prefix-");
        properties.put(S3SinkConfig.FILE_NAME_TEMPLATE_CONFIG, "{{topic}}-{{partition}}-{{start_offset}}");

        final S3SinkTask task = new S3SinkTask();
        task.start(properties);
        int timestamp = 1000;
        int offset1 = 10;
        int offset2 = 20;
        int offset3 = 30;
        final List<List<SinkRecord>> allRecords = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            allRecords.add(
                    List.of(createRecordWithStructValueSchema("topic0", 0, "key0", "name0", offset1++, timestamp++),
                            createRecordWithStructValueSchema("topic0", 1, "key1", "name1", offset2++, timestamp++),
                            createRecordWithStructValueSchema("topic1", 0, "key2", "name2", offset3++, timestamp++)));
        }
        final TopicPartition tp00 = new TopicPartition("topic0", 0);
        final TopicPartition tp01 = new TopicPartition("topic0", 1);
        final TopicPartition tp10 = new TopicPartition("topic1", 0);
        final Collection<TopicPartition> tps = List.of(tp00, tp01, tp10);
        task.open(tps);

        allRecords.forEach(task::put);

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp00, new OffsetAndMetadata(offset1));
        offsets.put(tp01, new OffsetAndMetadata(offset2));
        offsets.put(tp10, new OffsetAndMetadata(offset3));
        task.flush(offsets);

        final CompressionType compressionType = CompressionType.forName(compression);

        List<String> expectedBlobs = Lists.newArrayList(
                "prefix-topic0-0-00000000000000000010" + compressionType.extension(),
                "prefix-topic0-1-00000000000000000020" + compressionType.extension(),
                "prefix-topic1-0-00000000000000000030" + compressionType.extension());
        assertThat(expectedBlobs).allMatch(blobName -> testBucketAccessor.doesObjectExist(blobName));

        assertThat(testBucketAccessor.readLines("prefix-topic0-0-00000000000000000010", compression))
                .containsExactly("[", "{\"name\":\"name0\"},", "{\"name\":\"name0\"},", "{\"name\":\"name0\"}", "]");
        assertThat(testBucketAccessor.readLines("prefix-topic0-1-00000000000000000020", compression))
                .containsExactly("[", "{\"name\":\"name1\"},", "{\"name\":\"name1\"},", "{\"name\":\"name1\"}", "]");
        assertThat(testBucketAccessor.readLines("prefix-topic1-0-00000000000000000030", compression))
                .containsExactly("[", "{\"name\":\"name2\"},", "{\"name\":\"name2\"},", "{\"name\":\"name2\"}", "]");
        // Reset and send another batch of records to S3
        allRecords.clear();
        for (int i = 0; i < 3; i++) {
            allRecords.add(
                    List.of(createRecordWithStructValueSchema("topic0", 0, "key0", "name0", offset1++, timestamp++),
                            createRecordWithStructValueSchema("topic0", 1, "key1", "name1", offset2++, timestamp++),
                            createRecordWithStructValueSchema("topic1", 0, "key2", "name2", offset3++, timestamp++)));
        }
        allRecords.forEach(task::put);
        offsets.clear();
        offsets.put(tp00, new OffsetAndMetadata(offset1));
        offsets.put(tp01, new OffsetAndMetadata(offset2));
        offsets.put(tp10, new OffsetAndMetadata(offset3));
        task.flush(offsets);
        expectedBlobs.clear();
        expectedBlobs = Lists.newArrayList("prefix-topic0-0-00000000000000000010" + compressionType.extension(),
                "prefix-topic0-1-00000000000000000020" + compressionType.extension(),
                "prefix-topic1-0-00000000000000000030" + compressionType.extension(),
                "prefix-topic0-0-00000000000000000013" + compressionType.extension(),
                "prefix-topic0-1-00000000000000000023" + compressionType.extension(),
                "prefix-topic1-0-00000000000000000033" + compressionType.extension());
        assertThat(expectedBlobs).allMatch(blobName -> testBucketAccessor.doesObjectExist(blobName));

    }

    @Test
    void mutliPartUploadUsingKeyPartitioning() throws IOException {
        final String compression = "none";
        properties.put(S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(OutputFormatArgs.FORMAT_OUTPUT_FIELDS_CONFIG.key(), "value");
        properties.put(OutputFormatArgs.FORMAT_OUTPUT_ENVELOPE_CONFIG.key(), "false");
        properties.put(OutputFormatArgs.FORMAT_OUTPUT_TYPE_CONFIG.key(), "json");
        properties.put(AWS_S3_PREFIX_CONFIG, "prefix-");
        // Compact/key 'mode' value only updated
        properties.put(S3SinkConfig.FILE_NAME_TEMPLATE_CONFIG, "{{key}}-{{topic}}");

        final S3SinkTask task = new S3SinkTask();
        task.start(properties);
        int timestamp = 1000;
        int offset1 = 10;
        int offset2 = 20;
        int offset3 = 30;
        final List<List<SinkRecord>> allRecords = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            allRecords.add(List.of(
                    createRecordWithStructValueSchema("topic0", 0, "key0", "name0" + i, offset1++, timestamp++),
                    createRecordWithStructValueSchema("topic0", 1, "key1", "name1" + i, offset2++, timestamp++),
                    createRecordWithStructValueSchema("topic1", 0, "key2", "name2" + i, offset3++, timestamp++)));
        }
        final TopicPartition tp00 = new TopicPartition("topic0", 0);
        final TopicPartition tp01 = new TopicPartition("topic0", 1);
        final TopicPartition tp10 = new TopicPartition("topic1", 0);
        final Collection<TopicPartition> tps = List.of(tp00, tp01, tp10);
        task.open(tps);

        allRecords.forEach(task::put);

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp00, new OffsetAndMetadata(offset1));
        offsets.put(tp01, new OffsetAndMetadata(offset2));
        offsets.put(tp10, new OffsetAndMetadata(offset3));
        task.flush(offsets);

        final CompressionType compressionType = CompressionType.forName(compression);

        List<String> expectedBlobs = Lists.newArrayList(
                "prefix-topic0-0-00000000000000000012" + compressionType.extension(),
                "prefix-topic0-1-00000000000000000022" + compressionType.extension(),
                "prefix-topic1-0-00000000000000000032" + compressionType.extension());

        assertThat(expectedBlobs).allMatch(blobName -> testBucketAccessor.doesObjectExist(blobName));

        assertThat(testBucketAccessor.readLines("prefix-topic0-0-00000000000000000012", compression))
                .containsExactly("[", "{\"name\":\"name02\"}", "]");
        assertThat(testBucketAccessor.readLines("prefix-topic0-1-00000000000000000022", compression))
                .containsExactly("[", "{\"name\":\"name12\"}", "]");
        assertThat(testBucketAccessor.readLines("prefix-topic1-0-00000000000000000032", compression))
                .containsExactly("[", "{\"name\":\"name22\"}", "]");
        // Reset and send another batch of records to S3
        allRecords.clear();
        for (int i = 0; i < 3; i++) {
            allRecords.add(List.of(
                    createRecordWithStructValueSchema("topic0", 0, "key0", "name01" + i, offset1++, timestamp++),
                    createRecordWithStructValueSchema("topic0", 1, "key1", "name11" + i, offset2++, timestamp++),
                    createRecordWithStructValueSchema("topic1", 0, "key2", "name21" + i, offset3++, timestamp++)));
        }
        allRecords.forEach(task::put);
        offsets.clear();
        offsets.put(tp00, new OffsetAndMetadata(offset1));
        offsets.put(tp01, new OffsetAndMetadata(offset2));
        offsets.put(tp10, new OffsetAndMetadata(offset3));
        task.flush(offsets);
        expectedBlobs.clear();

        expectedBlobs = Lists.newArrayList("prefix-topic0-0-00000000000000000015" + compressionType.extension(),
                "prefix-topic0-1-00000000000000000025" + compressionType.extension(),
                "prefix-topic1-0-00000000000000000035" + compressionType.extension());
        assertThat(expectedBlobs).allMatch(blobName -> testBucketAccessor.doesObjectExist(blobName));
        assertThat(testBucketAccessor.readLines("prefix-topic0-0-00000000000000000015", compression))
                .containsExactly("[", "{\"name\":\"name012\"}", "]");
        assertThat(testBucketAccessor.readLines("prefix-topic0-1-00000000000000000025", compression))
                .containsExactly("[", "{\"name\":\"name112\"}", "]");
        assertThat(testBucketAccessor.readLines("prefix-topic1-0-00000000000000000035", compression))
                .containsExactly("[", "{\"name\":\"name212\"}", "]");

    }

    private SinkRecord createRecordWithStringValueSchema(final String topic, final int partition, final String key,
            final String value, final int offset, final long timestamp) {
        return new SinkRecord(topic, partition, Schema.STRING_SCHEMA, key, Schema.STRING_SCHEMA, value, offset,
                timestamp, TimestampType.CREATE_TIME);
    }

    private Collection<SinkRecord> createBatchOfRecord(final int offsetFrom, final int offsetTo) {
        final ArrayList<SinkRecord> records = new ArrayList<>();
        for (int offset = offsetFrom; offset < offsetTo; offset++) {
            final ConnectHeaders connectHeaders = createTestHeaders();
            final SinkRecord record = new SinkRecord( // NOPMD AvoidInstantiatingObjectsInLoops
                    "test-topic", 0, Schema.BYTES_SCHEMA, "test-key".getBytes(StandardCharsets.UTF_8),
                    Schema.BYTES_SCHEMA, "test-value".getBytes(StandardCharsets.UTF_8), offset, 1000L,
                    TimestampType.CREATE_TIME, connectHeaders);
            records.add(record);

        }
        return records;
    }

    private SinkRecord createRecordWithStructValueSchema(final String topic, final int partition, final String key,
            final String name, final int offset, final long timestamp) {
        final Schema schema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA);
        final Struct struct = new Struct(schema).put("name", name);
        return new SinkRecord(topic, partition, Schema.STRING_SCHEMA, key, schema, struct, offset, timestamp,
                TimestampType.CREATE_TIME);
    }

    private ConnectHeaders createTestHeaders() {
        final ConnectHeaders connectHeaders = new ConnectHeaders();
        connectHeaders.addBytes("test-header-key-1", "test-header-value-1".getBytes(StandardCharsets.UTF_8));
        connectHeaders.addBytes("test-header-key-2", "test-header-value-2".getBytes(StandardCharsets.UTF_8));
        return connectHeaders;
    }

    private ConnectHeaders readHeaders(final String headerString) {
        final ConnectHeaders connectHeaders = new ConnectHeaders();
        final String[] headers = headerString.split(";");
        for (final String header : headers) {
            final String[] keyValue = header.split(":");
            final String key = new String(Base64.getDecoder().decode(keyValue[0]), StandardCharsets.UTF_8); // NOPMD
                                                                                                            // AvoidInstantiatingObjectsInLoops
            final byte[] value = Base64.getDecoder().decode(keyValue[1]);
            final SchemaAndValue schemaAndValue = byteArrayConverter.toConnectHeader("topic0", key, value);
            connectHeaders.add(key, schemaAndValue);
        }
        return connectHeaders;
    }

    private boolean headersEquals(final Iterable<Header> headers1, final Iterable<Header> headers2) {
        final Iterator<Header> h1Iterator = headers1.iterator();
        final Iterator<Header> h2Iterator = headers2.iterator();
        while (h1Iterator.hasNext() && h2Iterator.hasNext()) {
            final Header header1 = h1Iterator.next();
            final Header header2 = h2Iterator.next();
            if (!Objects.equals(header1.key(), header2.key())) {
                return false;
            }
            if (!Objects.equals(header1.schema().type(), header2.schema().type())) {
                return false;
            }
            if (header1.schema().type() != Schema.Type.BYTES) {
                return false;
            }
            if (header2.schema().type() != Schema.Type.BYTES) {
                return false;
            }
            if (!Arrays.equals((byte[]) header1.value(), (byte[]) header2.value())) {
                return false;
            }
        }
        return !h1Iterator.hasNext() && !h2Iterator.hasNext();
    }

}
