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

package io.aiven.kafka.connect.s3.source;

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.TARGET_TOPICS;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.TARGET_TOPIC_PARTITIONS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.input.ByteArrayTransformer;
import io.aiven.kafka.connect.s3.source.input.InputFormat;
import io.aiven.kafka.connect.s3.source.input.Transformer;
import io.aiven.kafka.connect.s3.source.testutils.BucketAccessor;
import io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry;
import io.aiven.kafka.connect.s3.source.utils.S3SourceRecord;
import io.aiven.kafka.connect.s3.source.utils.SourceRecordIterator;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.findify.s3mock.S3Mock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
final class S3SourceTaskTest {

    private static final Random RANDOM = new Random();
    private Map<String, String> properties;

    private static BucketAccessor testBucketAccessor;
    private static final String TEST_BUCKET = "test-bucket";

    private static S3Mock s3Api;
    private static AmazonS3 s3Client;

    private static Map<String, String> commonProperties;

    @Mock
    private SourceTaskContext mockedSourceTaskContext;

    @Mock
    private OffsetStorageReader mockedOffsetStorageReader;

    @BeforeAll
    public static void setUpClass() {
        final int s3Port = RANDOM.nextInt(10_000) + 10_000;

        s3Api = new S3Mock.Builder().withPort(s3Port).withInMemoryBackend().build();
        s3Api.start();

        commonProperties = Map.of(S3SourceConfig.AWS_ACCESS_KEY_ID_CONFIG, "test_key_id",
                S3SourceConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "test_secret_key",
                S3SourceConfig.AWS_S3_BUCKET_NAME_CONFIG, TEST_BUCKET, S3SourceConfig.AWS_S3_ENDPOINT_CONFIG,
                "http://localhost:" + s3Port, S3SourceConfig.AWS_S3_REGION_CONFIG, "us-west-2");

        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        final BasicAWSCredentials awsCreds = new BasicAWSCredentials(
                commonProperties.get(S3SourceConfig.AWS_ACCESS_KEY_ID_CONFIG),
                commonProperties.get(S3SourceConfig.AWS_SECRET_ACCESS_KEY_CONFIG));
        builder.withCredentials(new AWSStaticCredentialsProvider(awsCreds));
        builder.withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(commonProperties.get(S3SourceConfig.AWS_S3_ENDPOINT_CONFIG),
                        commonProperties.get(S3SourceConfig.AWS_S3_REGION_CONFIG)));
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
        mockedSourceTaskContext = mock(SourceTaskContext.class);
        mockedOffsetStorageReader = mock(OffsetStorageReader.class);
    }

    @AfterEach
    public void tearDown() {
        s3Client.deleteBucket(TEST_BUCKET);
    }

    @Test
    void testS3SourceTaskInitialization() {
        final S3SourceTask s3SourceTask = new S3SourceTask();
        startSourceTask(s3SourceTask);

        final Optional<Converter> keyConverter = s3SourceTask.getKeyConverter();
        assertThat(keyConverter).isPresent();
        assertThat(keyConverter.get()).isInstanceOf(ByteArrayConverter.class);

        final Converter valueConverter = s3SourceTask.getValueConverter();
        assertThat(valueConverter).isInstanceOf(ByteArrayConverter.class);

        final Transformer transformer = s3SourceTask.getTransformer();
        assertThat(transformer).isInstanceOf(ByteArrayTransformer.class);

        final boolean taskInitialized = s3SourceTask.isTaskInitialized();
        assertThat(taskInitialized).isTrue();
    }

    @Test
    void testPoll() throws Exception {
        final S3SourceTask s3SourceTask = new S3SourceTask();
        startSourceTask(s3SourceTask);

        SourceRecordIterator mockSourceRecordIterator;

        mockSourceRecordIterator = mock(SourceRecordIterator.class);
        setPrivateField(s3SourceTask, "sourceRecordIterator", mockSourceRecordIterator);
        when(mockSourceRecordIterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);

        final S3SourceRecord s3SourceRecordList = getAivenS3SourceRecord();
        when(mockSourceRecordIterator.next()).thenReturn(s3SourceRecordList);

        final List<SourceRecord> sourceRecordList = s3SourceTask.poll();
        assertThat(sourceRecordList).isNotEmpty();
    }

    @Test
    void testStop() {
        final S3SourceTask s3SourceTask = new S3SourceTask();
        startSourceTask(s3SourceTask);
        s3SourceTask.stop();

        final boolean taskInitialized = s3SourceTask.isTaskInitialized();
        assertThat(taskInitialized).isFalse();
        assertThat(s3SourceTask.getConnectorStopped()).isTrue();
    }

    private static S3SourceRecord getAivenS3SourceRecord() {
        final S3OffsetManagerEntry offsetManagerEntry = new S3OffsetManagerEntry("bucket", "s3ObjectKey", "testtopic",
                0);
        return new S3SourceRecord(offsetManagerEntry, "mock-key".getBytes(StandardCharsets.UTF_8),
                "mock-value".getBytes(StandardCharsets.UTF_8));
    }

    @SuppressWarnings("PMD.AvoidAccessibilityAlteration")
    private void setPrivateField(final Object object, final String fieldName, final Object value)
            throws NoSuchFieldException, IllegalAccessException {
        Field field;
        field = object.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(object, value);
    }

    private void startSourceTask(final S3SourceTask s3SourceTask) {
        s3SourceTask.initialize(mockedSourceTaskContext);
        when(mockedSourceTaskContext.offsetStorageReader()).thenReturn(mockedOffsetStorageReader);

        setBasicProperties();
        s3SourceTask.start(properties);
    }

    private void setBasicProperties() {
        properties.put(S3SourceConfig.INPUT_FORMAT_KEY, InputFormat.BYTES.getValue());
        properties.put("name", "test_source_connector");
        properties.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        properties.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        properties.put("tasks.max", "1");
        properties.put("connector.class", AivenKafkaConnectS3SourceConnector.class.getName());
        properties.put(TARGET_TOPIC_PARTITIONS, "0,1");
        properties.put(TARGET_TOPICS, "testtopic");
    }
}
