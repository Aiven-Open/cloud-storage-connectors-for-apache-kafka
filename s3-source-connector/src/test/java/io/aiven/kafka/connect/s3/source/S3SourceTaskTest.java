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

import static io.aiven.kafka.connect.common.config.SchemaRegistryFragment.INPUT_FORMAT_KEY;
import static io.aiven.kafka.connect.common.config.SourceConfigFragment.TARGET_TOPICS;
import static io.aiven.kafka.connect.common.config.SourceConfigFragment.TARGET_TOPIC_PARTITIONS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import io.aiven.kafka.connect.common.source.input.ByteArrayTransformer;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;
import io.aiven.kafka.connect.iam.AwsCredentialProviderFactory;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.testutils.BucketAccessor;
import io.aiven.kafka.connect.s3.source.utils.S3SourceRecord;
import io.aiven.kafka.connect.s3.source.utils.SourceRecordIterator;

import io.findify.s3mock.S3Mock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

@ExtendWith(MockitoExtension.class)
final class S3SourceTaskTest {

    private static final Random RANDOM = new Random();
    private Map<String, String> properties;

    private static BucketAccessor testBucketAccessor;
    private static final String TEST_BUCKET = "test-bucket";
    // TODO S3Mock has not been maintained in 4 years
    // Adobe have an alternative we can move to.
    private static S3Mock s3Api;
    private static S3Client s3Client;

    private static Map<String, String> commonProperties;

    @Mock
    private SourceTaskContext mockedSourceTaskContext;

    @Mock
    private OffsetStorageReader mockedOffsetStorageReader;

    @BeforeAll
    public static void setUpClass() throws URISyntaxException {
        final int s3Port = RANDOM.nextInt(10_000) + 10_000;

        s3Api = new S3Mock.Builder().withPort(s3Port).withInMemoryBackend().build();
        s3Api.start();

        commonProperties = Map.of(S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG, "test_key_id",
                S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG, "test_secret_key",
                S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG, TEST_BUCKET, S3ConfigFragment.AWS_S3_ENDPOINT_CONFIG,
                "http://localhost:" + s3Port, S3ConfigFragment.AWS_S3_REGION_CONFIG, "us-west-2");

        final AwsCredentialProviderFactory credentialFactory = new AwsCredentialProviderFactory();
        final S3SourceConfig config = new S3SourceConfig(commonProperties);
        final ClientOverrideConfiguration clientOverrideConfiguration = ClientOverrideConfiguration.builder()
                .retryStrategy(RetryMode.STANDARD)
                .build();

        s3Client = S3Client.builder()
                .overrideConfiguration(clientOverrideConfiguration)
                .region(config.getAwsS3Region())
                .endpointOverride(URI.create(config.getAwsS3EndPoint()))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .credentialsProvider(credentialFactory.getAwsV2Provider(config.getS3ConfigFragment()))
                .build();

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
        s3Client.createBucket(create -> create.bucket(TEST_BUCKET).build());
        mockedSourceTaskContext = mock(SourceTaskContext.class);
        mockedOffsetStorageReader = mock(OffsetStorageReader.class);
    }

    @AfterEach
    public void tearDown() {
        s3Client.deleteBucket(delete -> delete.bucket(TEST_BUCKET).build());
    }

    @Test
    void testS3SourceTaskInitialization() {
        final S3SourceTask s3SourceTask = new S3SourceTask();
        startSourceTask(s3SourceTask);

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
        return new S3SourceRecord(new HashMap<>(), new HashMap<>(), "testtopic", 0, "",
                new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, new byte[0]),
                new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, new byte[0]));
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
        properties.put(INPUT_FORMAT_KEY, InputFormat.BYTES.getValue());
        properties.put("name", "test_source_connector");
        properties.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        properties.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        properties.put("tasks.max", "1");
        properties.put("connector.class", AivenKafkaConnectS3SourceConnector.class.getName());
        properties.put(TARGET_TOPIC_PARTITIONS, "0,1");
        properties.put(TARGET_TOPICS, "testtopic");

    }
}
