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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import io.aiven.kafka.connect.common.source.input.ByteArrayTransformer;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
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

final class S3SourceTaskTest {

    private static final Random RANDOM = new Random();
    private Map<String, String> properties;

    private static final String TEST_BUCKET = "test-bucket";

    private static final String TOPIC = "TOPIC1";

    private static final int PARTITION = 1;

    private static final String OBJECT_KEY = "object_key";

    private static S3Mock s3Api;
    private static AmazonS3 s3Client;

    private static Map<String, String> commonProperties;

    @BeforeAll
    public static void setUpClass() {
        final int s3Port = RANDOM.nextInt(10_000) + 10_000;

        s3Api = new S3Mock.Builder().withPort(s3Port).withInMemoryBackend().build();
        s3Api.start();

        commonProperties = Map.of(S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG, "test_key_id",
                S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG, "test_secret_key",
                S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG, TEST_BUCKET, S3ConfigFragment.AWS_S3_ENDPOINT_CONFIG,
                "http://localhost:" + s3Port, S3ConfigFragment.AWS_S3_REGION_CONFIG, "us-west-2");

        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        final BasicAWSCredentials awsCreds = new BasicAWSCredentials(
                commonProperties.get(S3ConfigFragment.AWS_ACCESS_KEY_ID_CONFIG),
                commonProperties.get(S3ConfigFragment.AWS_SECRET_ACCESS_KEY_CONFIG));
        builder.withCredentials(new AWSStaticCredentialsProvider(awsCreds));
        builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                commonProperties.get(S3ConfigFragment.AWS_S3_ENDPOINT_CONFIG),
                commonProperties.get(S3ConfigFragment.AWS_S3_REGION_CONFIG)));
        builder.withPathStyleAccessEnabled(true);

        s3Client = builder.build();

        final BucketAccessor testBucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET);
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
        s3SourceTask.setS3SourceRecordIterator(mockSourceRecordIterator);
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
        assertThat(taskInitialized).isTrue();
        assertThat(s3SourceTask.isRunning()).isFalse();
    }

    private static S3SourceRecord getAivenS3SourceRecord() {
        final S3OffsetManagerEntry entry = new S3OffsetManagerEntry(TEST_BUCKET, OBJECT_KEY, TOPIC, PARTITION);
        return new S3SourceRecord(entry, Optional.empty(), new SchemaAndValue(null, new byte[0]));
    }

    private void startSourceTask(final S3SourceTask s3SourceTask) {
        final SourceTaskContext mockedSourceTaskContext = mock(SourceTaskContext.class);
        final OffsetStorageReader mockedOffsetStorageReader = mock(OffsetStorageReader.class);
        when(mockedSourceTaskContext.offsetStorageReader()).thenReturn(mockedOffsetStorageReader);
        s3SourceTask.initialize(mockedSourceTaskContext);

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

    @Test
    void testExtractSourceRecordsWithEmptyIterator() {
        final S3SourceConfig s3SourceConfig = mock(S3SourceConfig.class);
        when(s3SourceConfig.getMaxPollRecords()).thenReturn(5);
        final Iterator<S3SourceRecord> sourceRecordIterator = Collections.emptyIterator();

        final S3SourceTask s3SourceTask = new TestingS3SourceTask(sourceRecordIterator);
        startSourceTask(s3SourceTask);

        final List<SourceRecord> results = s3SourceTask.poll();
        assertThat(results).isEmpty();
    }

    private void assertEquals(final S3SourceRecord s3Record, final SourceRecord sourceRecord) {
        assertThat(sourceRecord).isNotNull();
        final S3OffsetManagerEntry offsetManagerEntry = s3Record.getOffsetManagerEntry();
        assertThat(sourceRecord.sourcePartition()).isEqualTo(offsetManagerEntry.getManagerKey().getPartitionMap());
        assertThat(sourceRecord.sourceOffset()).isEqualTo(offsetManagerEntry.getProperties());
        assertThat(sourceRecord.key()).isEqualTo(s3Record.key());
        assertThat(sourceRecord.value()).isEqualTo(s3Record.value().value());
    }

    private SchemaAndValue valueOf(final Object value) {
        return new SchemaAndValue(null, value);
    }

    private Optional<SchemaAndValue> keyOf(final Object value) {
        return value == null ? Optional.empty() : Optional.of(new SchemaAndValue(null, value));
    }

    @Test
    void testExtractSourceRecordsWithRecords() {
        final S3SourceConfig s3SourceConfig = mock(S3SourceConfig.class);
        when(s3SourceConfig.getMaxPollRecords()).thenReturn(5);
        final List<S3SourceRecord> lst = new ArrayList<>();
        S3OffsetManagerEntry offsetManagerEntry = new S3OffsetManagerEntry(TEST_BUCKET, OBJECT_KEY, TOPIC, PARTITION);
        lst.add(new S3SourceRecord(offsetManagerEntry, keyOf("Hello"), valueOf("Hello World")));
        offsetManagerEntry = new S3OffsetManagerEntry(TEST_BUCKET, OBJECT_KEY + "a", TOPIC, PARTITION);
        lst.add(new S3SourceRecord(offsetManagerEntry, keyOf("Goodbye"), valueOf("Goodbye cruel World")));

        final Iterator<S3SourceRecord> sourceRecordIterator = lst.iterator();

        final S3SourceTask s3SourceTask = new TestingS3SourceTask(sourceRecordIterator);
        startSourceTask(s3SourceTask);

        final List<SourceRecord> results = s3SourceTask.poll();
        assertThat(results).hasSize(2);
        assertEquals(lst.get(0), results.get(0));
        assertEquals(lst.get(1), results.get(1));
    }

    @Test
    void testExtractSourceRecordsWhenConnectorStopped() {
        final S3SourceConfig s3SourceConfig = mock(S3SourceConfig.class);
        when(s3SourceConfig.getMaxPollRecords()).thenReturn(5);
        final List<S3SourceRecord> lst = new ArrayList<>();
        S3OffsetManagerEntry offsetManagerEntry = new S3OffsetManagerEntry(TEST_BUCKET, OBJECT_KEY, TOPIC, PARTITION);
        lst.add(new S3SourceRecord(offsetManagerEntry, keyOf("Hello"), valueOf("Hello World")));
        offsetManagerEntry = new S3OffsetManagerEntry(TEST_BUCKET, OBJECT_KEY + "a", TOPIC, PARTITION);
        lst.add(new S3SourceRecord(offsetManagerEntry, keyOf("Goodbye"), valueOf("Goodbye cruel World")));

        final Iterator<S3SourceRecord> sourceRecordIterator = lst.iterator();

        final S3SourceTask s3SourceTask = new TestingS3SourceTask(sourceRecordIterator);
        startSourceTask(s3SourceTask);
        s3SourceTask.stop();

        final List<SourceRecord> results = s3SourceTask.poll();
        assertThat(results).isEmpty();
    }

    private static class TestingS3SourceTask extends S3SourceTask { // NOPMD not a test class

        TestingS3SourceTask(final Iterator<S3SourceRecord> realIterator) {
            super();
            super.setS3SourceRecordIterator(realIterator);
        }

        @Override
        protected void setS3SourceRecordIterator(final Iterator<S3SourceRecord> iterator) {
            // do nothing.
        }
    }
}
