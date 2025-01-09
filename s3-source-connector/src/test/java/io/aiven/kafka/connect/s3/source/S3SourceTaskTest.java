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
import static io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry.BUCKET;
import static io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry.OBJECT_KEY;
import static io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry.RECORD_COUNT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.source.AbstractSourceTask;
import io.aiven.kafka.connect.common.source.input.ByteArrayTransformer;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.config.s3.S3ConfigFragment;
import io.aiven.kafka.connect.iam.AwsCredentialProviderFactory;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry;
import io.aiven.kafka.connect.s3.source.utils.S3SourceRecord;

import io.findify.s3mock.S3Mock;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

final class S3SourceTaskTest {

    /**
     * The amount of extra time that we will allow for timing errors.
     */
    private static final long TIMING_DELTA = 500;

    private static final Random RANDOM = new Random();
    private Map<String, String> properties;

    private static final String TEST_BUCKET = "test-bucket";

    private static final String TOPIC = "TOPIC1";

    private static final int PARTITION = 1;

    private static final String TEST_OBJECT_KEY = "object_key";

    // TODO S3Mock has not been maintained in 4 years
    // Adobe have an alternative we can move to.
    private static S3Mock s3Api;
    private static S3Client s3Client;

    private static Map<String, String> commonProperties;

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
    }

    @AfterAll
    public static void tearDownClass() {
        s3Api.stop();
    }

    @BeforeEach
    public void setUp() {
        properties = new HashMap<>(commonProperties);
        s3Client.createBucket(create -> create.bucket(TEST_BUCKET).build());
    }

    @AfterEach
    public void tearDown() {
        s3Client.deleteBucket(delete -> delete.bucket(TEST_BUCKET).build());
    }

    @Test
    void testS3SourceTaskInitialization() {
        final S3SourceTask s3SourceTask = new S3SourceTask();
        startSourceTask(s3SourceTask);

        assertThat(s3SourceTask.getTransformer()).isInstanceOf(ByteArrayTransformer.class);

        assertThat(s3SourceTask.isRunning()).isTrue();
    }

    @Test
    void testStop() {
        final S3SourceTask s3SourceTask = new S3SourceTask();
        startSourceTask(s3SourceTask);
        s3SourceTask.stop();

        assertThat(s3SourceTask.isRunning()).isFalse();
    }

    private static S3SourceRecord createS3SourceRecord(final String topicName, final Integer defaultPartitionId,
            final String bucketName, final String objectKey, final byte[] key, final byte[] value) {
        return new S3SourceRecord(new S3OffsetManagerEntry(bucketName, objectKey, topicName, defaultPartitionId),
                new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, key),
                new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, value));
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
        properties.putIfAbsent(INPUT_FORMAT_KEY, InputFormat.BYTES.getValue());
        properties.putIfAbsent("name", "test_source_connector");
        properties.putIfAbsent("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        properties.putIfAbsent("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        properties.putIfAbsent("tasks.max", "1");
        properties.putIfAbsent("connector.class", AivenKafkaConnectS3SourceConnector.class.getName());
        properties.putIfAbsent(TARGET_TOPIC_PARTITIONS, "0,1");
        properties.putIfAbsent(TARGET_TOPICS, "testtopic");

    }

    @Test
    void testPollWithNoDataReturned() {
        final S3SourceConfig s3SourceConfig = mock(S3SourceConfig.class);
        when(s3SourceConfig.getMaxPollRecords()).thenReturn(5);
        final Iterator<S3SourceRecord> sourceRecordIterator = Collections.emptyIterator();
        final S3SourceTask s3SourceTask = new TestingS3SourceTask(sourceRecordIterator);

        startSourceTask(s3SourceTask);
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        final List<SourceRecord> results = s3SourceTask.poll();
        stopWatch.stop();
        assertThat(results).isNull();
        assertThat(stopWatch.getTime()).isLessThan(AbstractSourceTask.MAX_POLL_TIME.toMillis() + TIMING_DELTA);
    }

    private void assertEquals(final S3SourceRecord s3Record, final SourceRecord sourceRecord) {
        assertThat(sourceRecord).isNotNull();

        assertThat(sourceRecord.sourcePartition()).hasSize(2);
        assertThat(sourceRecord.sourcePartition().get(BUCKET)).isEqualTo(s3Record.getOffsetManagerEntry().getBucket());
        assertThat(sourceRecord.sourcePartition().get(OBJECT_KEY)).isEqualTo(s3Record.getOffsetManagerEntry().getKey());

        final Map<String, Object> map = (Map<String, Object>) sourceRecord.sourceOffset();

        assertThat(map.get(RECORD_COUNT)).isEqualTo(s3Record.getOffsetManagerEntry().getRecordCount());
        assertThat(sourceRecord.key()).isEqualTo(s3Record.getKey().value());
        assertThat(sourceRecord.value()).isEqualTo(s3Record.getValue().value());
    }

    @Test
    void testPollsWithRecords() {
        final List<S3SourceRecord> lst = createS3SourceRecords(2);
        final Iterator<S3SourceRecord> sourceRecordIterator = lst.iterator();
        final S3SourceTask s3SourceTask = new TestingS3SourceTask(sourceRecordIterator);

        startSourceTask(s3SourceTask);
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        final List<SourceRecord> results = s3SourceTask.poll();
        stopWatch.stop();

        assertThat(results).hasSize(2);
        assertEquals(lst.get(0), results.get(0));
        assertEquals(lst.get(1), results.get(1));
        assertThat(stopWatch.getTime()).isLessThan(AbstractSourceTask.MAX_POLL_TIME.toMillis());
    }

    private List<S3SourceRecord> createS3SourceRecords(final int count) {
        final List<S3SourceRecord> lst = new ArrayList<>();
        if (count > 0) {

            lst.add(createS3SourceRecord(TOPIC, PARTITION, TEST_BUCKET, TEST_OBJECT_KEY,
                    "Hello".getBytes(StandardCharsets.UTF_8), "Hello World".getBytes(StandardCharsets.UTF_8)));
            for (int i = 1; i < count; i++) {
                lst.add(createS3SourceRecord(TOPIC, PARTITION, TEST_BUCKET, TEST_OBJECT_KEY + i,
                        "Goodbye".getBytes(StandardCharsets.UTF_8),
                        String.format("Goodbye cruel World (%s)", i).getBytes(StandardCharsets.UTF_8)));
            }
        }
        return lst;
    }

    @Test
    void testPollWithInterruptedIterator() {
        final List<S3SourceRecord> lst = createS3SourceRecords(3);

        final Iterator<S3SourceRecord> inner1 = lst.subList(0, 2).iterator();
        final Iterator<S3SourceRecord> inner2 = lst.subList(2, 3).iterator();
        final Iterator<S3SourceRecord> sourceRecordIterator = new Iterator<>() {
            Iterator<S3SourceRecord> inner = inner1;
            @Override
            public boolean hasNext() {
                if (inner == null) {
                    inner = inner2;
                    return false;
                }
                return inner.hasNext();
            }

            @Override
            public S3SourceRecord next() {
                final S3SourceRecord result = inner.next();
                if (!inner.hasNext()) {
                    inner = null; // NOPMD null assignment
                }
                return result;
            }
        };

        final S3SourceTask s3SourceTask = new TestingS3SourceTask(sourceRecordIterator);
        startSourceTask(s3SourceTask);
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<SourceRecord> results = s3SourceTask.poll();
        stopWatch.stop();

        assertThat(results).hasSize(2);
        assertEquals(lst.get(0), results.get(0));
        assertEquals(lst.get(1), results.get(1));

        results = s3SourceTask.poll();
        assertThat(results).hasSize(1);

        assertThat(stopWatch.getTime()).isLessThan(AbstractSourceTask.MAX_POLL_TIME.toMillis());

    }

    @Test
    void testPollWithSlowProducer() {
        final List<S3SourceRecord> lst = createS3SourceRecords(3);

        final Iterator<S3SourceRecord> sourceRecordIterator = new Iterator<>() {
            Iterator<S3SourceRecord> inner = lst.iterator();
            @Override
            public boolean hasNext() {
                return inner.hasNext();
            }

            @Override
            public S3SourceRecord next() {
                try {
                    Thread.sleep(Duration.ofSeconds(6).toMillis());
                } catch (InterruptedException e) {
                    // do nothing.
                }
                return inner.next();
            }
        };

        final List<SourceRecord> results = new ArrayList<>();
        // since the polling is returning data at or near the time limit the 3 record may be returned as follows
        // Record 1 may be returned in Poll1 or Poll2
        // Record 2 may be returned in Poll2 or Poll2
        // Record 3 may be returned in Poll3 or Poll4

        final S3SourceTask s3SourceTask = new TestingS3SourceTask(sourceRecordIterator);
        startSourceTask(s3SourceTask);
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        // poll 1
        List<SourceRecord> pollResult = s3SourceTask.poll();
        stopWatch.stop();
        if (pollResult != null) {
            results.addAll(pollResult);
        }
        assertThat(results).hasSizeLessThanOrEqualTo(1);
        // poll 2
        stopWatch.reset();
        stopWatch.start();
        pollResult = s3SourceTask.poll();
        stopWatch.stop();
        if (pollResult != null) {
            results.addAll(pollResult);
        }
        assertThat(results).hasSizeLessThanOrEqualTo(2);
        // poll 3
        stopWatch.reset();
        stopWatch.start();
        pollResult = s3SourceTask.poll();
        stopWatch.stop();
        if (pollResult != null) {
            results.addAll(pollResult);
        }
        assertThat(results).hasSizeLessThanOrEqualTo(3);
        // poll 4
        stopWatch.reset();
        stopWatch.start();
        pollResult = s3SourceTask.poll();
        stopWatch.stop();
        if (results.size() == lst.size()) {
            assertThat(pollResult).isNull();
        } else {
            results.addAll(pollResult);
        }
        assertThat(results).hasSize(3);
    }

    @Test
    void testPollsWithExcessRecords() {
        // test that multiple polls to get all records succeeds.
        properties.put(SourceConfigFragment.MAX_POLL_RECORDS, "2");

        final List<S3SourceRecord> lst = createS3SourceRecords(3);

        final Iterator<S3SourceRecord> sourceRecordIterator = lst.iterator();
        final S3SourceTask s3SourceTask = new TestingS3SourceTask(sourceRecordIterator);

        startSourceTask(s3SourceTask);
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<SourceRecord> results = s3SourceTask.poll();
        assertThat(results).hasSize(2);
        results = s3SourceTask.poll();
        assertThat(results).hasSize(1);
        stopWatch.stop();
        assertThat(stopWatch.getTime()).isLessThan(AbstractSourceTask.MAX_POLL_TIME.toMillis() * 2);
    }

    @Test
    void testPollWhenConnectorStopped() {
        final List<S3SourceRecord> lst = createS3SourceRecords(3);
        final Iterator<S3SourceRecord> sourceRecordIterator = lst.iterator();
        final S3SourceTask s3SourceTask = new TestingS3SourceTask(sourceRecordIterator);

        startSourceTask(s3SourceTask);
        s3SourceTask.stop();
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        final List<SourceRecord> results = s3SourceTask.poll();
        stopWatch.stop();
        assertThat(results).isNull();
        assertThat(stopWatch.getTime()).isLessThan(TIMING_DELTA);

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
