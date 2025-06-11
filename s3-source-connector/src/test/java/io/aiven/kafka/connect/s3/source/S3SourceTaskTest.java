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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import io.aiven.kafka.connect.common.config.CommonConfigFragment;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.KafkaFragment;
import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.config.TransformerFragment;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.task.Context;
import io.aiven.kafka.connect.common.utils.CasedString;
import io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry;
import io.aiven.kafka.connect.s3.source.utils.S3SourceRecord;
import io.aiven.kakfa.connect.s3.source.testdata.AWSIntegrationTestData;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.S3Object;

@DisabledOnOs(value = { OS.WINDOWS, OS.MAC }, disabledReason = "Container testing does not run on Mac and Windows")
@Testcontainers
final class S3SourceTaskTest {

    /** The default timeout when polling for records */
    private static final Duration TIMEOUT = Duration.ofSeconds(2);
    /**
     * The default polling interval.
     */
    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);

    /**
     * The Test info provided before each test. Tests may access this info wihout capturing it themselves.
     */
    private TestInfo testInfo;

    /**
     * The amount of extra time that we will allow for timing errors.
     */
    private static final long TIMING_DELTA = 500;

    private static final String TEST_BUCKET = "test-bucket";

    private static final String TEST_OBJECT_KEY = "object_key";

    @Container
    static final LocalStackContainer LOCALSTACK = AWSIntegrationTestData.createS3Container();

    private AWSIntegrationTestData testData;

    /**
     * Sets up the AWS test container.
     *
     * @param testInfo
     *            the test info.
     */
    @BeforeEach
    void setupAWS(final TestInfo testInfo) {
        this.testInfo = testInfo;
        testData = new AWSIntegrationTestData(LOCALSTACK);
    }

    @AfterEach
    void tearDownAWS() {
        testData.tearDown();
    }

    /**
     * Get the topic from the TestInfo.
     *
     * @return The topic extracted from the testInfo for the current test.
     */
    public String getTopic() {
        return testInfo.getTestMethod().get().getName();
    }

    /**
     * Get the topic from the TestInfo.
     *
     * @return The topic extracted from the testInfo for the current test.
     */
    public String getBucket() {
        return new CasedString(CasedString.StringCase.CAMEL, testInfo.getTestMethod().get().getName())
                .toCase(CasedString.StringCase.KEBAB)
                .toLowerCase(Locale.ROOT);
    }

    /**
     * Creates a mock source context that has no data.
     *
     * @return the mock SourceTaskContext.
     */
    private SourceTaskContext createSourceTaskContext() {
        final SourceTaskContext mockedSourceTaskContext = mock(SourceTaskContext.class);
        final OffsetStorageReader mockedOffsetStorageReader = mock(OffsetStorageReader.class);
        when(mockedSourceTaskContext.offsetStorageReader()).thenReturn(mockedOffsetStorageReader);
        return mockedSourceTaskContext;
    }

    /**
     * Creates a default configuration data map.
     *
     * @return the default configuration data map.
     */
    private Map<String, String> createDefaultConfig() {
        final Map<String, String> props = testData.createConnectorConfig(null, getBucket());
        final String name = new CasedString(CasedString.StringCase.CAMEL, TestingS3SourceTask.class.getSimpleName())
                .toCase(CasedString.StringCase.KEBAB)
                .toLowerCase(Locale.ROOT) + "-" + UUID.randomUUID();

        TransformerFragment.setter(props).inputFormat(InputFormat.BYTES);
        KafkaFragment.setter(props)
                .keyConverter(ByteArrayConverter.class)
                .valueConverter(ByteArrayConverter.class)
                .tasksMax(1)
                .name(name);
        CommonConfigFragment.setter(props).taskId(0);
        SourceConfigFragment.setter(props).targetTopic(getTopic()).maxPollRecords(50);
        FileNameFragment.setter(props).template("any-old-file");
        return props;
    }

    private static SdkException createSdkException() {
        final SdkException exception = mock(SdkException.class);
        when(exception.getMessage()).thenReturn("Fake SdkException");
        when(exception.getCause()).thenReturn(new RuntimeException("Fake SdkException"));
        when(exception.retryable()).thenReturn(true);
        return exception;
    }

    @Test
    void testExceptionInGetIterator() {
        final List<S3SourceRecord> records = createS3SourceRecords(5);
        final ExceptionThrowingIterator iterator = new ExceptionThrowingIterator(records);
        final TestingS3SourceTask s3SourceTask = new TestingS3SourceTask(iterator);

        s3SourceTask.initialize(createSourceTaskContext());
        s3SourceTask.start(createDefaultConfig());
        await().atMost(TIMEOUT).until(s3SourceTask::isRunning);
        final List<SourceRecord> result = new ArrayList<>();
        await().atMost(TIMEOUT).pollInterval(POLL_INTERVAL).untilAsserted(() -> {
            final List<SourceRecord> pollResult = s3SourceTask.poll();
            if (pollResult != null) {
                result.addAll(pollResult);
            }
            assertThat(result).hasSize(5);
        });
        assertThat(iterator.wasExceptionThrown()).isTrue();
    }

    @Test
    void testGetIterator() {
        final List<S3SourceRecord> records = createS3SourceRecords(5);
        final TestingS3SourceTask s3SourceTask = new TestingS3SourceTask(records.iterator());

        s3SourceTask.initialize(createSourceTaskContext());
        s3SourceTask.start(createDefaultConfig());
        await().atMost(TIMEOUT).until(s3SourceTask::isRunning);
        final List<SourceRecord> result = new ArrayList<>();
        await().atMost(TIMEOUT).pollInterval(POLL_INTERVAL).untilAsserted(() -> {
            final List<SourceRecord> pollResult = s3SourceTask.poll();
            if (pollResult != null) {
                result.addAll(pollResult);
            }
            assertThat(result).hasSize(5);
        });
        assertThat(result).hasSize(5);
    }

    private static S3SourceRecord createS3SourceRecord(final String bucket, final String objectKey, final byte[] key,
            final byte[] value) {

        final S3SourceRecord result = new S3SourceRecord(
                S3Object.builder().key(objectKey).size((long) value.length).build());
        result.setOffsetManagerEntry(new S3OffsetManagerEntry(bucket, objectKey));
        result.setKeyData(new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, key));
        result.setValueData(new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, value));
        final Context<String> context = new Context<>(objectKey);
        context.setTopic("topic");
        context.setPartition(null);
        result.setContext(context);
        return result;
    }

    private List<S3SourceRecord> createS3SourceRecords(final int count) {
        final List<S3SourceRecord> lst = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            lst.add(createS3SourceRecord(TEST_BUCKET, TEST_OBJECT_KEY, ("Hello " + i).getBytes(StandardCharsets.UTF_8),
                    ("Hello World" + i).getBytes(StandardCharsets.UTF_8)));
        }
        return lst;
    }

    @Test
    void testPollWithSlowProducer() {
        final List<S3SourceRecord> lst = createS3SourceRecords(3);

        // an iterator that returns records in 6 second intervals.
        final Iterator<S3SourceRecord> sourceRecordIterator = new Iterator<>() {
            final Iterator<S3SourceRecord> inner = lst.iterator();
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

        final S3SourceTask s3SourceTask = new TestingS3SourceTask(sourceRecordIterator);
        s3SourceTask.initialize(createSourceTaskContext());
        s3SourceTask.start(createDefaultConfig());
        final int[] counter = { 0 };
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofSeconds(1)).until(() -> {
            final List<SourceRecord> results = s3SourceTask.poll();
            if (results != null) {
                counter[0] += results.size();
            }
            return counter[0] == 3;
        });
        assertThat(counter[0]).isEqualTo(3);
    }

    @Test
    void testPollsWithExcessRecords() {
        // test that multiple polls to get all records succeeds.
        final Map<String, String> properties = createDefaultConfig();
        SourceConfigFragment.setter(properties).maxPollRecords(2);

        final List<S3SourceRecord> lst = createS3SourceRecords(3);

        final Iterator<S3SourceRecord> sourceRecordIterator = lst.iterator();
        final S3SourceTask s3SourceTask = new TestingS3SourceTask(sourceRecordIterator);
        s3SourceTask.initialize(createSourceTaskContext());
        s3SourceTask.start(properties);
        final int[] counter = { 0, 0 };
        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofSeconds(1)).until(() -> {
            final List<SourceRecord> results = s3SourceTask.poll();
            if (results != null) {
                counter[0] += results.size();
                counter[1]++;
            }
            return counter[0] == 3;
        });
        assertThat(counter[1]).isEqualTo(2);
    }

    @Test
    void testPollWhenConnectorStopped() {
        final List<S3SourceRecord> lst = createS3SourceRecords(3);
        final Iterator<S3SourceRecord> sourceRecordIterator = lst.iterator();
        final S3SourceTask s3SourceTask = new TestingS3SourceTask(sourceRecordIterator);

        s3SourceTask.initialize(createSourceTaskContext());
        s3SourceTask.start(createDefaultConfig());
        s3SourceTask.stop();
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        final List<SourceRecord> results = s3SourceTask.poll();
        stopWatch.stop();
        assertThat(results).isNull();
        assertThat(stopWatch.getTime()).isLessThan(TIMING_DELTA);
    }

    private static class TestingS3SourceTask extends S3SourceTask { // NOPMD not a test class

        private final Iterator<S3SourceRecord> sourceRecordIterator;

        TestingS3SourceTask(final Iterator<S3SourceRecord> realIterator) {
            super();
            super.s3SourceRecordIterator = realIterator;
            this.sourceRecordIterator = realIterator;
        }

        public SourceTaskContext getContext() {
            return context;
        }

        @Override
        protected SourceCommonConfig configure(final Map<String, String> props) {
            final SourceCommonConfig cfg = super.configure(props);
            if (sourceRecordIterator != null) {
                super.s3SourceRecordIterator = sourceRecordIterator;
            }
            return cfg;
        }
    }

    private final static class ExceptionThrowingIterator implements Iterator<S3SourceRecord> {
        private boolean retry;
        private boolean exceptionThrown;
        private final SdkException exception;
        private final Iterator<S3SourceRecord> innerIterator;

        ExceptionThrowingIterator(final List<S3SourceRecord> records) {
            innerIterator = records.iterator();
            exception = createSdkException();
        }

        @Override
        public boolean hasNext() {
            retry = !retry;
            if (retry) {
                exceptionThrown = true;
                throw exception;
            }
            return innerIterator.hasNext();
        }

        @Override
        public S3SourceRecord next() {
            if (retry) {
                throw new IllegalStateException("Should not call next()");
            }
            return innerIterator.next();
        }

        public boolean wasExceptionThrown() {
            return exceptionThrown;
        }
    }
}
