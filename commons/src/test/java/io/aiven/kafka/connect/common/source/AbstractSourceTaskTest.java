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

package io.aiven.kafka.connect.common.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import io.aiven.kafka.connect.common.config.CommonConfigFragment;
import io.aiven.kafka.connect.common.config.FileNameFragment;
import io.aiven.kafka.connect.common.config.KafkaFragment;
import io.aiven.kafka.connect.common.config.OutputFormatFragment;
import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.config.TransformerFragment;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.utils.CasedString;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the AbstractSourceTask.
 */
class AbstractSourceTaskTest {

    /**
     * The amount of extra time that we will allow for timing errors.
     */
    private static final long TIMING_DELTA_MS = 250;
    /** The testing topic */
    private static final String TOPIC = "test-topic";
    /** The default timeout when polling for records */
    private static final Duration TIMEOUT = Duration.ofSeconds(2);
    /**
     * The default polling interval.
     */
    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);

    @Test
    void timerTest() {
        final AbstractSourceTask.Timer timer = new AbstractSourceTask.Timer(Duration.ofSeconds(1));
        assertThat(timer.millisecondsRemaining()).isEqualTo(Duration.ofSeconds(1).toMillis());
        timer.start();
        await().atMost(Duration.ofSeconds(2)).until(timer::isExpired);
        assertThat(timer.millisecondsRemaining()).isLessThan(0);
        timer.stop();
        assertThat(timer.millisecondsRemaining()).isEqualTo(Duration.ofSeconds(1).toMillis());
    }

    @Test
    void timerSequenceTest() {
        final AbstractSourceTask.Timer timer = new AbstractSourceTask.Timer(Duration.ofSeconds(1));
        // stopped state does not allow stop
        assertThatExceptionOfType(IllegalStateException.class).as("stop while not running")
                .isThrownBy(timer::stop)
                .withMessageStartingWith("Timer: ");
        timer.reset(); // verify that an exception is not thrown.

        // started state does not allow start
        timer.start();
        assertThatExceptionOfType(IllegalStateException.class).as("start while running")
                .isThrownBy(timer::start)
                .withMessageStartingWith("Timer: ");
        timer.reset();
        timer.start(); // restart the timer.
        timer.stop();

        // stopped state does not allow stop or start
        assertThatExceptionOfType(IllegalStateException.class).as("stop after stop")
                .isThrownBy(timer::stop)
                .withMessageStartingWith("Timer: ");
        assertThatExceptionOfType(IllegalStateException.class).as("start after stop")
                .isThrownBy(timer::start)
                .withMessageStartingWith("Timer: ");
        timer.reset();

        // stopped + reset does not allow stop.
        assertThatExceptionOfType(IllegalStateException.class).as("stop after reset (1)")
                .isThrownBy(timer::stop)
                .withMessageStartingWith("Timer: ");
        timer.start();
        timer.reset();

        // started + reset does not allow stop;
        assertThatExceptionOfType(IllegalStateException.class).as("stop after reset (2)")
                .isThrownBy(timer::stop)
                .withMessageStartingWith("Timer: ");
    }

    @Test
    void backoffTest() throws InterruptedException {
        final AbstractSourceTask.Timer timer = new AbstractSourceTask.Timer(Duration.ofSeconds(1));
        final AbstractSourceTask.Backoff backoff = new AbstractSourceTask.Backoff(timer.getBackoffConfig());
        final long estimatedDelay = backoff.estimatedDelay();
        assertThat(estimatedDelay).isLessThan(500);

        // execute delay without timer running.
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        backoff.delay();
        stopWatch.stop();
        assertThat(stopWatch.getTime()).as("Result without timer running")
                .isBetween(estimatedDelay - backoff.getMaxJitter() - TIMING_DELTA_MS,
                        estimatedDelay + backoff.getMaxJitter() + TIMING_DELTA_MS);

        timer.start();
        for (int i = 0; i < 9; i++) {
            stopWatch.reset();
            timer.reset();
            timer.start();
            stopWatch.start();
            await().atMost(Duration.ofSeconds(2)).until(() -> {
                backoff.delay();
                return backoff.estimatedDelay() == 0 || timer.isExpired();
            });
            stopWatch.stop();
            timer.stop();
            final int step = i;
            if (!timer.isExpired()) {
                assertThat(stopWatch.getTime()).as(() -> String.format("Result with timer running at step %s", step))
                        .isBetween(Duration.ofSeconds(1).toMillis() - backoff.getMaxJitter() - TIMING_DELTA_MS,
                                Duration.ofSeconds(1).toMillis() + backoff.getMaxJitter() + TIMING_DELTA_MS);
            }
        }
    }

    @Test
    void backoffIncrementalTimeTest() throws InterruptedException {
        final AtomicBoolean abortTrigger = new AtomicBoolean();
        // delay increases in powers of 2.
        final long maxDelay = 1000; // not a power of 2
        final AbstractSourceTask.BackoffConfig config = new AbstractSourceTask.BackoffConfig() {
            @Override
            public AbstractSourceTask.SupplierOfLong getSupplierOfTimeRemaining() {
                return () -> maxDelay;
            }

            @Override
            public AbstractSourceTask.AbortTrigger getAbortTrigger() {
                return () -> abortTrigger.set(true);
            }
        };

        final AbstractSourceTask.Backoff backoff = new AbstractSourceTask.Backoff(config);
        long expected = 2;
        while (backoff.estimatedDelay() < maxDelay) {
            assertThat(backoff.estimatedDelay()).isEqualTo(expected);
            backoff.delay();
            expected *= 2;
            assertThat(abortTrigger).isFalse();
        }
        assertThat(backoff.estimatedDelay()).isEqualTo(maxDelay);
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
        final Map<String, String> props = new HashMap<>();
        final String name = new CasedString(CasedString.StringCase.CAMEL, TestingSourceTask.class.getSimpleName())
                .toCase(CasedString.StringCase.KEBAB)
                .toLowerCase(Locale.ROOT) + "-" + UUID.randomUUID();

        TransformerFragment.setter(props).inputFormat(InputFormat.BYTES);
        KafkaFragment.setter(props)
                .keyConverter(ByteArrayConverter.class)
                .valueConverter(ByteArrayConverter.class)
                .tasksMax(1)
                .name(name);
        CommonConfigFragment.setter(props).taskId(0);
        SourceConfigFragment.setter(props).targetTopic(TOPIC).maxPollRecords(50);

        return props;
    }

    /**
     * Creates a simple source record.
     *
     * @param key
     *            The key for the source record.
     * @param value
     *            the value for the source record.
     * @return a SourceRecord.
     */
    private static SourceRecord createSourceRecord(final byte[] key, final byte[] value) {
        return new SourceRecord(new HashMap<>(), new HashMap<>(), TOPIC, Schema.BYTES_SCHEMA, key, Schema.BYTES_SCHEMA,
                value);
    }

    /**
     * Creates a list of source records.
     *
     * @param count
     *            the number of records to create
     * @return the list of unique SourceRecords.
     */
    private List<SourceRecord> createSourceRecords(final int count) {
        final List<SourceRecord> lst = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            lst.add(createSourceRecord(("Key " + i).getBytes(StandardCharsets.UTF_8),
                    ("Hello World " + i).getBytes(StandardCharsets.UTF_8)));
        }
        return lst;
    }

    @Test
    void initializationTest() {
        final TestingSourceTask sourceTask = new TestingSourceTask(Collections.emptyIterator());
        sourceTask.initialize(createSourceTaskContext());
        assertThat(sourceTask.getContext()).isNotNull();
        assertThat(sourceTask.isRunning()).isFalse();
    }

    @Test
    void testStartAndStop() {
        final TestingSourceTask sourceTask = new TestingSourceTask(Collections.emptyIterator());
        sourceTask.initialize(createSourceTaskContext());
        sourceTask.start(createDefaultConfig());
        assertThat(sourceTask.isRunning()).isTrue();
        sourceTask.stop();
        assertThat(sourceTask.isRunning()).isFalse();
    }

    @Test
    void testPollWithNoDataReturned() {
        final TestingSourceTask sourceTask = new TestingSourceTask(Collections.emptyIterator());
        sourceTask.initialize(createSourceTaskContext());
        try {
            sourceTask.start(createDefaultConfig());
            assertThat(sourceTask.isRunning()).isTrue();

            assertThat(sourceTask.poll()).isNull();
            assertThat(sourceTask.isRunning()).isTrue();
            assertThat(sourceTask.getBackoff().estimatedDelay()).isGreaterThan(0);
        } finally {
            sourceTask.stop();
        }
    }

    /**
     * Start the sourceTask and poll until the expected number of records is achieved or TIMEOUT is reached.
     *
     * @param sourceTask
     *            the SourceTask to start.
     * @param config
     *            the configuration data for the source task.
     * @param expectedRecordCount
     *            the number of expected recrods.
     * @return the list of records that were read.
     */
    private List<SourceRecord> startAndPoll(final AbstractSourceTask sourceTask, final Map<String, String> config,
            final int expectedRecordCount) {
        sourceTask.start(config);
        await().atMost(TIMEOUT).until(sourceTask::isRunning);
        final List<SourceRecord> result = new ArrayList<>();
        await().atMost(TIMEOUT).pollInterval(POLL_INTERVAL).untilAsserted(() -> {
            final List<SourceRecord> pollResult = sourceTask.poll();
            if (pollResult != null) {
                result.addAll(pollResult);
            }
            assertThat(result).hasSize(expectedRecordCount);
        });
        return result;
    }

    @Test
    void testPollsWithRecords() {
        final List<SourceRecord> lst = createSourceRecords(2);
        final TestingSourceTask sourceTask = new TestingSourceTask(lst.iterator());
        try {
            sourceTask.initialize(createSourceTaskContext());
            final List<SourceRecord> result = startAndPoll(sourceTask, createDefaultConfig(), 2);
            assertThat(result).contains(lst.toArray(new SourceRecord[0]));
        } finally {
            sourceTask.stop();
        }
    }

    @Test
    void testPollWithInterruptedIterator() {
        final Iterator<SourceRecord> emptyIterator = null;
        final List<SourceRecord> lst = createSourceRecords(3);

        final Iterator<SourceRecord> inner1 = lst.subList(0, 2).iterator();
        final Iterator<SourceRecord> inner2 = lst.subList(2, 3).iterator();
        // iterator returns inner1 values followed by fasle hasNext then inner2 values.
        final Iterator<SourceRecord> sourceRecordIterator = new Iterator<>() {
            Iterator<SourceRecord> inner = inner1;
            @Override
            public boolean hasNext() {
                if (inner == null) {
                    inner = inner2;
                    return false;
                }
                return inner.hasNext();
            }

            @Override
            public SourceRecord next() {
                final SourceRecord result = inner.next();
                if (!inner.hasNext()) {
                    inner = emptyIterator;
                }
                return result;
            }
        };

        final TestingSourceTask sourceTask = new TestingSourceTask(sourceRecordIterator);
        sourceTask.initialize(createSourceTaskContext());
        final List<SourceRecord> result = startAndPoll(sourceTask, createDefaultConfig(), 3);
        assertThat(result).contains(lst.toArray(new SourceRecord[0]));
    }

    @Test
    void testPollWithSlowProducer() {
        final List<SourceRecord> lst = createSourceRecords(3);

        // an iterator that returns records in 2 second intervals.
        final Iterator<SourceRecord> sourceRecordIterator = new Iterator<>() {
            Iterator<SourceRecord> inner = lst.iterator();
            @Override
            public boolean hasNext() {
                return inner.hasNext();
            }

            @Override
            public SourceRecord next() {
                try {
                    Thread.sleep(Duration.ofSeconds(2).toMillis());
                } catch (InterruptedException e) {
                    // do nothing.
                }
                return inner.next();
            }
        };

        final TestingSourceTask sourceTask = new TestingSourceTask(sourceRecordIterator);
        sourceTask.initialize(createSourceTaskContext());
        sourceTask.start(createDefaultConfig());
        await().atMost(TIMEOUT).until(sourceTask::isRunning);
        final List<SourceRecord> result = new ArrayList<>();
        // use a much longer duration so that we have a chance to read all the data.
        await().atMost(Duration.ofSeconds(10)).pollInterval(POLL_INTERVAL).untilAsserted(() -> {
            final List<SourceRecord> pollResult = sourceTask.poll();
            if (pollResult != null) {
                result.addAll(pollResult);
            }
            assertThat(result).hasSize(3);
        });
        assertThat(result).contains(lst.toArray(new SourceRecord[0]));
    }

    @Test
    void testPollsWithExcessRecords() {
        // test that multiple polls to get all records succeeds.
        final List<SourceRecord> lst = createSourceRecords(3);

        final TestingSourceTask sourceTask = new TestingSourceTask(lst.iterator());
        sourceTask.initialize(createSourceTaskContext());
        // set max poll to 2 records
        sourceTask.start(SourceConfigFragment.setter(createDefaultConfig()).maxPollRecords(2).data());
        await().atMost(TIMEOUT).until(sourceTask::isRunning);
        final List<SourceRecord> result = new ArrayList<>();
        await().atMost(TIMEOUT).pollInterval(POLL_INTERVAL).untilAsserted(() -> {
            final List<SourceRecord> pollResult = sourceTask.poll();
            if (pollResult != null) {
                result.addAll(pollResult);
            }
            assertThat(pollResult).isNotNull();
        });
        // verify that we only got 2 records on the single poll.
        assertThat(result).hasSize(2);

        // now read and verify we got all the records.
        await().atMost(TIMEOUT).pollInterval(POLL_INTERVAL).untilAsserted(() -> {
            final List<SourceRecord> pollResult = sourceTask.poll();
            if (pollResult != null) {
                result.addAll(pollResult);
            }
            assertThat(pollResult).isNotNull();
        });
        assertThat(result).contains(lst.toArray(new SourceRecord[0]));
    }

    @Test
    void testPollWhenConnectorStopped() {
        final List<SourceRecord> lst = createSourceRecords(3);
        final TestingSourceTask sourceTask = new TestingSourceTask(lst.iterator());

        sourceTask.initialize(createSourceTaskContext());
        sourceTask.start(createDefaultConfig());
        sourceTask.stop();
        final List<SourceRecord> result = sourceTask.poll();
        assertThat(result).isNull();
    }

    /**
     * SourceTask that sets the SourceRecord iterator duing constructor.
     */
    @SuppressWarnings("PMD.TestClassWithoutTestCases")
    private static class TestingSourceTask extends AbstractSourceTask {
        /** The logger to write to */
        private static final Logger LOGGER = LoggerFactory.getLogger(TestingSourceTask.class);
        /** The iterator to read */
        final private Iterator<SourceRecord> iterator;

        TestingSourceTask(final Iterator<SourceRecord> sourceRecordIterator) {
            super(LOGGER);
            this.iterator = sourceRecordIterator;
        }

        /**
         * Gets the context that the task is using.
         *
         * @return the context.
         */
        public SourceTaskContext getContext() {
            return context;
        }

        /**
         * Gets the backoff that the task is using.
         *
         * @return the backoff.
         */
        public Backoff getBackoff() {
            return backoff;
        }

        @Override
        protected Iterator<SourceRecord> getIterator(final BackoffConfig config) {
            return iterator;
        }

        @Override
        protected SourceCommonConfig configure(final Map<String, String> props) {
            final ConfigDef configDef = TransformerFragment.update(new ConfigDef());
            SourceConfigFragment.update(configDef);
            FileNameFragment.update(configDef);
            OutputFormatFragment.update(configDef, null);
            return new SourceCommonConfig(configDef, props);
        }

        @Override
        protected void closeResources() {
        }

        @Override
        public String version() {
            return "unknown";
        }
    }
}
