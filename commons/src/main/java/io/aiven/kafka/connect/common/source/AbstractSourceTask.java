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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.config.enums.ErrorsTolerance;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;

/**
 * This class handles extracting records from an iterator and returning them to Kafka. It uses an exponential backoff
 * with jitter to reduce the number of calls to the backend when there is no data. This solution:
 * <ul>
 * <li>When polled this implementation moves available records from the SsourceRecord iterator to the return array.</li>
 * <li>if there are no records
 * <ul>
 * <li>{@link #poll()} will return null.</li>
 * <li>The poll will delay no more than approx 5 seconds.</li>
 * </ul>
 * </li>
 * <li>Upto {@link #maxPollRecords} will be sent in a single poll request</li>
 * <li>When the connector is stopped any collected records are returned to kafka before stopping.</li>
 * </ul>
 *
 *
 */
public abstract class AbstractSourceTask extends SourceTask {

    /**
     * The maximum time to spend polling. This is set to 5 seconds as that is the time that is allotted to a system for
     * shutdown.
     */
    protected static final Duration MAX_POLL_TIME = Duration.ofSeconds(5);
    /**
     * The boolean that indicates the connector is stopped.
     */
    private final AtomicBoolean connectorStopped;

    /**
     * The logger to use. Set from the class implementing AbstractSourceTask.
     */
    private final Logger logger;

    /**
     * The maximum number of records to put in a poll. Specified in the configuration.
     */
    private int maxPollRecords;

    /**
     * The Backoff implementation that executes the delay in the poll loop.
     */
    private final Backoff backoff;

    private final Timer timer;

    /**
     * The configuration
     */
    private SourceCommonConfig config;

    private Iterator<SourceRecord> sourceRecordIterator;

    /**
     * Constructor.
     *
     * @param logger
     *            the logger to use.
     */
    protected AbstractSourceTask(final Logger logger) {
        super();
        this.logger = logger;
        connectorStopped = new AtomicBoolean();
        backoff = new Backoff(MAX_POLL_TIME);
        timer = new Timer(MAX_POLL_TIME);
    }

    /**
     * Gets the iterator of SourceRecords. The iterator that SourceRecords are extracted from during a poll event. When
     * this iterator runs out of records it should attempt to reset and read more records from the backend on the next
     * {@code hasNext()} call. In this way it should detect when new data has been added to the backend and continue
     * processing.
     * <p>
     * This method should handle any backend exception that can be retried. Any runtime exceptions that are thrown when
     * this iterator executes may cause the task to abort.
     * </p>
     *
     * @return The iterator of SourceRecords.
     */
    abstract protected Iterator<SourceRecord> getIterator();

    /**
     * Called by {@link #start} to allows the concrete implementation to configure itself based on properties.
     *
     * @param props
     *            the properties to use for configuration.
     */
    abstract protected SourceCommonConfig configure(Map<String, String> props);

    @Override
    public final void start(final Map<String, String> props) {
        logger.debug("Starting");
        config = configure(props);
        maxPollRecords = config.getMaxPollRecords();
        sourceRecordIterator = getIterator();
    }

    /**
     * Try to add a SourceRecord to the results.
     *
     * @param results
     *            the result to add the recrod to.
     * @param sourceRecordIterator
     *            the source record iterator.
     * @return true if successful, false if the iterator is empty.
     */
    private boolean tryAdd(final List<SourceRecord> results, final Iterator<SourceRecord> sourceRecordIterator) {
        if (sourceRecordIterator.hasNext()) {
            backoff.reset();
            results.add(sourceRecordIterator.next());
            return true;
        }
        return false;
    }

    /**
     * Returns {@code true} if the connector is not stopped and the timer has not expired.
     *
     * @return {@code true} if the connector is not stopped and the timer has not expired.
     */
    protected boolean stillPolling() {
        return !connectorStopped.get() && !timer.expired();
    }

    @Override
    public final List<SourceRecord> poll() {
        logger.debug("Polling");
        if (connectorStopped.get()) {
            closeResources();
            return Collections.emptyList();
        } else {
            timer.start();
            try {
                return populateList();
            } finally {
                timer.stop();
            }
        }
    }

    /**
     * Attempts to populate the return list. Will read as many records into the list as it can until the timer expires
     * or the task is shut down.
     *
     * @return A list SourceRecords or {@code null} if the system hit a runtime exception.
     */
    private List<SourceRecord> populateList() {
        final List<SourceRecord> results = new ArrayList<>();
        try {
            while (stillPolling() && results.size() < maxPollRecords) {
                // if we could not get a record and the results are not empty return them
                if (!tryAdd(results, sourceRecordIterator) && !results.isEmpty()) {
                    break;
                }
                // attempt a backoff
                backoff.cleanDelay();
            }
        } catch (RuntimeException e) { // NOPMD must catch runtime here.
            logger.error("Error during poll(): {}", e.getMessage(), e);
            if (config.getErrorsTolerance() == ErrorsTolerance.NONE) {
                logger.error("Stopping Task");
                return null; // NOPMD must return null in this case.
            }
        }
        return results;
    }

    @Override
    public final void stop() {
        logger.debug("Stopping");
        connectorStopped.set(true);
    }

    /**
     * Returns the running state of the task.
     *
     * @return {@code true} if the connector is running, {@code false} otherwise.
     */
    public final boolean isRunning() {
        return !connectorStopped.get();
    }

    /**
     * Close any resources the source has open. Called by the IteratorRunnable when it is stopping.
     */
    abstract protected void closeResources();

    /**
     * Calculates elapsed time and flags when expired.
     */
    protected static class Timer extends StopWatch {
        /**
         * The length of time that the timer should run.
         */
        private final long duration;

        /**
         * Constructor.
         *
         * @param duration
         *            the length of time the timer should run.
         */
        private Timer(final Duration duration) {
            super();
            this.duration = duration.toMillis();
        }

        /**
         * Returns {@code true} if the timer has expired.
         *
         * @return {@code true} if the timer has expired.
         */
        public boolean expired() {
            return super.getTime() >= duration;
        }
    }

    /**
     * Calculates the amount of time to sleep during a backoff performs the sleep. Backoff calculation uses an
     * expenantially increasing delay until the maxDelay is reached. Then all delays are maxDelay length.
     */
    protected static class Backoff {
        /**
         * The maximum wait time.
         */
        private final long maxWait;
        /**
         * The maximum number of times {@link #delay()} will be called before maxWait is reached.
         */
        private final int maxCount;
        /**
         * The number of times {@link #delay()} has been called.
         */
        private int waitCount;

        /**
         * A random number generator to construct jitter.
         */
        Random random = new Random();

        /**
         * Constructor.
         *
         * @param maxDelay
         *            The maximum delay that this instance will use.
         */
        public Backoff(final Duration maxDelay) {
            // calculate the approx wait time.
            maxWait = maxDelay.toMillis();
            maxCount = (int) (Math.log10(maxWait) / Math.log10(2));
            waitCount = 0;
        }

        /**
         * Reset the backoff time so that delay is again at the minimum.
         */
        public void reset() {
            waitCount = 0;
        }

        /**
         * Delay execution based on the number of times this method has been called.
         *
         * @throws InterruptedException
         *             If any thread interrupts this thread.
         */
        public void delay() throws InterruptedException {
            // power of 2 next int is faster and so we generate approx +/- 0.512 seconds of jitter
            final int jitter = random.nextInt(1024) - 512;

            if (waitCount < maxCount) {
                waitCount++;
                final long sleep = (long) Math.pow(2, waitCount) + jitter;
                // don't allow jitter to set sleep argument negative.
                Thread.sleep(Math.max(0, sleep));
            } else {
                Thread.sleep(maxWait + jitter);
            }
        }

        /**
         * Like {@link #delay} but swallows the {@link InterruptedException}.
         */
        public void cleanDelay() {
            try {
                delay();
            } catch (InterruptedException exception) {
                // do nothing return results below
            }
        }
    }
}
