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
    public static final Duration MAX_POLL_TIME = Duration.ofSeconds(5);
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
        timer = new Timer(MAX_POLL_TIME);
        backoff = new Backoff(timer::millisecondsRemaining);
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
     * @param timer
     *            a SupplierOfLong that provides the amount of time remaining before the polling expires.
     * @return The iterator of SourceRecords.
     */
    abstract protected Iterator<SourceRecord> getIterator(SupplierOfLong timer);

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
        sourceRecordIterator = getIterator(timer::millisecondsRemaining);
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
                if (!tryAdd(results, sourceRecordIterator)) {
                    if (!results.isEmpty()) {
                        // if we could not get a record and the results are not empty return them
                        break;
                    }
                    // attempt a backoff
                    backoff.cleanDelay();
                }
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
         * Gets the maximum duration for this timer.
         *
         * @return the maximum duration for the timer.
         */
        public long millisecondsRemaining() {
            return super.isStarted() ? super.getTime() - duration : duration;
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
     * Performs a delay based on the number of successive {@link #delay()} or {@link #cleanDelay()} calls without a
     * {@link #reset()}. Delay increases exponentially but never exceeds the time remaining by more than 0.512 seconds.
     */
    protected static class Backoff {
        /**
         * A supplier of the time remaining (in milliseconds) on the overriding timer.
         */
        private final SupplierOfLong timeRemaining;

        /**
         * The maximum number of times {@link #delay()} will be called before maxWait is reached.
         */
        private int maxCount;
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
         * @param timeRemaining
         *            A supplier of long as milliseconds remaining before time expires.
         */
        public Backoff(final SupplierOfLong timeRemaining) {
            this.timeRemaining = timeRemaining;
            reset();
        }

        /**
         * Reset the backoff time so that delay is again at the minimum.
         */
        public final void reset() {
            // calculate the approx wait count.
            maxCount = (int) (Math.log10(timeRemaining.get()) / Math.log10(2));
            waitCount = 0;
        }

        private long timeWithJitter() {
            // generate approx +/- 0.512 seconds of jitter
            final int jitter = random.nextInt(1024) - 512;
            return (long) Math.pow(2, waitCount) + jitter;
        }
        /**
         * Delay execution based on the number of times this method has been called.
         *
         * @throws InterruptedException
         *             If any thread interrupts this thread.
         */
        public void delay() throws InterruptedException {

            long sleepTime = timeRemaining.get();
            if (waitCount < maxCount) {
                waitCount++;
                sleepTime = Math.min(sleepTime, timeWithJitter());
            }
            // don't sleep negative time.
            if (sleepTime > 0) {
                Thread.sleep(sleepTime);
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

    /**
     * A functional interface to return long values.
     */
    @FunctionalInterface
    public interface SupplierOfLong {
        long get();
    }
}
