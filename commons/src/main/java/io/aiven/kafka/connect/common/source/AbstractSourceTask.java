/*
 * Copyright 2024-2025 Aiven Oy
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
import org.slf4j.LoggerFactory;

/**
 * This class handles extracting records from an iterator and returning them to Kafka. It uses an exponential backoff
 * with jitter to reduce the number of calls to the backend when there is no data. This solution:
 * <ul>
 * <li>When polled this implementation moves available records from the SourceRecord iterator to the return array.</li>
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

    public static final List<SourceRecord> NULL_RESULT = null;

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
        backoff = new Backoff(timer.getBackoffConfig());
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
     * @param config
     *            the configuration for the Backoff.
     * @return The iterator of SourceRecords.
     */
    abstract protected Iterator<SourceRecord> getIterator(BackoffConfig config);

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
        sourceRecordIterator = getIterator(timer.getBackoffConfig());
    }

    /**
     * Try to add a SourceRecord to the results.
     *
     * @param results
     *            the result to add the record to.
     * @param sourceRecordIterator
     *            the source record iterator.
     * @return true if successful, false if the iterator is empty.
     */
    private boolean tryAdd(final List<SourceRecord> results, final Iterator<SourceRecord> sourceRecordIterator) {
        if (sourceRecordIterator.hasNext()) {
            backoff.reset();
            final SourceRecord sourceRecord = sourceRecordIterator.next();
            if (logger.isDebugEnabled()) {
                logger.debug("tryAdd() : read record {}", sourceRecord.sourceOffset());
            }
            results.add(sourceRecord);
            return true;
        }
        logger.info("No records found in tryAdd call");
        return false;
    }

    /**
     * Returns {@code true} if the connector is not stopped and the timer has not expired.
     *
     * @return {@code true} if the connector is not stopped and the timer has not expired.
     */
    protected final boolean stillPolling() {
        final boolean result = !connectorStopped.get() && !timer.isExpired();
        logger.debug("Still polling: {}", result);
        return result;
    }

    @Override
    public final List<SourceRecord> poll() {
        logger.debug("Polling");
        if (connectorStopped.get()) {
            logger.info("Stopping");
            closeResources();
            return NULL_RESULT;
        } else {
            timer.start();
            try {
                final List<SourceRecord> result = populateList();
                if (logger.isDebugEnabled()) {
                    logger.debug("Poll() returning {} SourceRecords.", result == null ? null : result.size());
                }
                return result;
            } finally {
                timer.stop();
                timer.reset();
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
                        logger.debug("tryAdd() did not add to the list, returning current results.");
                        // if we could not get a record and the results are not empty return them
                        break;
                    }
                    logger.debug("Attempting {}", backoff);
                    backoff.cleanDelay();
                }
            }

        } catch (RuntimeException e) { // NOPMD must catch runtime here.
            logger.error("Error during poll(): {}", e.getMessage(), e);
            if (config.getErrorsTolerance() == ErrorsTolerance.NONE) {
                logger.error("Stopping Task");
                throw e;
            }
        }
        return results.isEmpty() ? NULL_RESULT : results;
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
         * The flag that indicates the timer has been aborted.
         */
        private boolean hasAborted;

        /**
         * Constructor.
         *
         * @param duration
         *            the length of time the timer should run.
         */
        Timer(final Duration duration) {
            super();
            this.duration = duration.toMillis();
        }

        /**
         * Gets the maximum duration for this timer.
         *
         * @return the maximum duration for the timer.
         */
        public long millisecondsRemaining() {
            return super.isStarted() ? duration - super.getTime() : duration;
        }

        /**
         * Returns {@code true} if the timer has expired.
         *
         * @return {@code true} if the timer has expired.
         */
        public boolean isExpired() {
            return hasAborted || super.getTime() >= duration;
        }

        /**
         * Aborts the timer. Timer will report that it has expired until reset is called.
         */
        public void abort() {
            hasAborted = true;
        }

        @Override
        public void start() {
            try {
                hasAborted = false;
                super.start();
            } catch (IllegalStateException e) {
                throw new IllegalStateException("Timer: " + e.getMessage());
            }
        }

        @Override
        public void stop() {
            try {
                super.stop();
            } catch (IllegalStateException e) {
                throw new IllegalStateException("Timer: " + e.getMessage());
            }
        }

        @Override
        public void reset() {
            try {
                hasAborted = false;
                super.reset();
            } catch (IllegalStateException e) {
                throw new IllegalStateException("Timer: " + e.getMessage());
            }
        }

        /**
         * Gets a Backoff Config for this timer.
         *
         * @return a backoff Configuration.
         */
        public BackoffConfig getBackoffConfig() {
            return new BackoffConfig() {

                @Override
                public SupplierOfLong getSupplierOfTimeRemaining() {
                    return Timer.this::millisecondsRemaining;
                }

                @Override
                public AbortTrigger getAbortTrigger() {
                    return Timer.this::abort;
                }
            };
        }
    }

    /**
     * Performs a delay based on the number of successive {@link #delay()} or {@link #cleanDelay()} calls without a
     * {@link #reset()}. Delay increases exponentially but never exceeds the time remaining by more than 0.512 seconds.
     */
    public static class Backoff {
        /** The logger to write to */
        private static final Logger LOGGER = LoggerFactory.getLogger(Backoff.class);
        /**
         * The maximum jitter random number. Should be a power of 2 for speed.
         */
        public static final int MAX_JITTER = 1024;

        public static final int JITTER_SUBTRAHEND = MAX_JITTER / 2;
        /**
         * A supplier of the time remaining (in milliseconds) on the overriding timer.
         */
        private final SupplierOfLong timeRemaining;

        /**
         * A function to call to abort the timer.
         */
        private final AbortTrigger abortTrigger;

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
         * @param config
         *            The configuration for the backoff.
         */
        public Backoff(final BackoffConfig config) {
            this.timeRemaining = config.getSupplierOfTimeRemaining();
            this.abortTrigger = config.getAbortTrigger();
            reset();
        }

        /**
         * Reset the backoff time so that delay is again at the minimum.
         */
        public final void reset() {
            // if the reminaing time is 0 or negative the maxCount will be infinity
            // so make sure that it is 0 in that case.
            final long remainingTime = timeRemaining.get();
            maxCount = remainingTime < 1L ? 0 : (int) (Math.log10(remainingTime) / Math.log10(2));
            waitCount = 0;
            LOGGER.debug("Reset {}", this);
        }

        /**
         * Handle adjustment when maxCount could not be set.
         *
         * @return the corrected maxCount
         */
        private int getMaxCount() {
            if (maxCount == 0) {
                reset();
            }
            return maxCount;
        }

        /**
         * Calculates the delay wihtout jitter.
         *
         * @return the number of milliseconds the delay will be.
         */
        public long estimatedDelay() {
            long sleepTime = timeRemaining.get();
            if (sleepTime > 0 && waitCount < maxCount) {
                sleepTime = (long) Math.min(sleepTime, Math.pow(2, waitCount + 1));
            }
            return sleepTime < 0 ? 0 : sleepTime;
        }

        /**
         * Calculates the range of jitter in milliseconds.
         *
         * @return the maximum jitter in milliseconds. jitter is +/- maximum jitter.
         */
        public int getMaxJitter() {
            return MAX_JITTER - JITTER_SUBTRAHEND;
        }

        private long timeWithJitter() {
            // generate approx +/- 0.512 seconds of jitter
            final int jitter = random.nextInt(MAX_JITTER) - JITTER_SUBTRAHEND;
            return (long) Math.pow(2, waitCount) + jitter;
        }

        /**
         * Delay execution based on the number of times this method has been called.
         *
         * @throws InterruptedException
         *             If any thread interrupts this thread.
         */
        public void delay() throws InterruptedException {
            final long sleepTime = timeRemaining.get();
            if (sleepTime > 0 && waitCount < (maxCount == 0 ? getMaxCount() : maxCount)) {
                waitCount++;
                final long nextSleep = timeWithJitter();
                // don't sleep negative time. Jitter can introduce negative tme.
                if (nextSleep > 0) {
                    if (nextSleep >= sleepTime) {
                        LOGGER.debug("Backoff aborting timer");
                        abortTrigger.apply();
                    } else {
                        LOGGER.debug("Backoff sleepiing {}", nextSleep);
                        Thread.sleep(nextSleep);
                    }
                }
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

        @Override
        public String toString() {
            return String.format("Backoff %s/%s, %s milliseconds remaining.", waitCount, maxCount, timeRemaining.get());
        }
    }

    /**
     * A functional interface to return long values.
     */
    @FunctionalInterface
    public interface SupplierOfLong {
        long get();
    }

    /**
     * A functional interface that will abort the timer. After being called timer will indicate that it is expired,
     * until it is reset.
     */
    @FunctionalInterface
    public interface AbortTrigger {
        void apply();
    }

    /**
     * An interface to define the Backoff configuration. Used for convenience with Timer.
     */
    public interface BackoffConfig {
        SupplierOfLong getSupplierOfTimeRemaining();
        AbortTrigger getAbortTrigger();
    }
}
