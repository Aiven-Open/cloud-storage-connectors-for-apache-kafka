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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;

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
     * The transfer queue from concrete implementation to Kafka
     */
    private LinkedBlockingQueue<SourceRecord> queue;

    /**
     * The thread that is running the polling of the implementation.
     */
    private final Thread implemtationPollingThread;

    /**
     * The Backoff implementation that executes the delay in the poll loop.
     */
    private final Backoff backoff;

    private final BackoffConfig backoffConfig;

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
        backoffConfig = new BackoffConfig() {
            @Override
            public SupplierOfLong getSupplierOfTimeRemaining() {
                return MAX_POLL_TIME::toMillis;
            }

            @Override
            public AbortTrigger getAbortTrigger() {
                return () -> {
                };
            }

            @Override
            public boolean applyTimerRule() {
                return false;
            }
        };
        backoff = new Backoff(backoffConfig);
        implemtationPollingThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (stillPolling()) {
                        if (!tryAdd()) {
                            logger.debug("Attempting {}", backoff);
                            backoff.cleanDelay();
                        }
                    }
                } catch (InterruptedException e) {
                    logger.warn("{} interrupted -- EXITING", this.toString());
                } catch (RuntimeException e) { // NOPMD AvoidCatchingGenericException
                    logger.error("{} failed -- EXITING", this.toString(), e);
                }

            }
        }, this.getClass().getName() + " polling thread");
    }

    /**
     * Gets the iterator of SourceRecords. The iterator that SourceRecords are extracted from for a poll event. When
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
        final SourceCommonConfig config = configure(props);
        maxPollRecords = config.getMaxPollRecords();
        queue = new LinkedBlockingQueue<>(maxPollRecords * 2);
        sourceRecordIterator = getIterator(backoffConfig);
        implemtationPollingThread.start();
    }

    /**
     * Try to add a SourceRecord to the results.
     *
     * @return true if successful, false if the iterator is empty.
     */
    private boolean tryAdd() throws InterruptedException {
        if (queue.remainingCapacity() > 0) {
            if (sourceRecordIterator.hasNext()) {
                backoff.reset();
                final SourceRecord sourceRecord = sourceRecordIterator.next();
                if (logger.isDebugEnabled()) {
                    logger.debug("tryAdd() : read record {}", sourceRecord.sourceOffset());
                }
                queue.put(sourceRecord);
                return true;
            }
            logger.info("No records found in tryAdd call");
        } else {
            logger.info("No space in queue");
        }
        return false;
    }

    /**
     * Returns {@code true} if the connector is not stopped and the timer has not expired.
     *
     * @return {@code true} if the connector is not stopped and the timer has not expired.
     */
    protected final boolean stillPolling() {
        final boolean result = !connectorStopped.get();
        logger.debug("Still polling: {}", result);
        return result;
    }

    @Override
    public final List<SourceRecord> poll() {
        logger.debug("Polling");
        if (stillPolling()) {
            List<SourceRecord> results = new ArrayList<>(maxPollRecords);
            results = 0 == queue.drainTo(results, maxPollRecords) ? NULL_RESULT : results;
            if (logger.isDebugEnabled()) {
                logger.debug("Poll() returning {} SourceRecords.", results == null ? null : results.size());
            }
            if (results == null && !implemtationPollingThread.isAlive()) {
                throw new ConnectException(implemtationPollingThread.getName() + " has died");
            }
            return results;
        } else {
            logger.info("Stopping");
            closeResources();
            return NULL_RESULT;
        }
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
        protected final SupplierOfLong timeRemaining;

        /**
         * A function to call to abort the timer.
         */
        protected final AbortTrigger abortTrigger;

        /**
         * The maximum number of times {@link #delay()} will be called before maxWait is reached.
         */
        private int maxCount;
        /**
         * The number of times {@link #delay()} has been called.
         */
        private int waitCount;
        /**
         * If true then when wait count is exceeded {@link ##delay()} automatically returns without delay.
         */
        private final boolean applyTimerRule;

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
            this.applyTimerRule = config.applyTimerRule();
            reset();
        }

        /**
         * Reset the backoff time so that delay is again at the minimum.
         */
        public final void reset() {
            // if the remaining time is 0 or negative the maxCount will be infinity
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
         * Calculates the delay without jitter.
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
         * If {@link #applyTimerRule} is true then this method will return false if the wait count has exceeded the
         * maximum count. Otherwise it returns true. This method also increments the wait count if the wait count is
         * less than the maximum count.
         *
         * @return true if sleep should occur.
         */
        private boolean shouldSleep(final long sleepTime) {
            // maxcount may have been reset so check and set if necessary.
            final boolean result = sleepTime > 0
                    && (!applyTimerRule || waitCount < (maxCount == 0 ? getMaxCount() : maxCount));
            if (waitCount < maxCount) {
                waitCount++;
            }
            return result;
        }

        /**
         * Delay execution based on the number of times this method has been called.
         *
         * @throws InterruptedException
         *             If any thread interrupts this thread.
         */
        public void delay() throws InterruptedException {
            final long sleepTime = timeRemaining.get();
            if (shouldSleep(sleepTime)) {
                final long nextSleep = timeWithJitter();
                // don't sleep negative time. Jitter can introduce negative tme.
                if (nextSleep > 0) {
                    if (nextSleep >= sleepTime) {
                        LOGGER.debug("Backoff aborting timer");
                        abortTrigger.apply();
                    } else {
                        LOGGER.debug("Backoff sleeping {}", nextSleep);
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
        /**
         * Gets Supplier that will return the number of milliseconds remaining in the timer. Should be the maximum delay
         * for situations that do not use a timer.
         *
         * @return A supplier of the number of milliseconds until the timer expires.
         */
        SupplierOfLong getSupplierOfTimeRemaining();

        /**
         * The AbortTrigger that will abort the timer.
         *
         * @return the AbortTrigger.
         */
        AbortTrigger getAbortTrigger();

        /**
         * Gets the abort timer rule flag. If there is no timer that may expire and shorten the time for the delay then
         * this value should be {@code false} otherwise if the delay time will exceed the maximum time remaining no
         * delay is executed. By default, the false is {@code true}.
         *
         * @return The abort time rule flag.
         */
        default boolean applyTimerRule() {
            return true;
        }
    }
}
