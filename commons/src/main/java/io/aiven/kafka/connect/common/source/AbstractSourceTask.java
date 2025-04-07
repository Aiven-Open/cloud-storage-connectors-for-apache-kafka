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
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles extracting records from an iterator and returning them to Kafka. It uses an exponential backoff
 * with jitter to reduce the number of calls to the backend when there is no data. This solution:
 * <ul>
 * <li>Uses a thread to check the SoureeRecordIterator and
 * <ul>
 * <li>Will move available records from the SourceRecord iterator into a queue of records to be returned.</li>
 * <li>If there are no records delay no more than approx 5 seconds before attempting to retry. Delay is an exponentially
 * increasing factor up to approx 5 seconds between calls.</li>
 * <li>Detects and removes duplicates from the iterator.</li>
 * </ul>
 * <li>during polling ({@link #poll()})
 * <ul>
 * <li>The records available will be returned up to {@link #maxPollRecords} in a poll.</li>
 * <li>if there are no records {@link #poll()} will return null.</li>
 * </ul>
 * <li>When the connector is stopped any collected records are returned to kafka before stopping.</li>
 * </ul>
 */
public abstract class AbstractSourceTask extends SourceTask {

    /**
     * Value to return when there are no results.
     */
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
     * A queue of recently seen SourceRecords. Used to detect recently seen source records that may be generated in
     * specific edge cases involving slower polling that generation and disabled ring buffer. TODO replace the queue
     * with a fixed size LRU cache.
     */
    private CircularFifoQueue<SourceRecord> deduplicateQueue;

    /**
     * The thread that is running the polling of the implementation.
     */
    private final Thread implemtationPollingThread;

    /**
     * The Backoff implementation that executes the delay in the poll loop.
     */
    private final Backoff backoff;

    /**
     * The configuration for the backoff calculations.
     */
    private final BackoffConfig backoffConfig;

    /**
     * The SourceRecord iterator that we pull new records from.
     */
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
     * Gets the iterator of SourceRecords. This is the iterator that SourceRecords are extracted from for a poll event
     * and should be an implementation of {@link AbstractSourceRecordIterator}. If this iterator is not an instance of
     * {@link AbstractSourceRecordIterator} then it must guarantee the following:
     * <ul>
     * <li>When the iterator runs out of records it should attempt to reset from the underlying store with any newly
     * created native objects</li>
     * <li>As long as there are no further native objects to process it must continue to return {@code false} for calls
     * to {@link Iterator#hasNext()}.</li>
     * <li>Once a native object is available it must again return {@code true} for calls to
     * {@link Iterator#hasNext()}.</li>
     * <li>Should return {@code false} if {@link #stillPolling} returns {@code false}.</li>
     * </ul>
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
     * @return An instance of SourceCommonConfig that is correct for the concrete implementation.
     */
    abstract protected SourceCommonConfig configure(Map<String, String> props);

    @Override
    public final void start(final Map<String, String> props) {
        logger.debug("Starting");
        final SourceCommonConfig config = configure(props);
        maxPollRecords = config.getMaxPollRecords();
        queue = new LinkedBlockingQueue<>(maxPollRecords * 2);
        deduplicateQueue = new CircularFifoQueue<>(maxPollRecords * 2);
        sourceRecordIterator = getIterator(backoffConfig);
        implemtationPollingThread.start();
    }

    /**
     * Performs an equality check which accounts for nulls. If both objects are null the equality check passes.
     *
     * @param lhObject
     *            the left hand object to test.
     * @param rhObject
     *            the right hand object to test.
     * @return true if the objects are equal.
     */
    private boolean equalsNullCheck(final Object lhObject, final Object rhObject) {
        if (lhObject != null) {
            if (lhObject instanceof Map && rhObject instanceof Map) {
                final Map<?, ?> o1Map = (Map<?, ?>) lhObject;
                final Map<?, ?> o2Map = (Map<?, ?>) rhObject;
                return Objects.deepEquals(o1Map.keySet().toArray(), o2Map.keySet().toArray())
                        && Objects.deepEquals(o1Map.values().toArray(), o2Map.values().toArray());
            } else {
                return Objects.deepEquals(lhObject, rhObject);
            }
        }
        return rhObject == null;
    }

    /**
     * Equivalent to SourceRecord.equals() without the timestamp check. If the record is not in the queue it is added.
     *
     * @param record
     *            the record to attempt to find.
     * @return true if the record was found in the deduplicateQueue.
     */
    private boolean detectDuplicate(final SourceRecord record) {
        for (final SourceRecord queuedRecord : deduplicateQueue) {
            if (equalsNullCheck(record.kafkaPartition(), queuedRecord.kafkaPartition())
                    && equalsNullCheck(record.topic(), queuedRecord.topic())
                    && equalsNullCheck(record.keySchema(), queuedRecord.keySchema())
                    && equalsNullCheck(record.key(), queuedRecord.key())
                    && equalsNullCheck(record.valueSchema(), queuedRecord.valueSchema())
                    && equalsNullCheck(record.value(), queuedRecord.value())
                    && equalsNullCheck(record.headers(), queuedRecord.headers())
                    && equalsNullCheck(record.sourcePartition(), queuedRecord.sourcePartition())
                    && equalsNullCheck(record.sourceOffset(), queuedRecord.sourceOffset())) {
                return true;
            }
        }
        deduplicateQueue.add(record);
        return false;
    }

    /**
     * Try to add a SourceRecord to the results. This method is called by the implementationPollingThread to read
     * available records from the iterator. If this method returns {@code false} the system will execute a backoff and
     * delay before attempting again.
     *
     * @return true if successful, false if the iterator is empty, returned a duplicate or other result that was not
     *         added to the queue.
     */
    private boolean tryAdd() throws InterruptedException {
        if (queue.remainingCapacity() > 0) {
            if (sourceRecordIterator.hasNext()) {
                backoff.reset();
                final SourceRecord sourceRecord = sourceRecordIterator.next();
                if (detectDuplicate(sourceRecord)) {
                    logger.debug("tryAdd() : duplicate queue entry");
                    return false;
                }
                queue.put(sourceRecord);
                logger.debug("tryAdd() : enqueued record {} {} {}", sourceRecord, sourceRecord.sourceOffset(),
                        sourceRecord.sourcePartition());
                return true;
            }
            logger.debug("No records found in tryAdd call");
        } else {
            logger.debug("No space in queue");
        }
        return false;
    }

    /**
     * Returns {@code true} if the connector is not stopped.
     *
     * @return {@code true} if the connector is not stopped.
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
        logger.info("Stop requested");
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
    public static class Timer extends StopWatch {
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
        public Timer(final Duration duration) {
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

        /**
         * The subtrahend for the jitter calculation.
         */
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

        /**
         * Calculates the delay time with jitter. May return a negative number for large jitter and small waitCounts.
         *
         * @return the delay time in milliseconds.
         */
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
            // maxCount may have been reset so check and set if necessary.
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
