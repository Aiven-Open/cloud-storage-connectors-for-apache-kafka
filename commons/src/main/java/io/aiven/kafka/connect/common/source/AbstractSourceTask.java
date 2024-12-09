package io.aiven.kafka.connect.common.source;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * This class handles extracting records from an iterator and returning them to Kafka.  It uses an exponential backoff with
 * jitter to reduce the number of calls to the backend when there is no data.  This solution:
 * <ul>
 *     <li>When polled this implementation moves available records from the SsourceRecord iterator to the return array.</li>
 *     <li>if there are no records
 *     <ul><li>{@link #poll()} will return null.</li>
 *     <li>The poll will delay no more than approx 5 seconds.</li>
 *     </ul></li>
 *     <li>Upto {@link #maxPollRecords} will be sent in a single poll request</li>
 *     <li>When the connector is stopped any collected records are returned to kafka before stopping.</li>
 * </ul>
 *
 *
 */
public abstract class AbstractSourceTask extends SourceTask {

    /**
     * The maximum time to spend polling.  This is set to 5 seconds as that is the time that is allotted to a system
     * for shutdown.
     */
    private static final Duration MAX_POLL_TIME = Duration.ofSeconds(5);
    /**
     * The boolean that indicates the connector is stopped.
     */
    private final AtomicBoolean connectorStopped;

    /**
     * The logger to use.  Set from the class implementing AbstractSourceTask.
     */
    private final Logger logger;

    /**
     * The maximum number of records to put in a poll.  Specified in the configuration.
     */
    private int maxPollRecords;

    /**
     * The Backoff implementation that executes the delay in the poll loop.
     */
    private Backoff backoff;

    /**
     * Constructor.
     * @param logger the logger to use.
     */
    protected AbstractSourceTask(final Logger logger) {
        super();
        this.logger = logger;
        connectorStopped =  new AtomicBoolean();
        backoff = new Backoff(MAX_POLL_TIME);
    }

    /**
     * Gets the iterator of SourceRecords.
     * The iterator that SourceRecords are extracted from during a poll event.
     * When this iterator runs out of records it should attempt to reset and read
     * more records from the backend on the next {@code hasNext()} call.  In this way it should
     * detect when new data has been added to the backend and continue processing.
     * @return The iterator of SourceRecords.
     */
    abstract protected Iterator<SourceRecord> getIterator();

    /**
     * Called by {@link #start} to allows the concrete implementation to configure itself based on properties.
     * @param props the properties to use for configuration.
     */
    abstract protected void configure(final Map<String, String> props);

    @Override
    public final void start(Map<String, String> props) {
        SourceCommonConfig config = new SourceCommonConfig(new ConfigDef(), props);
        maxPollRecords = config.getMaxPollRecords();
        configure(props);
    }

    @Override
    public final List<SourceRecord> poll() throws InterruptedException {
        final List<SourceRecord> results = new ArrayList<>(maxPollRecords);
        final Timer timer = new Timer(MAX_POLL_TIME);
        final Iterator<SourceRecord> sourceRecordIterator = getIterator();

        if (!connectorStopped.get()) {
            while (!connectorStopped.get() && sourceRecordIterator.hasNext() && !timer.expired() && results.size() < maxPollRecords) {
                backoff.reset();
                results.add(sourceRecordIterator.next());
            }
            if (!results.isEmpty()) {
                return results;
            }
            if (!timer.expired() && !connectorStopped.get()) {
                backoff.delay();
            }
        } else {
            closeResources();
        }
        return null;
    }

    @Override
    public final void stop() {
        connectorStopped.set(true);
    }

    /**
     * Returns the running state of the task.
     * @return {@code true} if the connector is running, {@code false} otherwise.
     */
    public final boolean isRunning() {
        return !connectorStopped.get();
    }

    /**
     * Close any resources the source has open.  Called by the IteratorRunnable when it is
     * stopping.
     */
    abstract protected void closeResources();

    /**
     * Calculates elapsed time and flags when expired.
     */
    private class Timer {
        /**
         * The time the Timer was created.
         */
        private final long startTime;
        /**
         * The length of time that the timer should run.
         */
        private final long duration;

        /**
         * Constructor.
         * @param duration the length of time the timer should run.
         */
        Timer(Duration duration) {
            this.duration = duration.toMillis();
            this.startTime = System.currentTimeMillis();
        }

        /**
         * Returns {@code true} if the timer has expired.
         * @return {@code true} if the timer has expired.
         */
        boolean expired() {
            return System.currentTimeMillis() - startTime >= duration;
        }
    }

    /**
     * Calculates the amount of time to sleep during a backoff performs the sleep.
     * Backoff calculation uses an expenantially increasing delay until the maxDelay is
     * reached.  Then all delays are maxDelay length.
     */
    private class Backoff {
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
         * @param maxDelay The maximum delay that this instance will use.
         */
        Backoff(Duration maxDelay) {
            // calculate the approx wait time.
            maxWait = maxDelay.toMillis();
            maxCount = (int) (Math.log10(maxWait) / Math.log10(2));
            waitCount = 0;
        }

        /**
         * Reset the backoff time so that delay is again at the minimum.
         */
        private void reset() {
            waitCount = 0;
        }

        /**
         * Delay execution based on the number of times this method has been called.
         * @throws InterruptedException If any thread interrupts this thread.
         */
        private void delay() throws InterruptedException {
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
    }
}
