package io.aiven.kafka.connect.s3.source;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * This class handles extracting records from an iterator and returning them to Kafka.  It uses an exponential backoff with
 * jitter to reduce the number of calls to the backend when there is no data.  This solution:
 * <ul>
 *     <li>This solution uses a thread to read from an iterator of SourceRecords and writes them to a LinkedBlockingQueue until
 *     the queue is full or the iterator says it has no more entries.</li>
 *     <li>When polled this implementation moves available records from the LinkedBlockingQueue to the return array.</li>
 *     <li>if there are no records {@link #poll()} will return null.</li>
 *     <li>Upto {@link #getMaxPollRecords()} will be sent in a single poll request</li>
 *     <li>The queue size is 2 x {@link #getMaxPollRecords()}</li>
 *     <li>When the connector is stopped the iterator thread is stopped so that no more items are placed in the queue.</li>
 *     <li>The remaining items in the queue will be returned via the poll method.</li>
 * </ul>
 *
 *
 */
public abstract class AivenSourceTask extends SourceTask {

    /**
     * Place 2x the max poll records in the queue.
     */
    private static final int QUEUE_MULTIPLIER = 2;

    /**
     * The queue of records that are ready to send to Kafka.
     */
    private LinkedBlockingQueue<SourceRecord> queue;

    /**
     * The boolean that indicates the connector is stopped.
     */
    private final AtomicBoolean connectorStopped;

    /**
     * The logger to use.  Set from the class implementing AivenSourceTask.
     */
    private final Logger logger;

    /**
     * The maximum number of records to put in a poll.  Specified in the configuration.
     */
    private int maxPollRecords;

    /**
     * The thread that runs the iterator.
     */
    private Thread iteratorThread; // NOPMD not J2EE webapp

    /**
     * Constructor.
     * @param logger the logger to use.
     */
    protected AivenSourceTask(final Logger logger) {
        super();
        this.logger = logger;
        connectorStopped =  new AtomicBoolean();
    }

    /**
     * Starts the iterator.  Also configures the task environment.
     * @param iterator the iterator to start reading from.
     */
    protected void startIterator(final Iterator<SourceRecord> iterator) {
        iteratorThread = new Thread(new IteratorRunnable(iterator), "IteratorThread"); // NOPMD not J2EE webapp
        iteratorThread.start();
        maxPollRecords = getMaxPollRecords();
        queue = new LinkedBlockingQueue<>(maxPollRecords * QUEUE_MULTIPLIER);
    }

    @Override
    public final void start(Map<String, String> props) {
        SourceCommonConfig config = new SourceCommonConfig(props);

    }

    @Override
    public final List<SourceRecord> poll() throws InterruptedException {
        final List<SourceRecord> results = new ArrayList<>(maxPollRecords);
        final int recordCount = queue.drainTo(results);
        return recordCount > 0 ? results : null;
        /*
            @Override
    public List<SourceRecord> poll() throws InterruptedException {
        LOGGER.info("Polling for new records...");
        synchronized (pollLock) {
            final List<SourceRecord> results = new ArrayList<>(s3SourceConfig.getInt(MAX_POLL_RECORDS));

            if (connectorStopped.get()) {
                LOGGER.info("Connector has been stopped. Returning empty result list.");
                return results;
            }

            while (!connectorStopped.get()) {
                try {
                    extractSourceRecords(results);
                    LOGGER.info("Number of records extracted and sent: {}", results.size());
                    return results;
                } catch (AmazonS3Exception exception) {
                    if (exception.isRetryable()) {
                        LOGGER.warn("Retryable error encountered during polling. Waiting before retrying...",
                                exception);
                        pollLock.wait(ERROR_BACKOFF);

                        prepareReaderFromOffsetStorageReader();
                    } else {
                        LOGGER.warn("Non-retryable AmazonS3Exception occurred. Stopping polling.", exception);
                        return null; // NOPMD
                    }
                } catch (DataException exception) {
                    LOGGER.warn("DataException occurred during polling. No retries will be attempted.", exception);
                } catch (final Throwable t) { // NOPMD
                    LOGGER.error("Unexpected error encountered. Closing resources and stopping task.", t);
                    closeResources();
                    throw t;
                }
            }
            return results;
        }
    }
         */
    }

    protected abstract int getMaxPollRecords();

    @Override
    public void stop() {
        connectorStopped.set(true);
    }

    /**
     * Returns the state of the iterator thread.
     * @return {@code true} if the iterator thread is running, {@code false} otherwise.
     */
    protected boolean isRunning() {
        return iteratorThread.isAlive();
    }

    /**
     * Returns the stop state of the connector.
     * @return {@code true} if the connector has been stopped, {@code false} otherwise.
     */
    protected boolean isStopped() {
        return connectorStopped.get();
    }

    /**
     * Close any resources the source has open.  Called by the IteratorRunnable when it is
     * stopping.
     */
    abstract protected void closeResources();


    /**
     * Reads data from the SourceRecord iterator and feeds that into the queue.
     */
    private class IteratorRunnable implements Runnable {
        // establishes a limit of approx 5 seconds for the backoff.
        private final static int MAX_WAIT_COUNT = 10;

        /** The iterator we are reading from */
        private final Iterator<SourceRecord> iterator;
        private int waitCount;
        private final Random random;


        /**
         * Creates the runnable iterator.  This class will read from the iterator
         * until the connector is stopped.  The iterator must be able to detect new
         * data added to the storage after it was started.
         * @param iterator the iterator to read from.
         */
        IteratorRunnable(final Iterator<SourceRecord> iterator) {
            this.iterator = iterator;
            random = new Random();
        }

        @Override
        public void run() {
            try {
                while (!connectorStopped.get()) {
                    if (iterator.hasNext()) {
                        waitCount = 0;
                        queue.put(iterator.next());
                    } else {
                        backoff();
                    }
                }
            } catch (InterruptedException e) {
                logger.warn("Iterator thread interrupted -- stopping", e);
            } finally {
                closeResources();
            }
        }

        private void backoff() throws InterruptedException {
            if (waitCount < MAX_WAIT_COUNT) {
                waitCount++;
            }
            final int jitter = random.nextInt(-500, 500);
            // 4.88 * 1024 approx 5000 (5 seconds)
            // The default graceful shutdown time as defined by Kafka is 5 seconds,
            final Double sleep = Math.pow(2, waitCount) * 4.88 + jitter;
            Thread.sleep(sleep.longValue());
        }
    }
}
