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

import static io.aiven.kafka.connect.common.config.SourceConfigFragment.MAX_POLL_RECORDS;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import io.aiven.kafka.connect.common.OffsetManager;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.utils.AWSV2SourceClient;
import io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry;
import io.aiven.kafka.connect.s3.source.utils.S3SourceRecord;
import io.aiven.kafka.connect.s3.source.utils.SourceRecordIterator;
import io.aiven.kafka.connect.s3.source.utils.Version;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * S3SourceTask is a Kafka Connect SourceTask implementation that reads from source-s3 buckets and generates Kafka
 * Connect records.
 */
public class S3SourceTask extends SourceTask {
    /** The loger to write to */
    private static final Logger LOGGER = LoggerFactory.getLogger(S3SourceTask.class);
    /** How log to wait for data */
    private static final long S_3_POLL_INTERVAL_MS = 10_000L;
    /** How much to backoff when no data is available */
    private static final long ERROR_BACKOFF = 1000L;
    /** The S3Source configuration */
    private S3SourceConfig s3SourceConfig;
    /** An iterator or S3SourceRecords */
    private Iterator<S3SourceRecord> sourceRecordIterator;
    /** The transformer that we are using */
    private Transformer transformer;
    /** The task initialized flag */
    private boolean taskInitialized;
    /** The connector stopped flag */
    private final AtomicBoolean connectorStopped = new AtomicBoolean();
    /** The poll lock object */
    private final Object pollLock = new Object();
    /** The AWS Source client */
    private AWSV2SourceClient awsv2SourceClient;
    /** The list of failed object keys */
    private final Set<String> failedObjectKeys = new HashSet<>();
    /** The offset manager this task uses */
    private OffsetManager<S3OffsetManagerEntry> offsetManager;

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void start(final Map<String, String> props) {
        LOGGER.info("S3 Source task started.");
        s3SourceConfig = new S3SourceConfig(props);
        this.transformer = s3SourceConfig.getTransformer();
        offsetManager = new OffsetManager<>(context);
        awsv2SourceClient = new AWSV2SourceClient(s3SourceConfig, failedObjectKeys);
        setSourceRecordIterator(
                new SourceRecordIterator(s3SourceConfig, offsetManager, this.transformer, awsv2SourceClient));
        this.taskInitialized = true;
    }

    @Override
    public void commit() throws InterruptedException {
        LOGGER.info("Committed all records through last poll()");
    }

    @Override
    public void commitRecord(SourceRecord record) throws InterruptedException {
        if (LOGGER.isInfoEnabled()) {
            Map<String, Object> map = (Map<String, Object>) record.sourceOffset();
            S3OffsetManagerEntry entry = S3OffsetManagerEntry.wrap(map);
            LOGGER.info("Committed individual record {} {} {} committed", entry.getBucket(), entry.getKey(), entry.getRecordCount());
        }
    }

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
                    waitForObjects();
                    extractSourceRecords(results);
                    LOGGER.info("Number of records extracted and sent: {}", results.size());
                    return results;
                } catch (AmazonS3Exception exception) {
                    if (exception.isRetryable()) {
                        LOGGER.warn("Retryable error encountered during polling. Waiting before retrying...",
                                exception);
                        pollLock.wait(ERROR_BACKOFF);
                        // TODO validate that the iterator does not lose an S3Object.  Add test to S3ObjectIterator.
                    } else {
                        LOGGER.warn("Non-retryable AmazonS3Exception occurred. Stopping polling.", exception);
                        return null; // NOPMD
                    }
                } catch (DataException exception) {
                    LOGGER.warn("DataException occurred during polling. No retries will be attempted.", exception);
                } catch (final RuntimeException t) { // NOPMD
                    LOGGER.error("Unexpected Exception encountered. Closing resources and stopping task.", t);
                    closeResources();
                    throw t;
                }
            }
            LOGGER.debug("Poll returning {} records.", results.size());
            return results;
        }
    }

    /**
     * Create a list of source records. Package private for testing.
     *
     * @param results
     *            a list of SourceRecords to add the results to.
     * @return the {@code results} parameter.
     */
    List<SourceRecord> extractSourceRecords(final List<SourceRecord> results) {
        if (connectorStopped.get()) {
            return results;
        }
        final int maxPollRecords = s3SourceConfig.getMaxPollRecords();
        long lastRecord = 0;
        for (int i = 0; sourceRecordIterator.hasNext() && i < maxPollRecords && !connectorStopped.get(); i++) {
            final S3SourceRecord s3SourceRecord = sourceRecordIterator.next();
            if (s3SourceRecord != null) {
                try {
                    S3OffsetManagerEntry entry = s3SourceRecord.getOffsetManagerEntry();
                    offsetManager.updateCurrentOffsets(entry);
                    results.add(s3SourceRecord.getSourceRecord());
                    lastRecord = entry.getRecordCount();
                } catch (DataException e) {
                    LOGGER.error("Error in reading s3 object stream {}", e.getMessage(), e);
                    awsv2SourceClient.addFailedObjectKeys(s3SourceRecord.getObjectKey());
                }
            }
        }
        LOGGER.info("Last record in batch: {}", lastRecord);
        return results;
    }

    /**
     * Set the S3 source record iterator that this task is using. protected to be overridden in testing impl.
     *
     * @param iterator
     *            The S3SourceRecord iterator to use.
     */
    protected void setSourceRecordIterator(final Iterator<S3SourceRecord> iterator) {
        sourceRecordIterator = iterator;
    }

    /**
     * Wait until objects are available to be read
     *
     * @throws InterruptedException
     *             on error.
     */
    private void waitForObjects() throws InterruptedException {
        while (!sourceRecordIterator.hasNext() && !connectorStopped.get()) {
            LOGGER.debug("Blocking until new S3 files are available.");
            Thread.sleep(S_3_POLL_INTERVAL_MS);
        }
    }

    @Override
    public void stop() {
        this.taskInitialized = false;
        this.connectorStopped.set(true);
        synchronized (pollLock) {
            closeResources();
        }
    }

    private void closeResources() {
        awsv2SourceClient.shutdown();
    }

    // below for visibility in tests

    /**
     * Get the transformer that we are using.
     *
     * @return the transformer that we are using.
     */
    public Transformer getTransformer() {
        return transformer;
    }

    /**
     * Get the initialized flag.
     *
     * @return {@code true} if the task has been initialized, {@code false} otherwise.
     */
    public boolean isTaskInitialized() {
        return taskInitialized;
    }

    /**
     * Gets the state of the connector stopped flag.
     *
     * @return The state of the connector stopped flag.
     */
    public boolean isConnectorStopped() {
        return connectorStopped.get();
    }
}
