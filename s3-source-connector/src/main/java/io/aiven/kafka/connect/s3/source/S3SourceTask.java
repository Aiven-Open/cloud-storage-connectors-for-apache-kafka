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

import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.TransformerFactory;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.utils.AWSV2SourceClient;
import io.aiven.kafka.connect.s3.source.utils.OffsetManager;
import io.aiven.kafka.connect.s3.source.utils.RecordProcessor;
import io.aiven.kafka.connect.s3.source.utils.S3SourceRecord;
import io.aiven.kafka.connect.s3.source.utils.SourceRecordIterator;
import io.aiven.kafka.connect.s3.source.utils.Version;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * S3SourceTask is a Kafka Connect SourceTask implementation that reads from source-s3 buckets and generates Kafka
 * Connect records.
 */
@SuppressWarnings({ "PMD.TooManyMethods", "PMD.ExcessiveImports" })
public class S3SourceTask extends SourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3SourceTask.class);

    public static final String BUCKET = "bucket";
    public static final String TOPIC = "topic";

    public static final String OBJECT_KEY = "object_key";
    public static final String PARTITION = "topicPartition";

    private static final long S_3_POLL_INTERVAL_MS = 10_000L;
    private static final long ERROR_BACKOFF = 1000L;

    private S3SourceConfig s3SourceConfig;
    private S3Client s3Client;

    private Iterator<S3SourceRecord> sourceRecordIterator;
    private Transformer transformer;

    private boolean taskInitialized;

    private final AtomicBoolean connectorStopped = new AtomicBoolean();

    private final Object pollLock = new Object();
    private AWSV2SourceClient awsv2SourceClient;
    private final Set<String> failedObjectKeys = new HashSet<>();
    private final Set<String> inProcessObjectKeys = new HashSet<>();

    private OffsetManager offsetManager;

    @SuppressWarnings("PMD.UnnecessaryConstructor")
    public S3SourceTask() {
        super();
    }

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void start(final Map<String, String> props) {
        LOGGER.info("S3 Source task started.");
        s3SourceConfig = new S3SourceConfig(props);
        this.transformer = TransformerFactory.getTransformer(s3SourceConfig);
        offsetManager = new OffsetManager(context, s3SourceConfig);
        awsv2SourceClient = new AWSV2SourceClient(s3SourceConfig, failedObjectKeys);
        prepareReaderFromOffsetStorageReader();
        this.taskInitialized = true;
    }

    private void prepareReaderFromOffsetStorageReader() {
        sourceRecordIterator = new SourceRecordIterator(s3SourceConfig, offsetManager, this.transformer,
                awsv2SourceClient);
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
                    extractSourceRecords(results);
                    LOGGER.info("Number of records extracted and sent: {}", results.size());
                    return results;
                } catch (SdkException exception) {
                    if (exception.retryable()) {
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

    private List<SourceRecord> extractSourceRecords(final List<SourceRecord> results) throws InterruptedException {
        waitForObjects();
        if (connectorStopped.get()) {
            return results;
        }
        return RecordProcessor.processRecords(sourceRecordIterator, results, s3SourceConfig, connectorStopped,
                awsv2SourceClient, offsetManager);
    }

    private void waitForObjects() throws InterruptedException {
        while (!sourceRecordIterator.hasNext() && !connectorStopped.get()) {
            LOGGER.debug("Blocking until new S3 files are available.");
            Thread.sleep(S_3_POLL_INTERVAL_MS);
            prepareReaderFromOffsetStorageReader();
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
    public Transformer getTransformer() {
        return transformer;
    }

    public boolean isTaskInitialized() {
        return taskInitialized;
    }

    public AtomicBoolean getConnectorStopped() {
        return new AtomicBoolean(connectorStopped.get());
    }
}
