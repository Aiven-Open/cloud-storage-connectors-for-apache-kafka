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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import io.aiven.kafka.connect.common.source.input.TransformerFactory;
import io.aiven.kafka.connect.s3.source.utils.OffsetManager;
import org.apache.kafka.connect.source.SourceRecord;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.AbstractSourceTask;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.utils.AWSV2SourceClient;
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
public class S3SourceTask extends AbstractSourceTask {
    /** The logger to write to */
    private static final Logger LOGGER = LoggerFactory.getLogger(S3SourceTask.class);

    public static final String BUCKET = "bucket";
    public static final String TOPIC = "topic";

    public static final String OBJECT_KEY = "object_key";
    public static final String PARTITION = "topicPartition";


    /** An iterator or S3SourceRecords */
    private Iterator<S3SourceRecord> s3SourceRecordIterator;
    /**
     * The transformer that we are using TODO move this to AbstractSourceTask
     */
    private Transformer transformer;
    /** The task initialized flag */
    private boolean taskInitialized;
    /** The AWS Source client */
    private AWSV2SourceClient awsv2SourceClient;
    /** The list of failed object keys */
    private final Set<String> failedObjectKeys = new HashSet<>();
    /** The offset manager this task uses */
    private OffsetManager offsetManager;

    public S3SourceTask() {
        super(LOGGER);
    }

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    protected Iterator<SourceRecord> getIterator() { // NOPMD cognatavie complexity
        return new Iterator<SourceRecord>() {
            /** The backoff for Amazon retryable exceptions */
            final Backoff backoff = new Backoff(MAX_POLL_TIME);
            @Override
            public boolean hasNext() {
                try {
                    // this timer is the master timer from the AbstractSourceTask.
                    while (stillPolling()) {
                        try {
                            return s3SourceRecordIterator.hasNext();
                        } catch (AmazonS3Exception exception) {
                            if (exception.isRetryable()) {
                                LOGGER.warn("Retryable error encountered during polling. Waiting before retrying...",
                                        exception);
                                try {
                                    backoff.delay();
                                } catch (InterruptedException e) {
                                    LOGGER.warn("Backoff delay was interrupted.  Throwing original exception: {}",
                                            exception.getMessage());
                                    throw exception;
                                }
                            } else {
                                // TODO validate that the iterator does not lose an S3Object. Add test to
                                // S3ObjectIterator.
                                throw exception;
                            }
                        }
                    }
                    return false;
                } finally {
                    backoff.reset();
                }
            }

            @Override
            public SourceRecord next() {
                final S3SourceRecord s3SourceRecord = s3SourceRecordIterator.next();
                offsetManager.incrementAndUpdateOffsetMap(s3SourceRecord.getPartitionMap(), s3SourceRecord.getObjectKey(), 1L);
                return s3SourceRecord.getSourceRecord(s3SourceRecord.getTopic());
            }
        };
    }

    @Override
    protected SourceCommonConfig configure(final Map<String, String> props) {
        LOGGER.info("S3 Source task started.");
        final S3SourceConfig s3SourceConfig = new S3SourceConfig(props);
        this.transformer = TransformerFactory.getTransformer(s3SourceConfig);
        offsetManager = new OffsetManager(context, s3SourceConfig);
        awsv2SourceClient = new AWSV2SourceClient(s3SourceConfig, failedObjectKeys);
        setS3SourceRecordIterator(
                new SourceRecordIterator(s3SourceConfig, offsetManager, this.transformer, awsv2SourceClient));
        this.taskInitialized = true;
        return s3SourceConfig;
    }

    @Override
    public void commit() throws InterruptedException {
        LOGGER.info("Committed all records through last poll()");
    }

    @Override
    public void commitRecord(final SourceRecord record) throws InterruptedException {
        if (LOGGER.isInfoEnabled()) {
            final Map<String, Object> map = (Map<String, Object>) record.sourceOffset();
            LOGGER.info("Committed individual record {} {} {} committed", map.get(BUCKET), map.get(OBJECT_KEY), offsetManager.recordsProcessedForObjectKey((Map)record.sourcePartition(), map.get(OBJECT_KEY).toString()));
        }
    }

    /**
     * Set the S3 source record iterator that this task is using. protected to be overridden in testing impl.
     *
     * @param iterator
     *            The S3SourceRecord iterator to use.
     */
    protected void setS3SourceRecordIterator(final Iterator<S3SourceRecord> iterator) {
        s3SourceRecordIterator = iterator;
    }

    @Override
    protected void closeResources() {
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
}
