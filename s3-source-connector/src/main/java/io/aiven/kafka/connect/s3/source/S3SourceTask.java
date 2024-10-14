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

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.aiven.kafka.connect.s3.source.utils.*;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;

import io.aiven.kafka.connect.s3.source.config.S3ClientFactory;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.output.Transformer;
import io.aiven.kafka.connect.s3.source.output.TransformerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.*;

/**
 * S3SourceTask is a Kafka Connect SourceTask implementation that reads from source-s3 buckets and generates Kafka
 * Connect records.
 */
@SuppressWarnings("PMD.TooManyMethods")
public class S3SourceTask extends SourceTask {

    private  static final int PAGE_SIZE_FACTOR = 2;
    private static final Logger LOGGER = LoggerFactory.getLogger(S3SourceTask.class);

    public static final String BUCKET = "bucket";
    public static final String TOPIC = "topic";
    public static final String PARTITION = "partition";

    public static final String OBJECT_KEY = "object_key";

    private static final long S_3_POLL_INTERVAL_MS = 10_000L;
    private static final long ERROR_BACKOFF = 1000L;

    private S3SourceConfig s3SourceConfig;
    private AmazonS3 s3Client;

    private Optional<Converter> keyConverter;

    private Converter valueConverter;

    private Transformer outputWriter;

    private String s3Bucket;

    private boolean taskInitialized;

    private final AtomicBoolean connectorStopped = new AtomicBoolean();
    private final S3ClientFactory s3ClientFactory = new S3ClientFactory();

    private final Object pollLock = new Object();
    private final Set<String> failedObjectKeys = new HashSet<>();

    private S3ObjectSummaryIterator s3ObjectSummaryIterator;

    private AivenS3SourceRecordIterator aivenS3SourceRecordIterator;

    private final List<String> objectSkipList = new ArrayList<>();


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
        initializeConverters();
        initializeS3Client();
        this.s3Bucket = s3SourceConfig.getString(AWS_S3_BUCKET_NAME_CONFIG);
        this.outputWriter = TransformerFactory.transformer(s3SourceConfig);
        this.s3ObjectSummaryIterator = new S3ObjectSummaryIterator(s3Client, s3Bucket, s3SourceConfig.getInt(FETCH_PAGE_SIZE) * PAGE_SIZE_FACTOR, null);
        this.aivenS3SourceRecordIterator = new AivenS3SourceRecordIterator(s3SourceConfig, s3Client, s3Bucket, context,  outputWriter, new FilterIterator<S3ObjectSummary>(s3ObjectSummaryIterator, buildObjectSummaryPredicate()),
               objectSkipList::add);
        this.taskInitialized = true;
    }

    /**
     * Creates a Predicate that must return {@code true} for an {@link S3ObjectSummary} to be considered for output
     * @return the Predicate to filter {@link S3ObjectSummary} objects.
     */
    private Predicate<S3ObjectSummary> buildObjectSummaryPredicate() {
        Predicate<S3ObjectSummary> result = o -> !objectSkipList.contains(o.getKey());
        result = result.and(o -> o.getSize() != 0);
        // use Predicate.and or Predicate.or to construct a compound predicate.
        return result;
    }

    private void initializeConverters() {
        try {
            keyConverter = Optional
                    .of((Converter) s3SourceConfig.getClass("key.converter").getDeclaredConstructor().newInstance());
            valueConverter = (Converter) s3SourceConfig.getClass("value.converter")
                    .getDeclaredConstructor()
                    .newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException
                | NoSuchMethodException e) {
            throw new ConnectException("Connect converters could not be instantiated.", e);
        }
    }

    private void initializeS3Client() {
        this.s3Client = s3ClientFactory.createAmazonS3Client(s3SourceConfig);
        LOGGER.debug("S3 client initialized");
    }

    private void prepareReaderFromOffsetStorageReader() {
        aivenS3SourceRecordIterator.resetOffsetManager(context);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        int maxRecords = s3SourceConfig.getInt(MAX_POLL_RECORDS);
        synchronized (pollLock) {

            if (connectorStopped.get()) {
                return Collections.emptyList();
            }

            final List<SourceRecord> results = new ArrayList<>(maxRecords);
            while (!connectorStopped.get()) {
                try {
                    // FIXME there is a problem here.  If we throw an exception on a middle record the rest of the records do not get processed.
                    // but we have already marked the S3 objects as having been processed. (i.e. we lose data)
                    return extractSourceRecords(results, maxRecords);
                }
                catch (AmazonS3Exception | DataException exception) {
                    if (handleException(exception)) {
                        return null; // NOPMD
                    }
                } catch (final Throwable t) { // NOPMD
                    // This task has failed, so close any resources (may be reopened if needed) before throwing
                    closeResources();
                    throw t;
                }
            }
            return results;
        }
    }

    private boolean handleException(final RuntimeException exception) throws InterruptedException {
        if (exception instanceof AmazonS3Exception) {
            if (((AmazonS3Exception) exception).isRetryable()) {
                LOGGER.warn("Retryable error while polling. Will sleep and try again.", exception);
                Thread.sleep(ERROR_BACKOFF);
                prepareReaderFromOffsetStorageReader();
            } else {
                return true;
            }
        }
        if (exception instanceof DataException) {
            LOGGER.warn("DataException. Will NOT try again.", exception);
        }
        return false;
    }

    private List<SourceRecord> extractSourceRecords(final List<SourceRecord> results, final int maxRecords) throws InterruptedException {
        // FIXME : I think the following line is wrong.  We are supposed to return null from Poll if there is no data.
        waitForObjects();
        if (connectorStopped.get()) {
            return results;
        }
        final Map<String, String> conversionConfig = new HashMap<>();
        int idx = 0;
        while (idx<maxRecords && aivenS3SourceRecordIterator.hasNext()) {
            idx++;
            AivenS3SourceRecord aivenS3SourceRecord = aivenS3SourceRecordIterator.next();
            final String topic = aivenS3SourceRecord.getToTopic();
            final Optional<SchemaAndValue> keyData = keyConverter
                    .map(c -> c.toConnectData(topic, aivenS3SourceRecord.key()));

            outputWriter.configureValueConverter(conversionConfig, s3SourceConfig);
            valueConverter.configure(conversionConfig, false);
            try {
                final SchemaAndValue schemaAndValue = valueConverter.toConnectData(topic, aivenS3SourceRecord.value());
                results.add(aivenS3SourceRecord.getSourceRecord(topic, keyData, schemaAndValue));
            } catch (DataException e) {
                LOGGER.error("Error in reading s3 object stream " + e.getMessage());
                failedObjectKeys.add(aivenS3SourceRecord.getObjectKey());
                throw e;
            }
        }
        return results;
    }

    private void waitForObjects() throws InterruptedException {
        while (!s3ObjectSummaryIterator.hasNext() && !connectorStopped.get()) {
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
        s3Client.shutdown();
    }

    // below for visibility in tests
    public Optional<Converter> getKeyConverter() {
        return keyConverter;
    }

    public Converter getValueConverter() {
        return valueConverter;
    }

    public Transformer getOutputWriter() {
        return outputWriter;
    }

    public boolean isTaskInitialized() {
        return taskInitialized;
    }

    public AtomicBoolean getConnectorStopped() {
        return new AtomicBoolean(connectorStopped.get());
    }
}
