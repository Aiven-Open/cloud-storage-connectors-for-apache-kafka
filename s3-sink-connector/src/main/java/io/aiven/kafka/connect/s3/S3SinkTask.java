/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.kafka.connect.s3;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import io.aiven.kafka.connect.common.config.FilenameTemplateVariable;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.config.StableTimeFormatter;
import io.aiven.kafka.connect.common.grouper.RecordGrouper;
import io.aiven.kafka.connect.common.grouper.RecordGrouperFactory;
import io.aiven.kafka.connect.common.grouper.TopicPartitionKeyRecordGrouper;
import io.aiven.kafka.connect.common.grouper.TopicPartitionRecordGrouper;
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.common.templating.VariableTemplatePart;
import io.aiven.kafka.connect.s3.config.S3ClientFactory;
import io.aiven.kafka.connect.s3.config.S3SinkConfig;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

@SuppressWarnings("PMD.ExcessiveImports")
public final class S3SinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3SinkTask.class);

    private RecordGrouper recordGrouper;

    private S3SinkConfig config;

    private S3Client s3Client;

    private Map<String, S3ObjectWriter> s3ObjectWriters;

    private TopicPartitionManager topicPartitionManager;
    private BufferTracker bufferTracker;

    private long currentBufferedBytes;
    final Clock clock;
    private long lastWriteMs;
    private boolean isOneRecordPerFile;

    private static final long S3_WRITE_BUFFER_SIZE_BYTES_PER_TASK = 60 * 1024 * 1024L; // 60 MiB
    private static final long S3_WRITE_INTERVAL_MS = 10_000L; // 10 seconds

    S3ClientFactory s3ClientFactory = new S3ClientFactory();

    @SuppressWarnings("PMD.UnnecessaryConstructor") // required by Connect
    public S3SinkTask() {
        super();
        this.clock = Clock.systemUTC();
    }

    // Visible for testing
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "This is test code, and the S3 client is mutable")
    public S3SinkTask(final Map<String, String> props, final S3Client s3Client, final Clock clock) {
        super();
        this.clock = clock;
        start(props);
        // Overwrite s3Client with injected mock
        this.s3Client = s3Client;
    }

    @Override
    public void start(final Map<String, String> props) {
        Objects.requireNonNull(props, "props hasn't been set");
        config = new S3SinkConfig(props);
        this.topicPartitionManager = new TopicPartitionManager();
        this.recordGrouper = RecordGrouperFactory.newRecordGrouper(config);
        s3Client = s3ClientFactory.createAmazonS3Client(config);
        s3ObjectWriters = new HashMap<>();
        isOneRecordPerFile = isOfTypeKeyRecordGrouper(config.getFilenameTemplate());
        try {
            recordGrouper = RecordGrouperFactory.newRecordGrouper(config);
        } catch (final Exception e) { // NOPMD AvoidCatchingGenericException
            throw new ConnectException("Unsupported file name template " + config.getFilename(), e);
        }
        this.lastWriteMs = clock.millis();
        this.bufferTracker = new BufferTracker(config);
        if (Objects.nonNull(config.getKafkaRetryBackoffMs())) {
            context.timeout(config.getKafkaRetryBackoffMs());
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        Objects.requireNonNull(records, "records cannot be null");
        LOGGER.debug("Buffering {} records. Current buffer size: {} bytes", records.size(), currentBufferedBytes);
        boolean shouldRequestCommit = false;
        for (final SinkRecord record : records) {
            final String recordKey = recordGrouper.put(record);
            if (!isOneRecordPerFile) {
                final long recordSize = bufferTracker.addRecord(recordKey, record);
                currentBufferedBytes += recordSize;
                if (bufferTracker.isThresholdReached(recordKey)) {
                    shouldRequestCommit = true;
                }
            }
        }

        if (isOneRecordPerFile) {
            return;
        }

        // check if we should pause topics
        checkRecordSize();
        // Trigger write to S3 if the buffer size threshold is reached or the time interval has passed.
        if (currentBufferedBytes >= S3_WRITE_BUFFER_SIZE_BYTES_PER_TASK
                || clock.millis() - lastWriteMs >= S3_WRITE_INTERVAL_MS) {
            if (currentBufferedBytes >= S3_WRITE_BUFFER_SIZE_BYTES_PER_TASK) {
                LOGGER.debug("S3 write buffer size of {} bytes reached. Writing buffered records to S3.",
                        currentBufferedBytes);
            } else {
                LOGGER.debug("S3 write interval of {} ms reached. Writing buffered records to S3.",
                        S3_WRITE_INTERVAL_MS);
            }
            writeBufferedRecordsToS3();
        }
        // check if we should resume topics
        checkRecordSize();

        if (shouldRequestCommit) {
            topicPartitionManager.requestCommit();
        }
    }

    private void writeBufferedRecordsToS3() {
        final Map<String, List<SinkRecord>> recordsToWrite = recordGrouper.records();
        if (recordsToWrite.isEmpty()) {
            return;
        }

        ConnectException firstException = null;
        final List<String> recordKeys = new ArrayList<>(recordsToWrite.keySet());
        for (final String recordKey : recordKeys) {
            try {
                processSingleFileWrite(recordKey, recordsToWrite.get(recordKey));
                if (recordGrouper instanceof TopicPartitionRecordGrouper) {
                    ((TopicPartitionRecordGrouper) recordGrouper).clearFileBuffers(recordKey);
                } else if (recordGrouper instanceof TopicPartitionKeyRecordGrouper) {
                    ((TopicPartitionKeyRecordGrouper) recordGrouper).clearFileBuffers(recordKey);
                }
            } catch (final ConnectException e) {
                LOGGER.error("Failed to write records for record key {}: {}", recordKey, e.getMessage());
                if (firstException == null) {
                    firstException = e;
                }
            }
        }

        this.currentBufferedBytes = recordGrouper.records().isEmpty()
                ? 0
                : recordGrouper.records()
                        .values()
                        .stream()
                        .flatMap(List::stream)
                        .mapToLong(bufferTracker::estimateRecordSize)
                        .sum();
        this.lastWriteMs = clock.millis();

        if (firstException != null) {
            throw firstException;
        }

        if (!(recordGrouper instanceof TopicPartitionRecordGrouper
                || recordGrouper instanceof TopicPartitionKeyRecordGrouper)) {
            recordGrouper.clear();
        }
    }

    @SuppressWarnings("PMD.CloseResource")
    private void processSingleFileWrite(final String filename, final List<SinkRecord> records) {
        try {
            final S3ObjectWriter s3ObjectWriter = s3ObjectWriters.computeIfAbsent(filename, k -> {
                try {

                    return new S3ObjectWriter(s3Client, config, getFileNameTemplate(k, records.get(0)));
                } catch (final IOException e) {
                    throw new ConnectException("Failed to initialize S3ObjectWriter", e);
                }
            });
            s3ObjectWriter.writeRecords(records);
        } catch (final IOException e) {
            throw new ConnectException("Failed to write records to S3 for " + filename, e);
        }
    }

    /**
     * This determines if the file is key based, and possible to change a single file multiple times per flush or if
     * it's a roll over file which at each flush is reset.
     *
     * @param fileNameTemplate
     *            the format type to output files in supplied in the configuration
     * @return true if is of type RecordGrouperFactory.KEY_RECORD or RecordGrouperFactory.KEY_TOPIC_PARTITION_RECORD
     */
    private boolean isOfTypeKeyRecordGrouper(final Template fileNameTemplate) {
        return RecordGrouperFactory.KEY_RECORD.equals(RecordGrouperFactory.resolveRecordGrouperType(fileNameTemplate))
                || RecordGrouperFactory.KEY_TOPIC_PARTITION_RECORD
                        .equals(RecordGrouperFactory.resolveRecordGrouperType(fileNameTemplate));
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> offsets) {

        LOGGER.debug("Flush triggered. Writing any remaining buffered records and closing S3 files.");

        // Write any records still in the buffer that didn't meet thresholds.
        if (currentBufferedBytes > 0 || isOneRecordPerFile) {
            LOGGER.debug("Writing remaining {} buffered bytes during flush.", currentBufferedBytes);
            writeBufferedRecordsToS3();
        }

        ConnectException firstException = null;
        // Close all active S3 output writers, which finalizes the multipart uploads in S3.
        for (final Map.Entry<String, S3ObjectWriter> entry : s3ObjectWriters.entrySet()) {
            final String fullPath = entry.getKey();
            final S3ObjectWriter writer = entry.getValue();

            try (writer) {
                LOGGER.debug("Closing OutputWriter for: s3://{}/{}", config.getAwsS3BucketName(), fullPath);
            } catch (final Exception e) { // NOPMD broad exception caught
                LOGGER.error("Error closing S3 file s3://{}/{}: {}", config.getAwsS3BucketName(), fullPath,
                        e.getMessage());
                if (firstException == null) {
                    firstException = new ConnectException("Failed to close S3 file " + fullPath, e); // NOPMD
                                                                                                     // Instantiating in
                                                                                                     // a loop
                } else {
                    firstException.addSuppressed(e);
                }
            }
        }

        s3ObjectWriters.clear();
        recordGrouper.clear();
        bufferTracker.clearAll();

        if (firstException != null) {
            // If any file failed to close, throw an exception to prevent offset commit.
            throw firstException;
        }
        LOGGER.debug("Successfully flushed and closed all S3 files.");

        topicPartitionManager.resumeAll();
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public void stop() {
        LOGGER.info("Stopping S3SinkTask. Attempting to close any remaining active S3ObjectWriters.");
        for (final S3ObjectWriter s3ObjectWriter : s3ObjectWriters.values()) {
            try {
                s3ObjectWriter.close();
            } catch (IOException e) {
                LOGGER.warn("Error closing S3ObjectWriter during stop: {}", e.getMessage());
            }
        }
        s3ObjectWriters.clear();
        recordGrouper.clear();
        bufferTracker.clearAll();
        currentBufferedBytes = 0;
        lastWriteMs = clock.millis();
        LOGGER.info("S3 Sink task stopped");
    }

    private String getFileNameTemplate(final String filename, final SinkRecord record) {
        return config.usesFileNameTemplate() ? filename : oldFullKey(record);
    }

    @Override
    public String version() {
        return Version.VERSION;
    }

    // Important: this method is only safe to call during put(), flush(), or preCommit(); otherwise,
    // a ConcurrentModificationException may be triggered if the Connect framework is in the middle of
    // a method invocation on the consumer for this task. This becomes especially likely if all topics
    // have been paused as the framework will most likely be in the middle of a poll for that consumer
    // which, because all of its topics have been paused, will not return until it's time for the next
    // offset commit. Invoking context.requestCommit() won't wake up the consumer in that case, so we
    // really have no choice but to wait for the framework to call a method on this task that implies
    // that it's safe to pause or resume partitions on the consumer.
    private void checkRecordSize() {
        LOGGER.debug("Record soft limit: {} bytes, current record size: {} bytes", S3_WRITE_BUFFER_SIZE_BYTES_PER_TASK,
                currentBufferedBytes);
        if (currentBufferedBytes > S3_WRITE_BUFFER_SIZE_BYTES_PER_TASK) {
            topicPartitionManager.pauseAll();
        } else if (currentBufferedBytes <= S3_WRITE_BUFFER_SIZE_BYTES_PER_TASK / 2) {
            // resume only if there is a reasonable chance we won't immediately have to pause again.
            topicPartitionManager.resumeAll();
        }
    }

    private String oldFullKey(final SinkRecord record) {
        final var prefix = config.getPrefixTemplate()
                .instance()
                .bindVariable(FilenameTemplateVariable.TIMESTAMP.name,
                        new StableTimeFormatter(config.getTimestampSource()).apply(record))
                .bindVariable(FilenameTemplateVariable.PARTITION.name, () -> record.kafkaPartition().toString())
                .bindVariable(FilenameTemplateVariable.START_OFFSET.name,
                        parameter -> OldFullKeyFormatters.KAFKA_OFFSET.apply(record, parameter))
                .bindVariable(FilenameTemplateVariable.TOPIC.name, record::topic)
                .bindVariable("utc_date",
                        () -> ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_LOCAL_DATE))
                .bindVariable("local_date", () -> LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE))
                .render();
        final var key = String.format("%s-%s-%s", record.topic(), record.kafkaPartition(),
                OldFullKeyFormatters.KAFKA_OFFSET.apply(record, VariableTemplatePart.Parameter.of("padding", "true")));
        // Keep this in line with io.aiven.kafka.connect.common.config.AivenCommonConfig#getFilename
        final String formatSuffix = FormatType.AVRO.equals(config.getFormatType()) ? ".avro" : "";
        return prefix + key + formatSuffix + config.getCompressionType().extension();
    }

    private final static class BufferTracker {
        private final Map<String, Long> fileBufferBytes = new HashMap<>();
        private final Map<String, Integer> fileRecordCounts = new HashMap<>();
        private final S3SinkConfig config;

        BufferTracker(final S3SinkConfig config) {
            this.config = config;
        }

        // Add a record to the tracking maps.
        long addRecord(final String recordKey, final SinkRecord record) {
            final long recordSize = estimateRecordSize(record);
            fileBufferBytes.put(recordKey, fileBufferBytes.getOrDefault(recordKey, 0L) + recordSize);
            fileRecordCounts.put(recordKey, fileRecordCounts.getOrDefault(recordKey, 0) + 1);
            return recordSize;
        }

        boolean isThresholdReached(final String recordKey) {
            if (config.isMaxBytesPerFileLimited()) {
                final Long currentBytes = fileBufferBytes.get(recordKey);
                if (currentBytes != null && currentBytes >= config.getMaxBytesPerFile()) {
                    return true;
                }
            }
            return config.getMaxRecordsPerFile() > 0
                    && fileRecordCounts.getOrDefault(recordKey, 0) >= config.getMaxRecordsPerFile();
        }

        void clearAll() {
            fileBufferBytes.clear();
            fileRecordCounts.clear();
        }

        // Estimates the size of a SinkRecord in bytes. This is a rough approximation based on the byte
        // length of the key and value's String representation.
        long estimateRecordSize(final SinkRecord record) {
            long size = 20; // Constant overhead
            if (record.key() != null) {
                size += getObjectSize(record.key());
            }
            if (record.value() != null) {
                size += getObjectSize(record.value());
            }
            return size;
        }

        private long getObjectSize(final Object data) {
            if (data instanceof byte[]) {
                return ((byte[]) data).length;
            } else if (data instanceof String) {
                return ((String) data).getBytes(StandardCharsets.UTF_8).length;
            } else {
                return String.valueOf(data).getBytes(StandardCharsets.UTF_8).length;
            }
        }
    }

    private class TopicPartitionManager {

        private Long lastChangeMs;
        private Long lastCommitMs;
        private boolean isPaused;

        public TopicPartitionManager() {
            this.lastChangeMs = clock.millis();
            this.lastCommitMs = clock.millis();
            this.isPaused = false;
        }

        private void pauseAll() {
            if (!isPaused) {
                final long now = clock.millis();
                LOGGER.debug("Paused all partitions after {}ms", now - lastChangeMs);
                isPaused = true;
                lastChangeMs = now;
            }
            final Set<TopicPartition> assignment = context.assignment();
            final TopicPartition[] topicPartitions = new TopicPartition[assignment.size()];
            context.pause(assignment.toArray(topicPartitions));
        }

        private void resumeAll() {
            if (isPaused) {
                final long now = clock.millis();
                LOGGER.debug("Resumed all partitions after {}ms", now - lastChangeMs);
                isPaused = false;
                lastChangeMs = now;
                final Set<TopicPartition> assignment = context.assignment();
                final TopicPartition[] topicPartitions = new TopicPartition[assignment.size()];
                context.resume(assignment.toArray(topicPartitions));
            }
        }

        private void requestCommit() {
            final long now = clock.millis();
            LOGGER.debug("Requesting commit for all partitions after {}ms", now - lastCommitMs);
            lastCommitMs = now;
            context.requestCommit();
        }
    }

}
