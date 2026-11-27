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

package io.aiven.kafka.connect.gcs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
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

import io.aiven.kafka.connect.common.grouper.FileRecordGrouper;
import io.aiven.kafka.connect.common.grouper.RecordGrouper;
import io.aiven.kafka.connect.common.grouper.RecordGrouperFactory;

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("PMD.TooManyMethods")
public final class GcsSinkTask extends SinkTask {
    private static final Logger LOG = LoggerFactory.getLogger(GcsSinkTask.class);
    private static final String USER_AGENT_HEADER_KEY = "user-agent";

    private RecordGrouper recordGrouper;
    // Same instance as recordGrouper when files stay open across writes; null for one-record-per-file groupers.
    private FileRecordGrouper fileRecordGrouper;

    private GcsSinkConfig config;

    private Storage storage;

    private TopicPartitionManager topicPartitionManager;
    private BufferTracker bufferTracker;
    private final Map<String, GcsBlobWriter> activeBlobWriters = new HashMap<>();
    private static final long GCS_WRITE_BUFFER_SIZE_BYTES_PER_TASK = 60 * 1024 * 1024L; // 60 MiB
    private long gcsWriteIntervalMs = 10_000L; // 10 seconds

    private long currentBufferedBytes;
    final Clock clock;
    private long lastWriteMs;
    private boolean isOneRecordPerFile;

    // required by Connect
    public GcsSinkTask() {
        super();
        this.clock = Clock.systemUTC();
    }

    // for testing
    public GcsSinkTask(final Map<String, String> props, final Storage storage) {
        this(props, storage, Clock.systemUTC());
    }

    // for testing
    public GcsSinkTask(final Map<String, String> props, final Storage storage, final Clock clock) {
        super();
        Objects.requireNonNull(props, "props cannot be null");
        Objects.requireNonNull(storage, "storage cannot be null");
        Objects.requireNonNull(clock, "clock cannot be null");

        this.config = new GcsSinkConfig(props);
        this.storage = storage;
        this.clock = clock;
        initRest();
    }

    @Override
    public void start(final Map<String, String> props) {
        Objects.requireNonNull(props, "props cannot be null");

        this.config = new GcsSinkConfig(props);
        gcsWriteIntervalMs = config.getMaxLingerMs();
        this.storage = StorageOptions.newBuilder()
                .setHost(config.getGcsEndpoint())
                .setCredentials(config.getCredentials())
                .setHeaderProvider(FixedHeaderProvider.create(USER_AGENT_HEADER_KEY, config.getUserAgent()))
                .setRetrySettings(RetrySettings.newBuilder()
                        .setInitialRetryDelayDuration(config.getGcsRetryBackoffInitialDelay())
                        .setMaxRetryDelayDuration(config.getGcsRetryBackoffMaxDelay())
                        .setRetryDelayMultiplier(config.getGcsRetryBackoffDelayMultiplier())
                        .setTotalTimeoutDuration(config.getGcsRetryBackoffTotalTimeout())
                        .setMaxAttempts(config.getGcsRetryBackoffMaxAttempts())
                        .build())
                .build()
                .getService();
        initRest();
        if (Objects.nonNull(config.getKafkaRetryBackoffMs())) {
            context.timeout(config.getKafkaRetryBackoffMs());
        }
    }

    private void initRest() {
        try {
            this.topicPartitionManager = new TopicPartitionManager();
            this.recordGrouper = RecordGrouperFactory.newRecordGrouper(config);
            if (recordGrouper instanceof FileRecordGrouper) {
                this.fileRecordGrouper = (FileRecordGrouper) recordGrouper;
            }
            final String grType = RecordGrouperFactory.resolveRecordGrouperType(config.getFilenameTemplate());
            if (RecordGrouperFactory.KEY_RECORD.equals(grType)
                    || RecordGrouperFactory.KEY_TOPIC_PARTITION_RECORD.equals(grType)) {
                this.isOneRecordPerFile = true;
            }
            this.lastWriteMs = clock.millis();
            this.bufferTracker = new BufferTracker(config);
        } catch (final Exception e) { // NOPMD broad exception caught
            throw new ConnectException("Unsupported file name template " + config.getFilename(), e);
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        Objects.requireNonNull(records, "records cannot be null");
        LOG.debug("Buffering {} records. Current buffer size: {} bytes", records.size(), currentBufferedBytes);
        boolean shouldRequestCommit = false;
        for (final SinkRecord record : records) {
            shouldRequestCommit |= bufferRecord(record);
        }

        if (isOneRecordPerFile) {
            return;
        }

        // check if we should pause topics
        checkRecordSize();
        // Trigger write to GCS if the buffer size threshold is reached or the time interval has passed.
        if (currentBufferedBytes >= GCS_WRITE_BUFFER_SIZE_BYTES_PER_TASK
                || clock.millis() - lastWriteMs >= gcsWriteIntervalMs) {
            if (currentBufferedBytes >= GCS_WRITE_BUFFER_SIZE_BYTES_PER_TASK) {
                LOG.debug("GCS write buffer size of {} bytes reached. Writing buffered records to GCS.",
                        currentBufferedBytes);
            } else {
                LOG.debug("GCS write interval of {} ms reached. Writing buffered records to GCS.", gcsWriteIntervalMs);
            }
            writeBufferedRecordsToGcs();
        }
        // check if we should resume topics
        checkRecordSize();

        if (shouldRequestCommit) {
            topicPartitionManager.requestCommit();
        }
    }

    // Buffers one record and returns whether a file limit was reached, i.e. an offset commit should be requested.
    private boolean bufferRecord(final SinkRecord record) {
        final String recordKey = recordGrouper.put(record);
        if (!isOneRecordPerFile) {
            currentBufferedBytes += bufferTracker.addRecord(recordKey, record);
            if (fileRecordGrouper != null && fileRecordGrouper.isFileFull(recordKey)) {
                closeCompletedFile(recordKey);
                return true;
            }
            return bufferTracker.isThresholdReached(recordKey);
        }
        return false;
    }

    @Override
    @SuppressWarnings({ "PMD.CloseResource", "PMD.AvoidInstantiatingObjectsInLoops" })
    public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        LOG.debug("Flush triggered. Writing any remaining buffered records and closing GCS files.");

        // Write any records still in the buffer that didn't meet thresholds.
        if (currentBufferedBytes > 0 || isOneRecordPerFile) {
            LOG.debug("Writing remaining {} buffered bytes during flush.", currentBufferedBytes);
            writeBufferedRecordsToGcs();
        }

        ConnectException firstException = null;
        // Close all active GcsBlobWriters, which finalizes the resumable uploads in GCS.
        for (final Map.Entry<String, GcsBlobWriter> entry : activeBlobWriters.entrySet()) {
            final String fullPath = entry.getKey();
            final GcsBlobWriter blobWriter = entry.getValue();

            try (blobWriter) {
                LOG.debug("Closing GcsBlobWriter for: gs://{}/{}", config.getBucketName(), fullPath);
            } catch (final Exception e) { // NOPMD broad exception caught
                LOG.error("Error closing GCS file gs://{}/{}: {}", config.getBucketName(), fullPath, e.getMessage());
                if (firstException == null) {
                    firstException = new ConnectException("Failed to close GCS file " + fullPath, e);
                } else {
                    firstException.addSuppressed(e);
                }
            }
        }

        activeBlobWriters.clear();
        recordGrouper.clear();
        bufferTracker.clearAll();

        if (firstException != null) {
            // If any file failed to close, throw an exception to prevent offset commit.
            throw firstException;
        }
        LOG.debug("Successfully flushed and closed all GCS files.");

        topicPartitionManager.resumeAll();
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public void stop() {
        LOG.info("Stopping GcsSinkTask. Attempting to close any remaining active GcsBlobWriters.");
        for (final GcsBlobWriter blobWriter : activeBlobWriters.values()) {
            try {
                blobWriter.close();
            } catch (IOException e) {
                LOG.warn("Error closing GcsBlobWriter during stop: {}", e.getMessage());
            }
        }
        activeBlobWriters.clear();
        recordGrouper.clear();
        bufferTracker.clearAll();
        currentBufferedBytes = 0;
        lastWriteMs = clock.millis();
    }

    @Override
    public String version() {
        return Version.VERSION;
    }

    private void writeBufferedRecordsToGcs() {
        final Map<String, List<SinkRecord>> recordsToWrite = recordGrouper.records();
        if (recordsToWrite.isEmpty()) {
            return;
        }

        ConnectException firstException = null;
        final List<String> recordKeys = new ArrayList<>(recordsToWrite.keySet());
        for (final String recordKey : recordKeys) {
            try {
                processSingleFileWrite(recordKey, recordsToWrite.get(recordKey));
                if (fileRecordGrouper != null) {
                    fileRecordGrouper.clearFileBuffers(recordKey);
                }
            } catch (final ConnectException e) {
                LOG.error("Failed to write records for record key {}: {}", recordKey, e.getMessage());
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

        if (fileRecordGrouper == null) {
            recordGrouper.clear();
        }
    }

    @SuppressWarnings("PMD.CloseResource")
    private void processSingleFileWrite(final String filename, final List<SinkRecord> records) {
        if (isOneRecordPerFile) {
            writeAndCloseSingleFile(filename, records);
            return;
        }
        try {
            final GcsBlobWriter blobWriter = activeBlobWriters.computeIfAbsent(filename, k -> {
                try {
                    return new GcsBlobWriter(storage, config, k);
                } catch (final IOException e) {
                    throw new ConnectException("Failed to initialize GcsBlobWriter", e);
                }
            });
            blobWriter.writeRecords(records);
        } catch (final IOException e) {
            throw new ConnectException("Failed to write records to GCS for " + filename, e);
        }
    }

    // Each file is complete after a single write, so the writer is closed immediately instead of being kept in
    // activeBlobWriters. This keeps at most one upload buffer alive at a time, regardless of the number of files.
    // The writer is also closed when the write fails, so that its resources are released right away. Closing
    // finalizes the records written so far, and the partial object is replaced when the write is retried.
    private void writeAndCloseSingleFile(final String filename, final List<SinkRecord> records) {
        try (GcsBlobWriter blobWriter = new GcsBlobWriter(storage, config, filename)) {
            blobWriter.writeRecords(records);
        } catch (final IOException e) {
            throw new ConnectException("Failed to write records to GCS for " + filename, e);
        }
    }

    // Called as soon as a file reaches file.max.records: its remaining buffered records are written and the writer
    // is closed right away instead of lingering in activeBlobWriters until the next flush(). The writer is removed
    // and closed even when the write fails, mirroring writeAndCloseSingleFile; the records then stay buffered and
    // the partial object is replaced when the write is retried.
    @SuppressWarnings("PMD.CloseResource")
    private void closeCompletedFile(final String filename) {
        final List<SinkRecord> records = recordGrouper.records().getOrDefault(filename, List.of());
        LOG.debug("File {} reached file.max.records ({}). Writing {} buffered records and closing it.", filename,
                config.getMaxRecordsPerFile(), records.size());

        ConnectException writeFailure = null;
        try {
            processSingleFileWrite(filename, records);
        } catch (final ConnectException e) {
            writeFailure = e;
        }

        final GcsBlobWriter blobWriter = activeBlobWriters.remove(filename);
        if (blobWriter != null) {
            try {
                blobWriter.close();
            } catch (final IOException e) {
                if (writeFailure == null) {
                    throw new ConnectException("Failed to close GCS file " + filename, e);
                }
                writeFailure.addSuppressed(e);
            }
        }
        if (writeFailure != null) {
            throw writeFailure;
        }

        currentBufferedBytes -= records.stream().mapToLong(bufferTracker::estimateRecordSize).sum();
        fileRecordGrouper.completeFile(filename);
        bufferTracker.remove(filename);
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
        LOG.debug("Record soft limit: {} bytes, current record size: {} bytes", GCS_WRITE_BUFFER_SIZE_BYTES_PER_TASK,
                currentBufferedBytes);
        if (currentBufferedBytes > GCS_WRITE_BUFFER_SIZE_BYTES_PER_TASK) {
            topicPartitionManager.pauseAll();
        } else if (currentBufferedBytes <= GCS_WRITE_BUFFER_SIZE_BYTES_PER_TASK / 2) {
            // resume only if there is a reasonable chance we won't immediately have to pause again.
            topicPartitionManager.resumeAll();
        }
    }

    private final static class BufferTracker {
        private final Map<String, Long> fileBufferBytes = new HashMap<>();
        private final GcsSinkConfig config;

        BufferTracker(final GcsSinkConfig config) {
            this.config = config;
        }

        // Add a record to the tracking maps.
        long addRecord(final String recordKey, final SinkRecord record) {
            final long recordSize = estimateRecordSize(record);
            fileBufferBytes.put(recordKey, fileBufferBytes.getOrDefault(recordKey, 0L) + recordSize);
            return recordSize;
        }

        boolean isThresholdReached(final String recordKey) {
            if (config.isMaxBytesPerFileLimited()) {
                final Long currentBytes = fileBufferBytes.get(recordKey);
                if (currentBytes != null && currentBytes >= config.getMaxBytesPerFile()) {
                    return true;
                }
            }
            return false;
        }

        void remove(final String recordKey) {
            fileBufferBytes.remove(recordKey);
        }

        void clearAll() {
            fileBufferBytes.clear();
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
                LOG.debug("Paused all partitions after {}ms", now - lastChangeMs);
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
                LOG.debug("Resumed all partitions after {}ms", now - lastChangeMs);
                isPaused = false;
                lastChangeMs = now;
                final Set<TopicPartition> assignment = context.assignment();
                final TopicPartition[] topicPartitions = new TopicPartition[assignment.size()];
                context.resume(assignment.toArray(topicPartitions));
            }
        }

        private void requestCommit() {
            final long now = clock.millis();
            LOG.debug("Requesting commit for all partitions after {}ms", now - lastCommitMs);
            lastCommitMs = now;
            context.requestCommit();
        }
    }
}
