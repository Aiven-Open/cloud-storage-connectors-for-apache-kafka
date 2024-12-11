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

package io.aiven.kafka.connect.s3.source.utils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import io.aiven.kafka.connect.common.source.input.ByteArrayTransformer;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.collections4.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterator that processes S3 files and creates Kafka source records. Supports different output formats (Avro, JSON,
 * Parquet).
 */
public final class SourceRecordIterator implements Iterator<S3SourceRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceRecordIterator.class);

    public static final long BYTES_TRANSFORMATION_NUM_OF_RECS = 1L;

    private final OffsetManager offsetManager;

    private final S3SourceConfig s3SourceConfig;

    private final Transformer transformer;

    private final FileMatcherFilter fileMatcherFilter;

    private final Iterator<Iterator<S3SourceRecord>> iteratorIterator;

    private Iterator<S3SourceRecord> workingIterator;

    public SourceRecordIterator(final S3SourceConfig s3SourceConfig, final OffsetManager offsetManager,
            final Transformer transformer, final AWSV2SourceClient sourceClient) {
        this.s3SourceConfig = s3SourceConfig;
        this.offsetManager = offsetManager;
        this.transformer = transformer;
        this.fileMatcherFilter = new FileMatcherFilter();
        // add the fileMatcher and task assignment filters.
        sourceClient.andFilter(fileMatcherFilter.and(new TaskAssignmentFilter(s3SourceConfig)));
        iteratorIterator = IteratorUtils.transformedIterator(sourceClient.getObjectIterator(null),
                s3Object -> createIteratorForS3Object(s3Object));
        workingIterator = IteratorUtils.emptyIterator();
    }

    // For bytes transformation, read whole file as 1 record
    private boolean checkBytesTransformation(final Transformer transformer, final long numberOfRecsAlreadyProcessed) {
        return transformer instanceof ByteArrayTransformer
                && numberOfRecsAlreadyProcessed == BYTES_TRANSFORMATION_NUM_OF_RECS;
    }

    /**
     * Creates an Iterator of S3SourceRecords from an s3Object. package private for testing
     *
     * @param s3Object
     *            the object to get the S3SourceRecords from.
     * @return an Iterator of S3SourceRecords.
     */
    Iterator<S3SourceRecord> createIteratorForS3Object(final S3Object s3Object) {
        final Map<String, Object> partitionMap = ConnectUtils.getPartitionMap(fileMatcherFilter.getTopicName(),
                fileMatcherFilter.getPartitionId(), s3SourceConfig.getAwsS3BucketName());
        final long numberOfRecsAlreadyProcessed = offsetManager.recordsProcessedForObjectKey(partitionMap,
                s3Object.getKey());
        // Optimizing without reading stream again.
        if (checkBytesTransformation(transformer, numberOfRecsAlreadyProcessed)) {
            return IteratorUtils.emptyIterator();
        }
        final long startOffset = 1L;
        final byte[] keyBytes = s3Object.getKey().getBytes(StandardCharsets.UTF_8);
        final List<S3SourceRecord> sourceRecords = new ArrayList<>();
        try (Stream<Object> recordStream = transformer.getRecords(s3Object::getObjectContent,
                fileMatcherFilter.getTopicName(), fileMatcherFilter.getPartitionId(), s3SourceConfig,
                numberOfRecsAlreadyProcessed)) {
            final Iterator<Object> recordIterator = recordStream.iterator();
            while (recordIterator.hasNext()) {
                final Object record = recordIterator.next();
                final byte[] valueBytes = transformer.getValueBytes(record, fileMatcherFilter.getTopicName(),
                        s3SourceConfig);
                sourceRecords.add(getSourceRecord(s3Object.getKey(), keyBytes, valueBytes, offsetManager, startOffset,
                        partitionMap));
            }
        }

        return sourceRecords.iterator();
    }

    /**
     * Creates an S3SourceRecord. Package private for testing.
     *
     * @param objectKey
     *            the key for the object we read this record from.
     * @param key
     *            the key for this record
     * @param value
     *            the value for this record.
     * @param offsetManager
     *            the offsetManager.
     * @param startOffset
     *            the starting offset.
     * @param partitionMap
     *            the partition map for this object.
     * @return the S3SourceRecord.
     */
    S3SourceRecord getSourceRecord(final String objectKey, final byte[] key, final byte[] value,
            final OffsetManager offsetManager, final long startOffset, final Map<String, Object> partitionMap) {

        long currentOffset;

        if (offsetManager.getOffsets().containsKey(partitionMap)) {
            LOGGER.info("***** offsetManager.getOffsets() ***** {}", offsetManager.getOffsets());
            currentOffset = offsetManager.incrementAndUpdateOffsetMap(partitionMap, objectKey, startOffset);
        } else {
            LOGGER.info("Into else block ...");
            currentOffset = startOffset;
            offsetManager.createNewOffsetMap(partitionMap, objectKey, currentOffset);
        }

        final Map<String, Object> offsetMap = offsetManager.getOffsetValueMap(objectKey, currentOffset);

        return new S3SourceRecord(partitionMap, offsetMap, fileMatcherFilter.getTopicName(),
                fileMatcherFilter.getPartitionId(), key, value, objectKey);
    }

    @Override
    public boolean hasNext() {
        while (!workingIterator.hasNext() && iteratorIterator.hasNext()) {
            workingIterator = iteratorIterator.next();
        }
        return workingIterator.hasNext();
    }

    @Override
    public S3SourceRecord next() {
        if (!hasNext()) {
            throw new IllegalArgumentException();
        }
        return workingIterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("This iterator is unmodifiable");
    }

    /**
     * A filter for S3ObjectSummaries that extracts the topic name and partition id. Package private for testing.
     */
    static class FileMatcherFilter implements Predicate<S3ObjectSummary> {
        private static final String PATTERN_TOPIC_KEY = "topicName";
        private static final String PATTERN_PARTITION_KEY = "partitionId";

        private static final Pattern FILE_DEFAULT_PATTERN = Pattern.compile("(?<topicName>[^/]+?)-"
                + "(?<partitionId>\\d{5})-" + "(?<uniqueId>[a-zA-Z0-9]+)" + "\\.(?<fileExtension>[^.]+)$"); // topic-00001.txt

        private static final String NOT_ASSIGNED = null;
        /** The extracted topic name or null the last {@link #test(S3ObjectSummary)} returned false. */
        private String topicName;
        /** The extracted partition id or -1 if the last {@link #test(S3ObjectSummary)} returned false. */
        private int partitionId = -1;

        @Override
        public boolean test(final S3ObjectSummary s3ObjectSummary) {
            final Matcher fileMatcher = FILE_DEFAULT_PATTERN.matcher(s3ObjectSummary.getKey());
            if (fileMatcher.find()) {
                topicName = fileMatcher.group(PATTERN_TOPIC_KEY);
                partitionId = Integer.parseInt(fileMatcher.group(PATTERN_PARTITION_KEY));
                return true;
            }
            LOGGER.error("File naming doesn't match to any topic. {}", s3ObjectSummary.getKey());
            topicName = NOT_ASSIGNED;
            partitionId = -1;
            return false;
        }

        /**
         * Gets the extracted topic name.
         *
         * @return the topic name or {@code null} if the topic has not been set.
         */
        public String getTopicName() {
            return topicName;
        }

        /**
         * Gets the extracted partion Id
         *
         * @return the partition id or -1 if the value has not been set.
         */
        public int getPartitionId() {
            return partitionId;
        }
    }

    /**
     * A filter that determines if the S3ObjectSummary belongs to this task. Package private for testing. TODO: Should
     * be replaced with actual task assignment predicate.
     */
    static class TaskAssignmentFilter implements Predicate<S3ObjectSummary> {
        /** The maximum number of tasks */
        final int maxTasks;
        /** The task ID for this task */
        final int taskId;

        /**
         * Extracts integer values from original in config. TODO create acutal properties in the configuration.
         *
         * @param config
         *            the config to extract from.
         * @param key
         *            the key to fine.
         * @param dfltValue
         *            the default value on error.
         * @return the integer value parsed or default on error.
         */
        private static int fromOriginals(final S3SourceConfig config, final String key, final int dfltValue) {
            final Object obj = config.originals().get(key);
            if (obj != null) {
                try {
                    return Integer.parseInt(obj.toString());
                } catch (NumberFormatException e) {
                    return dfltValue;
                }
            }
            return dfltValue;
        }
        /**
         * Constructor.
         *
         * @param config
         *            the source config to get "tasks.max" and "task.id" values from.
         */
        TaskAssignmentFilter(final S3SourceConfig config) {
            maxTasks = fromOriginals(config, "tasks.max", 1);
            taskId = fromOriginals(config, "task.id", 0);
        }

        @Override
        public boolean test(final S3ObjectSummary s3ObjectSummary) {
            return taskId == Math.floorMod(s3ObjectSummary.getKey().hashCode(), maxTasks);
        }
    }
}
