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

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.utils.FilePatternUtils;
import io.aiven.kafka.connect.common.source.task.Context;
import io.aiven.kafka.connect.common.source.task.DistributionStrategy;
import io.aiven.kafka.connect.common.source.task.DistributionType;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Iterator that processes S3 files and creates Kafka source records. Supports different output formats (Avro, JSON,
 * Parquet).
 */
public final class SourceRecordIterator implements Iterator<S3SourceRecord> {
    /** The OffsetManager that we are using */
    private final OffsetManager<S3OffsetManagerEntry> offsetManager;

    /** The configuration for this S3 source */
    private final S3SourceConfig s3SourceConfig;
    /** The transformer for the data conversions */
    private final Transformer transformer;
    /** The AWS client that provides the S3Objects */
    private final AWSV2SourceClient sourceClient;
    /** the taskId of this running task */
    private int taskId;
    /**
     * The ring buffer which contains recently processed s3 object keys, this is used to set the startAfter variable in
     * the AWSV2Client request.
     */
    private final RingBuffer s3ObjectKeyRingBuffer;

    /** The S3 bucket we are processing */
    private final String bucket;

    /**
     * /** The inner iterator to provides a base S3SourceRecord for an S3Object that has passed the filters and
     * potentially had data extracted.
     */
    private Iterator<S3SourceRecord> inner;
    /**
     * The outer iterator that provides an S3SourceRecord for each record contained by the S3Object identified by the
     * inner record.
     */
    private Iterator<S3SourceRecord> outer;
    /** The topic(s) which have been configured with the 'topics' configuration */
    private final Optional<String> targetTopics;

    /** Check if the S3 Object Key is part of the 'target' files configured to be extracted from S3 */
    final FileMatching fileMatching;
    /** The predicate which will determine if an S3Object should be assigned to this task for processing */
    final Predicate<Optional<S3SourceRecord>> taskAssignment;
    /** The utility to extract the context from the S3 Object Key */
    private FilePatternUtils filePattern;
    /**
     * The object key which is currently being processed, when rehydrating from S3 we will seek S3 Objects after this
     * key
     */
    private String lastSeenObjectKey;

    private static final Logger LOGGER = LoggerFactory.getLogger(SourceRecordIterator.class);

    public SourceRecordIterator(final S3SourceConfig s3SourceConfig,
            final OffsetManager<S3OffsetManagerEntry> offsetManager, final Transformer transformer,
            final AWSV2SourceClient sourceClient) {

        super();
        this.s3SourceConfig = s3SourceConfig;
        this.offsetManager = offsetManager;
        this.bucket = s3SourceConfig.getAwsS3BucketName();
        this.transformer = transformer;
        this.sourceClient = sourceClient;
        this.targetTopics = Optional.ofNullable(s3SourceConfig.getTargetTopic());
        this.taskAssignment = new TaskAssignment(initializeDistributionStrategy());
        this.taskId = s3SourceConfig.getTaskId();
        this.fileMatching = new FileMatching(filePattern);
        s3ObjectKeyRingBuffer = new RingBuffer(s3SourceConfig.getS3FetchBufferSize());

        inner = getS3SourceRecordStream(sourceClient).iterator();
        outer = Collections.emptyIterator();
    }

    private Stream<S3SourceRecord> getS3SourceRecordStream(final AWSV2SourceClient sourceClient) {
        return sourceClient.getS3ObjectStream(s3ObjectKeyRingBuffer.getOldest())
                .map(fileMatching)
                .filter(taskAssignment)
                .map(Optional::get);
    }

    @Override
    public boolean hasNext() {
        if (!outer.hasNext()) {
            // update the buffer to contain this new objectKey
            s3ObjectKeyRingBuffer.enqueue(lastSeenObjectKey);
            // Remove the last seen Object Key from the offsets as the file has been completely processed.
            offsetManager
                    .removeEntry(S3OffsetManagerEntry.asKey(bucket, StringUtils.defaultIfBlank(lastSeenObjectKey, "")));
        }
        if (!inner.hasNext() && !outer.hasNext()) {
            inner = getS3SourceRecordStream(sourceClient).iterator();
        }
        while (!outer.hasNext() && inner.hasNext()) {
            outer = convert(inner.next()).iterator();
        }
        return outer.hasNext();
    }

    @Override
    public S3SourceRecord next() {
        return outer.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("This iterator is unmodifiable");
    }

    /**
     * Converts the S3Object into stream of S3SourceRecords.
     *
     * @param s3SourceRecord
     *            the SourceRecord that drives the creation of source records with values.
     * @return a stream of S3SourceRecords created from the input stream of the S3Object.
     */
    private Stream<S3SourceRecord> convert(final S3SourceRecord s3SourceRecord) {
        s3SourceRecord.setKeyData(
                transformer.getKeyData(s3SourceRecord.getObjectKey(), s3SourceRecord.getTopic(), s3SourceConfig));

        lastSeenObjectKey = s3SourceRecord.getObjectKey();

        return transformer
                .getRecords(sourceClient.getObject(s3SourceRecord.getObjectKey()), s3SourceRecord.getS3ObjectSize(),
                        s3SourceRecord.getContext(), s3SourceConfig, s3SourceRecord.getRecordCount())
                .map(new Mapper(s3SourceRecord));

    }

    private DistributionStrategy initializeDistributionStrategy() {
        final DistributionType distributionType = s3SourceConfig.getDistributionType();
        final int maxTasks = s3SourceConfig.getMaxTasks();
        this.taskId = s3SourceConfig.getTaskId() % maxTasks;
        this.filePattern = new FilePatternUtils(
                s3SourceConfig.getS3FileNameFragment().getFilenameTemplate().toString());
        return distributionType.getDistributionStrategy(maxTasks);
    }

    /**
     * maps the data from the @{link Transformer} stream to an S3SourceRecord given all the additional data required.
     */
    static class Mapper implements Function<SchemaAndValue, S3SourceRecord> {
        /**
         * The S3SourceRecord that produceces the values.
         */
        private final S3SourceRecord sourceRecord;

        public Mapper(final S3SourceRecord sourceRecord) {
            // TODO this is the point where the global S3OffsetManagerEntry becomes local and we can do a lookahead type
            // operation within the Transformer
            // to see if there are more records.
            this.sourceRecord = sourceRecord;
        }

        @Override
        public S3SourceRecord apply(final SchemaAndValue valueData) {
            sourceRecord.incrementRecordCount();
            final S3SourceRecord result = new S3SourceRecord(sourceRecord);
            result.setValueData(valueData);
            return result;
        }
    }

    class TaskAssignment implements Predicate<Optional<S3SourceRecord>> {
        final DistributionStrategy distributionStrategy;

        TaskAssignment(final DistributionStrategy distributionStrategy) {
            this.distributionStrategy = distributionStrategy;
        }

        @Override
        public boolean test(final Optional<S3SourceRecord> s3SourceRecord) {
            if (s3SourceRecord.isPresent()) {
                final S3SourceRecord record = s3SourceRecord.get();
                final Context<String> context = record.getContext();
                return taskId == distributionStrategy.getTaskFor(context);

            }
            return false;
        }

    }

    class FileMatching implements Function<S3Object, Optional<S3SourceRecord>> {

        final FilePatternUtils utils;
        FileMatching(final FilePatternUtils utils) {
            this.utils = utils;
        }

        @Override
        public Optional<S3SourceRecord> apply(final S3Object s3Object) {

            final Optional<Context<String>> optionalContext = utils.process(s3Object.key());
            if (optionalContext.isPresent() && !s3ObjectKeyRingBuffer.contains(s3Object.key())) {
                final S3SourceRecord s3SourceRecord = new S3SourceRecord(s3Object);
                final Context<String> context = optionalContext.get();
                overrideContextTopic(context);
                s3SourceRecord.setContext(context);
                S3OffsetManagerEntry offsetManagerEntry = new S3OffsetManagerEntry(bucket, s3Object.key());
                offsetManagerEntry = offsetManager
                        .getEntry(offsetManagerEntry.getManagerKey(), offsetManagerEntry::fromProperties)
                        .orElse(offsetManagerEntry);
                s3SourceRecord.setOffsetManagerEntry(offsetManagerEntry);
                return Optional.of(s3SourceRecord);
            }
            return Optional.empty();
        }

        private void overrideContextTopic(final Context<String> context) {
            // Set the target topic in the context if it has been set from configuration.
            if (targetTopics.isPresent()) {
                if (context.getTopic().isPresent()) {
                    LOGGER.debug(
                            "Overriding topic '{}' extracted from S3 Object Key with topic '{}' from configuration 'topics'. ",
                            context.getTopic().get(), targetTopics.get());
                }
                context.setTopic(targetTopics.get());
            }
        }
    }

}
