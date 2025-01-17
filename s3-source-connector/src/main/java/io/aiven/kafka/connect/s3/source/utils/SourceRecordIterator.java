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
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.kafka.connect.common.source.input.ByteArrayTransformer;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.utils.FilePatternUtils;
import io.aiven.kafka.connect.common.source.task.Context;
import io.aiven.kafka.connect.common.source.task.DistributionStrategy;
import io.aiven.kafka.connect.common.source.task.enums.ObjectDistributionStrategy;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Iterator that processes S3 files and creates Kafka source records. Supports different output formats (Avro, JSON,
 * Parquet).
 */
public final class SourceRecordIterator implements Iterator<S3SourceRecord> {
    public static final long BYTES_TRANSFORMATION_NUM_OF_RECS = 1L;

    private final OffsetManager offsetManager;

    private final S3SourceConfig s3SourceConfig;
    private final String bucketName;

    private final Transformer transformer;
    // Once we decouple the S3Object from the Source Iterator we can change this to be the SourceApiClient
    // At which point it will work for al our integrations.
    private final AWSV2SourceClient sourceClient;

    private Context<S3Key> context;

    private final DistributionStrategy distributionStrategy;
    private int taskId;

    private final Iterator<S3Object> inner;

    private Iterator<S3SourceRecord> outer;
    private FilePatternUtils<S3Key> filePattern;

    public SourceRecordIterator(final S3SourceConfig s3SourceConfig, final OffsetManager offsetManager,
            final Transformer transformer, final AWSV2SourceClient sourceClient) {
        super();
        this.s3SourceConfig = s3SourceConfig;
        this.offsetManager = offsetManager;

        this.bucketName = s3SourceConfig.getAwsS3BucketName();
        this.transformer = transformer;
        this.sourceClient = sourceClient;

        this.distributionStrategy = initializeObjectDistributionStrategy();

        // Initialize predicates
        sourceClient.addPredicate(this::isFileMatchingPattern);
        sourceClient.addPredicate(obj -> isFileAssignedToTask(context, taskId));

        // call filters out bad file names and extracts topic/partition
        inner = sourceClient.getS3ObjectIterator(null);
        outer = Collections.emptyIterator();
    }

    public boolean isFileMatchingPattern(final S3Object s3Object) {
        final Optional<Context<S3Key>> optionalCtx = filePattern.process(new S3Key(s3Object.key()));
        if (optionalCtx.isPresent()) {
            context = optionalCtx.get();
            return true;
        }
        return false;
    }

    public boolean isFileAssignedToTask(final Context<S3Key> ctx, final int taskId) {
        return taskId == distributionStrategy.getTaskFor(ctx);
    }

    @Override
    public boolean hasNext() {
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
     * @param s3Object
     *            the S3Object to read data from.
     * @return a stream of S3SourceRecords created from the input stream of the S3Object.
     */
    private Stream<S3SourceRecord> convert(final S3Object s3Object) {

        final Map<String, Object> partitionMap = ConnectUtils.getPartitionMap(context.getTopic().get(),
                context.getPartition().get(), bucketName);
        final long recordCount = offsetManager.recordsProcessedForObjectKey(partitionMap, s3Object.key());

        // Optimizing without reading stream again.
        if (transformer instanceof ByteArrayTransformer && recordCount > 0) {
            return Stream.empty();
        }

        final SchemaAndValue keyData = transformer.getKeyData(s3Object.key(), context.getTopic().get(), s3SourceConfig);

        return transformer
                .getRecords(sourceClient.getObject(s3Object.key()), context.getTopic().get(),
                        context.getPartition().get(), s3SourceConfig, recordCount)
                .map(new Mapper(partitionMap, recordCount, keyData, s3Object.key()));
    }

    private DistributionStrategy initializeObjectDistributionStrategy() {
        final ObjectDistributionStrategy objectDistributionStrategy = s3SourceConfig.getObjectDistributionStrategy();
        final int maxTasks = s3SourceConfig.getMaxTasks();
        this.taskId = s3SourceConfig.getTaskId() % maxTasks;
        this.filePattern = new FilePatternUtils<>(
                s3SourceConfig.getS3FileNameFragment().getFilenameTemplate().toString(),
                s3SourceConfig.getTargetTopics());
        return objectDistributionStrategy.getDistributionStrategy(maxTasks);
    }

    /**
     * maps the data from the @{link Transformer} stream to an S3SourceRecord given all the additional data required.
     */
    class Mapper implements Function<SchemaAndValue, S3SourceRecord> {
        /**
         * The partition map
         */
        private final Map<String, Object> partitionMap;
        /**
         * The record number for the record being created.
         */
        private long recordCount;
        /**
         * The schema and value for the key
         */
        private final SchemaAndValue keyData;
        /**
         * The object key from S3
         */
        private final String objectKey;

        public Mapper(final Map<String, Object> partitionMap, final long recordCount, final SchemaAndValue keyData,
                final String objectKey) {
            this.partitionMap = partitionMap;
            this.recordCount = recordCount;
            this.keyData = keyData;
            this.objectKey = objectKey;
        }

        @Override
        public S3SourceRecord apply(final SchemaAndValue valueData) {
            recordCount++;
            return new S3SourceRecord(partitionMap, recordCount, context.getTopic().get(), context.getPartition().get(),
                    objectKey, keyData, valueData);
        }
    }
}
