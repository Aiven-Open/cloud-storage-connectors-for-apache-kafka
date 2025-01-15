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
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.ByteArrayTransformer;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.utils.FilePatternUtils;
import io.aiven.kafka.connect.common.source.task.DistributionStrategy;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Iterator that processes S3 files and creates Kafka source records. Supports different output formats (Avro, JSON,
 * Parquet).
 */
public final class SourceRecordIterator implements Iterator<S3SourceRecord> {
    public static final long BYTES_TRANSFORMATION_NUM_OF_RECS = 1L;

    /** The OffsetManager that we are using */
    private final OffsetManager<S3OffsetManagerEntry> offsetManager;
    /** The offset manager Entry we are working with */
    private S3OffsetManagerEntry offsetManagerEntry;

    /** The configuration for this S3 source */
    private final S3SourceConfig s3SourceConfig;
    /** The transformer for the data conversions */
    private final Transformer transformer;
    /** The AWS client that provides the S3Objects */
    private final AWSV2SourceClient sourceClient;
    /** The S3 bucket we are processing */
    private final String bucket;
    /** The distrivbution strategy we will use */
    private final DistributionStrategy distributionStrategy;
    /** The task ID associated with this iterator */
    private final int taskId;

    /** The inner iterator to provides S3Object that have been filtered potentially had data extracted */
    private final Iterator<S3Object> inner;
    /** The outer iterator that provides S3SourceRecords */
    private Iterator<S3SourceRecord> outer;

    final FileMatching fileMatching;

    public SourceRecordIterator(final S3SourceConfig s3SourceConfig, final OffsetManager offsetManager,
            final Transformer transformer, final AWSV2SourceClient sourceClient,
            final DistributionStrategy distributionStrategy, final String filePattern, final int taskId) {
        super();
        this.s3SourceConfig = s3SourceConfig;
        this.offsetManager = offsetManager;
        this.bucket = s3SourceConfig.getAwsS3BucketName();
        this.transformer = transformer;
        this.sourceClient = sourceClient;
        this.distributionStrategy = distributionStrategy;
        this.taskId = taskId;

        fileMatching = new FileMatching(filePattern);
        // Initialize predicates
        sourceClient.addPredicate(fileMatching);
        sourceClient.addPredicate(s3Object -> distributionStrategy.isPartOfTask(taskId, s3Object.key(), fileMatching.pattern));

        // call filters out bad file names and extracts topic/partition
        inner = sourceClient.getS3ObjectIterator(null);
        outer = Collections.emptyIterator();
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

        final long recordCount = offsetManagerEntry.getRecordCount();

        // Optimizing without reading stream again.
        if (transformer instanceof ByteArrayTransformer && recordCount > 0) {
            return Stream.empty();
        }

        final SchemaAndValue keyData = transformer.getKeyData(s3Object.key(), offsetManagerEntry.getTopic(),
                s3SourceConfig);

        return transformer
                .getRecords(sourceClient.getObject(s3Object.key()), offsetManagerEntry.getTopic(),
                        offsetManagerEntry.getPartition(), s3SourceConfig, recordCount)
                .map(new Mapper(offsetManagerEntry, keyData));
    }

    /**
     * maps the data from the @{link Transformer} stream to an S3SourceRecord given all the additional data required.
     */
    static class Mapper implements Function<SchemaAndValue, S3SourceRecord> {
        /**
         * The partition map
         */
        private final S3OffsetManagerEntry entry;
        /**
         * The schema and value for the key
         */
        private final SchemaAndValue keyData;

        public Mapper(final S3OffsetManagerEntry entry, final SchemaAndValue keyData) {
            // TODO this is the point where the global S3OffsetManagerEntry becomes local and we can do a lookahead type
            // operation within the Transformer
            // to see if there are more records.
            this.entry = entry;
            this.keyData = keyData;
        }

        @Override
        public S3SourceRecord apply(final SchemaAndValue valueData) {
            entry.incrementRecordCount();
            return new S3SourceRecord(entry, keyData, valueData);
        }
    }

    class FileMatching implements Predicate<S3Object> {

        Pattern pattern;
        FileMatching(String filePattern) {
            pattern = FilePatternUtils.configurePattern(filePattern);
        }

        @Override
        public boolean test(S3Object s3Object) {
            Optional<String> topic = FilePatternUtils.getTopic(pattern, s3Object.key());
            OptionalInt partition = FilePatternUtils.getPartitionId(pattern, s3Object.key());
            if (topic.isPresent() && partition.isPresent()) {
                offsetManagerEntry = new S3OffsetManagerEntry(bucket, s3Object.key(), topic.get(), partition.getAsInt());
                return true;
            }
            return false;
        }
    }
}
