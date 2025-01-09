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
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.kafka.connect.common.OffsetManager;
import io.aiven.kafka.connect.common.source.input.ByteArrayTransformer;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.apache.commons.collections4.IteratorUtils;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Iterator that processes S3 files and creates Kafka source records. Supports different output formats (Avro, JSON,
 * Parquet).
 */
public final class SourceRecordIterator implements Iterator<S3SourceRecord> {
    public static final String PATTERN_TOPIC_KEY = "topicName";
    public static final String PATTERN_PARTITION_KEY = "partitionId";

    public static final Pattern FILE_DEFAULT_PATTERN = Pattern.compile("(?<topicName>[^/]+?)-"
            + "(?<partitionId>\\d{5})-" + "(?<uniqueId>[a-zA-Z0-9]+)" + "\\.(?<fileExtension>[^.]+)$"); // topic-00001.txt
    public static final long BYTES_TRANSFORMATION_NUM_OF_RECS = 1L;

    /** The OffsetManager that we are using */
    private final OffsetManager<S3OffsetManagerEntry> offsetManager;
    /** The offset manager Entry we are working with */
    private S3OffsetManagerEntry offsetManagerEntry;

    private final S3SourceConfig s3SourceConfig;

    private final Transformer transformer;
    // Once we decouple the S3Object from the Source Iterator we can change this to be the SourceApiClient
    // At which point it will work for al our integrations.
    private final AWSV2SourceClient sourceClient;

    private String topic;
    private int partitionId;
    private final String bucketName;
    private final Iterator<S3Object> inner;

    private Iterator<S3SourceRecord> outer;

    private final Predicate<S3Object> fileNamePredicate;

    public SourceRecordIterator(final S3SourceConfig s3SourceConfig,
            final OffsetManager<S3OffsetManagerEntry> offsetManager, final Transformer transformer,
            final AWSV2SourceClient sourceClient) {
        super();
        this.s3SourceConfig = s3SourceConfig;
        this.offsetManager = offsetManager;
        this.bucketName = s3SourceConfig.getAwsS3BucketName();
        this.fileNamePredicate = buildPredicate();

        this.transformer = transformer;
        this.sourceClient = sourceClient;

        // call filters out bad file names and extracts topic/partition
        inner = IteratorUtils.filteredIterator(sourceClient.getS3ObjectIterator(null),
                s3Object -> this.fileNamePredicate.test(s3Object));
        outer = Collections.emptyIterator();
    }

    private Predicate<S3Object> buildPredicate() {
        return s3Object -> {

            final Matcher fileMatcher = FILE_DEFAULT_PATTERN.matcher(s3Object.key());

            if (fileMatcher.find()) {
                // TODO move this from the SourceRecordIterator so that we can decouple it from S3 and make it API
                // agnostic
                topic = fileMatcher.group(PATTERN_TOPIC_KEY);
                partitionId = Integer.parseInt(fileMatcher.group(PATTERN_PARTITION_KEY));
                final S3OffsetManagerEntry keyEntry = new S3OffsetManagerEntry(bucketName, s3Object.key(),
                        fileMatcher.group(PATTERN_TOPIC_KEY),
                        Integer.parseInt(fileMatcher.group(PATTERN_PARTITION_KEY)));
                offsetManagerEntry = offsetManager.getEntry(keyEntry.getManagerKey(), keyEntry::fromProperties)
                        .orElse(keyEntry);
                return true;
            }
            return false;
        };
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

        final SchemaAndValue keyData = transformer.getKeyData(s3Object.key(), topic, s3SourceConfig);

        return transformer
                .getRecords(sourceClient.getObject(s3Object.key()), topic, partitionId, s3SourceConfig, recordCount)
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
            this.entry = entry;
            this.keyData = keyData;
        }

        @Override
        public S3SourceRecord apply(final SchemaAndValue valueData) {
            entry.incrementRecordCount();
            return new S3SourceRecord(entry, keyData, valueData);
        }
    }
}
