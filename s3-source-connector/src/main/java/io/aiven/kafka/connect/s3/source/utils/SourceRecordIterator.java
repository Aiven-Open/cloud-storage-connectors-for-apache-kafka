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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.kafka.connect.common.OffsetManager;
import io.aiven.kafka.connect.common.source.input.ByteArrayTransformer;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.collections4.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterator that processes S3 files and creates Kafka source records. Supports different output formats (Avro, JSON,
 * Parquet).
 */
public final class SourceRecordIterator implements Iterator<S3SourceRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceRecordIterator.class);
    public static final String PATTERN_TOPIC_KEY = "topicName";
    public static final String PATTERN_PARTITION_KEY = "partitionId";

    public static final Pattern FILE_DEFAULT_PATTERN = Pattern.compile("(?<topicName>[^/]+?)-"
            + "(?<partitionId>\\d{5})-" + "(?<uniqueId>[a-zA-Z0-9]+)" + "\\.(?<fileExtension>[^.]+)$"); // topic-00001.txt
    public static final long BYTES_TRANSFORMATION_NUM_OF_RECS = 1L;

    private final OffsetManager<S3OffsetManagerEntry> offsetManager;

    private final S3SourceConfig s3SourceConfig;
    private final String bucketName;

    private S3OffsetManagerEntry offsetManagerEntry;

    private Iterator<S3Object> s3ObjectIterator;

    private final Transformer transformer;
    // Once we decouple the S3Object from the Source Iterator we can change this to be the SourceApiClient
    // At which point it will work for al our integrations.
    private final AWSV2SourceClient sourceClient; // NOPMD

    private Iterator<S3SourceRecord> outerIterator;

    public SourceRecordIterator(final S3SourceConfig s3SourceConfig, final OffsetManager<S3OffsetManagerEntry> offsetManager,
            final Transformer transformer, final AWSV2SourceClient sourceClient) {
        this.s3SourceConfig = s3SourceConfig;
        this.offsetManager = offsetManager;
        this.bucketName = s3SourceConfig.getAwsS3BucketName();
        this.transformer = transformer;
        this.sourceClient = sourceClient;
        this.s3ObjectIterator = IteratorUtils.filteredIterator(sourceClient.getIteratorOfObjects(null), s3Object -> extractOffsetManagerEntry(s3Object));
    }

    private boolean extractOffsetManagerEntry(S3Object s3Object) {

        final Matcher fileMatcher = FILE_DEFAULT_PATTERN.matcher(s3Object.getKey());

        if (fileMatcher.find()) {
            // TODO move this from the SourceRecordIterator so that we can decouple it from S3 and make it API agnostic
            final S3OffsetManagerEntry keyEntry = new S3OffsetManagerEntry(bucketName, s3Object.getKey(), fileMatcher.group(PATTERN_TOPIC_KEY),
                 Integer.parseInt(fileMatcher.group(PATTERN_PARTITION_KEY)));
            offsetManagerEntry = offsetManager.getEntry(keyEntry.getManagerKey(), keyEntry::fromProperties);
            return checkBytesTransformation(transformer, offsetManagerEntry.getRecordCount()) ? false : true;
        }
        LOGGER.error("File naming doesn't match to any topic. {}", s3Object.getKey());
        return false;
    }

    // For bytes transformation, read whole file as 1 record
    private boolean checkBytesTransformation(final Transformer transformer,
                                             final long numberOfRecsAlreadyProcessed) {
        return transformer instanceof ByteArrayTransformer
                && numberOfRecsAlreadyProcessed == BYTES_TRANSFORMATION_NUM_OF_RECS;
    }


    private Iterator<S3SourceRecord> getS3SourceRecordIterator(S3Object s3Object) {
        Optional<SchemaAndValue> key =Optional.of(new SchemaAndValue(transformer.getKeySchema(),s3Object.getKey().getBytes(StandardCharsets.UTF_8)));
        return transformer.getRecords(s3Object::getObjectContent, offsetManagerEntry)
                .map(value -> new S3SourceRecord(offsetManagerEntry, key, value))
                .iterator();
    }

    @Override
    public boolean hasNext() {
        if (outerIterator.hasNext()) {
            return true;
        }

        while (s3ObjectIterator.hasNext()) {
            outerIterator = getS3SourceRecordIterator(s3ObjectIterator.next());
            if (outerIterator.hasNext()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public S3SourceRecord next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return outerIterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("This iterator is unmodifiable");
    }

}
