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
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.kafka.connect.common.OffsetManager;
import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.input.ByteArrayTransformer;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.io.function.IOSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterator that reads from an S3Object iterator and processes each S3Object into one or more records to be returned as
 * S3SourceRecords.
 */
public final class SourceRecordIterator implements Iterator<S3SourceRecord> {
    /** The logger to write to */
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceRecordIterator.class);

    /** The file name pattern topic key. TODO move this to a file matching class */
    public static final String PATTERN_TOPIC_KEY = "topicName";
    /** The file name pattern partition key. TODO move this to a file matching class */
    public static final String PATTERN_PARTITION_KEY = "partitionId";
    /** The file name matching pattern. TODO move this to a file matching class */
    public static final Pattern FILE_DEFAULT_PATTERN = Pattern.compile("(?<topicName>[^/]+?)-"
            + "(?<partitionId>\\d{5})-" + "(?<uniqueId>[a-zA-Z0-9]+)" + "\\.(?<fileExtension>[^.]+)$"); // e.g.
                                                                                                        // topic-00001.txt
    /** Maximum number of records that the Byte transformer returns. This should be handled by the Transformer itself */
    public static final long BYTES_TRANSFORMATION_NUM_OF_RECS = 1L;
    /** The OffsetManager that we are using */
    private final OffsetManager<S3OffsetManagerEntry> offsetManager;
    /** The offset manager Entry we are working with */
    private S3OffsetManagerEntry offsetManagerEntry;
    /** The Configuration that we are using */
    private final S3SourceConfig s3SourceConfig;
    /** The S3Object iterator that we read from */
    private final Iterator<S3Object> s3ObjectIterator;
    /** The transformer we apply to the data */
    private final Transformer transformer;
    /** THe iterator we will return data from */
    private Iterator<S3SourceRecord> outerIterator;

    /**
     * Constructor.
     *
     * @param s3SourceConfig
     *            The configuration.
     * @param offsetManager
     *            the offset manager.
     * @param transformer
     *            the transformer to sue.
     * @param sourceClient
     *            the source client to read from.
     */
    public SourceRecordIterator(final S3SourceConfig s3SourceConfig,
            final OffsetManager<S3OffsetManagerEntry> offsetManager, final Transformer transformer,
            final AWSV2SourceClient sourceClient) {
        this.s3SourceConfig = s3SourceConfig;
        this.offsetManager = offsetManager;
        this.transformer = transformer;
        this.s3ObjectIterator = IteratorUtils.filteredIterator(sourceClient.getIteratorOfObjects(null),
                this::extractOffsetManagerEntry);
        this.outerIterator = Collections.emptyIterator();
    }

    /**
     * Construct the OffsetManagerEntry from the S3Object file name. This probalby should occur earlier in the chain. If
     * this method returns false the object will be skipped.
     *
     * @param s3Object
     *            The object to extract the offset manager data from.
     * @return true if the offset can be extracted, false otherwise.
     */
    private boolean extractOffsetManagerEntry(final S3Object s3Object) {
        final Matcher fileMatcher = FILE_DEFAULT_PATTERN.matcher(s3Object.getKey());
        if (fileMatcher.find()) {
            // TODO move this from the SourceRecordIterator so that we can decouple it from S3 and make it API agnostic
            final S3OffsetManagerEntry keyEntry = new S3OffsetManagerEntry(s3SourceConfig.getAwsS3BucketName(),
                    s3Object.getKey(), fileMatcher.group(PATTERN_TOPIC_KEY),
                    Integer.parseInt(fileMatcher.group(PATTERN_PARTITION_KEY)));
            offsetManagerEntry = offsetManager.getEntry(keyEntry.getManagerKey(), keyEntry::fromProperties);
            return !checkBytesTransformation(transformer, offsetManagerEntry.getRecordCount());
        }
        LOGGER.error("File naming doesn't match to any topic. {}", s3Object.getKey());
        // TODO log bad key here.
        return false;
    }

    /**
     * Checks if the transformer should be skipped becasue the number of records exceeds what it returns. This check
     * should probalby be converted into a S3OffsetManager entry call for "isComplete" and converted into a predicate
     * check.
     *
     * @param transformer
     *            The transformer we are using.
     * @param numberOfRecsAlreadyProcessed
     *            the number of records processed.
     * @return true if we should skip the object.
     */
    private boolean checkBytesTransformation(final Transformer transformer, final long numberOfRecsAlreadyProcessed) {
        return transformer instanceof ByteArrayTransformer
                && numberOfRecsAlreadyProcessed == BYTES_TRANSFORMATION_NUM_OF_RECS;
    }

    /**
     * Get the S3SourceRecord iterator that reads from a single object. This method applies the transformer to the
     * object and returns an iterator based on the stream returned from
     * {@link Transformer#getRecords(IOSupplier, OffsetManager.OffsetManagerEntry, SourceCommonConfig)}.
     *
     * @param s3Object
     *            the object to get S3Source records from.
     * @return An iterator over the S3SourceRecords from the Object.
     */
    private Iterator<S3SourceRecord> getS3SourceRecordIterator(final S3Object s3Object) {
        final Optional<SchemaAndValue> key = Optional
                .of(new SchemaAndValue(transformer.getKeySchema(), s3Object.getKey().getBytes(StandardCharsets.UTF_8)));
        return transformer.getRecords(s3Object::getObjectContent, offsetManagerEntry, s3SourceConfig)
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
