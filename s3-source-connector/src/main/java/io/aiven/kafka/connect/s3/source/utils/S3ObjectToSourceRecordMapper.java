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

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.LOGGER;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.input.Transformer;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

/**
 * Converts S3Objects into an Iterator of S3SourceRecords.
 */
public final class S3ObjectToSourceRecordMapper implements Function<S3Object, Iterator<S3SourceRecord>> {
    /**
     * The source of topic and partition for the current S3Object. As the S3ObjectSummaries are read this instance will
     * be updated. It will always be the values fro the S3Object that is being processed.
     */
    private final TopicPartitionExtractingPredicate topicPartitionExtractingPredicate;
    /** The transformer to transform the data streams with */
    private final Transformer transformer;
    /**
     * She S3Source configuration.
     */
    private final S3SourceConfig s3SourceConfig;
    /**
     * THe offset manager.
     */
    private final OffsetManager offsetManager;

    /**
     * Creats the record mapper.
     *
     * @param transformer
     *            the Transformer to transform data from S3 input streams into records.
     * @param topicPartitionExtractingPredicate
     *            The class that contains the topic and partition the current S3Object.
     * @param s3SourceConfig
     *            The configuration.
     * @param offsetManager
     *            The OffsetManager.
     */
    S3ObjectToSourceRecordMapper(final Transformer transformer,
            final TopicPartitionExtractingPredicate topicPartitionExtractingPredicate,
            final S3SourceConfig s3SourceConfig, final OffsetManager offsetManager) {
        this.topicPartitionExtractingPredicate = topicPartitionExtractingPredicate;
        this.transformer = transformer;
        this.s3SourceConfig = s3SourceConfig;
        this.offsetManager = offsetManager;
    }

    /**
     * Filters out offsets that we have already processed. This filter will filter out records that have been seen. Once
     * a new record is located it will stop filtering. The {@code Object} being filtered is the Object returned from the
     * {@link Transformer}
     */
    private class RecordFilter implements Predicate<Object> {
        private final S3OffsetManagerEntry offsetManagerEntry;
        private boolean checkRecordSkip = true;

        RecordFilter(final S3OffsetManagerEntry offsetManagerEntry) {
            this.offsetManagerEntry = offsetManagerEntry;
        }
        @Override
        public boolean test(final Object recordData) {
            // once we find a record to process (not skip) we no longer skip. Thus skip record is always the
            // negation of checkRecordSkip.
            if (checkRecordSkip) {
                final S3OffsetManagerEntry stored = offsetManager.getEntry(offsetManagerEntry.getManagerKey(),
                        map -> offsetManagerEntry.fromProperties(map));
                checkRecordSkip = stored != null && stored.compareTo(offsetManagerEntry) <= 0;
            }
            offsetManagerEntry.incrementRecordCount();
            // this class is a predicate so we have to return true if we want to process the record.
            return !checkRecordSkip;
        }
    }

    @Override
    public Iterator<S3SourceRecord> apply(final S3Object s3Object) {
        final S3OffsetManagerEntry offsetManagerEntry = topicPartitionExtractingPredicate.getOffsetMnagerEntry();
        // TODO: Make Transformer set values in the OffsetManagerEntry directly.
        final Map<String, String> propertyMap = new HashMap<>();
        transformer.configureValueConverter(propertyMap, s3SourceConfig);
        propertyMap.entrySet().forEach(e -> offsetManagerEntry.setProperty(e.getKey(), e.getValue()));
        // TODO remove the above block
        Stream<Object> records = null;
        try (S3ObjectInputStream inputStream = s3Object.getObjectContent()) {
            records = transformer.getRecords(() -> inputStream, offsetManagerEntry.getTopic(),
                    offsetManagerEntry.getPartition(), s3SourceConfig);
        } catch (IOException | RuntimeException e) { // NOPMD Avoid catching Generic exception
            LOGGER.error("Error in input stream for {}", s3Object.getKey(), e);
            if (records == null) {
                return Collections.emptyIterator();
            }
        }

        // This filter will determine which records to process. It may make more sense to
        // TODO: push this down into the Transformer. We may also want to implement more complex checks.
        final RecordFilter recordFilter = new RecordFilter(offsetManagerEntry);
        final byte[] keyBytes = s3Object.getKey().getBytes(StandardCharsets.UTF_8);

        return records.filter(recordFilter)
                .map(o -> transformer.getValueBytes(o, offsetManagerEntry.getTopic(), s3SourceConfig))
                // convert byte[] to sourceRecord while updating offset manager.
                .map(valueBytes -> {
                    return new S3SourceRecord(offsetManagerEntry, keyBytes, valueBytes);
                })
                .iterator();
    }
}
