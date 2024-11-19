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

public final class S3ObjectToSourceRecordMapper implements Function<S3Object, Iterator<S3SourceRecord>> {

    private final TopicPartitionExtractingPredicate topicPartitionExtractingPredicate;
    private final Transformer transformer;

    private final S3SourceConfig s3SourceConfig;

    private final OffsetManager offsetManager;

    S3ObjectToSourceRecordMapper(final Transformer transformer,
            final TopicPartitionExtractingPredicate topicPartitionExtractingPredicate,
            final S3SourceConfig s3SourceConfig, final OffsetManager offsetManager) {
        this.topicPartitionExtractingPredicate = topicPartitionExtractingPredicate;
        this.transformer = transformer;
        this.s3SourceConfig = s3SourceConfig;
        this.offsetManager = offsetManager;
    }

    /**
     * Filters out offsets that we have already processed.
     */
    private class RecordFilter implements Predicate<Object> {
        private final S3OffsetManagerEntry offsetManagerEntry;
        private boolean checkRecordSkip = true;

        RecordFilter(final S3OffsetManagerEntry offsetManagerEntry) {
            this.offsetManagerEntry = offsetManagerEntry;
        }
        @Override
        public boolean test(final Object recordData) {
            // once we find a record to process (not skip) we no longer skip.  Thus skip record is always the
            // negation of checkRecordSkip.
            if (checkRecordSkip) {
               S3OffsetManagerEntry stored = offsetManager.getEntry(offsetManagerEntry.getManagerKey(),  map -> offsetManagerEntry.fromProperties(map));
               if (stored != null) {
                   boolean skipRecord = stored.compareTo(offsetManagerEntry) <= 0 && stored.wasProcessed();
                   checkRecordSkip = !skipRecord;
               } else {
                   checkRecordSkip = false;
               }
            }
            offsetManagerEntry.incrementRecordCount();
            return !checkRecordSkip;
        }
    }

    @Override
    public Iterator<S3SourceRecord> apply(final S3Object s3Object) {
        final S3OffsetManagerEntry offsetManagerEntry = topicPartitionExtractingPredicate.getOffsetMapEntry();
        // TODO: Make Transformer set values in the OffsetManagerEntry directly.
        final Map<String, String> propertyMap = new HashMap<>();
        transformer.configureValueConverter(propertyMap, s3SourceConfig);
        propertyMap.entrySet().forEach(e -> offsetManagerEntry.setProperty(e.getKey(), e.getValue()));
        // TODO remove the above block
        Stream<Object> records = null;
        try (S3ObjectInputStream inputStream = s3Object.getObjectContent()) {
            records = transformer.getRecords(() -> inputStream, offsetManagerEntry.getTopic(),
                    offsetManagerEntry.getPartition(), s3SourceConfig);
        } catch (IOException | RuntimeException e) {
            LOGGER.error("Error in input stream for {}", s3Object.getKey(), e);
            if (records == null) {
                return Collections.emptyIterator();
            }
        }

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
