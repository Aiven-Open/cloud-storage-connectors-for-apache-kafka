package io.aiven.kafka.connect.s3.source.utils;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.input.Transformer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.LOGGER;

public final class S3ObjectToSourceRecordMapper implements Function<S3Object, Iterator<S3SourceRecord>> {

    private final TopicPartitionExtractingPredicate topicPartitionExtractingPredicate;
    private final Transformer transformer;

    private final S3SourceConfig s3SourceConfig;

    private final OffsetManager offsetManager;

    S3ObjectToSourceRecordMapper(final Transformer transformer, final TopicPartitionExtractingPredicate topicPartitionExtractingPredicate, final S3SourceConfig s3SourceConfig, OffsetManager offsetManager) {
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
        RecordFilter(S3OffsetManagerEntry offsetManagerEntry) {
            this.offsetManagerEntry = offsetManagerEntry;
        }
        private boolean result = false;

        @Override
        public boolean test(Object o) {
            if (!result) {
                result = !offsetManager.shouldSkipRecord(offsetManagerEntry);
            }
            offsetManagerEntry.incrementRecordCount();
            return result;
        }
    }

    @Override
    public Iterator<S3SourceRecord> apply(final S3Object s3Object) {
        final byte[] keyBytes = s3Object.getKey().getBytes(StandardCharsets.UTF_8);
        S3OffsetManagerEntry offsetManagerEntry = topicPartitionExtractingPredicate.getOffsetMapEntry();
        // TODO: Make Transformer set values in the OffsetManagerEntry directly.
        Map<String, String> propertyMap = new HashMap<>();
        transformer.configureValueConverter(propertyMap, s3SourceConfig);
        propertyMap.entrySet().forEach(e -> offsetManagerEntry.setProperty(e.getKey(), e.getValue()));
        // TODO remove the above block
        List<Object> records = null;
        try (S3ObjectInputStream inputStream = s3Object.getObjectContent()) {
           records = transformer.getRecords(inputStream, offsetManagerEntry.getTopic(), offsetManagerEntry.getPartition(), s3SourceConfig);
        } catch (IOException e) {
            LOGGER.error("Error closing input stream for {}", s3Object.getKey(), e);
            if (records == null) {
                return Collections.emptyIterator();
            }
        }

        RecordFilter recordFilter = new RecordFilter(offsetManagerEntry);

        return records.stream().filter(recordFilter).map(o -> transformer.getValueBytes(o, offsetManagerEntry.getTopic(), s3SourceConfig))
                // convert byte[] to sourceRecord while updating offset manager.
                .map(valueBytes -> {
                    return new S3SourceRecord(offsetManagerEntry, keyBytes, valueBytes);
                })
                .iterator();
    }
}
