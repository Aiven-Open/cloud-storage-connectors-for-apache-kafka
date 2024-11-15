package io.aiven.kafka.connect.s3.source.utils;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class TopicPartitionExtractingPredicate implements Predicate<S3ObjectSummary> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceRecordIterator.class);
    public static final String PATTERN_TOPIC_KEY = "topicName";
    public static final String PATTERN_PARTITION_KEY = "partitionId";

    public static final Pattern FILE_DEFAULT_PATTERN = Pattern.compile("(?<topicName>[^/]+?)-"
            + "(?<partitionId>\\d{5})-" + "(?<uniqueId>[a-zA-Z0-9]+)" + "\\.(?<fileExtension>[^.]+)$"); // topic-00001.txt

    private S3OffsetManagerEntry offsetManagerEntry;

    public TopicPartitionExtractingPredicate() {
    }

    public S3OffsetManagerEntry getOffsetMapEntry() {
        return offsetManagerEntry;
    }

    @Override
    public boolean test(S3ObjectSummary s3ObjectSummary) {
        final Matcher fileMatcher = FILE_DEFAULT_PATTERN.matcher(s3ObjectSummary.getKey());

        if (fileMatcher.find()) {
            try {
                offsetManagerEntry = new S3OffsetManagerEntry(s3ObjectSummary.getBucketName(), s3ObjectSummary.getKey(), fileMatcher.group(PATTERN_TOPIC_KEY), Integer.parseInt(fileMatcher.group(PATTERN_PARTITION_KEY)));
            } catch (Exception e) {
                LOGGER.error("Error parsing offset data from {}", s3ObjectSummary.getKey(), e);
                offsetManagerEntry = null;
                return false;
            }
            return true;
        }
        LOGGER.error("File naming doesn't match to any topic. {}", s3ObjectSummary.getKey());
        return false;
    }
}
