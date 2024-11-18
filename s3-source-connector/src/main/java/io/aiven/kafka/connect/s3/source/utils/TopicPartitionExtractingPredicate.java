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

import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TopicPartitionExtractingPredicate implements Predicate<S3ObjectSummary> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceRecordIterator.class);
    public static final String PATTERN_TOPIC_KEY = "topicName";
    public static final String PATTERN_PARTITION_KEY = "partitionId";

    public static final Pattern FILE_DEFAULT_PATTERN = Pattern.compile("(?<topicName>[^/]+?)-"
            + "(?<partitionId>\\d{5})-" + "(?<uniqueId>[a-zA-Z0-9]+)" + "\\.(?<fileExtension>[^.]+)$"); // topic-00001.txt

    private S3OffsetManagerEntry offsetManagerEntry;

    public S3OffsetManagerEntry getOffsetMapEntry() {
        return offsetManagerEntry;
    }

    @Override
    public boolean test(final S3ObjectSummary s3ObjectSummary) {
        final Matcher fileMatcher = FILE_DEFAULT_PATTERN.matcher(s3ObjectSummary.getKey());

        if (fileMatcher.find()) {
            try {
                offsetManagerEntry = new S3OffsetManagerEntry(s3ObjectSummary.getBucketName(), s3ObjectSummary.getKey(),
                        fileMatcher.group(PATTERN_TOPIC_KEY),
                        Integer.parseInt(fileMatcher.group(PATTERN_PARTITION_KEY)));
            } catch (RuntimeException e) { // NOPMD AvoidCatchingGenericException
                LOGGER.error("Error parsing offset data from {}", s3ObjectSummary.getKey(), e);
                offsetManagerEntry = null; // NOPMD NullAssignment
                return false;
            }
            return true;
        }
        LOGGER.error("File naming doesn't match to any topic. {}", s3ObjectSummary.getKey());
        return false;
    }
}
