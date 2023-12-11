/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.kafka.connect.common.grouper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.gcs.GcsSinkConfig;
import io.aiven.kafka.connect.gcs.GcsSinkTask;
import io.aiven.kafka.connect.gcs.testutils.BucketAccessor;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.collect.Lists;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import org.junit.jupiter.api.Disabled;

/**
 * This is a property-based test for {@link GcsSinkTask} (grouping records by the topic and partition) using
 * <a href="https://jqwik.net/docs/current/user-guide.html">jqwik</a>.
 *
 * <p>
 * The idea is to generate random batches of {@link SinkRecord} (see {@link PbtBase#recordBatches()}, put them into a
 * task, and check certain properties of the written files afterwards. Files are written virtually using the in-memory
 * GCS mock.
 */
final class GcsSinkTaskGroupByTopicPartitionPropertiesTest extends PbtBase {

    @Property
    void unlimited(@ForAll("recordBatches") final List<List<SinkRecord>> recordBatches) {
        genericTry(recordBatches, null);
    }

    @Property
    @Disabled("See https://github.com/aiven/gcs-connector-for-apache-kafka/issues/143")
    void limited(@ForAll("recordBatches") final List<List<SinkRecord>> recordBatches,
            @ForAll @IntRange(min = 1, max = 100) final int maxRecordsPerFile) {
        genericTry(recordBatches, maxRecordsPerFile);
    }

    private void genericTry(final List<List<SinkRecord>> recordBatches, final Integer maxRecordsPerFile) {
        final Storage storage = LocalStorageHelper.getOptions().getService(); // NOPMD No need to close
        final BucketAccessor testBucketAccessor = new BucketAccessor(storage, TEST_BUCKET, true);

        final Map<String, String> taskProps = basicTaskProps();
        if (maxRecordsPerFile != null) {
            taskProps.put(GcsSinkConfig.FILE_MAX_RECORDS, Integer.toString(maxRecordsPerFile));
        }
        final GcsSinkTask task = new GcsSinkTask(taskProps, storage);

        for (final List<SinkRecord> recordBatch : recordBatches) {
            task.put(recordBatch);
            task.flush(null);
        }

        checkExpectedFileNames(recordBatches, maxRecordsPerFile, testBucketAccessor);
        checkFileSizes(testBucketAccessor, maxRecordsPerFile);
        final int expectedRecordCount = recordBatches.stream().mapToInt(List::size).sum();
        checkTotalRecordCountAndNoMultipleWrites(expectedRecordCount, testBucketAccessor);
        checkTopicPartitionPartInFileNames(testBucketAccessor);
        checkOffsetOrderInFiles(testBucketAccessor);
    }

    /**
     * Checks that written files have expected names.
     */
    private void checkExpectedFileNames(final List<List<SinkRecord>> recordBatches, final Integer maxRecordsPerFile,
            final BucketAccessor bucketAccessor) {
        final List<String> expectedFileNames = new ArrayList<>();

        for (final List<SinkRecord> recordBatch : recordBatches) {
            final Map<TopicPartition, List<SinkRecord>> groupedPerTopicPartition = recordBatch.stream()
                    .collect(Collectors.groupingBy(r -> new TopicPartition(r.topic(), r.kafkaPartition()))); // NOPMD
                                                                                                             // instantiation
                                                                                                             // in a
                                                                                                             // loop

            for (final Map.Entry<TopicPartition, List<SinkRecord>> entry : groupedPerTopicPartition.entrySet()) {
                final List<List<SinkRecord>> chunks = Lists.partition(groupedPerTopicPartition.get(entry.getKey()),
                        effectiveMaxRecordsPerFile(maxRecordsPerFile));
                for (final List<SinkRecord> chunk : chunks) {
                    expectedFileNames.add(createFilename(chunk.get(0)));
                }
            }
        }

        assertThat(bucketAccessor.getBlobNames(), containsInAnyOrder(expectedFileNames.toArray()));
    }

    /**
     * For each written file, checks that it's not empty and is not exceeding the maximum size.
     */
    private void checkFileSizes(final BucketAccessor bucketAccessor, final Integer maxRecordsPerFile) {
        final int effectiveMax = effectiveMaxRecordsPerFile(maxRecordsPerFile);
        for (final String filename : bucketAccessor.getBlobNames()) {
            assertThat(bucketAccessor.readLines(filename, "none"),
                    hasSize(allOf(greaterThan(0), lessThanOrEqualTo(effectiveMax))));
        }
    }

    /**
     * Checks, that:
     * <ul>
     * <li>the total number of records written to all files is correct;</li>
     * <li>each record is written only once.</li>
     * </ul>
     */
    private void checkTotalRecordCountAndNoMultipleWrites(final int expectedCount,
            final BucketAccessor bucketAccessor) {
        final Set<String> seenRecords = new HashSet<>();
        for (final String filename : bucketAccessor.getBlobNames()) {
            for (final String line : bucketAccessor.readLines(filename, "none")) {
                // Ensure no multiple writes.
                assertFalse(seenRecords.contains(line));
                seenRecords.add(line);
            }
        }
        assertEquals(expectedCount, seenRecords.size());
    }

    /**
     * For each written file, checks that its filename (the topic and partition part) is correct for each record in it.
     */
    private void checkTopicPartitionPartInFileNames(final BucketAccessor bucketAccessor) {
        for (final String filename : bucketAccessor.getBlobNames()) {
            final String filenameWithoutOffset = cutOffsetPart(filename);

            final List<List<String>> lines = bucketAccessor.readAndDecodeLines(filename, "none", FIELD_KEY,
                    FIELD_VALUE);
            final String firstLineTopicAndPartition = lines.get(0).get(FIELD_VALUE);
            final String firstLineOffset = lines.get(0).get(FIELD_OFFSET);
            assertEquals(PREFIX + firstLineTopicAndPartition + "-" + firstLineOffset, filename);

            for (final List<String> line : lines) {
                final String value = line.get(FIELD_VALUE);
                assertEquals(PREFIX + value, filenameWithoutOffset);
            }
        }
    }

    /**
     * Cuts off the offset part from a string like "topic-partition-offset".
     */
    private String cutOffsetPart(final String topicPartitionOffset) {
        return topicPartitionOffset.substring(0, topicPartitionOffset.lastIndexOf('-'));
    }

    /**
     * For each written file, checks that offsets of records are increasing.
     */
    private void checkOffsetOrderInFiles(final BucketAccessor bucketAccessor) {
        for (final String filename : bucketAccessor.getBlobNames()) {
            final List<List<String>> lines = bucketAccessor.readAndDecodeLines(filename, "none", FIELD_KEY,
                    FIELD_VALUE);
            final List<Integer> offsets = lines.stream()
                    .map(line -> Integer.parseInt(line.get(FIELD_OFFSET)))
                    .collect(Collectors.toList());
            for (int i = 0; i < offsets.size() - 1; i++) {
                assertTrue(offsets.get(i) < offsets.get(i + 1));
            }
        }
    }

    private String createFilename(final SinkRecord record) {
        return PREFIX + record.topic() + "-" + record.kafkaPartition() + "-" + record.kafkaOffset();
    }

    private int effectiveMaxRecordsPerFile(final Integer maxRecordsPerFile) {
        if (maxRecordsPerFile == null) {
            return Integer.MAX_VALUE;
        } else {
            return maxRecordsPerFile;
        }
    }
}
