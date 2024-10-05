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

import static io.aiven.kafka.connect.s3.source.S3SourceTask.BUCKET;
import static io.aiven.kafka.connect.s3.source.S3SourceTask.PARTITION;
import static io.aiven.kafka.connect.s3.source.S3SourceTask.TOPIC;
import static io.aiven.kafka.connect.s3.source.utils.SourceRecordIterator.OFFSET_KEY;
import static java.util.stream.Collectors.toMap;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceTaskContext;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

public class OffsetManager {
    private final Map<Map<String, Object>, Map<String, Object>> offsets;

    /**
     * Constructor for OffsetManager. Initializes with the task context and S3 source configuration, and retrieves
     * offsets.
     *
     * @param context
     *            SourceTaskContext that provides access to the offset storage
     * @param s3SourceConfig
     *            S3SourceConfig that contains the source configuration details
     */
    public OffsetManager(final SourceTaskContext context, final S3SourceConfig s3SourceConfig) {
        final String s3Bucket = s3SourceConfig.getString(S3SourceConfig.AWS_S3_BUCKET_NAME_CONFIG);
        final Set<Integer> partitions = parsePartitions(s3SourceConfig);
        final Set<String> topics = parseTopics(s3SourceConfig);

        // Build the partition keys and fetch offsets from offset storage
        final List<Map<String, Object>> partitionKeys = buildPartitionKeys(s3Bucket, partitions, topics);
        final Map<Map<String, Object>, Map<String, Object>> offsetMap = context.offsetStorageReader()
                .offsets(partitionKeys);

        this.offsets = offsetMap.entrySet()
                .stream()
                .filter(e -> e.getValue() != null)
                .collect(toMap(entry -> new HashMap<>(entry.getKey()), entry -> new HashMap<>(entry.getValue())));
    }

    /**
     * Fetches all offsets for the current partitions and topics from the context.
     *
     * @return Map of partition keys and their corresponding offsets
     */
    public Map<Map<String, Object>, Map<String, Object>> getOffsets() {
        return offsets;
    }

    public long getIncrementedOffsetForPartition(final Map<String, Object> partitionMap) {
        return (long) (offsets.get(partitionMap)).get(OFFSET_KEY) + 1L;
    }

    /**
     * Updates the offset for a specific partition.
     *
     * @param partitionMap
     *            The partition map.
     */
    public void updateOffset(final Map<String, Object> partitionMap, final long currentOffset) {
        final Map<String, Object> newOffset = new HashMap<>();
        // increment offset id by 1
        newOffset.put(OFFSET_KEY, currentOffset + 1);
        offsets.put(partitionMap, newOffset);
    }

    /**
     * Helper method to parse partitions from the configuration.
     *
     * @param s3SourceConfig
     *            The S3 source configuration.
     * @return Set of partitions.
     */
    private static Set<Integer> parsePartitions(final S3SourceConfig s3SourceConfig) {
        final String partitionString = s3SourceConfig.getString(S3SourceConfig.TARGET_TOPIC_PARTITIONS);
        return Arrays.stream(partitionString.split(",")).map(Integer::parseInt).collect(Collectors.toSet());
    }

    /**
     * Helper method to parse topics from the configuration.
     *
     * @param s3SourceConfig
     *            The S3 source configuration.
     * @return Set of topics.
     */
    private static Set<String> parseTopics(final S3SourceConfig s3SourceConfig) {
        final String topicString = s3SourceConfig.getString(S3SourceConfig.TARGET_TOPICS);
        return Arrays.stream(topicString.split(",")).collect(Collectors.toSet());
    }

    String getFirstConfiguredTopic(final S3SourceConfig s3SourceConfig) throws ConnectException {
        final String topicString = s3SourceConfig.getString(S3SourceConfig.TARGET_TOPICS);
        return Arrays.stream(topicString.split(","))
                .findFirst()
                .orElseThrow(() -> new ConnectException("Topic could not be derived"));
    }

    /**
     * Builds partition keys to be used for offset retrieval.
     *
     * @param bucket
     *            The S3 bucket name.
     * @param partitions
     *            The set of partitions.
     * @param topics
     *            The set of topics.
     * @return List of partition keys (maps) used for fetching offsets.
     */
    private static List<Map<String, Object>> buildPartitionKeys(final String bucket, final Set<Integer> partitions,
            final Set<String> topics) {
        final List<Map<String, Object>> partitionKeys = new ArrayList<>();
        partitions.forEach(partition -> topics.forEach(topic -> {
            final Map<String, Object> partitionMap = new HashMap<>();
            partitionMap.put(BUCKET, bucket);
            partitionMap.put(TOPIC, topic);
            partitionMap.put(PARTITION, partition);
            partitionKeys.add(partitionMap);
        }));
        return partitionKeys;
    }
}
