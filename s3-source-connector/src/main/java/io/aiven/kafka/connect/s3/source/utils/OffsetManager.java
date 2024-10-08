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

import static io.aiven.kafka.connect.s3.source.utils.SourceRecordIterator.OFFSET_KEY;
import static java.util.stream.Collectors.toMap;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceTaskContext;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

public class OffsetManager {
    private final Map<Map<String, Object>, Map<String, Object>> offsets;

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

    public Map<Map<String, Object>, Map<String, Object>> getOffsets() {
        return Collections.unmodifiableMap(offsets);
    }

    public long incrementAndUpdateOffsetMap(final Map<String, Object> partitionMap) {
        if (offsets.containsKey(partitionMap)) {
            final Map<String, Object> offsetValue = offsets.get(partitionMap);
            if (offsetValue.containsKey(OFFSET_KEY)) {
                final long newOffsetVal = (long) offsetValue.get(OFFSET_KEY) + 1L;
                offsetValue.put(OFFSET_KEY, newOffsetVal);
                return newOffsetVal;
            }
        }
        return 0L;
    }

    private static Set<Integer> parsePartitions(final S3SourceConfig s3SourceConfig) {
        final String partitionString = s3SourceConfig.getString(S3SourceConfig.TARGET_TOPIC_PARTITIONS);
        return Arrays.stream(partitionString.split(",")).map(Integer::parseInt).collect(Collectors.toSet());
    }

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

    private static List<Map<String, Object>> buildPartitionKeys(final String bucket, final Set<Integer> partitions,
            final Set<String> topics) {
        final List<Map<String, Object>> partitionKeys = new ArrayList<>();
        partitions.forEach(partition -> topics.forEach(topic -> {
            partitionKeys.add(ConnectUtils.getPartitionMap(topic, partition, bucket));
        }));
        return partitionKeys;
    }
}
