
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

import static java.util.stream.Collectors.toMap;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffsetManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetManager.class);
    public static final String SEPARATOR = "_";
    private final Map<Map<String, Object>, Map<String, Object>> offsets;

    public OffsetManager(final SourceTaskContext context, final S3SourceConfig s3SourceConfig) {
        final String s3Bucket = s3SourceConfig.getString(S3SourceConfig.AWS_S3_BUCKET_NAME_CONFIG);
        final Set<Integer> partitions = parsePartitions(s3SourceConfig);
        final Set<String> topics = parseTopics(s3SourceConfig);

        // Build the partition keys and fetch offsets from offset storage
        final List<Map<String, Object>> partitionKeys = buildPartitionKeys(s3Bucket, partitions, topics);
        final Map<Map<String, Object>, Map<String, Object>> offsetMap = context.offsetStorageReader()
                .offsets(partitionKeys);

        LOGGER.info(" ********** offsetMap ***** {}", offsetMap);
        this.offsets = offsetMap.entrySet()
                .stream()
                .filter(e -> e.getValue() != null)
                .collect(toMap(entry -> new HashMap<>(entry.getKey()), entry -> new HashMap<>(entry.getValue())));
        LOGGER.info(" ********** offsets ***** {}", offsets);
    }

    public Map<Map<String, Object>, Map<String, Object>> getOffsets() {
        return Collections.unmodifiableMap(offsets);
    }

    public long incrementAndUpdateOffset(final Map<String, Object> partitionMap, final String s3ObjectKey,
                                         final long defaultOffset) {
        Map<String, Object> innerMap = offsets.get(partitionMap);
        if (innerMap == null) {
            innerMap = new HashMap<>();
        }
        String innerKey = getOffsetKey(s3ObjectKey);
        Object value = innerMap.get(innerKey);
        if (value != null) {
            final long newOffsetVal = 1L + (long) value;
            innerMap.put(innerKey, newOffsetVal);
            return newOffsetVal;
        }
        innerMap.put(innerKey, defaultOffset);
        offsets.put(partitionMap, innerMap);
        return defaultOffset;
    }

    public Long getOffset(final Map<String, Object> partitionMap, final String s3ObjectKey) {
        Map<String, Object> innerMap = offsets.get(partitionMap);
        if (innerMap == null) {
            return null;
        }
        String innerKey = getOffsetKey(s3ObjectKey);
        return (Long) innerMap.get(innerKey);
    }

    public static String getOffsetKey(final String currentObjectKey) {
        return "offset" + SEPARATOR + currentObjectKey;
    }

    private static Set<Integer> parsePartitions(final S3SourceConfig s3SourceConfig) {
        final String partitionString = s3SourceConfig.getString(S3SourceConfig.TARGET_TOPIC_PARTITIONS);
        return Arrays.stream(partitionString.split(",")).map(Integer::parseInt).collect(Collectors.toSet());
    }

    private static Set<String> parseTopics(final S3SourceConfig s3SourceConfig) {
        final String topicString = s3SourceConfig.getString(S3SourceConfig.TARGET_TOPICS);
        return Arrays.stream(topicString.split(",")).collect(Collectors.toSet());
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
