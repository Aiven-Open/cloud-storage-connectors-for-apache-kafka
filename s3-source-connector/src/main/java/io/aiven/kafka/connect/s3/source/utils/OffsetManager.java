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

    /**
     * FOR TESTING ONLY
     *
     * @param offsets
     *            the offsets
     */
    protected OffsetManager(final Map<Map<String, Object>, Map<String, Object>> offsets) {
        this.offsets = offsets;
    }

    public Map<Map<String, Object>, Map<String, Object>> getOffsets() {
        return Collections.unmodifiableMap(offsets);
    }

    /**
     * Updates the offset map. The {@code partitionMap} parameter is only used as a key all data is managed internally.
     * If the {@code partitionMap} is not found in the internal map then no changes occur and the {@code startOffset}
     * value is returned. Otherwise:
     * <ul>
     * <li>If the internal map contains a value for {@code currentObjectKey} the offset is incremented and
     * returned.</li>
     * <li>If the internal map does NOT contain a value for {@code currentObjectKey} the offset set to the value of
     * {@code startOffset} and that value returned.</li>
     * </ul>
     *
     * @param partitionMap
     *            the key to lookup the data in the offsets table.
     * @param currentObjectKey
     *            The object to update the offset for.
     * @param startOffset
     *            The initial value if necessary.
     * @return the value of the calculation outlined above.
     */
    public long incrementAndUpdateOffsetMap(final Map<String, Object> partitionMap, final String currentObjectKey,
            final long startOffset) {
        if (offsets.containsKey(partitionMap)) {
            final Map<String, Object> offsetValue = new HashMap<>(offsets.get(partitionMap));
            if (offsetValue.containsKey(currentObjectKey)) {
                final long newOffsetVal = (long) offsetValue.get(currentObjectKey) + 1L;
                offsetValue.put(currentObjectKey, newOffsetVal);
                offsets.put(partitionMap, offsetValue);
                return newOffsetVal;
            } else {
                offsetValue.put(currentObjectKey, startOffset);
                offsets.put(partitionMap, offsetValue);
                return startOffset;
            }
        }
        return startOffset;
    }

    public boolean shouldSkipRecord(final S3OffsetManagerEntry offsetManagerEntry) {
        boolean result = false;
        Map<String, Object> partitionMap = offsetManagerEntry.getManagerKey().getPartitionMap();
        if (offsets.containsKey(offsetManagerEntry.getManagerKey().getPartitionMap())) {
            S3OffsetManagerEntry stored = offsetManagerEntry
                    .fromProperties(offsets.get(offsetManagerEntry.getManagerKey().getPartitionMap()));
            result = stored.shouldSkipRecord(offsetManagerEntry.getRecordCount());
        }
        return result;
    }

    public Map<String, Object> getOffsetValueMap(final String currentObjectKey, final long offsetId) {
        final Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put(currentObjectKey, offsetId);

        return offsetMap;
    }

    void updateCurrentOffsets(final Map<String, Object> partitionMap, final Map<String, Object> offsetValueMap) {
        if (offsets.containsKey(partitionMap)) {
            final Map<String, Object> offsetMap = new HashMap<>(offsets.get(partitionMap));
            offsetMap.putAll(offsetValueMap);
            offsets.put(partitionMap, offsetMap);
        } else {
            offsets.put(partitionMap, offsetValueMap);
        }
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

    /**
     * The definition of an entry in the OffsetManager.
     */
    public interface OffsetManagerEntry {

        /**
         * Creates a new OffsetManagerEntry by wrapping the properties with the current implementation.
         *
         * @param properties
         *            the properties to wrap.
         * @return an OffsetManagerProperty
         */
        OffsetManagerEntry fromProperties(Map<String, Object> properties);

        /**
         * Extract the data from the entry in the correct format to return to Kafka.
         *
         * @return
         */
        Map<String, Object> getProperties();

        /**
         * Sets a key/value pair. Will overwrite any existing value. Implementations of OffsetManagerEntry may declare
         * specific keys as restricted. These are generally keys that are managed internally by the OffsetManagerEntry
         * and may not be set except through provided setter methods or the constructor.
         *
         * @param key
         *            the key to set.
         * @param value
         *            the value to set.
         * @throws IllegalArgumentException
         *             if the key is restricted.
         */
        void setProperty(String key, Object value);

        /**
         * ManagerKey getManagerKey
         *
         * @return The offset manager key for this entry.
         */
        OffsetManagerKey getManagerKey();
    }

    /**
     * The OffsetManager Key. Must override hashCode() and equals().
     */
    @FunctionalInterface
    public interface OffsetManagerKey {
        Map<String, Object> getPartitionMap();
    }
}
