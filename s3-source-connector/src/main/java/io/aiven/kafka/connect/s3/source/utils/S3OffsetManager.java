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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceTaskContext;

import io.aiven.kafka.connect.common.source.offsets.OffsetManager;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

public class S3OffsetManager extends OffsetManager {

    public S3OffsetManager(final SourceTaskContext context, final S3SourceConfig s3SourceConfig) {
        super(context, initializePartitionKeys(s3SourceConfig));
    }

    /**
     * FOR TESTING ONLY
     *
     * @param offsets
     *            the offsets
     */
    protected S3OffsetManager(final Map<Map<String, Object>, Map<String, Object>> offsets) {
        super(offsets);
    }

    private static List<Map<String, Object>> initializePartitionKeys(final S3SourceConfig s3SourceConfig) {
        final String s3Bucket = s3SourceConfig.getString(S3SourceConfig.AWS_S3_BUCKET_NAME_CONFIG);
        final Set<Integer> partitions = parsePartitions(s3SourceConfig);
        final Set<String> topics = parseTopics(s3SourceConfig);
        // Build the partition keys and fetch offsets from offset storage
        return buildPartitionKeys(s3Bucket, partitions, topics);
    }

    private static Set<Integer> parsePartitions(final S3SourceConfig s3SourceConfig) {
        final String partitionString = s3SourceConfig.getString(S3SourceConfig.TARGET_TOPIC_PARTITIONS);
        return Arrays.stream(partitionString.split(",")).map(Integer::parseInt).collect(Collectors.toSet());
    }

    private static Set<String> parseTopics(final S3SourceConfig s3SourceConfig) {
        final String topicString = s3SourceConfig.getString(S3SourceConfig.TARGET_TOPICS);
        return Arrays.stream(topicString.split(",")).collect(Collectors.toSet());
    }

    // TODO can this be done in a cleaner way? May not be needed.
    private static List<Map<String, Object>> buildPartitionKeys(final String bucket, final Set<Integer> partitions,
            final Set<String> topics) {
        final List<Map<String, Object>> partitionKeys = new ArrayList<>();
        partitions.forEach(partition -> topics.forEach(topic -> {
            partitionKeys.add(ConnectUtils.getPartitionMap(topic, partition, bucket));
        }));
        return partitionKeys;
    }
}
