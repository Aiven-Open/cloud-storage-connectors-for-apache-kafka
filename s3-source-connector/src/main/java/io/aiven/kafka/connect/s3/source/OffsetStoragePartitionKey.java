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

package io.aiven.kafka.connect.s3.source;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class OffsetStoragePartitionKey {

    public static final String BUCKET_NAME = "bucketName";
    public static final String TOPIC = "topic";
    public static final String TOPIC_PARTITION = "topicPartition";
    private final String s3BucketName;
    private final String topic;
    private final int partition;

    public OffsetStoragePartitionKey(final String s3BucketName, final String topic, final int partition) {
        if (s3BucketName == null || s3BucketName.isEmpty()) {
            throw new IllegalArgumentException("S3 bucket name cannot be null or empty");
        }
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("Topic cannot be null or empty");
        }
        if (partition < 0) {
            throw new IllegalArgumentException("Partition must be a non-negative integer");
        }

        this.s3BucketName = s3BucketName;
        this.topic = topic;
        this.partition = partition;
    }

    public static OffsetStoragePartitionKey fromPartitionMap(final String bucket, final String topic,
            final int partition) {
        return new OffsetStoragePartitionKey(bucket, topic, partition);
    }

    public static OffsetStoragePartitionKey fromPartitionMap(final Map<String, Object> map) {
        Objects.requireNonNull(map, "Input map cannot be null");
        final String bucket = (String) map.getOrDefault(BUCKET_NAME, "");
        final String topic = (String) map.getOrDefault(TOPIC, "");
        final int partition = ((Number) map.getOrDefault(TOPIC_PARTITION, -1)).intValue();
        return fromPartitionMap(bucket, topic, partition);
    }

    public Map<String, Object> toPartitionMap() {
        final Map<String, Object> map = new HashMap<>();
        map.put(BUCKET_NAME, s3BucketName);
        map.put(TOPIC, topic);
        map.put(TOPIC_PARTITION, partition);
        return map;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final OffsetStoragePartitionKey other = (OffsetStoragePartitionKey) obj;
        return partition == other.partition && Objects.equals(s3BucketName, other.s3BucketName)
                && Objects.equals(topic, other.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(s3BucketName, topic, partition);
    }

    @Override
    public String toString() {
        return String.format("OffsetStoragePartitionKey{bucketName='%s', topic='%s', partition=%d}", s3BucketName,
                topic, partition);
    }
}
