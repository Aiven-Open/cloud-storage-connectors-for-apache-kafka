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

public class S3Partition {

    private final String bucket;
    private final String keyPrefix;
    private final String topic;
    private final int partition;

    public S3Partition(final String bucket, final String keyPrefix, final String topic, final int partition) {
        this.bucket = bucket;
        this.keyPrefix = normalizePrefix(keyPrefix);
        this.topic = topic;
        this.partition = partition;
    }

    public static S3Partition from(final String bucket, final String keyPrefix, final String topic,
            final int partition) {
        return new S3Partition(bucket, keyPrefix, topic, partition);
    }

    public static S3Partition from(final Map<String, Object> map) {
        final String bucket = (String) map.get("bucket");
        final String keyPrefix = (String) map.get("keyPrefix");
        final String topic = (String) map.get("topic");
        final int partition = ((Number) map.get("kafkaPartition")).intValue();
        return from(bucket, keyPrefix, topic, partition);
    }

    public static String normalizePrefix(final String keyPrefix) {
        return keyPrefix == null ? "" : keyPrefix.endsWith("/") ? keyPrefix : keyPrefix + "/";
    }

    public Map<String, Object> asMap() {
        final Map<String, Object> map = new HashMap<>();
        map.put("bucket", bucket);
        map.put("keyPrefix", keyPrefix);
        map.put("topic", topic);
        map.put("kafkaPartition", partition);
        return map;
    }

    public String getBucket() {
        return bucket;
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    @Override
    public boolean equals(final Object s3Partition) {
        if (this == s3Partition) {
            return true;
        }
        if (s3Partition == null || getClass() != s3Partition.getClass()) {
            return false;
        }
        final S3Partition thatS3Partition = (S3Partition) s3Partition;
        return partition == thatS3Partition.partition && Objects.equals(bucket, thatS3Partition.bucket)
                && Objects.equals(keyPrefix, thatS3Partition.keyPrefix) && Objects.equals(topic, thatS3Partition.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucket, keyPrefix, topic, partition);
    }

    @Override
    public String toString() {
        return bucket + "/" + keyPrefix + "/" + topic + "-" + partition;
    }
}
