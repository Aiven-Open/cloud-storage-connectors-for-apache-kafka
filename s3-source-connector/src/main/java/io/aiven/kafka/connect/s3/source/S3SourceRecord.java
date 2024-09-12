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

import java.util.Arrays;

public class S3SourceRecord {
    private final S3Partition s3Partition;
    private final S3Offset s3Offset;
    private final String toTopic;
    private final int topicPartition;
    private final byte[] recordKey;
    private final byte[] recordValue;

    public S3SourceRecord(final S3Partition s3Partition, final S3Offset s3Offset, final String toTopic,
            final int topicPartition, final byte[] recordKey, final byte[] recordValue) {
        this.s3Partition = s3Partition;
        this.s3Offset = s3Offset;
        this.toTopic = toTopic;
        this.topicPartition = topicPartition;
        this.recordKey = Arrays.copyOf(recordKey, recordKey.length);
        this.recordValue = Arrays.copyOf(recordValue, recordValue.length);
    }

    public S3Partition file() {
        return s3Partition;
    }

    public S3Offset offset() {
        return s3Offset;
    }

    public String getToTopic() {
        return toTopic;
    }

    public int partition() {
        return topicPartition;
    }

    public byte[] key() {
        return recordKey.clone();
    }

    public byte[] value() {
        return recordValue.clone();
    }
}
