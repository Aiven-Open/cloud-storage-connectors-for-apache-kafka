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

import java.util.HashMap;
import java.util.Map;

final public class ConnectUtils {

    private ConnectUtils() {
        // hidden
    }
    static Map<String, Object> getPartitionMap(final String topicName, final Integer defaultPartitionId,
            final String bucketName) {
        final Map<String, Object> partitionMap = new HashMap<>();
        partitionMap.put(BUCKET, bucketName);
        partitionMap.put(TOPIC, topicName);
        partitionMap.put(PARTITION, defaultPartitionId);
        return partitionMap;
    }
}
