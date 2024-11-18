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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class S3OffsetManagerEntry implements OffsetManager.OffsetManagerEntry {

    // package private statics for testing.
    static final String BUCKET = "bucket";
    static final String OBJECT_KEY = "objectKey";
    static final String TOPIC = "topic";
    static final String PARTITION = "partition";
    static final String RECORD_COUNT = "recordCount";

    private static final long UNSET = -1L;

    static final List<String> RESTRICTED_KEYS = List.of(BUCKET, OBJECT_KEY, TOPIC, PARTITION, RECORD_COUNT);
    private final Map<String, Object> data;
    private long recordCount;

    public S3OffsetManagerEntry(final String bucket, final String s3ObjectKey, final String topic,
            final Integer partition) {
        data = new HashMap<>();
        data.put(BUCKET, bucket);
        data.put(OBJECT_KEY, s3ObjectKey);
        data.put(TOPIC, topic);
        data.put(PARTITION, partition);
    }

    @Override
    public S3OffsetManagerEntry fromProperties(final Map<String, Object> properties) {
        S3OffsetManagerEntry result = new S3OffsetManagerEntry(properties);
        Long recordCount = (Long) properties.get(RECORD_COUNT);
        if (recordCount != null) {
            result.recordCount = recordCount;
        }
        return result;
    }

    private S3OffsetManagerEntry(final Map<String, Object> properties) {
        data = new HashMap<>();
        data.putAll(properties);
    }

    @Override
    public void setProperty(final String property, final Object value) {
        if (RESTRICTED_KEYS.contains(property)) {
            throw new IllegalArgumentException(
                    String.format("'%s' is a restricted key and may not be set using setProperty()", property));
        }
        data.put(property, value);
    }

    public long incrementRecordCount() {
        return recordCount += 1;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public String getKey() {
        return (String) data.get(OBJECT_KEY);
    }

    public Integer getPartition() {
        return (Integer) data.get(PARTITION);
    }

    public String getTopic() {
        return (String) data.get(TOPIC);
    }

    public boolean shouldSkipRecord(final long candidateRecord) {
        return candidateRecord < recordCount;
    }

    /**
     * Creates a new offset map. No defensive copy is necessary.
     *
     * @return a new map of properties and values.
     */
    @Override
    public Map<String, Object> getProperties() {
        final Map<String, Object> result = new HashMap<>(data);
        result.put(RECORD_COUNT, recordCount);
        return result;
    }

    @Override
    public OffsetManager.OffsetManagerKey getManagerKey() {
        return () -> Map.of(BUCKET, data.get(BUCKET), TOPIC, data.get(TOPIC), PARTITION, data.get(PARTITION));
    }

}
