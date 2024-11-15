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

    static final List<String> RESTRICTED_KEYS = List.of(BUCKET, OBJECT_KEY, TOPIC, PARTITION, RECORD_COUNT);
    private final Map<String, Object> data;
    private long recordCount;


    public S3OffsetManagerEntry(final String bucket, final String s3ObjectKey, final String topic, final Integer partition) {
        data = new HashMap<>();
        data.put(BUCKET, bucket);
        data.put(OBJECT_KEY, s3ObjectKey);
        data.put(TOPIC, topic);
        data.put(PARTITION, partition);
    }

    public void setProperty(String property, Object value) {
        if (RESTRICTED_KEYS.contains(property)) {
            throw new IllegalArgumentException(String.format("'%s' is a restricted key and may not be set using setProperty()", property));
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

    public boolean shouldSkipRecord(long candidateRecord) {
        return candidateRecord < recordCount;
    };

    /**
     * Creates a new offset map.  No defensive copy is necessary.
     * @return a new map of properties and values.
     */
    @Override
    public Map<String, Object> getProperties() {
        Map<String, Object> result = new HashMap<>(data);
        result.put(RECORD_COUNT, recordCount);
        return result;
    }

    public OffsetManager.OffsetManagerKey getManagerKey() {
        return () -> Map.of(
            BUCKET, data.get(BUCKET), TOPIC, data.get(TOPIC), PARTITION, data.get(PARTITION));
    }

}
