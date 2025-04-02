package io.aiven.kafka.connect.s3.source.testdata;

import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public class S3OffsetManagerIntegrationTestData {

    private static S3OffsetManagerEntry createOffsetManagerEntry(final String nativeKey) {
        return new S3OffsetManagerEntry(AWSIntegrationTestData.BUCKET_NAME, nativeKey);
    }

    public static OffsetManager.OffsetManagerKey createOffsetManagerKey(String nativeKey) {
        return createOffsetManagerEntry(nativeKey).getManagerKey();
    }

    public static Function<Map<String, Object>, S3OffsetManagerEntry> getOffsetManagerEntryCreator(OffsetManager.OffsetManagerKey key) {
        return map ->  new S3OffsetManagerEntry(key.getPartitionMap().get(S3OffsetManagerEntry.BUCKET).toString(), key.getPartitionMap().get(S3OffsetManagerEntry.OBJECT_KEY).toString()).fromProperties(map);
    }

    public static BiFunction<Map<String, Object>, Map<String, Object>, S3OffsetManagerEntry> offsetManagerEntryFactory() {
        return S3OffsetManagerIntegrationTestData::getOffsetManagerEntry;
    }

    public static S3OffsetManagerEntry getOffsetManagerEntry(final Map<String, Object> key, final Map<String, Object> data) {
        S3OffsetManagerEntry entry = new S3OffsetManagerEntry(key.get(S3OffsetManagerEntry.BUCKET).toString(), key.get(S3OffsetManagerEntry.OBJECT_KEY).toString());
        return entry.fromProperties(data);
    }
}
