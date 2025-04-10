package io.aiven.kafka.connect.azure.source.testdata;

import io.aiven.kafka.connect.azure.source.utils.AzureBlobOffsetManagerEntry;
import io.aiven.kafka.connect.common.source.OffsetManager;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public class AzureOffsetManagerIntegrationTestData {

    private static AzureBlobOffsetManagerEntry createOffsetManagerEntry(final String nativeKey) {
        return new AzureBlobOffsetManagerEntry(AzureIntegrationTestData.DEFAULT_CONTAINER, nativeKey);
    }

    public static OffsetManager.OffsetManagerKey createOffsetManagerKey(String nativeKey) {
        return createOffsetManagerEntry(nativeKey).getManagerKey();
    }

    public static Function<Map<String, Object>, AzureBlobOffsetManagerEntry> getOffsetManagerEntryCreator(OffsetManager.OffsetManagerKey key) {
        return map ->  new AzureBlobOffsetManagerEntry(key.getPartitionMap().get(AzureBlobOffsetManagerEntry.CONTAINER).toString(), key.getPartitionMap().get(AzureBlobOffsetManagerEntry.BLOB_NAME).toString()).fromProperties(map);
    }

    public static BiFunction<Map<String, Object>, Map<String, Object>, AzureBlobOffsetManagerEntry> offsetManagerEntryFactory() {
        return AzureOffsetManagerIntegrationTestData::getOffsetManagerEntry;
    }

    public static AzureBlobOffsetManagerEntry getOffsetManagerEntry(final Map<String, Object> key, final Map<String, Object> data) {
        AzureBlobOffsetManagerEntry entry = new AzureBlobOffsetManagerEntry(key.get(AzureBlobOffsetManagerEntry.CONTAINER).toString(), key.get(AzureBlobOffsetManagerEntry.BLOB_NAME).toString());
        return entry.fromProperties(data);
    }
}
