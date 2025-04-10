package io.aiven.kafka.connect.azure.source.testdata;

import io.aiven.kafka.connect.azure.source.utils.AzureBlobOffsetManagerEntry;

import java.util.Map;
import java.util.function.BiFunction;

/**
 * Creates data for Offset Manager testing.
 */
public class AzureOffsetManagerIntegrationTestData {

    private AzureOffsetManagerIntegrationTestData() {
        // do not instantiate
    }

    /**
     * Creates an Azure blob OffsetManagerEntry
     * @param nativeKey the native key to create the entry for
     * @return an OffsetManagerEntry.
     */
    private static AzureBlobOffsetManagerEntry createOffsetManagerEntry(final String nativeKey) {
        return new AzureBlobOffsetManagerEntry(AzureIntegrationTestData.DEFAULT_CONTAINER, nativeKey);
    }

    /**
     * Creates an offset manger entry factory.
     * @return the offset manager entry factory.
     */
    public static BiFunction<Map<String, Object>, Map<String, Object>, AzureBlobOffsetManagerEntry> offsetManagerEntryFactory() {
        return AzureOffsetManagerIntegrationTestData::getOffsetManagerEntry;
    }

    /**
     * Creates an offset manager entry from the Connector key map and value map.
     * @param key the key map of data.
     * @param data the value map of data.
     * @return an OffsetManagerEntry
     */
    public static AzureBlobOffsetManagerEntry getOffsetManagerEntry(final Map<String, Object> key, final Map<String, Object> data) {
        AzureBlobOffsetManagerEntry entry = new AzureBlobOffsetManagerEntry(key.get(AzureBlobOffsetManagerEntry.CONTAINER).toString(), key.get(AzureBlobOffsetManagerEntry.BLOB_NAME).toString());
        return entry.fromProperties(data);
    }
}
