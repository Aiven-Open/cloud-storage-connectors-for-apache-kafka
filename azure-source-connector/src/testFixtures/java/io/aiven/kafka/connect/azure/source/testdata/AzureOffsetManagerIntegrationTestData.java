/*
 * Copyright 2025 Aiven Oy
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

package io.aiven.kafka.connect.azure.source.testdata;

import java.util.Map;
import java.util.function.BiFunction;

import io.aiven.kafka.connect.azure.source.utils.AzureBlobOffsetManagerEntry;

/**
 * Creates data for Offset Manager testing.
 */
public final class AzureOffsetManagerIntegrationTestData {

    private AzureOffsetManagerIntegrationTestData() {
        // do not instantiate
    }

    /**
     * Creates an offset manger entry factory.
     *
     * @return the offset manager entry factory.
     */
    public static BiFunction<Map<String, Object>, Map<String, Object>, AzureBlobOffsetManagerEntry> offsetManagerEntryFactory() {
        return AzureOffsetManagerIntegrationTestData::getOffsetManagerEntry;
    }

    /**
     * Creates an offset manager entry from the Connector key map and value map.
     *
     * @param key
     *            the key map of data.
     * @param data
     *            the value map of data.
     * @return an OffsetManagerEntry
     */
    public static AzureBlobOffsetManagerEntry getOffsetManagerEntry(final Map<String, Object> key,
            final Map<String, Object> data) {
        final AzureBlobOffsetManagerEntry entry = new AzureBlobOffsetManagerEntry(
                key.get(AzureBlobOffsetManagerEntry.CONTAINER).toString(),
                key.get(AzureBlobOffsetManagerEntry.BLOB_NAME).toString());
        return entry.fromProperties(data);
    }
}
