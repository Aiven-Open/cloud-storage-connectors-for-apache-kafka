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

package io.aiven.kafka.connect.azure.source.utils;

import io.aiven.kafka.connect.common.source.OffsetManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class AzureBlobOffsetManagerEntry
        implements
            OffsetManager.OffsetManagerEntry<AzureBlobOffsetManagerEntry> {

    public static final String CONTAINER = "container";
    public static final String BLOB_NAME = "blobName";
    public static final String RECORD_COUNT = "recordCount";

    /**
     * THe list of Keys that may not be set via {@link #setProperty(String, Object)}.
     */
    static final List<String> RESTRICTED_KEYS = List.of(RECORD_COUNT);
    /** The data map that stores all the values */
    private final Map<String, Object> data;
    /** THe record count for the data map. Extracted here because it is used/updated frequently during processing */
    private long recordCount;

    private final String container;
    private final String blobName;

    /**
     * Construct the AzureOffsetManagerEntry.
     *
     * @param container
     *            the bucket we are using.
     * @param blobName
     *            the blob name.
     */
    public AzureBlobOffsetManagerEntry(final String container, final String blobName) {
        this.container = container;
        this.blobName = blobName;
        data = new HashMap<>();
    }

    /**
     * Constructs an OffsetManagerEntry from an existing map. Used to reconstitute previously serialized
     * AzureOffsetManagerEntries. used by {@link #fromProperties(Map)}
     *
     * @param properties
     *            the property map.
     */
    private AzureBlobOffsetManagerEntry(final String container, final String blobName,
            final Map<String, Object> properties) {
        this(container, blobName);
        data.putAll(properties);
        final Object recordCountProperty = data.computeIfAbsent(RECORD_COUNT, s -> 0L);
        if (recordCountProperty instanceof Number) {
            recordCount = ((Number) recordCountProperty).longValue();
        }
    }

    /**
     *
     * @param bucket
     *            the bucket we are using.
     * @param blobName
     *            the blob name.
     * @return a new instance of OffsetManagerKey
     */
    public static OffsetManager.OffsetManagerKey asKey(final String bucket, final String blobName) {
        return new OffsetManager.OffsetManagerKey(Map.of(CONTAINER, bucket, BLOB_NAME, blobName));
    }

    /**
     * Creates an AzureOffsetManagerEntry. Will return {@code null} if properties is {@code null}.
     *
     * @param properties
     *            the properties to wrap. May be {@code null}.
     * @return an AzureOffsetManagerEntry.
     * @throws IllegalArgumentException
     *             if one of the {@link #RESTRICTED_KEYS} is missing.
     */
    @Override
    public AzureBlobOffsetManagerEntry fromProperties(final Map<String, Object> properties) {
        if (properties == null) {
            return null;
        }
        return new AzureBlobOffsetManagerEntry(container, blobName, properties);
    }

    @Override
    public Object getProperty(final String key) {
        if (RECORD_COUNT.equals(key)) {
            return recordCount;
        }
        return data.get(key);
    }

    @Override
    public void setProperty(final String property, final Object value) {
        if (RESTRICTED_KEYS.contains(property)) {
            throw new IllegalArgumentException(
                    String.format("'%s' is a restricted key and may not be set using setProperty()", property));
        }
        data.put(property, value);
    }

    @Override
    public void incrementRecordCount() {
        recordCount++;
    }

    /**
     * Gets the umber of records extracted from data returned from Azure Blob.
     *
     * @return the number of records extracted from data returned from Azure Blob.
     */
    @Override
    public long getRecordCount() {
        return recordCount;
    }

    /**
     * Gets the blob name for the current object.
     *
     * @return the blob name.
     */
    public String getKey() {
        return blobName;
    }

    /**
     * Gets the Azure container for the current object.
     *
     * @return the Azure container for the current object.
     */
    public String getContainer() {
        return container;
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
    /**
     * Returns the OffsetManagerKey for this Entry.
     *
     * @return the OffsetManagerKey for this Entry.
     */
    @Override
    public OffsetManager.OffsetManagerKey getManagerKey() {
        return asKey(container, blobName);
    }

    @Override
    public boolean equals(final Object other) {
        if (other instanceof AzureBlobOffsetManagerEntry) {
            return compareTo((AzureBlobOffsetManagerEntry) other) == 0;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(container, blobName);
    }

    @Override
    public int compareTo(final AzureBlobOffsetManagerEntry other) {
        if (this == other) { // NOPMD comparing instance
            return 0;
        }
        int result = getContainer().compareTo(other.getContainer());
        if (result == 0) {
            result = getKey().compareTo(other.getKey());
            if (result == 0) {
                result = Long.compare(getRecordCount(), other.getRecordCount());
            }
        }
        return result;
    }
}
