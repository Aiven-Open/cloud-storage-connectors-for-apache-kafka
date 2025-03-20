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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import io.aiven.kafka.connect.common.source.OffsetManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class AzureBlobOffsetManagerEntryTest {

    static final String TEST_CONTAINER = "test-container";

    static final String BLOB_NAME = "blob_1";

    private SourceTaskContext sourceTaskContext;

    private OffsetManager<AzureBlobOffsetManagerEntry> offsetManager;

    private OffsetStorageReader offsetStorageReader;

    @BeforeEach
    public void setUp() {
        offsetStorageReader = mock(OffsetStorageReader.class);
        sourceTaskContext = mock(SourceTaskContext.class);
        when(sourceTaskContext.offsetStorageReader()).thenReturn(offsetStorageReader);
        offsetManager = new OffsetManager<>(sourceTaskContext);
    }

    private Map<String, Object> createPartitionMap() {
        final Map<String, Object> partitionKey = new HashMap<>();
        partitionKey.put(AzureBlobOffsetManagerEntry.CONTAINER, TEST_CONTAINER);
        partitionKey.put(AzureBlobOffsetManagerEntry.BLOB_NAME, BLOB_NAME);
        return partitionKey;
    }

    public static AzureBlobOffsetManagerEntry newEntry() {
        return new AzureBlobOffsetManagerEntry(TEST_CONTAINER, BLOB_NAME);
    }

    @Test
    void testGetEntry() {
        final Map<String, Object> storedData = new HashMap<>();
        storedData.putAll(createPartitionMap());
        storedData.put("random_entry", 5L);

        when(offsetStorageReader.offset(any())).thenReturn(storedData);

        final AzureBlobOffsetManagerEntry keyEntry = newEntry();
        final Optional<AzureBlobOffsetManagerEntry> entry = offsetManager.getEntry(keyEntry.getManagerKey(),
                keyEntry::fromProperties);
        assertThat(entry).isPresent();
        assertThat(entry.get().getContainer()).isEqualTo(TEST_CONTAINER);
        assertThat(entry.get().getProperty("random_entry")).isEqualTo(5L);
        verify(sourceTaskContext, times(1)).offsetStorageReader();

        // verify second read reads from local data

        final Optional<AzureBlobOffsetManagerEntry> entry2 = offsetManager.getEntry(entry.get().getManagerKey(),
                entry.get()::fromProperties);
        assertThat(entry2).isPresent();
        assertThat(entry2.get().getContainer()).isEqualTo(TEST_CONTAINER);
        assertThat(entry2.get().getProperty("random_entry")).isEqualTo(5L);
        verify(sourceTaskContext, times(1)).offsetStorageReader();
    }

    @Test
    void testFromProperties() {
        final AzureBlobOffsetManagerEntry entry = new AzureBlobOffsetManagerEntry(TEST_CONTAINER, BLOB_NAME);
        assertThat(entry.getRecordCount()).isEqualTo(0L);
        assertThat(entry.getProperty("random_entry")).isNull();

        entry.setProperty("random_entry", 5L);
        entry.incrementRecordCount();
        assertThat(entry.getRecordCount()).isEqualTo(1L);
        assertThat(entry.getProperty("random_entry")).isEqualTo(5L);

        final AzureBlobOffsetManagerEntry other = entry.fromProperties(entry.getProperties());
        assertThat(other.getRecordCount()).isEqualTo(1L);
        assertThat(other.getProperty("random_entry")).isEqualTo(5L);

    }
}
