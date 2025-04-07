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

package io.aiven.kafka.connect.common.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import io.aiven.kafka.connect.common.source.impl.ExampleOffsetManagerEntry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class OffsetManagerTest {

    private OffsetStorageReader offsetStorageReader;

    private OffsetManager<ExampleOffsetManagerEntry> offsetManager;

    @BeforeEach
    void setup() {
        offsetStorageReader = mock(OffsetStorageReader.class);
        final SourceTaskContext sourceTaskContext = mock(SourceTaskContext.class);
        when(sourceTaskContext.offsetStorageReader()).thenReturn(offsetStorageReader);
        offsetManager = new OffsetManager<>(sourceTaskContext);
    }

    @Test
    void testNewEntryWithDataFromContext() {
        final Map<String, Object> partitionKey = new HashMap<>();
        partitionKey.put("segment1", "topic1");
        partitionKey.put("segment2", "a value");
        partitionKey.put("segment3", "something else");
        final Map<String, Object> offsetValue = new HashMap<>(partitionKey);
        offsetValue.put("object_key_file", 5L);
        when(offsetStorageReader.offset(partitionKey)).thenReturn(offsetValue);

        final Optional<ExampleOffsetManagerEntry> result = offsetManager
                .getEntry(new OffsetManager.OffsetManagerKey(partitionKey), ExampleOffsetManagerEntry::new);
        assertThat(result).isPresent();
        assertThat(result.get().data).isEqualTo(offsetValue);
    }

    @Test
    void testNewEntryWithoutDataFromContext() {
        final Map<String, Object> partitionKey = new HashMap<>();
        partitionKey.put("segment1", "topic1");
        partitionKey.put("segment2", "a value");
        partitionKey.put("segment3", "something else");
        when(offsetStorageReader.offset(partitionKey)).thenReturn(new HashMap<>());

        final Optional<ExampleOffsetManagerEntry> result = offsetManager
                .getEntry(new OffsetManager.OffsetManagerKey(partitionKey), ExampleOffsetManagerEntry::new);
        assertThat(result).isNotPresent();
    }

    @Test
    void testPopulateOffsetManagerWithPartitionMapsNoExistingEntries() {
        final List<OffsetManager.OffsetManagerKey> partitionMaps = new ArrayList<>();

        final ExampleOffsetManagerEntry entry = new ExampleOffsetManagerEntry("key", "something else");

        for (int i = 0; i < 10; i++) {
            partitionMaps.add(entry.getManagerKey());
            entry.incrementRecordCount();
        }

        when(offsetStorageReader.offsets(anyCollection())).thenReturn(Map.of());

        offsetManager.populateOffsetManager(partitionMaps);
        verify(offsetStorageReader, times(1)).offsets(anyList());

        // No Existing entries so we expect nothing to exist and for it to try check the offsets again.
        final Optional<ExampleOffsetManagerEntry> result = offsetManager.getEntry(
                new OffsetManager.OffsetManagerKey(partitionMaps.get(0).getPartitionMap()),
                ExampleOffsetManagerEntry::new);
        assertThat(result).isEmpty();
        verify(offsetStorageReader, times(1)).offset(eq(partitionMaps.get(0).getPartitionMap()));

    }

    // @SuppressWarnings("PMD.AvoidInstantiationInLoops")
    @Test
    void testPopulateOffsetManagerWithPartitionMapsAllEntriesExist() {
        final List<OffsetManager.OffsetManagerKey> partitionMaps = new ArrayList<>();
        final Map<Map<String, Object>, Map<String, Object>> offsetReaderMap = new HashMap<>();

        final ExampleOffsetManagerEntry entry = new ExampleOffsetManagerEntry("key", "something else");

        for (int i = 0; i < 10; i++) {
            final OffsetManager.OffsetManagerKey key = entry.getManagerKey();
            partitionMaps.add(key);
            offsetReaderMap.put(key.getPartitionMap(), entry.getProperties());
            entry.incrementRecordCount();
        }

        when(offsetStorageReader.offsets(anyList())).thenReturn(offsetReaderMap);

        offsetManager.populateOffsetManager(partitionMaps);
        verify(offsetStorageReader, times(1)).offsets(anyList());

        // No Existing entries so we expect nothing to exist and for it to try check the offsets again.
        final Optional<ExampleOffsetManagerEntry> result = offsetManager.getEntry(
                new OffsetManager.OffsetManagerKey(partitionMaps.get(0).getPartitionMap()),
                ExampleOffsetManagerEntry::new);
        assertThat(result).isPresent();
        verify(offsetStorageReader, times(0)).offset(eq(partitionMaps.get(0).getPartitionMap()));

    }

    @Test
    void testKeyEqualityAndHashCode() {
        final OffsetManager.OffsetManagerKey key1 = new OffsetManager.OffsetManagerKey(
                Map.of("one", "uno", "two", "dos"));
        final OffsetManager.OffsetManagerKey key2 = new OffsetManager.OffsetManagerKey(
                Map.of("one", "uno", "two", "dos"));
        final OffsetManager.OffsetManagerKey key3 = new OffsetManager.OffsetManagerKey(
                Map.of("one", "aon", "two", "dó"));

        assertThat(key1).isEqualTo(key2);
        assertThat(key1.hashCode()).isEqualTo(key2.hashCode());
        assertThat(key1).isNotEqualTo(key3);
        assertThat(key1.hashCode()).isNotEqualTo(key3.hashCode());
    }
}
