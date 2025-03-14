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

import com.google.common.base.Objects;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class OffsetManagerTest {

    private OffsetStorageReader offsetStorageReader;

    private OffsetManager<TestingOffsetManagerEntry> offsetManager;

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

        final Optional<TestingOffsetManagerEntry> result = offsetManager.getEntry(() -> partitionKey,
                TestingOffsetManagerEntry::new);
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

        final Optional<TestingOffsetManagerEntry> result = offsetManager.getEntry(() -> partitionKey,
                TestingOffsetManagerEntry::new);
        assertThat(result).isNotPresent();
    }

    @Test
    void testPopulateOffsetManagerWithPartitionMapsNoExistingEntries() {
        final List<OffsetManager.OffsetManagerKey> partitionMaps = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            partitionMaps.add(new TestingOffsetManagerEntry("topic" + i, String.valueOf(i), "something else " + i)
                    .getManagerKey());// NOPMD avoid instantiating objects in loops.
        }

        when(offsetStorageReader.offsets(anyCollection())).thenReturn(Map.of());

        offsetManager.populateOffsetManager(partitionMaps);
        verify(offsetStorageReader, times(1)).offsets(anyList());

        // No Existing entries so we expect nothing to exist and for it to try check the offsets again.
        final Optional<TestingOffsetManagerEntry> result = offsetManager
                .getEntry(() -> partitionMaps.get(0).getPartitionMap(), TestingOffsetManagerEntry::new);
        assertThat(result).isEmpty();
        verify(offsetStorageReader, times(1)).offset(eq(partitionMaps.get(0).getPartitionMap()));

    }

    @Test
    void testPopulateOffsetManagerWithPartitionMapsAllEntriesExist() {
        final List<OffsetManager.OffsetManagerKey> partitionMaps = new ArrayList<>();
        final Map<Map<String, Object>, Map<String, Object>> offsetReaderMap = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            final OffsetManager.OffsetManagerKey key = new TestingOffsetManagerEntry("topic" + i, String.valueOf(i), // NOPMD
                                                                                                                     // avoid
                                                                                                                     // instantiating
                                                                                                                     // objects
                                                                                                                     // in
                                                                                                                     // loops.
                    "something else " + i).getManagerKey();
            partitionMaps.add(key);// NOPMD avoid instantiating objects in loops.
            offsetReaderMap.put(key.getPartitionMap(), Map.of("recordCount", (long) i));
        }

        when(offsetStorageReader.offsets(anyList())).thenReturn(offsetReaderMap);

        offsetManager.populateOffsetManager(partitionMaps);
        verify(offsetStorageReader, times(1)).offsets(anyList());

        // No Existing entries so we expect nothing to exist and for it to try check the offsets again.
        final Optional<TestingOffsetManagerEntry> result = offsetManager
                .getEntry(() -> partitionMaps.get(0).getPartitionMap(), TestingOffsetManagerEntry::new);
        assertThat(result).isPresent();
        verify(offsetStorageReader, times(0)).offset(eq(partitionMaps.get(0).getPartitionMap()));

    }

    @SuppressWarnings("PMD.TestClassWithoutTestCases") // TODO figure out why this fails.
    public static class TestingOffsetManagerEntry // NOPMD the above suppress warnings does not work.
            implements
                OffsetManager.OffsetManagerEntry<TestingOffsetManagerEntry> {
        public Map<String, Object> data;

        public int recordCount;

        public TestingOffsetManagerEntry(final String one, final String two, final String three) {
            this();
            data.put("segment1", one);
            data.put("segment2", two);
            data.put("segment3", three);
        }

        public TestingOffsetManagerEntry() {
            data = new HashMap<>();
            data.put("segment1", "The First Segment");
            data.put("segment2", "The Second Segment");
            data.put("segment3", "The Third Segment");
        }

        public TestingOffsetManagerEntry(final Map<String, Object> properties) {
            this();
            data.putAll(properties);
        }

        @Override
        public TestingOffsetManagerEntry fromProperties(final Map<String, Object> properties) {
            return new TestingOffsetManagerEntry(properties);
        }

        @Override
        public Map<String, Object> getProperties() {
            return data;
        }

        @Override
        public Object getProperty(final String key) {
            return data.get(key);
        }

        @Override
        public void setProperty(final String key, final Object value) {
            data.put(key, value);
        }

        @Override
        public OffsetManager.OffsetManagerKey getManagerKey() {
            return () -> Map.of("segment1", data.get("segment1"), "segment2", data.get("segment2"), "segment3",
                    data.get("segment3"));
        }

        @Override
        public void incrementRecordCount() {
            recordCount++;
        }

        @Override
        public boolean equals(final Object other) {
            if (other instanceof TestingOffsetManagerEntry) {
                return this.compareTo((TestingOffsetManagerEntry) other) == 0;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getProperty("segment1"), getProperty("segment2"), getProperty("segment3"));
        }

        @Override
        public int compareTo(final TestingOffsetManagerEntry other) {
            if (other == this) { // NOPMD
                return 0;
            }
            int result = ((String) getProperty("segment1")).compareTo((String) other.getProperty("segment1"));
            if (result == 0) {
                result = ((String) getProperty("segment2")).compareTo((String) other.getProperty("segment2"));
                if (result == 0) {
                    result = ((String) getProperty("segment3")).compareTo((String) other.getProperty("segment3"));
                }
            }
            return result;
        }
    }
}
