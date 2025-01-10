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

final class S3OffsetManagerEntryTest {

    static final String TEST_BUCKET = "test-bucket";

    static final String TOPIC = "TOPIC1";

    static final int PARTITION = 1;

    static final String OBJECT_KEY = "object_key";

    private SourceTaskContext sourceTaskContext;

    private OffsetManager<S3OffsetManagerEntry> offsetManager;

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
        partitionKey.put(S3OffsetManagerEntry.TOPIC, TOPIC);
        partitionKey.put(S3OffsetManagerEntry.PARTITION, PARTITION);
        partitionKey.put(S3OffsetManagerEntry.BUCKET, TEST_BUCKET);
        partitionKey.put(S3OffsetManagerEntry.OBJECT_KEY, OBJECT_KEY);
        return partitionKey;
    }

    public static S3OffsetManagerEntry newEntry() {
        return new S3OffsetManagerEntry(TEST_BUCKET, OBJECT_KEY, TOPIC, PARTITION);
    }

    @Test
    void testGetEntry() {
        final Map<String, Object> storedData = new HashMap<>();
        storedData.putAll(createPartitionMap());
        storedData.put("random_entry", 5L);

        when(offsetStorageReader.offset(any())).thenReturn(storedData);

        final S3OffsetManagerEntry keyEntry = newEntry();
        final Optional<S3OffsetManagerEntry> entry = offsetManager.getEntry(keyEntry.getManagerKey(),
                keyEntry::fromProperties);
        assertThat(entry).isPresent();
        assertThat(entry.get().getPartition()).isEqualTo(PARTITION);
        assertThat(entry.get().getRecordCount()).isEqualTo(0);
        assertThat(entry.get().getTopic()).isEqualTo(TOPIC);
        assertThat(entry.get().getBucket()).isEqualTo(TEST_BUCKET);
        assertThat(entry.get().getProperty("random_entry")).isEqualTo(5L);
        verify(sourceTaskContext, times(1)).offsetStorageReader();

        // verify second read reads from local data

        final Optional<S3OffsetManagerEntry> entry2 = offsetManager.getEntry(entry.get().getManagerKey(),
                entry.get()::fromProperties);
        assertThat(entry2).isPresent();
        assertThat(entry2.get().getPartition()).isEqualTo(PARTITION);
        assertThat(entry2.get().getRecordCount()).isEqualTo(0);
        assertThat(entry2.get().getTopic()).isEqualTo(TOPIC);
        assertThat(entry2.get().getBucket()).isEqualTo(TEST_BUCKET);
        assertThat(entry2.get().getProperty("random_entry")).isEqualTo(5L);
        verify(sourceTaskContext, times(1)).offsetStorageReader();
    }

    @Test
    void testFromProperties() {
        final S3OffsetManagerEntry entry = new S3OffsetManagerEntry(TEST_BUCKET, OBJECT_KEY, TOPIC, PARTITION);
        assertThat(entry.getRecordCount()).isEqualTo(0L);
        assertThat(entry.getProperty("random_entry")).isNull();

        entry.setProperty("random_entry", 5L);
        entry.incrementRecordCount();
        assertThat(entry.getRecordCount()).isEqualTo(1L);
        assertThat(entry.getProperty("random_entry")).isEqualTo(5L);

        final S3OffsetManagerEntry other = entry.fromProperties(entry.getProperties());
        assertThat(other.getRecordCount()).isEqualTo(1L);
        assertThat(other.getProperty("random_entry")).isEqualTo(5L);

    }
}
