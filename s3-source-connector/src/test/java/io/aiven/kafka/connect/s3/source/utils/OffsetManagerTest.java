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

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.TARGET_TOPICS;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.TARGET_TOPIC_PARTITIONS;
import static io.aiven.kafka.connect.s3.source.utils.SourceRecordIterator.OFFSET_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

final class OffsetManagerTest {

    private Map<String, String> properties;
    private static final String TEST_BUCKET = "test-bucket";

    @Mock
    private SourceTaskContext sourceTaskContext;

    private S3SourceConfig s3SourceConfig;

    private OffsetManager offsetManager;

    @BeforeEach
    public void setUp() {
        properties = new HashMap<>();
        setBasicProperties();
        s3SourceConfig = new S3SourceConfig(properties);
    }

    @Test
    void testWithOffsets() {
        sourceTaskContext = mock(SourceTaskContext.class);
        final OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
        when(sourceTaskContext.offsetStorageReader()).thenReturn(offsetStorageReader);

        final Map<String, Object> partitionKey = new HashMap<>();
        partitionKey.put("topic", "topic1");
        partitionKey.put("partition", 0);
        partitionKey.put("bucket", TEST_BUCKET);

        final Map<String, Object> offsetValue = new HashMap<>();
        offsetValue.put(OFFSET_KEY, 5L);
        final Map<Map<String, Object>, Map<String, Object>> offsets = new HashMap<>();
        offsets.put(partitionKey, offsetValue);

        when(offsetStorageReader.offsets(any())).thenReturn(offsets);

        offsetManager = new OffsetManager(sourceTaskContext, s3SourceConfig);

        final Map<Map<String, Object>, Map<String, Object>> retrievedOffsets = offsetManager.getOffsets();
        assertThat(retrievedOffsets.size()).isEqualTo(1);
        assertThat(retrievedOffsets.values().iterator().next().get(OFFSET_KEY)).isEqualTo(5L);
    }

    @Test
    void testIncrementAndUpdateOffsetMapExistingOffset() {
        sourceTaskContext = mock(SourceTaskContext.class);
        final OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
        when(sourceTaskContext.offsetStorageReader()).thenReturn(offsetStorageReader);

        final Map<String, Object> partitionKey = new HashMap<>();
        partitionKey.put("topic", "topic1");
        partitionKey.put("partition", 0);

        final Map<String, Object> offsetValue = new HashMap<>();
        offsetValue.put(OFFSET_KEY, 1L);
        final Map<Map<String, Object>, Map<String, Object>> offsets = new HashMap<>();
        offsets.put(partitionKey, offsetValue);

        when(offsetStorageReader.offsets(any())).thenReturn(offsets);

        offsetManager = new OffsetManager(sourceTaskContext, s3SourceConfig);
        final long newOffset = offsetManager.incrementAndUpdateOffsetMap(partitionKey);

        assertThat(newOffset).isEqualTo(2L);
        assertThat(offsetManager.getOffsets().get(partitionKey).get(OFFSET_KEY)).isEqualTo(2L);
    }

    @Test
    void testIncrementAndUpdateOffsetMapNonExistingOffset() {
        sourceTaskContext = mock(SourceTaskContext.class);
        final OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
        when(sourceTaskContext.offsetStorageReader()).thenReturn(offsetStorageReader);

        final Map<String, Object> partitionKey = new HashMap<>();
        partitionKey.put("topic", "topic1");
        partitionKey.put("partition", 0);

        when(offsetStorageReader.offsets(any())).thenReturn(Collections.emptyMap());

        offsetManager = new OffsetManager(sourceTaskContext, s3SourceConfig);
        final long newOffset = offsetManager.incrementAndUpdateOffsetMap(partitionKey);

        assertThat(newOffset).isEqualTo(0L);
    }

    @Test
    void testGetFirstConfiguredTopic() throws Exception {
        sourceTaskContext = mock(SourceTaskContext.class);
        final OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
        when(sourceTaskContext.offsetStorageReader()).thenReturn(offsetStorageReader);

        offsetManager = new OffsetManager(sourceTaskContext, s3SourceConfig);

        final String firstTopic = offsetManager.getFirstConfiguredTopic(s3SourceConfig);
        assertEquals("topic1", firstTopic);
    }

    private void setBasicProperties() {
        properties.put(S3SourceConfig.AWS_S3_BUCKET_NAME_CONFIG, TEST_BUCKET);
        properties.put(TARGET_TOPIC_PARTITIONS, "0,1");
        properties.put(TARGET_TOPICS, "topic1,topic2");
    }
}
