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
import static io.aiven.kafka.connect.s3.source.utils.AivenS3SourceRecordIterator.OFFSET_KEY;
import static io.aiven.kafka.connect.s3.source.utils.OffsetManager.OBJECT_KEY;
import static org.assertj.core.api.Assertions.assertThat;
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

    private static final String TEST_BUCKET = "test-bucket";

    @Mock
    private SourceTaskContext sourceTaskContext;

    private OffsetStorageReader offsetStorageReader;

    private S3SourceConfig s3SourceConfig;

    private OffsetManager underTest;

    private Map<Map<String, Object>, Map<String, Object>> offsets;

    @BeforeEach
    public void setUp() {
        s3SourceConfig = new S3SourceConfig(getBasicProperties());

        offsetStorageReader = mock(OffsetStorageReader.class);
        offsets = new HashMap<>();
        when(offsetStorageReader.offsets(any())).thenReturn(offsets);

        sourceTaskContext = mock(SourceTaskContext.class);
        when(sourceTaskContext.offsetStorageReader()).thenReturn(offsetStorageReader);

    }

    @Test
    void testWithOffsets() {
        final Map<String, Object> partitionKey = new HashMap<>();
        partitionKey.put("topic", "topic1");
        partitionKey.put("partition", 0);
        partitionKey.put("bucket", TEST_BUCKET);

        final Map<String, Object> offsetValue = new HashMap<>();
        offsetValue.put(OFFSET_KEY, 5L);
        offsets.put(partitionKey, offsetValue);

        underTest = new OffsetManager(sourceTaskContext, s3SourceConfig);

        final Map<Map<String, Object>, Map<String, Object>> retrievedOffsets = underTest.getOffsets();
        assertThat(retrievedOffsets.size()).isEqualTo(1);
        assertThat(retrievedOffsets.values().iterator().next().get(OFFSET_KEY)).isEqualTo(5L);
    }

    @Test
    void testIncrementAndUpdateOffsetMapExistingOffset() {
        sourceTaskContext = mock(SourceTaskContext.class);
        final OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
        when(sourceTaskContext.offsetStorageReader()).thenReturn(offsetStorageReader);

        // Mock partition and offset values
        final String objectKey = "testObject";
        final String offsetObjectKey = OBJECT_KEY + "_" + objectKey;

        final Map<String, Object> partitionKey = new HashMap<>();
        partitionKey.put("topic", "topic1");
        partitionKey.put("partition", 0);
        partitionKey.put("bucket", "bucket");

        final Map<String, Object> offsetValue = new HashMap<>();
        offsetValue.put(offsetObjectKey, 1L); // Existing offset value
        final Map<Map<String, Object>, Map<String, Object>> offsets = new HashMap<>();
        offsets.put(partitionKey, offsetValue);

        when(offsetStorageReader.offsets(any())).thenReturn(offsets); // Mock offset retrieval

        // Initialize offset manager
        underTest = new OffsetManager(sourceTaskContext, s3SourceConfig);

        // Invoke method and assert new offset value
        final long newOffset = underTest.incrementAndUpdateOffset(partitionKey, objectKey, 2L);

        assertThat(newOffset).isEqualTo(2L); // Expect incremented offset
        assertThat(underTest.getOffsets().get(partitionKey).get(offsetObjectKey)).isEqualTo(2L); // Verify updated
                                                                                                 // offset in map
    }

    @Test
    void testIncrementAndUpdateOffsetMapNonExistingOffset() {
        sourceTaskContext = mock(SourceTaskContext.class);
        final OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
        when(sourceTaskContext.offsetStorageReader()).thenReturn(offsetStorageReader);

        // Mock partition without any existing offset
        final Map<String, Object> partitionKey = new HashMap<>();
        partitionKey.put("topic", "topic1");
        partitionKey.put("partition", 0);

        when(offsetStorageReader.offsets(any())).thenReturn(Collections.emptyMap()); // No existing offset

        // Initialize offset manager
        underTest = new OffsetManager(sourceTaskContext, s3SourceConfig);

        // Invoke method and assert new offset value
        final long startOffset = 5L;
        final long newOffset = underTest.incrementAndUpdateOffset(partitionKey, "", startOffset);

        // Expect the startOffset to be returned when no existing offset is found
        assertThat(newOffset).isEqualTo(startOffset);
    }

    private Map<String, String> getBasicProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(S3SourceConfig.AWS_S3_BUCKET_NAME_CONFIG, TEST_BUCKET);
        properties.put(TARGET_TOPIC_PARTITIONS, "0,1");
        properties.put(TARGET_TOPICS, "topic1,topic2");
        return properties;
    }
}
