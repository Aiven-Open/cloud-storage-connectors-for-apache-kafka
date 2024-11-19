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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


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
        offsetValue.put("object_key_file", 5L);
        final Map<Map<String, Object>, Map<String, Object>> offsets = new HashMap<>();
        offsets.put(partitionKey, offsetValue);

        when(offsetStorageReader.offsets(any())).thenReturn(offsets);

        offsetManager = new OffsetManager(sourceTaskContext, s3SourceConfig);

        final Map<Map<String, Object>, Map<String, Object>> retrievedOffsets = offsetManager.getOffsets();
        assertThat(retrievedOffsets.size()).isEqualTo(1);
        assertThat(retrievedOffsets.values().iterator().next().get("object_key_file")).isEqualTo(5L);
    }

    @Test
    void testUpdateCurrentOffsets() {
        TestingManagerEntry offsetEntry = new TestingManagerEntry("bucket", "topic1", 0);

        final Map<Map<String, Object>, Map<String, Object>> offsets = new HashMap<>();
        offsets.put(offsetEntry.getManagerKey().getPartitionMap(), offsetEntry.getProperties());

        OffsetManager underTest = new OffsetManager(offsets);

        offsetEntry.setProperty("MyProperty", "WOW");

        underTest.updateCurrentOffsets(offsetEntry);

        Map<Map<String, Object>, Map<String, Object>> offsetMap = underTest.getOffsets();
        assertTrue(offsetMap.containsKey(offsetEntry.getManagerKey().getPartitionMap()));
        TestingManagerEntry stored = offsetEntry.fromProperties(offsetMap.get(offsetEntry.getManagerKey().getPartitionMap()));
        assertThat(stored.getManagerKey().getPartitionMap()).isEqualTo(offsetEntry.getManagerKey().getPartitionMap());
        assertThat(stored.properties).isEqualTo(offsetEntry.properties);
    }

    @Test
    void updateCurrentOffsetsTestNewEntry() {

        final Map<Map<String, Object>, Map<String, Object>> offsets = new HashMap<>();
        OffsetManager underTest = new OffsetManager(new HashMap<>());


        TestingManagerEntry offsetEntry = new TestingManagerEntry("bucket", "topic1", 0);
        underTest.updateCurrentOffsets(offsetEntry);

        Map<Map<String, Object>, Map<String, Object>> offsetMap = underTest.getOffsets();
        assertTrue(offsetMap.containsKey(offsetEntry.getManagerKey().getPartitionMap()));
        TestingManagerEntry stored = offsetEntry.fromProperties(offsetMap.get(offsetEntry.getManagerKey().getPartitionMap()));
        assertThat(stored.getManagerKey().getPartitionMap()).isEqualTo(offsetEntry.getManagerKey().getPartitionMap());
        assertThat(stored.properties).isEqualTo(offsetEntry.properties);

    }

    @Test
    void updateCurrentOffsetsDataNotLost() {

        final Map<Map<String, Object>, Map<String, Object>> offsets = new HashMap<>();
        OffsetManager underTest = new OffsetManager(new HashMap<>());


        TestingManagerEntry offsetEntry = new TestingManagerEntry("bucket", "topic1", 0);
        offsetEntry.setProperty("test", "WOW");
        underTest.updateCurrentOffsets(offsetEntry);

        TestingManagerEntry offsetEntry2 = new TestingManagerEntry("bucket", "topic1", 0);
        offsetEntry.setProperty("test2", "a thing");
        underTest.updateCurrentOffsets(offsetEntry);

        Map<Map<String, Object>, Map<String, Object>> offsetMap = underTest.getOffsets();
        assertTrue(offsetMap.containsKey(offsetEntry.getManagerKey().getPartitionMap()));
        TestingManagerEntry stored = offsetEntry.fromProperties(offsetMap.get(offsetEntry.getManagerKey().getPartitionMap()));
        assertThat(stored.getManagerKey().getPartitionMap()).isEqualTo(offsetEntry.getManagerKey().getPartitionMap());
        assertThat(stored.properties.get("test")).isEqualTo("WOW");
        assertThat(stored.properties.get("test2")).isEqualTo("a thing");
    }

    private void setBasicProperties() {
        properties.put(S3SourceConfig.AWS_S3_BUCKET_NAME_CONFIG, TEST_BUCKET);
        properties.put(TARGET_TOPIC_PARTITIONS, "0,1");
        properties.put(TARGET_TOPICS, "topic1,topic2");
    }

    private static class TestingManagerEntry implements OffsetManager.OffsetManagerEntry<TestingManagerEntry> {
        final Map<String, Object> properties = new HashMap<>();

        TestingManagerEntry(String bucket, String topic, int partition) {
            properties.put("topic", topic);
            properties.put("partition", partition);
            properties.put("bucket", bucket);
        }

        @Override
        public TestingManagerEntry fromProperties(Map<String, Object> properties) {
            TestingManagerEntry result = new TestingManagerEntry(null, null, 0);
            result.properties.clear();
            result.properties.putAll(properties);
            return result;
        }

        @Override
        public Map<String, Object> getProperties() {
            return properties;
        }

        @Override
        public Object getProperty(String key) {
            return properties.get(key);
        }

        @Override
        public void setProperty(String key, Object value) {
            properties.put(key, value);
        }

        @Override
        public OffsetManager.OffsetManagerKey getManagerKey() {
            return new OffsetManager.OffsetManagerKey() {
                @Override
                public Map<String, Object> getPartitionMap() {
                    return Map.of("topic", properties.get("topic"), "partition", properties.get("topic"), "bucket", properties.get("bucket"));
                }
            };
        }

        @Override
        public int compareTo(TestingManagerEntry other) {
            int result = ((String) getProperty("bucket")).compareTo((String) other.getProperty("bucket"));
            if (result == 0) {
                result = ((String) getProperty("topic")).compareTo((String) other.getProperty("topic"));
                if (result == 0) {
                    result = ((String) getProperty("partition")).compareTo((String) other.getProperty("partition"));
                }
            }
            return result;
        }
    }
}
