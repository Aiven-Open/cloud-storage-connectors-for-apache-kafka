

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

package io.aiven.kafka.connect.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

final class OffsetManagerTest {

    private static final String TEST_BUCKET = "test-bucket";

    @Mock
    private SourceTaskContext sourceTaskContext;

    private OffsetManager<TestingOffsetManagerEntry> offsetManager;



    @Test
    void testWithOffsets() {
        sourceTaskContext = mock(SourceTaskContext.class);
        final OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
        when(sourceTaskContext.offsetStorageReader()).thenReturn(offsetStorageReader);

        final Map<String, Object> partitionKey = new HashMap<>();
        partitionKey.put("segment1", "topic1");
        partitionKey.put("segment2", "a value");
        partitionKey.put("segment3", "something else");

        final Map<String, Object> offsetValue = new HashMap<>(partitionKey);
        offsetValue.put("object_key_file", 5L);

        when(offsetStorageReader.offset(partitionKey)).thenReturn(offsetValue);

        offsetManager = new OffsetManager<>(sourceTaskContext);
        TestingOffsetManagerEntry result = offsetManager.getEntry(() -> partitionKey, map -> new TestingOffsetManagerEntry(map));

        assertThat(result.data).isEqualTo(offsetValue);
    }

    @Test
    void testUpdateCurrentOffsets() {
        TestingOffsetManagerEntry offsetEntry = new TestingOffsetManagerEntry("bucket", "topic1", "thing");

        final Map<Map<String, Object>, Map<String, Object>> offsets = new HashMap<>();
        offsets.put(offsetEntry.getManagerKey().getPartitionMap(), offsetEntry.getProperties());

        OffsetManager<TestingOffsetManagerEntry> underTest = new OffsetManager<>(mock(SourceTaskContext.class), offsets);

        offsetEntry.setProperty("MyProperty", "WOW");

        underTest.updateCurrentOffsets(offsetEntry);

        TestingOffsetManagerEntry result = underTest.getEntry(offsetEntry.getManagerKey(), TestingOffsetManagerEntry::new);


//        Map<Map<String, Object>, Map<String, Object>> offsetMap = underTest.getOffsets();
//        assertTrue(offsetMap.containsKey(offsetEntry.getManagerKey().getPartitionMap()));
//        TestingOffsetManagerEntry stored = offsetEntry.fromProperties(offsetMap.get(offsetEntry.getManagerKey().getPartitionMap()));
//        assertThat(stored.getManagerKey().getPartitionMap()).isEqualTo(offsetEntry.getManagerKey().getPartitionMap());
//        assertThat(stored.properties).isEqualTo(offsetEntry.properties);
    }
//
//    @Test
//    void updateCurrentOffsetsTestNewEntry() {
//
//        final Map<Map<String, Object>, Map<String, Object>> offsets = new HashMap<>();
//        OffsetManager underTest = new OffsetManager(new HashMap<>());
//
//
//        TestingManagerEntry offsetEntry = new TestingManagerEntry("bucket", "topic1", 0);
//        underTest.updateCurrentOffsets(offsetEntry);
//
//        Map<Map<String, Object>, Map<String, Object>> offsetMap = underTest.getOffsets();
//        assertTrue(offsetMap.containsKey(offsetEntry.getManagerKey().getPartitionMap()));
//        TestingManagerEntry stored = offsetEntry.fromProperties(offsetMap.get(offsetEntry.getManagerKey().getPartitionMap()));
//        assertThat(stored.getManagerKey().getPartitionMap()).isEqualTo(offsetEntry.getManagerKey().getPartitionMap());
//        assertThat(stored.properties).isEqualTo(offsetEntry.properties);
//
//    }
//
//    @Test
//    void updateCurrentOffsetsDataNotLost() {
//
//        final Map<Map<String, Object>, Map<String, Object>> offsets = new HashMap<>();
//        OffsetManager underTest = new OffsetManager(new HashMap<>());
//
//
//        TestingManagerEntry offsetEntry = new TestingManagerEntry("bucket", "topic1", 0);
//        offsetEntry.setProperty("test", "WOW");
//        underTest.updateCurrentOffsets(offsetEntry);
//
//        TestingManagerEntry offsetEntry2 = new TestingManagerEntry("bucket", "topic1", 0);
//        offsetEntry.setProperty("test2", "a thing");
//        underTest.updateCurrentOffsets(offsetEntry);
//
//        Map<Map<String, Object>, Map<String, Object>> offsetMap = underTest.getOffsets();
//        assertTrue(offsetMap.containsKey(offsetEntry.getManagerKey().getPartitionMap()));
//        TestingManagerEntry stored = offsetEntry.fromProperties(offsetMap.get(offsetEntry.getManagerKey().getPartitionMap()));
//        assertThat(stored.getManagerKey().getPartitionMap()).isEqualTo(offsetEntry.getManagerKey().getPartitionMap());
//        assertThat(stored.properties.get("test")).isEqualTo("WOW");
//        assertThat(stored.properties.get("test2")).isEqualTo("a thing");
//    }
//

//
//    private static class TestingManagerEntry implements OffsetManager.OffsetManagerEntry<TestingManagerEntry> {
//        final Map<String, Object> properties = new HashMap<>();
//
//        TestingManagerEntry(String bucket, String topic, int partition) {
//            properties.put("topic", topic);
//            properties.put("partition", partition);
//            properties.put("bucket", bucket);
//        }
//
//        @Override
//        public TestingManagerEntry fromProperties(Map<String, Object> properties) {
//            TestingManagerEntry result = new TestingManagerEntry(null, null, 0);
//            result.properties.clear();
//            result.properties.putAll(properties);
//            return result;
//        }
//
//        @Override
//        public Map<String, Object> getProperties() {
//            return properties;
//        }
//
//        @Override
//        public Object getProperty(String key) {
//            return properties.get(key);
//        }
//
//        @Override
//        public void setProperty(String key, Object value) {
//            properties.put(key, value);
//        }
//
//        @Override
//        public OffsetManager.OffsetManagerKey getManagerKey() {
//            return new OffsetManager.OffsetManagerKey() {
//                @Override
//                public Map<String, Object> getPartitionMap() {
//                    return Map.of("topic", properties.get("topic"), "partition", properties.get("topic"), "bucket", properties.get("bucket"));
//                }
//            };
//        }
//
//        @Override
//        public int compareTo(TestingManagerEntry other) {
//            int result = ((String) getProperty("bucket")).compareTo((String) other.getProperty("bucket"));
//            if (result == 0) {
//                result = ((String) getProperty("topic")).compareTo((String) other.getProperty("topic"));
//                if (result == 0) {
//                    result = ((String) getProperty("partition")).compareTo((String) other.getProperty("partition"));
//                }
//            }
//            return result;
//        }
//    }

    public static class TestingOffsetManagerEntry implements OffsetManager.OffsetManagerEntry<TestingOffsetManagerEntry> {

        public Map<String, Object> data;

        public TestingOffsetManagerEntry(String one, String two, String three) {
            this();
            data.put("segment1", one);
            data.put("segment2", two);
            data.put("segment3", three);
        }

        public TestingOffsetManagerEntry() {
            data = new HashMap<>();
            data.put("segment1", "The First Segment" );
            data.put("segment2", "The Second Segment" );
            data.put("segment3", "The Third Segment" );
        }

        public TestingOffsetManagerEntry(Map<String, Object> properties) {
            this();
            data.putAll(properties);
        }

        @Override
        public TestingOffsetManagerEntry fromProperties(Map<String, Object> properties) {
            return new TestingOffsetManagerEntry(properties);
        }

        @Override
        public Map<String, Object> getProperties() {
            return data;
        }

        @Override
        public Object getProperty(String key) {
            return data.get(key);
        }

        @Override
        public void setProperty(String key, Object value) {
            data.put(key, value);
        }

        @Override
        public OffsetManager.OffsetManagerKey getManagerKey() {
            return () -> Map.of("segment1", data.get("segment1"), "segment2", data.get("segment2"), "segment3", data.get("segment3"));
        }

        @Override
        public String getTopic() {
            return getProperty("topic").toString();
        }

        @Override
        public Integer getPartition() {
            Object value = getProperty("partition");
            return value != null && value instanceof Integer ? (Integer) value : 0;
        }

        @Override
        public int compareTo(TestingOffsetManagerEntry other) {
            if (this == other) {
                return 0;
            }
            int result = ((String) getProperty("segment1")).compareTo((String) other.getProperty("segment1"));
            if (result == 0) {
                result =((String) getProperty("segment2")).compareTo((String) other.getProperty("segment2"));
                if (result == 0) {
                    ((String) getProperty("segment3")).compareTo((String) other.getProperty("segment3"));
                }
            }
            return result;
        }
    }
}
